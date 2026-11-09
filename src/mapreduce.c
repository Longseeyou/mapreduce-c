#include "mapreduce.h"
#include "threadpool.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef struct KVNode{
    char* key;
    char* value;
    struct KVNode* next;
} KVNode;

typedef struct {
    KVNode* head;
    size_t size_bytes;
    pthread_mutex_t mutex;
} Partition;

typedef struct {
    Mapper mapper;
    char* filename;
    off_t file_size;
} MapJobArgs;

typedef struct {
    Reducer reducer;
    unsigned int partition_idx;
    size_t size_bytes;
} ReduceJobArgs;

static Partition* partitions = NULL;
static unsigned int partition_count = 0;
static pthread_mutex_t run_mutex = PTHREAD_MUTEX_INITIALIZER;

unsigned int MR_Partitioner(char* key, unsigned int num_partitions)
{
    unsigned long hash = 5381;
    unsigned char c;

    while ((c = (unsigned char)*key++) != '\0') {
        hash = ((hash << 5) + hash) + c;
    }

    return hash % num_partitions;
}

void MR_Emit(char* key, char* value){
    KVNode* node = malloc(sizeof(*node));

    if(node == NULL)
        abort();

    node->key = strdup(key);
    node->value = strdup(value);
    node->next = NULL;

    if (node->key == NULL || node->value == NULL) {
        free(node->key);
        free(node->value);
        free(node);
        abort();
    }

    unsigned int partition_idx = MR_Partitioner(key, partition_count);
    Partition* p = &partitions[partition_idx];

    pthread_mutex_lock(&p->mutex);

    KVNode** pos = &p->head;
    while(*pos != NULL && strcmp((*pos)->key, key) < 0)
        pos = &(*pos)->next;

    node->next = *pos;
    *pos = node;

    p->size_bytes += strlen(node->key) + 1 + strlen(node->value) + 1;

    pthread_mutex_unlock(&p->mutex);
}

char* MR_GetNext(char* key, unsigned int partition_idx){
    Partition* p = &partitions[partition_idx];

    pthread_mutex_lock(&p->mutex);

    KVNode* node = p->head;

    if (node == NULL || strcmp(node->key, key) != 0) {
        pthread_mutex_unlock(&p->mutex);
        return NULL;
    }

    p->head = node->next;

    char* value = node->value;

    pthread_mutex_unlock(&p->mutex);

    free(node->key);
    free(node);

    return value;
}

static void reduce_job(void* threadarg){
    ReduceJobArgs* args = threadarg;
    Partition* p = &partitions[args->partition_idx];

    while(true){
        pthread_mutex_lock(&p->mutex);

        if(p->head == NULL){
            pthread_mutex_unlock(&p->mutex);
            return;
        }

        char* key = strdup(p->head->key);

        pthread_mutex_unlock(&p->mutex);

        if(key == NULL)
            abort();

        args->reducer(key, args->partition_idx);

        /* A reducer may only need some values. Always advance to the next key. */
        char* remaining;
        while ((remaining = MR_GetNext(key, args->partition_idx)) != NULL)
            free(remaining);

        free(key);
    }
}

static void map_job(void* threadarg){
    MapJobArgs* arg = threadarg;
    arg->mapper(arg->filename);
}

// Compare operator for sort func
static int compare_map_jobs(const void* left, const void* right){
    const MapJobArgs* a = left;
    const MapJobArgs* b = right;

    if(a->file_size < b->file_size)
        return -1;

    if(a->file_size > b->file_size)
        return 1;

    return 0;
}

static int compare_reduce_jobs(const void* left, const void* right){
    const ReduceJobArgs* a = left;
    const ReduceJobArgs* b = right;

    if(a->size_bytes < b->size_bytes)
        return -1;

    if(a->size_bytes > b->size_bytes)
        return 1;

    return 0;
}

static void partition_destroy(unsigned int num_parts){
    for (unsigned int i = 0; i < num_parts; i++) {
        KVNode* node = partitions[i].head;

        while (node != NULL) {
            KVNode* next = node->next;
            free(node->key);
            free(node->value);
            free(node);
            node = next;
        }

        pthread_mutex_destroy(&partitions[i].mutex);
    }

    free(partitions);
    partitions = NULL;
    partition_count = 0;
}

static MR_Status map_phase(unsigned int file_count, char* file_names[], Mapper mapper, ThreadPool_t* tp){
    MapJobArgs* map_jobs = NULL;
    if(file_count > 0){
        map_jobs = calloc(file_count, sizeof(*map_jobs));

        if(map_jobs == NULL){
            return MR_RESOURCE_ERROR;
        }
    }

    for(unsigned int i=0; i<file_count; ++i){
        struct stat info;
        if(stat(file_names[i], &info) != 0 || !S_ISREG(info.st_mode)){
            free(map_jobs);
            return MR_INPUT_ERROR;
        }

        map_jobs[i].mapper = mapper;
        map_jobs[i].filename = file_names[i];
        map_jobs[i].file_size = info.st_size;
    }

    if(file_count > 1)
        qsort(map_jobs, file_count, sizeof(*map_jobs), compare_map_jobs);

    for(unsigned int i=0; i<file_count; ++i)
        if(!ThreadPool_add_job(tp, map_job, &map_jobs[i])){
            ThreadPool_check(tp);
            free(map_jobs);
            return MR_RESOURCE_ERROR;
        }

    ThreadPool_check(tp);

    free(map_jobs);
    return MR_OK;
}

static MR_Status reduce_phase(Reducer reducer, ThreadPool_t* tp, unsigned int num_parts){
    ReduceJobArgs* reduce_jobs = calloc(num_parts, sizeof(*reduce_jobs));

    if (reduce_jobs == NULL) {
        return MR_RESOURCE_ERROR;
    }

    for (unsigned int i = 0; i < num_parts; i++) {
        reduce_jobs[i].reducer = reducer;
        reduce_jobs[i].partition_idx = i;
        reduce_jobs[i].size_bytes = partitions[i].size_bytes;
    }

    if (num_parts > 1)
        qsort(reduce_jobs, num_parts, sizeof(*reduce_jobs), compare_reduce_jobs);

    for (unsigned int i = 0; i < num_parts; i++)
        if (!ThreadPool_add_job(tp, reduce_job, &reduce_jobs[i])) {
            ThreadPool_check(tp);
            free(reduce_jobs);
            return MR_RESOURCE_ERROR;
        }

    ThreadPool_check(tp);
    free(reduce_jobs);

    return MR_OK;
}

MR_Status MR_Run(unsigned int file_count, char* file_names[],
            Mapper mapper, Reducer reducer,
            unsigned int num_workers, unsigned int num_parts)
{
    if(num_parts == 0 || num_workers == 0 || mapper == NULL || reducer == NULL || (file_count > 0 && file_names == NULL))
        return MR_INVALID_ARGUMENT;

    for (unsigned int i = 0; i < file_count; ++i)
        if (file_names[i] == NULL)
            return MR_INVALID_ARGUMENT;

    if (pthread_mutex_trylock(&run_mutex) != 0)
        return MR_BUSY;

    partitions = calloc(num_parts, sizeof(*partitions));

    if(partitions == NULL) {
        pthread_mutex_unlock(&run_mutex);
        return MR_RESOURCE_ERROR;
    }
    partition_count = num_parts;

    for(unsigned int i=0; i<num_parts; ++i)
        if(pthread_mutex_init(&partitions[i].mutex, NULL) != 0){
            partition_destroy(i);
            pthread_mutex_unlock(&run_mutex);
            return MR_RESOURCE_ERROR;
        }

    // Create thread_pool
    ThreadPool_t* tp = ThreadPool_create(num_workers);

    if(tp == NULL){
        partition_destroy(num_parts);
        pthread_mutex_unlock(&run_mutex);
        return MR_RESOURCE_ERROR;
    }

    MR_Status status = map_phase(file_count, file_names, mapper, tp);
    if (status == MR_OK)
        status = reduce_phase(reducer, tp, num_parts);
    ThreadPool_destroy(tp);
    partition_destroy(num_parts);
    pthread_mutex_unlock(&run_mutex);
    return status;
}

const char *MR_StatusString(MR_Status status)
{
    switch (status) {
    case MR_OK: return "success";
    case MR_INVALID_ARGUMENT: return "invalid argument";
    case MR_INPUT_ERROR: return "input is missing or is not a regular file";
    case MR_RESOURCE_ERROR: return "allocation, thread, or synchronization setup failed";
    case MR_BUSY: return "another MapReduce run is active";
    default: return "unknown status";
    }
}
