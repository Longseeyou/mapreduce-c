#include "mapreduce.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static atomic_int mapped;
static atomic_int reduced;
static atomic_int values;
static unsigned int parts;
static int expected_maps;

static void map(char *filename)
{
    (void)filename;
    for (int i = 0; i < 40; ++i) {
        char key[16], value[16];
        snprintf(key, sizeof(key), "key-%d", i % 7);
        snprintf(value, sizeof(value), "%d", i);
        MR_Emit(key, value);
        /* The engine owns copies, not these stack buffers. */
        memset(key, 'x', sizeof(key));
        memset(value, 'x', sizeof(value));
    }
    atomic_fetch_add(&mapped, 1);
}

static void reduce(char *key, unsigned int partition)
{
    assert(atomic_load(&mapped) == expected_maps);
    assert(partition == MR_Partitioner(key, parts));
    int index = -1;
    assert(sscanf(key, "key-%d", &index) == 1);
    assert(index >= 0 && index < 7);
    int count = 0;
    char *value;
    while ((value = MR_GetNext(key, partition)) != NULL) {
        int n = atoi(value);
        assert(n >= 0 && n < 40 && n % 7 == index);
        ++count;
        free(value);
    }
    assert(count == expected_maps * (index < 5 ? 6 : 5));
    atomic_fetch_add(&values, count);
    atomic_fetch_add(&reduced, 1);
}

static void skip_values(char *key, unsigned int partition)
{
    (void)key;
    (void)partition;
    atomic_fetch_add(&reduced, 1);
}

static void nested_map(char *filename)
{
    (void)filename;
    assert(MR_Run(0, NULL, map, reduce, 1, 1) == MR_BUSY);
}

static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t changed = PTHREAD_COND_INITIALIZER;
static int entered, released;

static void blocked_map(char *filename)
{
    (void)filename;
    pthread_mutex_lock(&gate);
    entered = 1;
    pthread_cond_signal(&changed);
    while (!released) pthread_cond_wait(&changed, &gate);
    pthread_mutex_unlock(&gate);
}

static void *background_run(void *unused)
{
    (void)unused;
    char *files[] = {"examples/words.txt"};
    assert(MR_Run(1, files, blocked_map, reduce, 2, 3) == MR_OK);
    return NULL;
}

int main(void)
{
    char *files[] = {"examples/words.txt", "examples/amounts.tsv",
                     "examples/words.txt", "examples/amounts.tsv"};
    unsigned int workers[] = {1, 2, 8};
    unsigned int partitions[] = {1, 3, 32};
    for (size_t w = 0; w < 3; ++w) {
        for (size_t p = 0; p < 3; ++p) {
            for (int repeat = 0; repeat < 3; ++repeat) {
                parts = partitions[p];
                expected_maps = 4;
                atomic_store(&mapped, 0);
                atomic_store(&reduced, 0);
                atomic_store(&values, 0);
                assert(MR_Run(4, files, map, reduce, workers[w], parts) == MR_OK);
                assert(atomic_load(&reduced) == 7);
                assert(atomic_load(&values) == 160);
            }
        }
    }
    atomic_store(&reduced, 0);
    assert(MR_Run(1, files, map, skip_values, 2, 4) == MR_OK);
    assert(atomic_load(&reduced) == 7);
    assert(MR_Run(0, NULL, map, reduce, 2, 4) == MR_OK);
    assert(MR_Run(1, files, map, reduce, 0, 1) == MR_INVALID_ARGUMENT);
    assert(MR_Run(1, files, map, reduce, 1, 0) == MR_INVALID_ARGUMENT);
    assert(MR_Run(1, files, NULL, reduce, 1, 1) == MR_INVALID_ARGUMENT);
    assert(MR_Run(1, files, map, NULL, 1, 1) == MR_INVALID_ARGUMENT);
    assert(MR_Run(1, NULL, map, reduce, 1, 1) == MR_INVALID_ARGUMENT);
    char *null_file[] = {NULL};
    assert(MR_Run(1, null_file, map, reduce, 1, 1) == MR_INVALID_ARGUMENT);
    char *missing[] = {"examples/words.txt", "this-file-does-not-exist"};
    atomic_store(&mapped, 0);
    assert(MR_Run(2, missing, map, reduce, 1, 1) == MR_INPUT_ERROR);
    assert(atomic_load(&mapped) == 0); /* Validate all paths before mapping. */
    char *directory[] = {"examples"};
    assert(MR_Run(1, directory, map, reduce, 1, 1) == MR_INPUT_ERROR);
    assert(MR_Run(1, files, nested_map, reduce, 1, 1) == MR_OK);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, background_run, NULL) == 0);
    pthread_mutex_lock(&gate);
    while (!entered) pthread_cond_wait(&changed, &gate);
    assert(MR_Run(0, NULL, map, reduce, 1, 1) == MR_BUSY);
    released = 1;
    pthread_cond_signal(&changed);
    pthread_mutex_unlock(&gate);
    assert(pthread_join(thread, NULL) == 0);
    assert(MR_Run(0, NULL, map, reduce, 1, 1) == MR_OK);
    assert(strcmp(MR_StatusString(MR_OK), "success") == 0);
    puts("MapReduce API tests passed (27 aggregation runs plus lifecycle/error checks).");
    return 0;
}
