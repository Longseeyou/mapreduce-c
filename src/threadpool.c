#include "threadpool.h"
#include <stdlib.h>

ThreadPool_job_t* ThreadPool_get_job(ThreadPool_t* tp){
    ThreadPool_job_t* job = tp->jobs.head;
    if(job == NULL)
        return NULL;

    tp->jobs.head = job->next;
    if (tp->jobs.head == NULL)
        tp->jobs.tail = NULL;
    tp->jobs.size--;
    job->next = NULL;

    return job;
}

bool ThreadPool_add_job(ThreadPool_t* tp, thread_func_t func, void* arg)
{
    ThreadPool_job_t* job = malloc(sizeof(*job));
    if (job == NULL) {
        return false;
    }

    job->func = func;
    job->arg = arg;
    job->next = NULL;

    // critical section
    pthread_mutex_lock(&tp->mutex);

    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->mutex);
        free(job);
        return false;
    }

    if (tp->jobs.head == NULL) {
        tp->jobs.head = job;
    } else {
        tp->jobs.tail->next = job;
    }
    tp->jobs.tail = job;

    tp->jobs.size++;

    pthread_cond_signal(&tp->work_available);
    pthread_mutex_unlock(&tp->mutex);

    return true;
}

void* Thread_run(ThreadPool_t* tp){
    while(true){
        pthread_mutex_lock(&tp->mutex);

        while (tp->jobs.size == 0 && !tp->shutdown) {
            pthread_cond_wait(&tp->work_available, &tp->mutex);
        }

        if (tp->shutdown && tp->jobs.size == 0) {
            pthread_mutex_unlock(&tp->mutex);
            return NULL;
        }

        ThreadPool_job_t* job = ThreadPool_get_job(tp);
        tp->active_jobs++;

        pthread_mutex_unlock(&tp->mutex);

        job->func(job->arg);
        free(job);

        pthread_mutex_lock(&tp->mutex);

        tp->active_jobs--;

        if (tp->jobs.size == 0 && tp->active_jobs == 0) {
            pthread_cond_broadcast(&tp->all_done);
        }

        pthread_mutex_unlock(&tp->mutex);
    }
}

static void* worker_start(void* arg){
    return Thread_run((ThreadPool_t*)arg);
}

ThreadPool_t* ThreadPool_create(unsigned int num){
    if(num == 0)
        return NULL;

    // init a threadpool
    ThreadPool_t* tp = calloc(1, sizeof(*tp));
    if(tp == NULL)
        return NULL;

    tp->threads = malloc(num * sizeof(*tp->threads));

    if (tp->threads == NULL) {
        free(tp);
        return NULL;
    }

    if (pthread_mutex_init(&tp->mutex, NULL) != 0) {
        goto fail_memory;
    }

    if (pthread_cond_init(&tp->work_available, NULL) != 0) {
        goto fail_mutex;
    }

    if (pthread_cond_init(&tp->all_done, NULL) != 0) {
        goto fail_work_available;
    }

    for (unsigned int i = 0; i < num; i++) {
        if (pthread_create(&tp->threads[i], NULL, worker_start, tp) != 0) {
            pthread_mutex_lock(&tp->mutex);
            tp->shutdown = true;
            pthread_cond_broadcast(&tp->work_available);
            pthread_mutex_unlock(&tp->mutex);

            for (unsigned int j = 0; j < i; j++) {
                pthread_join(tp->threads[j], NULL);
            }

            goto fail_all_done;
        }

        tp->num_threads++;
    }

    return tp;

    fail_all_done:
        pthread_cond_destroy(&tp->all_done);
    fail_work_available:
        pthread_cond_destroy(&tp->work_available);
    fail_mutex:
        pthread_mutex_destroy(&tp->mutex);
    fail_memory:
        free(tp->threads);
        free(tp);
        return NULL;
}

void ThreadPool_check(ThreadPool_t* tp){
    pthread_mutex_lock(&tp->mutex);

    while (tp->jobs.size > 0 || tp->active_jobs > 0) {
        pthread_cond_wait(&tp->all_done, &tp->mutex);
    }

    pthread_mutex_unlock(&tp->mutex);
}

void ThreadPool_destroy(ThreadPool_t* tp){
    if(tp == NULL)
        return;

    ThreadPool_check(tp);

    pthread_mutex_lock(&tp->mutex);
    tp->shutdown = 1;
    pthread_cond_broadcast(&tp->work_available);
    pthread_mutex_unlock(&tp->mutex);

    for(unsigned int i=0; i< tp->num_threads; ++i)
        pthread_join(tp->threads[i], NULL);

    pthread_cond_destroy(&tp->work_available);
    pthread_cond_destroy(&tp->all_done);
    pthread_mutex_destroy(&tp->mutex);

    free(tp->threads);
    free(tp);
}
