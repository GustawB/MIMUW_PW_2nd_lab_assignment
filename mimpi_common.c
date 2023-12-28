/**
 * This file is for implementation of common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#include "mimpi_common.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>

_Noreturn void syserr(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);
    fprintf(stderr, " (%d; %s)\n", errno, strerror(errno));
    exit(1);
}

_Noreturn void fatal(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    exit(1);
}

/////////////////////////////////////////////////
// Put your implementation here

// Mutex used to synchronize the barrier
pthread_mutex_t barrier_mutex;
int waiting_for_barrier = -1;
int inside_barrier = -1;
int world_size = -1;
int is_barrier_ending = -1;

void common_init(int size) {
    ASSERT_ZERO(pthread_mutex_init(&barrier_mutex, NULL));
    world_size = size;
    printf("world size: %d\n", world_size);
    waiting_for_barrier = 0;
    inside_barrier = 0;
    is_barrier_ending = 0;
}

void common_finalize() {
    ASSERT_ZERO(pthread_mutex_destroy(&barrier_mutex));
    world_size = -1;
    waiting_for_barrier = -1;
    inside_barrier = -1;
    world_size = -1;
    is_barrier_ending = -1;
}

void* synchronizeProcesses() {
    if (world_size == -1) {
        fatal("mimpi_common library not initialized");
    }

    if (is_barrier_ending == 1) {
        ++waiting_for_barrier;
        ASSERT_ZERO(pthread_mutex_lock(&barrier_mutex));
        --waiting_for_barrier;
        if (waiting_for_barrier > 0) {
            ASSERT_ZERO(pthread_mutex_unlock(&barrier_mutex));
        }
    }
    ++inside_barrier;
    int* return_value = malloc(sizeof(int));
    *return_value = 0;
    printf("%d, %d\n", inside_barrier, world_size);
    if (inside_barrier == world_size) { // Everyone is synchronized.
        printf("sex\n");
        is_barrier_ending = 1;
        *return_value = 1;
    }

    return return_value;
}

void unlock_barrier() {
    is_barrier_ending = 0;
    ASSERT_ZERO(pthread_mutex_unlock(&barrier_mutex));
}