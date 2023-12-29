/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

struct buffer_list {
    char buffer;
    struct buffer_list* next;
};

struct reader_params {
    int my_rank;
    int source;
};

pthread_t* pipe_threads;
struct buffer_list* root;

void* read_data(void* data) {
    struct reader_params params = *((struct reader_params*)data);
    char buffer;
    int count = 4096;
    while (1) {
        chrecv(20 + params.my_rank * 16 + params.source, &buffer, count);
    }
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    //TODO
    int nr_of_threads_to_create = MIMPI_World_size();
    pthread_t threads[nr_of_threads_to_create];
    for (int i = 0; i < nr_of_threads_to_create; ++i) {
        struct reader_params* params = malloc(sizeof(struct reader_params));
        params->my_rank = MIMPI_World_rank();
        params->source = i;
        ASSERT_ZERO(pthread_create(&threads[i], NULL, read_data, params));
        free(params);
    }
    root = NULL;

    pipe_threads = &threads[0]; // Get access to the array of threads.
}

void MIMPI_Finalize() {
    //TODO
    int rank = MIMPI_World_rank(); // Get the id of the process.
    // Close the read descriptor.
    ASSERT_SYS_OK(close(rank + 36));
    // Close the write descriptor.
    ASSERT_SYS_OK(close(rank + 52));

    channels_finalize();
}

int MIMPI_World_size() {
    //TODO
    const char* envvar_name_world_size = "world_size";
    char* world_size = getenv(envvar_name_world_size);
    ASSERT_NOT_NULL(world_size);
    return atoi(world_size);
}

int MIMPI_World_rank() {
    //TODO
    const char* envvar_name_id = "process_id";
    char* process_id = getenv(envvar_name_id);
    ASSERT_NOT_NULL(process_id);
    return atoi(process_id);
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    //TODO
    int my_rank = MIMPI_World_rank();
    ssize_t sent = chsend(276 + destination*16 + my_rank, data, count);
    ASSERT_SYS_OK(sent);
    if (sent != count)
        fatal("Wrote less than expected.");
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    //TODO
    int my_rank = MIMPI_World_rank();
    ssize_t read = chrecv(20 + my_rank*16 + source, data, count);
    ASSERT_SYS_OK(read);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    //TODO
    int id = MIMPI_World_rank();
    int world_size = MIMPI_World_size();
    char dummy_data = 69;
    char buffer;

    pthread_t thread;
    ASSERT_ZERO(pthread_create(&thread, NULL, synchronizeProcesses, NULL));
    int* result;
    ASSERT_ZERO(pthread_join(thread, (void**)&result));
    printf("%d\n", *result);
    if (*result == 1) { // We are the last process in the barrier.
        chsend(52, &dummy_data, 1); // Wake up the first process.
    }

    chrecv(36 + id, &buffer, 1);
    if (id == world_size - 1) {
        // We are the last process, it's time to free-up the barrier.
        unlock_barrier();
    }
    else {
        int first = 2 * id;
        int second = first + 1;
        if (first < world_size) {
            chsend(52 + first, &dummy_data, 1);
        }
        if (second < world_size) {
            chsend(52 + second, &dummy_data, 1);
        }
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}