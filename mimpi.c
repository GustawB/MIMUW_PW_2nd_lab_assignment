/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

struct buffer_list {
    char* buffer;
    struct buffer_list* next;
};
typedef struct buffer_list buffer_list;

struct reader_params {
    int my_rank;
    int source;
};
typedef struct reader_params reader_params;

struct writer_params {
    void const* data;
    int count;
    int tag;
};
typedef struct writer_params writer_params;

struct metadata {
    int nr_of_chunks;
    int count;
    int tag;
};
typedef struct metadata metadata;

pthread_t* pipe_threads;
pthread_mutex_t* read_mutex;
pthread_mutex_t* no_data_mutex;
pthread_cond_t* read_cond;
int* is_waiting_for_data;
buffer_list** head_list;
buffer_list** end_list;

void* read_data(void* data) {
    reader_params params = *((reader_params*)data);
    //printf("Created thread %d from process %d.\n", params.source, params.my_rank);
    void* temp = malloc(sizeof(metadata));
    while (1) {
        chrecv(20 + params.my_rank * 16 + params.source, temp, sizeof(metadata));
        metadata md = *((metadata*)temp);
        char* buffer = malloc(md.count);
        chrecv(20 + params.my_rank * 16 + params.source, buffer, md.count);
        //printf("Process: %d; thread: %d; tag: %d\n", params.my_rank, params.source, md.tag);
        if (md.tag == -1) {
            free(buffer);
            //printf("killing thread\n");
            break;
        }
        //printf("NOT killing thread %d from process %d.\n", params.source, params.my_rank);
        //printf("buffer: %d\n", *((char*)buffer));
        ASSERT_ZERO(pthread_mutex_lock(&read_mutex[params.source]));
        //printf("Entering CR in thread %d from process %d.\n", params.source, params.my_rank);
        end_list[params.source]->next = malloc(sizeof(buffer_list));
        end_list[params.source] = end_list[params.source]->next;
        end_list[params.source]->next = NULL;
        end_list[params.source]->buffer = malloc(md.count);
        ASSERT_NOT_NULL(strcpy(end_list[params.source]->buffer, buffer));
        //printf("%d\n", *((char*)end_list[params.source]->buffer));
        //printf("I'm about to check whether source %d is waiting for data (%d)\n", 
            //params.source, is_waiting_for_data[params.source]);
        if (is_waiting_for_data[params.source] == 1) {
            //printf("Someone is waiting for data\n");
            is_waiting_for_data[params.source] = 0;
            ASSERT_ZERO(pthread_cond_signal(&read_cond[params.source]));
            //printf("I woke up yo mama hehehe\n");
        }
        ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[params.source]));
    }

    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    //TODO
    int nr_of_threads_to_create = MIMPI_World_size();

    pipe_threads = malloc(nr_of_threads_to_create * sizeof(pthread_t));
    read_mutex = malloc(nr_of_threads_to_create * sizeof(pthread_mutex_t));
    no_data_mutex = malloc(nr_of_threads_to_create * sizeof(pthread_mutex_t));
    read_cond = malloc(nr_of_threads_to_create * sizeof(pthread_cond_t));
    is_waiting_for_data = malloc(nr_of_threads_to_create * sizeof(int));
    head_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));
    end_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));

    for (int i = 0; i < nr_of_threads_to_create; ++i) {
        is_waiting_for_data[i] = 0;
        head_list[i] = malloc(sizeof(buffer_list));
        head_list[i]->next = NULL;
        end_list[i] = head_list[i];
        char* buffer = malloc(sizeof(reader_params));
        reader_params* params = (reader_params*)buffer;
        params->my_rank = MIMPI_World_rank();
        params->source = i;
        ASSERT_ZERO(pthread_create(&pipe_threads[i], NULL, read_data, params));
        ASSERT_ZERO(pthread_mutex_init(&no_data_mutex[i], NULL));
        ASSERT_ZERO(pthread_mutex_init(&read_mutex[i], NULL));
    }
}

void kill_thread(int my_rank, int i, void* params, size_t count) {
    char* buffer = malloc(sizeof(metadata) + count);
    metadata* md = (metadata*)buffer;
    md->nr_of_chunks = 0;
    md->count = count;
    md->tag = -1;
    memcpy(buffer + sizeof(metadata), params, count);
    ssize_t sent = chsend(276 + my_rank * 16 + i, buffer, count + sizeof(metadata));
    ASSERT_SYS_OK(sent);
}

void MIMPI_Finalize() {
    //TODO
    int my_rank = MIMPI_World_rank(); // Get the id of the process.
    int world_size = MIMPI_World_size();

    // Close all threads.
    writer_params params;
    params.data = 0;
    params.count = 1;
    params.tag = -1;
    for (int i = 0; i < world_size; ++i) {
        kill_thread(my_rank, i, &params, sizeof(writer_params));
        ASSERT_ZERO(pthread_join(pipe_threads[i], NULL));
    }
    // Close all descriptors.
    for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < world_size; ++j) {
            // Close read descriptor.
            ASSERT_SYS_OK(close(20 + j + i * 16));
            // Close write descriptor.
            ASSERT_SYS_OK(close(276 + j + i * 16));
        }
    }
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
    char* buffer = malloc(sizeof(metadata) + count);
    metadata* md = (metadata*)buffer;
    md->nr_of_chunks = 1;
    md->count = count;
    md->tag = tag;
    memcpy(buffer + sizeof(metadata), data, count);
    ssize_t sent = chsend(276 + destination*16 + my_rank, buffer, count + sizeof(metadata));
    //printf("The message has been send: %d\n", my_rank);
    ASSERT_SYS_OK(sent);
    if (sent != count + sizeof(metadata))
        fatal("Wrote less than expected.");
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    //printf("Entered mimpi_receive with source %d\n", source);
    //TODO
    int my_rank = MIMPI_World_rank();
    ASSERT_ZERO(pthread_mutex_lock(&read_mutex[source]));
    if (head_list[source] == end_list[source]) {
        //printf("Awaiting data...\n");
        while (head_list[source] == end_list[source]) {
            is_waiting_for_data[source] = 1;
            //printf("Receive waiting in loop; source: %d; waiting: %d;.\n",
                //source, is_waiting_for_data[source]);
            ASSERT_ZERO(pthread_cond_wait(&read_cond[source], &read_mutex[source]));
            //printf("I woke up :))))))\n");
        }
    }
    //printf("Reading data...\n");
    char* received_data = (char*)head_list[source]->next->buffer;
    ASSERT_NOT_NULL(strcpy(data, received_data));
    buffer_list* pointer_to_delete = head_list[source]->next;
    head_list[my_rank]->next = head_list[source]->next->next;
    free(pointer_to_delete);
    ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[source]));


    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
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