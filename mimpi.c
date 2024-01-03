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
#include <limits.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

struct buffer_list {
    char* buffer;
    int tag;
    int count;
    int size;
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
    int size;
    int count;
    int tag;
};
typedef struct metadata metadata;

#define PIPE_BUFF_UPDT (PIPE_BUF - sizeof(metadata))
#define KILL_THREAD -1
#define BARRIER_MESSAGE -1

pthread_t* pipe_threads;
pthread_t* barrier_pipe_threads;
pthread_mutex_t* read_mutex;
pthread_mutex_t* no_data_mutex;
pthread_cond_t* read_cond;
int* waiting_for_count;
int* waiting_for_tag;
buffer_list** head_list;
buffer_list** end_list;
barrier_list** head_list;
barrier_list** end_list;

void* detect_barrier(void* data) {
    reader_params params = *((reader_params*)data);
    while (1) {
        void* temp = malloc(sizeof(metadata));
        chrecv(532 + params.my_rank * 16 + params.source, temp, sizeof(metadata));
        metadata md = *((metadata*)temp);
    }
}

void* read_data(void* data) {
    reader_params params = *((reader_params*)data);
    void* temp = malloc(sizeof(metadata));
    while (1) {
        chrecv(20 + params.my_rank * 16 + params.source, temp, sizeof(metadata));
        metadata md = *((metadata*)temp);
        //printf("size: %d\n", md.size);
        char* buffer = malloc(md.size);
        //printf("fsugyahdjkl\n");
        chrecv(20 + params.my_rank * 16 + params.source, buffer, md.size);
        if (md.tag == KILL_THREAD) {
            free(buffer);
            free(temp);
            break;
        }
        ASSERT_ZERO(pthread_mutex_lock(&read_mutex[params.source]));
        end_list[params.source]->next = malloc(sizeof(buffer_list));
        end_list[params.source] = end_list[params.source]->next;
        end_list[params.source]->next = NULL;
        end_list[params.source]->buffer = malloc(md.size);
        end_list[params.source]->tag = md.tag;
        end_list[params.source]->count = md.count;
        end_list[params.source]->size = md.size;
        ASSERT_NOT_NULL(memcpy(end_list[params.source]->buffer, buffer, md.size));
        free(buffer);
        int nr_of_chunks = md.count / PIPE_BUFF_UPDT;
        if (md.count % PIPE_BUFF_UPDT != 0) {
            ++nr_of_chunks;
        }
        for (int i = 1; i < nr_of_chunks; ++i) {
            chrecv(20 + params.my_rank * 16 + params.source, temp, sizeof(metadata));
            metadata md2 = *((metadata*)temp);
            char* buffer2 = malloc(md2.size);
            chrecv(20 + params.my_rank * 16 + params.source, buffer2, md2.size);
            end_list[params.source]->next = malloc(sizeof(buffer_list));
            end_list[params.source] = end_list[params.source]->next;
            end_list[params.source]->next = NULL;
            end_list[params.source]->buffer = malloc(md2.size);
            end_list[params.source]->tag = md2.tag;
            end_list[params.source]->count = md2.count;
            end_list[params.source]->size = md2.size;
            
            ASSERT_NOT_NULL(memcpy(end_list[params.source]->buffer, buffer2, md2.size));
            free(buffer2);
            //printf("%d out of %d; size: %d\n", i, nr_of_chunks, end_list[params.source]->size);
        }

        if (waiting_for_count[params.source] == md.count &&
            (waiting_for_tag[params.source] == MIMPI_ANY_TAG ||
                (waiting_for_tag[params.source] == md.tag))) {
            waiting_for_count[params.source] = -1;
            waiting_for_tag[params.source] = -1;
            ASSERT_ZERO(pthread_cond_signal(&read_cond[params.source]));
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
    waiting_for_count = malloc(nr_of_threads_to_create * sizeof(int));
    waiting_for_tag = malloc(nr_of_threads_to_create * sizeof(int));
    head_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));
    end_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));

    for (int i = 0; i < nr_of_threads_to_create; ++i) {
        waiting_for_count[i] = -1;
        waiting_for_tag[i] = -1;
        head_list[i] = malloc(sizeof(buffer_list));
        head_list[i]->next = NULL;
        head_list[i]->tag = -1;
        head_list[i]->count = -1;
        head_list[i]->size = -1;
        end_list[i] = head_list[i];
        char* buffer = malloc(sizeof(reader_params));
        reader_params* params = (reader_params*)buffer;
        params->my_rank = MIMPI_World_rank();
        params->source = i;
        ASSERT_ZERO(pthread_create(&pipe_threads[i], NULL, read_data, params));
        ASSERT_ZERO(pthread_mutex_init(&no_data_mutex[i], NULL));
        ASSERT_ZERO(pthread_mutex_init(&read_mutex[i], NULL));
    }

    ASSERT_ZERO(pthread_cond_init(read_cond, NULL));
}

void kill_thread(int my_rank, int i, void* params, size_t count) {
    //printf("kill_thread: %ld\n", count);
    char* buffer = malloc(sizeof(metadata) + count);
    metadata* md = (metadata*)buffer;
    writer_params* params_p = (writer_params*)params;
    md->size = params_p->count;
    md->count = count;
    md->tag = KILL_THREAD;
    //printf("kill_thread: %d\n", md->size);
    
    memcpy(buffer + sizeof(metadata), params, count);
    ssize_t sent = chsend(276 + my_rank * 16 + i, buffer, count + sizeof(metadata));
    free(buffer);
    ASSERT_SYS_OK(sent);
}

void MIMPI_Finalize() {
    //TODO
    int my_rank = MIMPI_World_rank(); // Get the id of the process.
    int world_size = MIMPI_World_size();

    // Close all threads.
    writer_params params;
    int data = 0;
    params.data = &data;
    params.count = 1;
    params.tag = -1;
    for (int i = 0; i < world_size; ++i) {
        kill_thread(my_rank, i, &params, sizeof(writer_params));
        ASSERT_ZERO(pthread_join(pipe_threads[i], NULL));
    }
    // Close all read/write descriptors.
    for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < world_size; ++j) {
            // Close read descriptor.
            ASSERT_SYS_OK(close(20 + j + i * 16));
            // Close write descriptor.
            ASSERT_SYS_OK(close(276 + j + i * 16));
        }
    }
    // Close all barrier descriptors.
    for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < 3; ++j) {
            // Close read descriptor.
            ASSERT_SYS_OK(close(532 + j + i * 3));
            // Close write descriptor.
            ASSERT_SYS_OK(close(580 + j + i * 3));
            // Close read descriptor.
            ASSERT_SYS_OK(close(628 + j + i * 3));
            // Close write descriptor.
            ASSERT_SYS_OK(close(676 + j + i * 3));
            // Close read descriptor.
            ASSERT_SYS_OK(close(724 + j + i * 3));
            // Close write descriptor.
            ASSERT_SYS_OK(close(772 + j + i * 3));
        }
    }

    for (int i = 0; i < world_size; ++i) {
        buffer_list* cleaner = head_list[i];
        while (head_list[i] != NULL)
        {
            head_list[i] = head_list[i]->next;
            free(cleaner->buffer);
            free(cleaner);
            cleaner = head_list[i];
        }
        ASSERT_ZERO(pthread_mutex_destroy(&no_data_mutex[i]));
        ASSERT_ZERO(pthread_mutex_destroy(&read_mutex[i]));
        ASSERT_ZERO(pthread_cond_destroy(read_cond));
    }
    free(no_data_mutex);
    free(read_mutex);
    free(read_cond);
    free(pipe_threads);
    free(head_list);
    free(end_list);
    free(waiting_for_count);
    free(waiting_for_tag);
    //printf("End of finalize in rank: %d\n", my_rank);
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
    if (tag == BARRIER_MESSAGE) {
        char* buffer = malloc(PIPE_BUF);
        metadata* md = (metadata*)buffer;
        md->size = count;
        md->count = count;
        md->tag = tag;
        ssize_t sent = chsend(destination, buffer, sizeof(metadata));
        free(buffer);
        ASSERT_SYS_OK(sent);
        if (sent != PIPE_BUF)
            fatal("Wrote less than expected.");
    }
    int my_rank = MIMPI_World_rank();
    int alloc_size = count % PIPE_BUFF_UPDT;
    if (PIPE_BUFF_UPDT == count) {
        alloc_size = count;
    }
    if (count > PIPE_BUFF_UPDT) {
        int chunks = count / PIPE_BUFF_UPDT;
        if (count % PIPE_BUFF_UPDT == 0) {
            alloc_size = 0;
        }
        for (int i = 0; i < chunks; ++i) {
            char* buffer = malloc(PIPE_BUF);
            metadata* md = (metadata*)buffer;
            md->size = PIPE_BUFF_UPDT;
            md->count = count;
            md->tag = tag;
            memcpy(buffer + sizeof(metadata), data + (i * PIPE_BUFF_UPDT), PIPE_BUFF_UPDT);
            ssize_t sent = chsend(276 + destination * 16 + my_rank, buffer, PIPE_BUF);
            free(buffer);
            ASSERT_SYS_OK(sent);
            if (sent != PIPE_BUF)
                fatal("Wrote less than expected.");
        }
    }
    if (alloc_size > 0) {
        char* buffer = malloc(sizeof(metadata) + alloc_size);
        metadata* md = (metadata*)buffer;
        md->size = alloc_size;
        md->count = count;
        md->tag = tag;
        memcpy(buffer + sizeof(metadata), data + ((count/PIPE_BUFF_UPDT) * PIPE_BUFF_UPDT), alloc_size);
        ssize_t sent = chsend(276 + destination * 16 + my_rank, buffer, alloc_size + sizeof(metadata));
        free(buffer);
        ASSERT_SYS_OK(sent);
        if (sent != alloc_size + sizeof(metadata))
            fatal("Wrote less than expected.");
    }
    
    //printf("The message has been send: %d\n", my_rank);
    
    return MIMPI_SUCCESS;
}

bool is_there_data_to_read(int source, int count, int tag) {
    if (head_list[source] == end_list[source]) {
        return false;
    }
    buffer_list* iter = head_list[source]->next;
    while (iter != NULL) {
        if (iter->count == count && (iter->tag == tag || tag == MIMPI_ANY_TAG)) {
            return true;
        }
        iter = iter->next;
    }
    return false;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    //TODO
    ASSERT_ZERO(pthread_mutex_lock(&read_mutex[source]));
    if (!is_there_data_to_read(source, count, tag)) {
        printf("Waiting for data\n");
        while (!is_there_data_to_read(source, count, tag)) {
            waiting_for_count[source] = count;
            waiting_for_tag[source] = tag;
            ASSERT_ZERO(pthread_cond_wait(&read_cond[source], &read_mutex[source]));
            printf("Waking up\n");
        }
    }
    printf("Getting data\n");
    buffer_list* prev = head_list[source];
    buffer_list* iter = head_list[source]->next;
    while (iter != NULL && iter->count != count) {
        if (tag == MIMPI_ANY_TAG || iter->tag == tag) {
            break;
        }
        iter = iter->next;
        prev = prev->next;
    }
    int nr_of_chunks = count / PIPE_BUFF_UPDT;
    if (count % PIPE_BUFF_UPDT != 0) {
        ++nr_of_chunks;
    }
    for (int i = 0; i < nr_of_chunks; ++i) {
        //printf("Receive %d of %d; size: %d\n", i, nr_of_chunks, iter->size);
        ASSERT_NOT_NULL(memcpy((data + i * PIPE_BUFF_UPDT), iter->buffer, iter->size));
        prev->next = iter->next;
        free(iter->buffer);
        free(iter);
        iter = prev->next;
    }
    ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[source]));

    return MIMPI_SUCCESS;
}

int calc_tree_size(int elements) {
    int height = 1;
    int iter = 1;
    while (iter <= elements) {
        ++height;
        iter *= 2;
    }

    return height;
}

int calc_tree_leaves(int elements) {
    int iter = 1;
    while (iter <= elements) {
        iter *= 2;
    }
    iter /= 2;

    return elements - iter;
}

MIMPI_Retcode MIMPI_Barrier() {
    //TODO
    int world_size = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    //int tree_height = calc_tree_height(world_size);
    int left_subtree = 2 * my_rank + 1;
    int right_subtree = 2 * my_rank + 2;
    char send_buffer = my_rank;
    char* recv_buffer = malloc(sizeof(int));
    int count = sizeof(int);
    if (left_subtree < world_size) {
        //printf("Process %d waiting for child %d\n", my_rank, left_subtree);
        chrecv(532 + my_rank * 3 + 1, recv_buffer, count);
    }
    if (right_subtree < world_size) {
        //printf("Process %d waiting for child %d\n", my_rank, right_subtree);
        chrecv(532 + my_rank * 3 + 2, recv_buffer, count);
    }
    if (my_rank > 0) {
        if (my_rank % 2 != 0) {
            chsend(580 + ((my_rank / 2) * 3) + 1, &send_buffer, count);
        }
        else {
            chsend(580 + ((my_rank / 2 - 1) * 3) + 2, &send_buffer, count);
        }
        chrecv(532 + my_rank * 3, recv_buffer, count);
    }
    if (left_subtree < world_size) {
        chsend(580 + left_subtree * 3, &send_buffer, count);
    }
    if (right_subtree < world_size) {
        chsend(580 + right_subtree * 3, &send_buffer, count);
    }

    free(recv_buffer);
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