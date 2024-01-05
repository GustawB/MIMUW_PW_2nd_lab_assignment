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
#include <stdint.h>

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
    int world_size;
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
#define KILL_THREAD -2
#define BARRIER_MESSAGE -3
#define BROADCAST_MESSAGE -6
#define REDUCE_MESSAGE -9
#define SMALL_FINALIZE -12
#define FINALIZE_MESSAGE -13

pthread_t* pipe_threads;
pthread_mutex_t* read_mutex;
pthread_mutex_t* no_data_mutex;
pthread_cond_t* read_cond;
int* waiting_for_count;
int* waiting_for_tag;
int* pipes_state;
buffer_list** head_list;
buffer_list** end_list;

void* read_data(void* data) {
    reader_params params = *((reader_params*)data);
    //printf("Created thread %d for process %d\n", params.source, params.my_rank);
    int local_source;
    if (params.source < params.world_size) {// normal read
        local_source = 20 + params.my_rank * 16 + params.source;
    }
    else if (params.source >= params.world_size && params.source < params.world_size + 3) { // barrier
        local_source = 532 + params.my_rank * 3 + params.source - params.world_size;
    }
    else if (params.source >= params.world_size + 3 && params.source < params.world_size + 6) { // broadcast
        local_source = 628 + params.my_rank * 3 + params.source - params.world_size - 3;
    }
    else { // reduce
        local_source = 724 + params.my_rank * 3 + params.source - params.world_size - 6;
    }
    void* temp = malloc(sizeof(metadata));
    while (1) {
        chrecv(local_source, temp, sizeof(metadata));
        metadata md = *((metadata*)temp);
        char* buffer = malloc(md.size);
        chrecv(local_source, buffer, md.size);
        if (md.tag == KILL_THREAD) {
            free(buffer);
            free(temp);
            break;
        }
        //printf("Process %d received data in thread with tag %d from source %d\n", params.my_rank, md.tag,params.source);
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
            chrecv(local_source, temp, sizeof(metadata));
            metadata md2 = *((metadata*)temp);
            char* buffer2 = malloc(md2.size);
            chrecv(local_source, buffer2, md2.size);
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
        //("Process %d waiting for tag %d and count %d\n", params.my_rank, waiting_for_tag[params.source], waiting_for_count[params.source]);
        if (waiting_for_count[params.source] == md.count &&
            ((waiting_for_tag[params.source] == MIMPI_ANY_TAG && md.tag > 0) ||
                (waiting_for_tag[params.source] == md.tag))) {
            waiting_for_count[params.source] = -1;
            waiting_for_tag[params.source] = -1;
            ASSERT_ZERO(pthread_cond_signal(&read_cond[params.source]));
        }
        else if (waiting_for_count[params.source] != -1 &&
            md.tag == FINALIZE_MESSAGE) {
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
    int world_size = MIMPI_World_size();
    int nr_of_threads_to_create = world_size + 9;

    pipe_threads = malloc(nr_of_threads_to_create * sizeof(pthread_t));
    read_mutex = malloc(nr_of_threads_to_create * sizeof(pthread_mutex_t));
    read_cond = malloc(nr_of_threads_to_create * sizeof(pthread_cond_t));
    waiting_for_count = malloc(nr_of_threads_to_create * sizeof(int));
    waiting_for_tag = malloc(nr_of_threads_to_create * sizeof(int));
    head_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));
    end_list = malloc(nr_of_threads_to_create * sizeof(buffer_list));
    pipes_state = malloc(nr_of_threads_to_create * sizeof(int));

    for (int i = 0; i < nr_of_threads_to_create; ++i) {
        pipes_state[i] = 0;
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
        params->world_size = world_size;
        ASSERT_ZERO(pthread_create(&pipe_threads[i], NULL, read_data, params));
        ASSERT_ZERO(pthread_mutex_init(&read_mutex[i], NULL));
    }

    ASSERT_ZERO(pthread_cond_init(read_cond, NULL));
}

void kill_thread(void* params, size_t count, int destination) {
    char* buffer = malloc(sizeof(metadata) + count);
    metadata* md = (metadata*)buffer;
    writer_params* params_p = (writer_params*)params;
    md->size = params_p->count;
    md->count = count;
    md->tag = KILL_THREAD;

    memcpy(buffer + sizeof(metadata), params, count);
    //ssize_t sent = chsend(276 + my_rank * 16 + i, buffer, count + sizeof(metadata));
    ssize_t sent = chsend(destination, buffer, count + sizeof(metadata));
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

    // Send info to all other processes that we are leaving.
    char finalize_data = FINALIZE_MESSAGE;
    for (int i = 0; i < world_size; ++i) {
        MIMPI_Send(&finalize_data, sizeof(finalize_data), i, SMALL_FINALIZE);
    }
    /*for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < 3; ++j) {
            MIMPI_Send(&finalize_data, sizeof(finalize_data), 580 + j + i * 3, FINALIZE_MESSAGE);
            //MIMPI_Send(&finalize_data, sizeof(finalize_data), 676 + j + i * 3, FINALIZE_MESSAGE);
            //MIMPI_Send(&finalize_data, sizeof(finalize_data), 772 + j + i * 3, FINALIZE_MESSAGE);
        }
    }*/
    int left_subtree = 2 * my_rank + 1;
    int right_subtree = 2 * my_rank + 2;
    if (my_rank > 0) {
        if (my_rank % 2 != 0) {
            MIMPI_Send(&finalize_data, sizeof(finalize_data), 580 + ((my_rank / 2) * 3) + 1, FINALIZE_MESSAGE - 1);
        }
        else {
            MIMPI_Send(&finalize_data, sizeof(finalize_data), 580 + ((my_rank / 2 - 1) * 3) + 2, FINALIZE_MESSAGE - 2);
        }
    }
    if (left_subtree < world_size) {
        MIMPI_Send(&finalize_data, sizeof(finalize_data), 580 + left_subtree * 3, FINALIZE_MESSAGE);
    }
    if (right_subtree < world_size) {
        MIMPI_Send(&finalize_data, sizeof(finalize_data), 580 + right_subtree * 3, FINALIZE_MESSAGE);
    }





    // Kill all helper threads.
    for (int i = 0; i < world_size; ++i) {
        kill_thread(&params, sizeof(writer_params), 276 + my_rank * 16 + i);
        ASSERT_ZERO(pthread_join(pipe_threads[i], NULL));
    }
    int iter = world_size;
    for (int i = 0; i < 3; ++i) {
        kill_thread(&params, sizeof(writer_params), 580 + my_rank * 3 + i);
        ASSERT_ZERO(pthread_join(pipe_threads[iter], NULL));
        ++iter;
    }


    // DO SOMETHING ABOUT PROCESSES STILL HANGING ON MUTEXES!!!!!!!!!!!!!!!!!!!!!!!!!!


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
           /* ASSERT_SYS_OK(close(628 + j + i * 3));
            // Close write descriptor.
            ASSERT_SYS_OK(close(676 + j + i * 3));
            // Close read descriptor.
            ASSERT_SYS_OK(close(724 + j + i * 3));
            // Close write descriptor.
            ASSERT_SYS_OK(close(772 + j + i * 3));*/
        }
    }

    for (int i = 0; i < world_size + 9; ++i) {
        buffer_list* cleaner = head_list[i];
        while (head_list[i] != NULL)
        {
            head_list[i] = head_list[i]->next;
            free(cleaner->buffer);
            free(cleaner);
            cleaner = head_list[i];
        }
        ASSERT_ZERO(pthread_mutex_destroy(&read_mutex[i]));
        ASSERT_ZERO(pthread_cond_destroy(&read_cond[i]));
    }
    free(read_mutex);
    free(read_cond);
    free(pipe_threads);
    free(head_list);
    free(end_list);
    free(waiting_for_count);
    free(waiting_for_tag);
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

bool was_pipe_closed(int source) {
    if (head_list[source] == end_list[source]) {
        //printf("no error data for %d\n", source);
        return false;
    }
    buffer_list* iter = head_list[source]->next;
    buffer_list* prev = head_list[source];
    while (iter != NULL) {
        //printf("loop for source %d, iter-> tag is %d\n", source, iter->tag);
        if (iter->tag == FINALIZE_MESSAGE) {
            pipes_state[source] = FINALIZE_MESSAGE;
            prev->next = iter->next;
            free(iter->buffer);
            free(iter);
            if (prev->next == NULL) {
                end_list[source] = prev;
            }
            return true;
        }
        iter = iter->next;
        prev = prev->next;
    }
    //printf("out of loop for source %d\n", source);
    return false;
}

MIMPI_Retcode MIMPI_Send(
    void const* data,
    int count,
    int destination,
    int tag
) {
    // Lock to check if we still have somewhere to send
    //Calculate destinations
    int dest;
    if (tag == BARRIER_MESSAGE || tag == BROADCAST_MESSAGE || tag == REDUCE_MESSAGE || tag == FINALIZE_MESSAGE) {
        dest = (destination - 580) / 3;
    }
    else if (tag == BARRIER_MESSAGE - 1 || tag == BROADCAST_MESSAGE - 1 || tag == REDUCE_MESSAGE - 1 || tag == FINALIZE_MESSAGE - 1) {
        if (tag == BARRIER_MESSAGE - 1) { tag = BARRIER_MESSAGE; }
        else if (tag == BROADCAST_MESSAGE - 1) { tag = BROADCAST_MESSAGE; }
        else if (tag == REDUCE_MESSAGE - 1) { tag = REDUCE_MESSAGE; }
        else if (tag == FINALIZE_MESSAGE - 1) { tag = FINALIZE_MESSAGE; }
        dest = (destination - 1 - 580)/3;
    }
    else if (tag == BARRIER_MESSAGE - 2 || tag == BROADCAST_MESSAGE - 2 || tag == REDUCE_MESSAGE - 2 || tag == FINALIZE_MESSAGE - 2) {
        if (tag == BARRIER_MESSAGE - 2) { tag = BARRIER_MESSAGE; }
        else if (tag == BROADCAST_MESSAGE - 2) { tag = BROADCAST_MESSAGE; }
        else if (tag == REDUCE_MESSAGE - 2) { tag = REDUCE_MESSAGE; }
        else if (tag == FINALIZE_MESSAGE - 2) { tag = FINALIZE_MESSAGE; }
        dest = (destination - 2 - 580)/3;
    }
    else {
        dest = destination;
    }
    ASSERT_ZERO(pthread_mutex_lock(&read_mutex[dest]));
    if (pipes_state[dest] == FINALIZE_MESSAGE || was_pipe_closed(dest)) {
        ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[dest]));
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[dest]));
    //TODO

    int my_rank = MIMPI_World_rank();
    if (my_rank == destination) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    else if (destination >= MIMPI_World_size() && tag > 0) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    int alloc_size = count % PIPE_BUFF_UPDT;
    int local_dest;
    if (tag == BARRIER_MESSAGE || tag == BROADCAST_MESSAGE || tag == REDUCE_MESSAGE || tag == FINALIZE_MESSAGE) {
        local_dest = destination;
    }
    else {
        local_dest = 276 + destination * 16 + my_rank;
    }
    if (PIPE_BUFF_UPDT == count) {
        alloc_size = count;
    }
    if (tag == SMALL_FINALIZE) {
        tag = FINALIZE_MESSAGE;
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
            ssize_t sent = chsend(local_dest, buffer, PIPE_BUF);
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
        memcpy(buffer + sizeof(metadata), data + ((count / PIPE_BUFF_UPDT) * PIPE_BUFF_UPDT), alloc_size);
        
        ssize_t sent = chsend(local_dest, buffer, alloc_size + sizeof(metadata));
        free(buffer);
        if (sent == -1) {
            printf("Local dest: %d; destination: %d; tag: %d\n", local_dest, destination, tag);
        }
        ASSERT_SYS_OK(sent);
        if (sent != alloc_size + sizeof(metadata))
            fatal("Wrote less than expected.");
    }

    return MIMPI_SUCCESS;
}

bool is_there_data_to_read(int source, int count, int tag) {
    if (head_list[source] == end_list[source]) {
        return false;
    }
    buffer_list* iter = head_list[source]->next;
    while (iter != NULL) {
        if (iter->count == count && (iter->tag == tag || (tag == MIMPI_ANY_TAG && iter->tag > 0))) {
            return true;
        }
        iter = iter->next;
    }
    return false;
}

MIMPI_Retcode MIMPI_Recv(
    void* data,
    int count,
    int source,
    int tag
) {
    if (MIMPI_World_rank() == source) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    else if (source >= MIMPI_World_size() && tag > 0) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    //TODO
    ASSERT_ZERO(pthread_mutex_lock(&read_mutex[source]));
    if (!is_there_data_to_read(source, count, tag)) {
        while (!is_there_data_to_read(source, count, tag)) {
            //printf("Process %d waiting for data from source: %d.\n", MIMPI_World_rank(), source);
            if (pipes_state[source] == FINALIZE_MESSAGE || was_pipe_closed(source)) {
                ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[source]));
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            waiting_for_count[source] = count;
            waiting_for_tag[source] = tag;
            ASSERT_ZERO(pthread_cond_wait(&read_cond[source], &read_mutex[source]));
            //printf("%d Waking up from source: %d\n", MIMPI_World_rank(), source);
        }
    }
    //printf("Getting data\n");
    buffer_list* prev = head_list[source];
    buffer_list* iter = head_list[source]->next;
    while (iter != NULL) {
        if (iter->count == count && (iter->tag == tag || (tag == MIMPI_ANY_TAG && iter->tag > 0))) {
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
        ASSERT_NOT_NULL(memcpy((data + i * PIPE_BUFF_UPDT), iter->buffer, iter->size));
        prev->next = iter->next;
        free(iter->buffer);
        free(iter);
        iter = prev->next;
    }
    if (prev->next == NULL) {
        end_list[source] = prev;
    }
    ASSERT_ZERO(pthread_mutex_unlock(&read_mutex[source]));

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    //TODO
    int world_size = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int left_subtree = 2 * my_rank + 1;
    int right_subtree = 2 * my_rank + 2;
    int lse = -1;
    int rse = -1;
    int parent_send = -1;
    int parent_recv = -1;
    char send_buffer = my_rank;
    char* recv_buffer = malloc(sizeof(int));
    int count = sizeof(int);
    if (left_subtree < world_size) {
        lse = MIMPI_Recv(recv_buffer, count, world_size + 1, BARRIER_MESSAGE);
    }
    if (right_subtree < world_size) {
        rse = MIMPI_Recv(recv_buffer, count, world_size + 2, BARRIER_MESSAGE);
    }
    //printf("Process: %d; lse: %d; rse: %d\n", my_rank, lse, rse);
    if (my_rank > 0) {
        if (my_rank % 2 != 0) {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(&send_buffer, count, 580 + ((my_rank / 2) * 3) + 1, FINALIZE_MESSAGE - 1);
            }
            else {
                parent_send = MIMPI_Send(&send_buffer, count, 580 + ((my_rank / 2) * 3) + 1, BARRIER_MESSAGE - 1);
            }
        }
        else {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(&send_buffer, count, 580 + ((my_rank / 2 - 1) * 3) + 2, FINALIZE_MESSAGE - 2);
            }
            else {
                parent_send = MIMPI_Send(&send_buffer, count, 580 + ((my_rank / 2 - 1) * 3) + 2, BARRIER_MESSAGE - 2);
            }
        }
        parent_recv = MIMPI_Recv(recv_buffer, count, world_size, BARRIER_MESSAGE);
    }
    //printf("Process: %d; parent_send: %d; parent_recv: %d\n", my_rank, parent_send, parent_recv);
    if (left_subtree < world_size) {
        if (lse == 0) {
            if (parent_send > 0 || parent_recv > 0 || rse > 0) {
                MIMPI_Send(&send_buffer, count, 580 + left_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(&send_buffer, count, 580 + left_subtree * 3, BARRIER_MESSAGE);
            }
        }
    }
    if (right_subtree < world_size) {
        if (rse == 0) {
            if (parent_send > 0 || parent_recv > 0 || lse > 0) {
                MIMPI_Send(&send_buffer, count, 580 + right_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(&send_buffer, count, 580 + right_subtree * 3, BARRIER_MESSAGE);
            }
        }
    }
    free(recv_buffer);
    if (lse > 0 || rse > 0 || parent_send > 0 || parent_recv > 0) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void* data,
    int count,
    int root
) {
    //TODO
    int world_size = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int left_subtree = 2 * my_rank + 1;
    int right_subtree = 2 * my_rank + 2;
    int lse = -1;
    int rse = -1;
    int parent_send = -1;
    int parent_recv = -1;
    void* temp_buffer = malloc(count);
    memcpy(temp_buffer, data, count);
    if (left_subtree < world_size) {
        if (root != my_rank) {
            lse = MIMPI_Recv(data, count, world_size + 1, BROADCAST_MESSAGE);
        }
        else {
            char* recv_buffer = malloc(count);
            lse = MIMPI_Recv(recv_buffer, count, world_size + 1, BROADCAST_MESSAGE);
            free(recv_buffer);
        }
    }
    if (right_subtree < world_size) {
        if (root != my_rank) {
            rse = MIMPI_Recv(data, count, world_size + 2, BROADCAST_MESSAGE);
        }
        else {
            char* recv_buffer = malloc(count);
            rse = MIMPI_Recv(recv_buffer, count, world_size + 2, BROADCAST_MESSAGE);
            free(recv_buffer);
        }
    }
    //printf("Process: %d; lse: %d; rse: %d\n", my_rank, lse, rse);
    if (my_rank > 0) {
        if (my_rank % 2 != 0) {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(data, count, 580 + ((my_rank / 2) * 3) + 1, FINALIZE_MESSAGE - 1);
            }
            else {
                parent_send = MIMPI_Send(data, count, 580 + ((my_rank / 2) * 3) + 1, BROADCAST_MESSAGE - 1);
            }
        }
        else {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(data, count, 580 + ((my_rank / 2 - 1) * 3) + 2, FINALIZE_MESSAGE - 2);
            }
            else {
                parent_send = MIMPI_Send(data, count, 580 + ((my_rank / 2 - 1) * 3) + 2, BROADCAST_MESSAGE - 2);
            }
        }

        if (root != my_rank) {
            parent_recv = MIMPI_Recv(data, count, world_size, BROADCAST_MESSAGE);
        }
        else {
            char* recv_buffer = malloc(count);
            parent_recv = MIMPI_Recv(recv_buffer, count, world_size, BROADCAST_MESSAGE);
            free(recv_buffer);
        }
    }
    //printf("Process: %d; parent_send: %d; parent_recv: %d\n", my_rank, parent_send, parent_recv);
    if (left_subtree < world_size) {
        if (lse == 0) {
            if (parent_send > 0 || parent_recv > 0 || rse > 0) {
                MIMPI_Send(data, count, 580 + left_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(data, count, 580 + left_subtree * 3, BROADCAST_MESSAGE);
            }
        }
    }
    if (right_subtree < world_size) {
        if (rse == 0) {
            if (parent_send > 0 || parent_recv > 0 || lse > 0) {
                MIMPI_Send(data, count, 580 + right_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(data, count, 580 + right_subtree * 3, BROADCAST_MESSAGE);
            }
        }
    }

    if (lse > 0 || rse > 0 || parent_send > 0 || parent_recv > 0) {
        memcpy(data, temp_buffer, count);
        free(temp_buffer);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    free(temp_buffer);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const* send_data,
    void* recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    //TODO
    int world_size = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int left_subtree = 2 * my_rank + 1;
    int right_subtree = 2 * my_rank + 2;
    int lse = -1;
    int rse = -1;
    int parent_send = -1;
    int parent_recv = -1;
    void* recv_buffer = malloc(sizeof(uint8_t) * count);
    uint8_t send_buffer[count];
    memcpy(&send_buffer, send_data, count);
    if (left_subtree < world_size) {
        lse = MIMPI_Recv(recv_buffer, count, world_size + 1, REDUCE_MESSAGE);
        for (int i = 0; i < count && lse == 0; ++i) {
            if (op == MIMPI_MAX) {
                send_buffer[i] = MAX(send_buffer[i], ((uint8_t*)recv_buffer)[i]);
            }
            else if (op == MIMPI_MIN) {
                send_buffer[i] = MIN(send_buffer[i], ((uint8_t*)recv_buffer)[i]);
            }
            else if (op == MIMPI_SUM) {
                send_buffer[i] += ((uint8_t*)recv_buffer)[i];
            }
            else if (op == MIMPI_PROD) {
                send_buffer[i] *= ((uint8_t*)recv_buffer)[i];
            }
        }
    }
    free(recv_buffer);
    recv_buffer = malloc(sizeof(uint8_t) * count);
    if (right_subtree < world_size) {
        rse = MIMPI_Recv(recv_buffer, count, world_size + 2, REDUCE_MESSAGE);
        for (int i = 0; i < count && rse == 0; ++i) {
            if (op == MIMPI_MAX) {
                send_buffer[i] = MAX(send_buffer[i], ((uint8_t*)recv_buffer)[i]);
            }
            else if (op == MIMPI_MIN) {
                send_buffer[i] = MIN(send_buffer[i], ((uint8_t*)recv_buffer)[i]);
            }
            else if (op == MIMPI_SUM) {
                send_buffer[i] += ((uint8_t*)recv_buffer)[i];
            }
            else if (op == MIMPI_PROD) {
                send_buffer[i] *= ((uint8_t*)recv_buffer)[i];
            }
        }
    }
    free(recv_buffer);
    recv_buffer = malloc(sizeof(uint8_t) * count);
    if (my_rank > 0) {
        if (my_rank % 2 != 0) {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(send_buffer, count, 580 + ((my_rank / 2) * 3) + 1, FINALIZE_MESSAGE - 1);
            }
            else {
                parent_send = MIMPI_Send(send_buffer, count, 580 + ((my_rank / 2) * 3) + 1, REDUCE_MESSAGE - 1);
            }
        }
        else {
            if (lse > 0 || rse > 0) {
                MIMPI_Send(send_buffer, count, 580 + ((my_rank / 2 - 1) * 3) + 2, FINALIZE_MESSAGE - 2);
            }
            else {
                parent_send = MIMPI_Send(send_buffer, count, 580 + ((my_rank / 2 - 1) * 3) + 2, REDUCE_MESSAGE - 2);
            }
        }
        parent_recv = MIMPI_Recv(recv_buffer, count, world_size, REDUCE_MESSAGE);
    }
    if (root == my_rank && lse <= 0 && rse <= 0 && parent_recv <= 0 && parent_send <= 0) {
        if (root == 0) {
            memcpy(recv_data, send_buffer, count);
        }
        else {
            memcpy(recv_data, recv_buffer, count);
        }
    }
    if (left_subtree < world_size) {
        if (lse == 0) {
            if (parent_send > 0 || parent_recv > 0 || rse > 0) {
                MIMPI_Send(send_buffer, count, 580 + left_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(send_buffer, count, 580 + left_subtree * 3, REDUCE_MESSAGE);
            }
        }
    }
    if (right_subtree < world_size) {
        if (rse == 0) {
            if (parent_send > 0 || parent_recv > 0 || lse > 0) {
                MIMPI_Send(send_buffer, count, 580 + right_subtree * 3, FINALIZE_MESSAGE);
            }
            else {
                MIMPI_Send(send_buffer, count, 580 + right_subtree * 3, REDUCE_MESSAGE);
            }
        }
    }

    free(recv_buffer);
    if (lse > 0 || rse > 0 || parent_send > 0 || parent_recv > 0) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    return MIMPI_SUCCESS;
}