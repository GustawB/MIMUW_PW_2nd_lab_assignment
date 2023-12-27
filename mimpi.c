/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();


    //TODO
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
    ssize_t sent = chsend(52 + destination, data, count);
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
    ssize_t read = chrecv(36 + my_rank, data, count);
    ASSERT_SYS_OK(read);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    //TODO
    int id = MIMPI_World_rank();
    int world_size = MIMPI_World_size();
    char total_count_buffer = 1;
    unsigned int total_count = 0;
    char buffer;
    int count = 1;

    if (id == 0) {
        total_count = 1;
        //printf("%d\n", total_count);
        chsend(52 + ((id+1)%world_size), &total_count_buffer, count);
    }
    else {
        chrecv(36 + id, &buffer, count);
        total_count = buffer + 1;
        total_count_buffer = total_count;
        //printf("%d\n", total_count);
        chsend(52 + ((id + 1) % world_size), &total_count_buffer, count);
    }
    while (total_count < world_size) {
        chrecv(36 + id, &buffer, count);
        total_count = buffer;
        total_count_buffer = total_count;
        //printf("%d\n", total_count);
        chsend(52 + ((id + 1) % world_size), &total_count_buffer, count);
    }

    
    /*// 1. Send garbage to the next process.
    chsend(52 + ((id+1)%world_size), &garbageData, count);
    //2. Wait for the garbage from the previous process.
    chrecv(36 + id, &buffer, count);
    //3. Send second garbage to the next process.
    chsend(52 + ((id + 1) % world_size), &garbageData, count);
    //4. Wait for the second garbage from the previous process.
    chrecv(36 + id, &buffer, count);*/
    
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