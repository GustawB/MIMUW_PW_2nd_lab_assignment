/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <unistd.h>

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();


    //TODO
}

void MIMPI_Finalize() {
    //TODO
    int rank = MIMPI_World_rank(); // Get the id of the process.
    // Close the read descriptor.
    ASSERT_SYS_OK(close(rank + 20));
    // Close the write descriptor.
    ASSERT_SYS_OK(close(rank + 36));

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
    //ssize_t sent = chsend(20 + destination, data, count);
    chsend(20 + destination, data, count);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    //TODO
    char buffer[1024];
    ssize_t read = chrecv(26 + source, buffer, 1024);
    ASSERT_SYS_OK(read);
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