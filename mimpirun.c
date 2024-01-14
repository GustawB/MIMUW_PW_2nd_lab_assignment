/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include "mimpi_common.h"
#include "channel.h"

int main(int argc, char** argv) {
    //TODO;
    // Parse cmd args.
    if (argc < 3) {
        fatal("Not enough params for: %s\n", argv[0]);
    }
    int nr_of_copies = atoi(argv[1]);
    char* fp_prog = argv[2];
    // Make array of args for processes that will be executed later.
    if (argc > 3) {
        //printf("%s\n", argv[3]);
    }

    // Create nr_of_copies^2 pipes.    
    for (int i = 0; i < nr_of_copies; ++i) {
        for (int j = 0; j < nr_of_copies; ++j) {
            // Create channel.
            int channel_dsc[2];
            ASSERT_SYS_OK(channel(channel_dsc));
            // Move read descriptor to the index = 20+id+i*16.
            ASSERT_SYS_OK(dup2(channel_dsc[0], 20 + j + i * 16));
            // Close the old read descriptor.
            ASSERT_SYS_OK(close(channel_dsc[0]));
            // Move write descriptor to the index = 20+(16*16)+id+i*16.
            ASSERT_SYS_OK(dup2(channel_dsc[1], 276 + j + i * 16));
            // Close the old write descriptor.
            ASSERT_SYS_OK(close(channel_dsc[1]));
            //printf("%d; %d\n", 20 + j + i * 16, 276 + j + i * 16);
        }
    }
    
    // Create 3n pipes for the group communication.
    for (int i = 0; i < nr_of_copies; ++i) {
        for (int j = 0; j < 3; ++j) {
            int channel_dsc[2];
            ASSERT_SYS_OK(channel(channel_dsc));
            // Move read descriptor to the index = 532+id+j.
            ASSERT_SYS_OK(dup2(channel_dsc[0], 532 + j + i * 3));
            // Close the old read descriptor.
            ASSERT_SYS_OK(close(channel_dsc[0]));
            // Move write descriptor to the index = 580+id+j.
            ASSERT_SYS_OK(dup2(channel_dsc[1], 580 + j + i * 3));
            // Close the old write descriptor.
            ASSERT_SYS_OK(close(channel_dsc[1]));
        }
    }

    const char* MIMPI_envvar_name_id = "process_id";
    const char* MIMPI_envvar_name_world_size = "world_size";
    // Start processes.
    for (int i = 0; i < nr_of_copies; ++i) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {// Child
            char id_buffer[sizeof(int)];
            // This will cast 'int i' to char*;
            int ret = sprintf(id_buffer, "%d", i);
            if (ret < 0 || ret >= (int)sizeof(id_buffer))
                fatal("Adding envvar_name_id failed");
            ASSERT_ZERO(setenv(MIMPI_envvar_name_id, id_buffer, 0));
            // Add enviromental variable describing the id
            // of the executed program.
            char world_size_buffer[sizeof(int)];
            // This will cast 'int nr_of_copies' to char*;
            sprintf(world_size_buffer, "%d", nr_of_copies);
            if (ret < 0 || ret >= (int)sizeof(world_size_buffer))
                fatal("Adding envvar_name_world_size failed");
            // Add enviromental variable describing the size
            // of the current world.
            ASSERT_ZERO(setenv(MIMPI_envvar_name_world_size, world_size_buffer, 0));
            char* const* arg = &argv[2];
            for (int j = 0; j < nr_of_copies; ++j) {
                for (int k = 0; k < nr_of_copies; ++k) {
                    if (j != i) {
                        ASSERT_SYS_OK(close(20 + j * 16 + k));
                    }
                    if (k != i) {
                        ASSERT_SYS_OK(close(276 + j * 16 + k));
                    }
                }
            }
            int parent_desc;
            int left_child = 580 + (2 * i + 1) * 3;
            int right_child = 580 + (2 * i + 2) * 3;
            if (i % 2 != 0) {
                parent_desc = 580 + ((i / 2) * 3) + 1;
            }
            else{
                parent_desc = 580 + ((i / 2 - 1) * 3) + 2;
            }
            for (int j = 0; j < nr_of_copies; ++j) {
                for (int k = 0; k < 3; ++k) {
                    int fd = 580 + k + j * 3;
                    if (fd != parent_desc && fd != left_child && fd != right_child) {
                        ASSERT_SYS_OK(close(fd));
                    }
                    if (j != i) {
                        ASSERT_SYS_OK(close(532 + k + j * 3));
                    }
                }
            }
            ASSERT_SYS_OK(execvp(fp_prog, arg));
        }
    }

    for (int i = 0; i < nr_of_copies; ++i) {
        for (int j = 0; j < nr_of_copies; ++j) {
            // Close the old read descriptor.
            ASSERT_SYS_OK(close(20 + j + i * 16));
            // Close the old write descriptor.
            ASSERT_SYS_OK(close(276 + j + i * 16));
        }
    }

    for (int i = 0; i < nr_of_copies; ++i) {
        for (int j = 0; j < 3; ++j) {
            // Close the old read descriptor.
            ASSERT_SYS_OK(close(532 + j + i * 3));
            // Close the old write descriptor.
            ASSERT_SYS_OK(close(580 + j + i * 3));
        }
    }

    // Wait for processes to finish.
    for (int i = 0; i < nr_of_copies; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }
    
    return 0;
}