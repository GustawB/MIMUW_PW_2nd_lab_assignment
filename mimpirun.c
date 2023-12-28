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
    // Parse program's file path.
    if (strlen(fp_prog) == 0 || (strlen(fp_prog) < 3 
        && fp_prog[0] == '.' && fp_prog[1] == '/'))
    {
        free(fp_prog);
        fatal("Passed empty file path to the %s\n", argv[0]);
    }
    if (fp_prog[0] == '.' && fp_prog[1] == '/')
    {
        char* substr = malloc((strlen(fp_prog) - 1) * sizeof(char));
        strncpy(substr, &fp_prog[2], strlen(fp_prog) - 2);
        fp_prog = substr;
    }
    // Make array of args for processes that will be executed later.
    char** program_args = NULL;
    if (argc > 3) {
        program_args = &argv[3];
    }

    printf("beforum\n");
    // Initialize mimpi_common library.
    common_init(nr_of_copies);
    printf("afterum\n");

    // Create nr_of_copies pipes.
    for (int i = 0; i < nr_of_copies; ++i) {
        // Create channel.
        int channel_dsc[2];
        ASSERT_SYS_OK(channel(channel_dsc));
        // Move read descriptor to the index = 36+id.
        ASSERT_SYS_OK(dup2(channel_dsc[0], 36 + i));
        // Close the old read descriptor.
        ASSERT_SYS_OK(close(channel_dsc[0]));
        // Move write descriptor to the index = 36+16+id.
        ASSERT_SYS_OK(dup2(channel_dsc[1], 52 + i));
        // Close the old write descriptor.
        ASSERT_SYS_OK(close(channel_dsc[1]));
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

            if (program_args == NULL) {
                ASSERT_SYS_OK(execlp(fp_prog, fp_prog, NULL));
            }
            else {
                ASSERT_SYS_OK(execlp(fp_prog, fp_prog, program_args, NULL));
            }
        }
    }

    // Wait for processes to finish.
    for (int i = 0; i < nr_of_copies; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }

    // Clear mimpi_common library.
    common_finalize();

    return 0;
}