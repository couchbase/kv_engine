/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <platform/platform.h>
#include "extensions/protocol_extension.h"

union c99hack {
    void *pointer;
    void (*exit_function)(void);
};

/*
 * The stdin_term_handler allows you to shut down memcached from
 * another process by the use of a pipe. It operates in a line mode
 * with the following syntax: "command\n"
 *
 * The following commands exists:
 *   shutdown - Request memcached to initiate a clean shutdown
 *   die!     - Request memcached to die as fast as possible! like
 *              the unix "kill -9"
 *
 * Please note that you may try to shut down cleanly and give
 * memcached a grace period to complete, and if you don't want to wait
 * any longer you may send "die!" and have it die immediately. All
 * unknown commands will be ignored.
 *
 * If the input stream is closed a clean shutdown is initiated
 */
static void check_stdin_thread(void* arg)
{
    char command[80];
    union c99hack chack;
    chack.pointer = arg;

    while (fgets(command, sizeof(command), stdin) != NULL) {
        /* Handle the command */
        if (strcmp(command, "die!\n") == 0) {
            fprintf(stderr, "'die!' on stdin.  Exiting super-quickly\n");
            fflush(stderr);
            _exit(0);
        } else if (strcmp(command, "shutdown\n") == 0) {
            if (chack.pointer != NULL) {
                fprintf(stderr, "EOL on stdin.  Initiating shutdown\n");
                chack.exit_function();
                chack.pointer = NULL;
            }
        } else {
            fprintf(stderr, "Unknown command received on stdin.  Ignored\n");
        }
    }

    /* The stream is closed.. do a nice shutdown */
    if (chack.pointer != NULL) {
        fprintf(stderr, "EOF on stdin.  Initiating shutdown \n");
        chack.exit_function();
    }
}

static const char *get_name(void) {
    return "stdin_check";
}

static EXTENSION_DAEMON_DESCRIPTOR descriptor;

MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(const char *config,
                                                     GET_SERVER_API get_server_api) {
    SERVER_HANDLE_V1 *server = get_server_api();
    union c99hack ch;
    cb_thread_t t;

    descriptor.get_name = get_name;
    if (server == NULL) {
        return EXTENSION_FATAL;
    }

    if (!server->extension->register_extension(EXTENSION_DAEMON, &descriptor)) {
        return EXTENSION_FATAL;
    }

    ch.exit_function = server->core->shutdown;
    if (cb_create_thread(&t, check_stdin_thread, ch.pointer, 1) != 0) {
        perror("couldn't create stdin checking thread.");
        server->extension->unregister_extension(EXTENSION_DAEMON, &descriptor);
        return EXTENSION_FATAL;
    }

    return EXTENSION_SUCCESS;
}
