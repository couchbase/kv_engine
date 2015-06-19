/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef WIN32
#include <sys/poll.h>
#include <errno.h>
#include <fcntl.h>
#endif

#include <platform/platform.h>
#include "extensions/protocol_extension.h"

union c99hack {
    void *pointer;
    void (*exit_function)(void);
};

static char *get_command(char *buffer, size_t buffsize) {
#ifdef WIN32
    if (fgets(buffer, buffsize, stdin) == NULL) {
        return NULL;
    }
    return buffer;

#else
    /**
     * We've seen deadlocks on various versions of linux where calling exit()
     * from one thread starts running the handlers registered by atexit() (as
     * specified in the C standard), but the problem is that on some
     * platforms it tries to flush the io buffers and as part of that it
     * tries to acquire the mutex used to protect stdin.
     *
     * To work around that try to "poll" the standard input for read
     * events wiht a 1 minute timeout to allow the atexit() handler to
     * aqcuire the mutex.
     *
     * This would of course lead to "undefined" behavior if this thread
     * tries to run again. We should _really_, _really_ refactor the code
     * so that we use a better way to signal shutdown...
     *
     * This could cause memcached to hang "forever"
     */
    struct pollfd fds;
    fds.fd = fileno(stdin);
    fds.events = POLLIN;

    while (true) {
        switch (poll(&fds, 1, 60000)) {
        case 1:
            if (fgets(buffer, buffsize, stdin) == NULL) {
                return NULL;
            }
            return buffer;
        case 0:
            break;
        default:
            fprintf(stderr,
                    "ERROR: Failed to run poll() on standard input %s\n",
                    strerror(errno));
            /* sleep(6) to avoid busywait */
            sleep(1);
        }
    }
#endif
}

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

    while (get_command(command, sizeof(command)) != NULL) {
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
            fprintf(stderr, "Unknown command received on stdin. Ignored\n");
        }
    }

    /* The stream is closed.. do a nice shutdown */
    if (chack.pointer != NULL) {
        fprintf(stderr, "EOF on stdin. Initiating shutdown\n");
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
