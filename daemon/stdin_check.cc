/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <cstdio>
#include <cstring>

#ifndef WIN32
#include <sys/poll.h>
#include <cerrno>
#endif

#include <memcached/engine.h>
#include <platform/platform_thread.h>

#include "memcached.h"

std::function<void()> exit_function;

static char* get_command(char* buffer, size_t buffsize) {
#ifdef WIN32
    if (fgets(buffer, gsl::narrow<int>(buffsize), stdin) == NULL) {
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
            if (fgets(buffer, buffsize, stdin) == nullptr) {
                return nullptr;
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
static void check_stdin_thread(void* arg) {
    char command[80];

    bool call_exit_handler = true;

    while (get_command(command, sizeof(command)) != nullptr) {
        /* Handle the command */
        if (strcmp(command, "die!\n") == 0) {
            fprintf(stderr, "'die!' on stdin.  Exiting super-quickly\n");
            fflush(stderr);
            _exit(0);
        } else if (strcmp(command, "shutdown\n") == 0) {
            if (call_exit_handler) {
                fprintf(stderr, "EOL on stdin.  Initiating shutdown\n");
                exit_function();
                call_exit_handler = false;
            }
        } else {
            fprintf(stderr, "Unknown command received on stdin. Ignored\n");
        }
    }

    /* The stream is closed.. do a nice shutdown */
    if (call_exit_handler) {
        fprintf(stderr, "EOF on stdin. Initiating shutdown\n");
        exit_function();
    }
}

void start_stdin_listener(std::function<void()> function) {
    exit_function = function;
    cb_thread_t t;

    if (cb_create_named_thread(
                &t, check_stdin_thread, nullptr, 1, "mc:check_stdin") != 0) {
        throw std::system_error(errno,
                                std::system_category(),
                                "couldn't create stdin checking thread.");
    }
}
