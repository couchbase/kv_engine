/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stdin_check.h"
#include <fmt/format.h>
#include <platform/platform_thread.h>
#include <cstdio>

#ifndef WIN32
#include <sys/poll.h>
#include <unistd.h>
#include <cerrno>
#endif

#include <array>
#include <atomic>
#include <functional>
#include <string_view>

#define PRINT_ERROR(msg, ...)                     \
    do {                                          \
        try {                                     \
            fmt::print(stderr, msg, __VA_ARGS__); \
        } catch (const std::exception&) {         \
        }                                         \
    } while (false)

#define PRINT_ERROR_RAW(msg)              \
    do {                                  \
        try {                             \
            fmt::print(stderr, msg);      \
        } catch (const std::exception&) { \
        }                                 \
    } while (false)

std::function<void()> exit_function;
static constexpr int command_buffer_size = 80;
std::atomic<std::chrono::milliseconds> max_abnormal_shutdown_duration;

static char* get_command(char* buffer) {
#ifdef WIN32
    if (fgets(buffer, command_buffer_size, stdin) == NULL) {
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
            if (fgets(buffer, command_buffer_size, stdin) == nullptr) {
                return nullptr;
            }
            return buffer;
        case 0:
            break;
        default:
            PRINT_ERROR("ERROR: Failed to run poll() on standard input {}\n",
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
static void check_stdin_thread() {
    using namespace std::string_view_literals;
    std::array<char, command_buffer_size> command;

    bool call_exit_handler = true;

    while (get_command(command.data()) != nullptr) {
        std::string_view cmd(command.data(), strlen(command.data()));
        /* Handle the command */
        if (cmd.starts_with("die!")) {
            PRINT_ERROR_RAW("'die!' on stdin. Exiting super-quickly\n");
            fflush(stderr);
            std::_Exit(0);
        }
        if (cmd.starts_with("shutdown")) {
            if (call_exit_handler) {
                PRINT_ERROR_RAW("EOL on stdin. Initiating shutdown\n");
                exit_function();
                call_exit_handler = false;
            }
        } else if (cmd.starts_with("get_abnormal_timeout")) {
            PRINT_ERROR("{}ms\n",
                        max_abnormal_shutdown_duration.load().count());
        } else {
            PRINT_ERROR_RAW("Unknown command received on stdin. Ignored\n");
        }
    }

    /* The stream is closed.. do a nice shutdown */
    if (call_exit_handler) {
        PRINT_ERROR_RAW("EOF on stdin. Initiating shutdown\n");
        exit_function();
    }

    auto timeout =
            max_abnormal_shutdown_duration.load(std::memory_order_acquire);
    if (timeout != std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(timeout);
        PRINT_ERROR("Shutdown timed out! Exit({})\n",
                    abnormal_exit_handler_exit_code);
        std::_Exit(abnormal_exit_handler_exit_code);
    }
}

void start_stdin_listener(std::function<void()> function) {
    // Disable buffering on standard input (otherwise we may not detec
    // a rapid "shutdown\ndie!\n" sequence as the second one is in the
    // buffer and poll won't return until new data arrives)
    setbuf(stdin, nullptr);
    exit_function = std::move(function);
    auto thr = create_thread([]() { check_stdin_thread(); }, "mc:check_stdin");
    thr.detach();
}

void abrupt_shutdown_timeout_changed(std::chrono::milliseconds timeout) {
    max_abnormal_shutdown_duration.store(timeout, std::memory_order_release);
}
