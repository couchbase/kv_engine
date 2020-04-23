/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "clustertest.h"

#include <event2/thread.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/platform_socket.h>
#include <array>
#include <csignal>
#include <cstdlib>
#include <string>

int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    cb_initialize_sockets();

#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    const auto failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    const auto failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    if (failed) {
        std::cerr << "Failed to enable libevent locking. Terminating program"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    static std::array<char, 80> envvar;
    snprintf(envvar.data(), envvar.size(), "MEMCACHED_UNIT_TESTS=true");
    putenv(envvar.data());

    const auto isasl_file_name = cb::io::sanitizePath(
            SOURCE_ROOT "/tests/testapp_cluster/cbsaslpw.json");

    // Add the file to the exec environment
    static std::array<char, 1024> isasl_env_var;
    snprintf(isasl_env_var.data(),
             isasl_env_var.size(),
             "CBSASL_PWFILE=%s",
             isasl_file_name.c_str());
    putenv(isasl_env_var.data());

#ifndef WIN32
    if (sigignore(SIGPIPE) == -1) {
        std::cerr << "Fatal: failed to ignore SIGPIPE; sigaction" << std::endl;
        return 1;
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
