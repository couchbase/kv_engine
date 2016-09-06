#include "parent_monitor.h"

#include "memcached.h"
#include <platform/strerror.h>

ParentMonitor::ParentMonitor(int parent_id) {
    active = true;
#ifdef WIN32
    parent = OpenProcess(SYNCHRONIZE, FALSE, parent_id);
    if (parent == INVALID_HANDLE_VALUE) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to open parent process: %s",
                    cb_strerror(GetLastError()).c_str());
    }
#else
    parent = parent_id;
#endif
    if (cb_create_named_thread(&thread,
                               ParentMonitor::thread_main,
                               this, 0, "mc:parent_mon") != 0) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to open parent process: %s",
                    cb_strerror(GetLastError()).c_str());
    }
}

ParentMonitor::~ParentMonitor() {
    active = false;
    cb_join_thread(thread);
}

void ParentMonitor::thread_main(void* arg) {
    auto* monitor = reinterpret_cast<ParentMonitor*>(arg);

    while (true) {
        // Wait for either the shutdown condvar to be notified, or for
        // 1s. If we hit the timeout then time to check the parent.
        std::unique_lock<std::mutex> lock(monitor->mutex);
        if (monitor->shutdown_cv.wait_for(
                lock,
                std::chrono::seconds(1),
                [monitor]{return !monitor->active;})) {
            // No longer monitoring - exit thread.
            return;
        } else {
            // Check our parent.
#ifdef WIN32
            if (WAIT_TIMEOUT != WaitForSingleObject(monitor->parent,
                                                    0)) {
                ExitProcess(EXIT_FAILURE);
            }
#else
            if (kill(monitor->parent, 0) == -1 && errno == ESRCH) {
                _exit(1);
            }
#endif
        }
    }
}
