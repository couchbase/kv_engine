/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
/*
 * Thread management for memcached.
 */
#include "connection.h"
#include "connections.h"
#include "cookie.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "log_macros.h"
#include "memcached.h"
#include "settings.h"
#include "stats.h"
#include "tracing.h"
#include <hdrhistogram/hdrhistogram.h>

#include <openssl/conf.h>
#include <phosphor/phosphor.h>
#include <platform/socket.h>
#include <platform/strerror.h>

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif
#include <mutex>
#include <queue>

/* An item in the connection queue. */
FrontEndThread::ConnectionQueue::~ConnectionQueue() {
    connections.withLock([](auto& c) {
        for (auto& entry : c) {
            safe_close(entry.sock);
        }
    });
}

void FrontEndThread::ConnectionQueue::push(SOCKET sock,
                                           std::shared_ptr<ListeningPort> descr,
                                           uniqueSslPtr ssl) {
    connections.lock()->emplace_back(
            Entry{sock, std::move(descr), std::move(ssl)});
}

void FrontEndThread::ConnectionQueue::swap(std::vector<Entry>& other) {
    connections.lock()->swap(other);
}

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static std::vector<FrontEndThread> threads;
std::vector<Hdr1sfMicroSecHistogram> scheduler_info;

/*
 * Number of worker threads that have finished setting themselves up.
 */
static size_t init_count = 0;
static std::mutex init_mutex;
static std::condition_variable init_cond;

/*
 * Creates a worker thread.
 */
static void create_worker(void (*func)(void*),
                          void* arg,
                          cb_thread_t* id,
                          const std::string& name) {
    if (cb_create_named_thread(id, func, arg, 0, name.c_str()) != 0) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Can't create thread {}: {}",
                    name,
                    cb_strerror());
    }
}

/****************************** LIBEVENT THREADS *****************************/

void iterate_all_connections(std::function<void(Connection&)> callback) {
    for (auto& thr : threads) {
        thr.eventBase.runInEventBaseThreadAndWait([&callback, &thr]() {
            TRACE_LOCKGUARD_TIMED(thr.mutex,
                                  "mutex",
                                  "iterate_all_connections::threadLock",
                                  SlowMutexThreshold);
            iterate_thread_connections(&thr, callback);
        });
    }
}

/// Worker thread: main event loop
static void worker_libevent(void *arg) {
    auto& me = *reinterpret_cast<FrontEndThread*>(arg);

    // Any per-thread setup can happen here; thread_init() will block until
    // all threads have finished initializing.
    {
        std::lock_guard<std::mutex> guard(init_mutex);
        me.running = true;
        init_count++;
        init_cond.notify_all();
    }

    me.eventBase.loopForever();
    me.running = false;
}

void FrontEndThread::dispatch_new_connections() {
    std::vector<ConnectionQueue::Entry> connections;
    new_conn_queue.swap(connections);

    for (auto& entry : connections) {
        const bool system = entry.descr->system;
        if (conn_new(entry.sock,
                     *this,
                     std::move(entry.descr),
                     std::move(entry.ssl)) == nullptr) {
            if (system) {
                --stats.system_conns;
            }
            safe_close(entry.sock);
        }
    }
}

void notifyIoComplete(Cookie& cookie, cb::engine_errc status) {
    auto& thr = cookie.getConnection().getThread();
    thr.eventBase.runInEventBaseThread([&cookie, status]() {
        TRACE_LOCKGUARD_TIMED(cookie.getConnection().getThread().mutex,
                              "mutex",
                              "notifyIoComplete",
                              SlowMutexThreshold);
        cookie.getConnection().processNotifiedCookie(cookie, status);
    });
}

void scheduleDcpStep(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    if (!connection.isDCP()) {
        LOG_ERROR(
                "scheduleDcpStep: Must only be called with a DCP "
                "connection: {}",
                connection.toJSON().dump());
        throw std::logic_error(
                "scheduleDcpStep(): Provided cookie is not bound to a "
                "connection set up for DCP");
    }

    if (cookie.getRefcount() == 0) {
        LOG_ERROR(
                "scheduleDcpStep: DCP connection did not reserve the "
                "cookie: {}",
                cookie.getConnection().toJSON().dump());
        throw std::logic_error("scheduleDcpStep: cookie must be reserved!");
    }

    connection.getThread().eventBase.runInEventBaseThread([&connection]() {
        TRACE_LOCKGUARD_TIMED(connection.getThread().mutex,
                              "mutex",
                              "scheduleDcpStep",
                              SlowMutexThreshold);
        connection.triggerCallback();
    });
}

void FrontEndThread::dispatch(SOCKET sfd,
                              std::shared_ptr<ListeningPort> descr,
                              uniqueSslPtr ssl) {
    // Which thread we assigned a connection to most recently.
    static std::atomic<size_t> last_thread = 0;
    size_t tid = (last_thread + 1) % Settings::instance().getNumWorkerThreads();
    auto& thread = threads[tid];
    last_thread = tid;

    try {
        thread.new_conn_queue.push(sfd, std::move(descr), move(ssl));
        thread.eventBase.runInEventBaseThread([&thread]() {
            if (is_memcached_shutting_down()) {
                if (signal_idle_clients(thread, false) == 0) {
                    LOG_INFO("Stopping worker thread {}", thread.index);
                    thread.eventBase.terminateLoopSoon();
                    return;
                }
            }
            thread.dispatch_new_connections();
        });
    } catch (const std::bad_alloc& e) {
        LOG_WARNING("dispatch_conn_new: Failed to dispatch new connection: {}",
                    e.what());

        if (descr->system) {
            --stats.system_conns;
        }
        safe_close(sfd);
    }
}

/******************************* GLOBAL STATS ******************************/

void threadlocal_stats_reset(std::vector<thread_stats>& thread_stats) {
    for (auto& ii : thread_stats) {
        ii.reset();
    }
}

void worker_threads_init() {
    const auto nthr = Settings::instance().getNumWorkerThreads();

    scheduler_info.resize(nthr);

    try {
        threads = std::vector<FrontEndThread>(nthr);
    } catch (const std::bad_alloc&) {
        FATAL_ERROR(EXIT_FAILURE, "Can't allocate thread descriptors");
    }

    for (size_t ii = 0; ii < nthr; ii++) {
        threads[ii].index = ii;
    }

    /* Create threads after we've done all the libevent setup. */
    for (auto& thread : threads) {
        create_worker(worker_libevent,
                      &thread,
                      &thread.thread_id,
                      fmt::format("mc:worker_{:02d}", uint64_t(thread.index)));
    }

    // Wait for all the threads to set themselves up before returning.
    std::unique_lock<std::mutex> lock(init_mutex);
    init_cond.wait(lock, [&nthr] { return !(init_count < nthr); });
}

void threads_shutdown() {
    // Notify all of the threads and let them shut down
    for (auto& thread : threads) {
        thread.eventBase.runInEventBaseThread([&thread]() {
            if (signal_idle_clients(thread, false) == 0) {
                LOG_INFO("Stopping worker thread {}", thread.index);
                thread.eventBase.terminateLoopSoon();
                return;
            }
        });
    }

    // Wait for all of them to complete
    for (auto& thread : threads) {
        // When using bufferevents we need to run a few iterations here.
        // Calling signalIfIdle won't run the event immediately, but when
        // the control goes back to libevent. That means that some of the
        // connections could be "stuck" for another round in the event loop.
        while (thread.running) {
            thread.eventBase.runInEventBaseThread([&thread]() {
                if (signal_idle_clients(thread, false) == 0) {
                    LOG_INFO("Stopping worker thread {}", thread.index);
                    thread.eventBase.terminateLoopSoon();
                    return;
                }
            });
            std::this_thread::sleep_for(std::chrono::microseconds(250));
        }
        cb_join_thread(thread.thread_id);
    }
    threads.clear();
}

FrontEndThread::~FrontEndThread() = default;
