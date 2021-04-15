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
#include "log_macros.h"
#include "memcached.h"
#include "settings.h"
#include "stats.h"
#include "tracing.h"
#include <utilities/hdrhistogram.h>

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
                                           bool system,
                                           in_port_t port,
                                           uniqueSslPtr ssl) {
    connections.lock()->emplace_back(Entry{sock, system, port, std::move(ssl)});
}

void FrontEndThread::ConnectionQueue::swap(std::vector<Entry>& other) {
    connections.lock()->swap(other);
}

bool FrontEndThread::NotificationList::push(Connection* c) {
    std::lock_guard<std::mutex> lock(mutex);
    auto iter = std::find(connections.begin(), connections.end(), c);
    if (iter == connections.end()) {
        try {
            connections.push_back(c);
            return true;
        } catch (const std::bad_alloc&) {
            // Just ignore and hopefully we'll be able to signal it at a later
            // time.
            try {
                LOG_WARNING(
                        "FrontEndThread::NotificationList::push(): Failed to "
                        "enqueue {}",
                        c->getId());
            } catch (const std::bad_alloc&) {
            }
        }
    }
    return false;
}

void FrontEndThread::NotificationList::remove(Connection* c) {
    std::lock_guard<std::mutex> lock(mutex);
    auto iter = std::find(connections.begin(), connections.end(), c);
    if (iter != connections.end()) {
        connections.erase(iter);
    }
}

void FrontEndThread::NotificationList::swap(std::vector<Connection*>& other) {
    std::lock_guard<std::mutex> lock(mutex);
    connections.swap(other);
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
        TRACE_LOCKGUARD_TIMED(thr.mutex,
                              "mutex",
                              "iterate_all_connections::threadLock",
                              SlowMutexThreshold);
        iterate_thread_connections(&thr, callback);
    }
}

bool create_nonblocking_socketpair(std::array<SOCKET, 2>& sockets) {
    if (cb::net::socketpair(SOCKETPAIR_AF,
                            SOCK_STREAM,
                            0,
                            reinterpret_cast<SOCKET*>(sockets.data())) ==
        SOCKET_ERROR) {
        LOG_WARNING("Failed to create socketpair: {}",
                    cb_strerror(cb::net::get_socket_error()));
        return false;
    }

    for (auto sock : sockets) {
        int flags = 1;
        const auto* flag_ptr = reinterpret_cast<const void*>(&flags);
        cb::net::setsockopt(
                sock, IPPROTO_TCP, TCP_NODELAY, flag_ptr, sizeof(flags));
        cb::net::setsockopt(
                sock, SOL_SOCKET, SO_REUSEADDR, flag_ptr, sizeof(flags));

        if (evutil_make_socket_nonblocking(sock) == -1) {
            LOG_WARNING("Failed to make socket non-blocking: {}",
                        cb_strerror(cb::net::get_socket_error()));
            safe_close(sockets[0]);
            safe_close(sockets[1]);
            sockets[0] = sockets[1] = INVALID_SOCKET;
            return false;
        }
    }
    return true;
}

/*
 * Set up a thread's information.
 */
static void setup_thread(FrontEndThread& me) {
    /* Listen for notifications from other threads */
    if ((event_assign(&me.notify_event,
                      me.eventBase.getLibeventBase(),
                      me.notify[0],
                      EV_READ | EV_PERSIST,
                      FrontEndThread::libevent_callback,
                      &me) == -1) ||
        (event_add(&me.notify_event, nullptr) == -1)) {
        FATAL_ERROR(EXIT_FAILURE, "Can't monitor libevent notify pipe");
    }
}

/*
 * Worker thread: main event loop
 */
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

void drain_notification_channel(evutil_socket_t fd) {
    /* Every time we want to notify a thread, we send 1 byte to its
     * notification pipe. When the thread wakes up, it tries to drain
     * it's notification channel before executing any other events.
     * Other threads (listener and other background threads) may notify
     * this thread up to 512 times since the last time we checked the
     * notification pipe, before we'll start draining the it again.
     */

    ssize_t nread;
    // Using a small size for devnull will avoid blowing up the stack
    char devnull[512];

    while ((nread = cb::net::recv(fd, devnull, sizeof(devnull), 0)) ==
           (int)sizeof(devnull)) {
        /* empty */
    }

    if (nread == -1) {
        LOG_WARNING("Can't read from libevent pipe: {}",
                    cb_strerror(cb::net::get_socket_error()));
    }
}

void FrontEndThread::dispatch_new_connections() {
    std::vector<ConnectionQueue::Entry> connections;
    new_conn_queue.swap(connections);

    for (auto& entry : connections) {
        if (conn_new(entry.sock,
                     *this,
                     entry.system,
                     entry.port,
                     std::move(entry.ssl)) == nullptr) {
            if (entry.system) {
                --stats.system_conns;
            }
            safe_close(entry.sock);
        }
    }
}

/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
void FrontEndThread::libevent_callback(evutil_socket_t fd, short, void* arg) {
    // Start by draining the notification channel before doing any work.
    // By doing so we know that we'll be notified again if someone
    // tries to notify us while we're doing the work below (so we don't have
    // to care about race conditions for stuff people try to notify us
    // about.
    drain_notification_channel(fd);

    auto& me = *reinterpret_cast<FrontEndThread*>(arg);
    me.libevent_callback();
}

void FrontEndThread::libevent_callback() {
    if (is_memcached_shutting_down()) {
        if (signal_idle_clients(*this, false) == 0) {
            LOG_INFO("Stopping worker thread {}", index);
            eventBase.terminateLoopSoon();
            return;
        }
    }

    dispatch_new_connections();

    FrontEndThread::PendingIoMap pending;
    {
        std::lock_guard<std::mutex> lock(pending_io.mutex);
        pending_io.map.swap(pending);
    }

    TRACE_LOCKGUARD_TIMED(mutex,
                          "mutex",
                          "thread_libevent_process::threadLock",
                          SlowMutexThreshold);

    std::vector<Connection*> toNotify;
    notification.swap(toNotify);

    for (auto& io : pending) {
        auto* c = io.first;

        for (const auto& pair : io.second) {
            if (pair.first) {
                pair.first->setAiostat(pair.second);
                pair.first->setEwouldblock(false);
            }
        }

        // Remove from the notify list if it's there as we don't
        // want to run them twice
        {
            auto iter = std::find(toNotify.begin(), toNotify.end(), c);
            if (iter != toNotify.end()) {
                toNotify.erase(iter);
            }
        }

        c->triggerCallback();
    }

    // Notify the connections we haven't notified yet
    for (auto& c : toNotify) {
        c->triggerCallback();
    }

    if (is_memcached_shutting_down()) {
        // Someone requested memcached to shut down. If we don't have
        // any connections bound to this thread we can just shut down
        time_t now = time(nullptr);
        bool log = now > shutdown_next_log;
        if (log) {
            shutdown_next_log = now + 5;
        }

        auto connected = signal_idle_clients(*this, log);
        if (connected == 0) {
            LOG_INFO("Stopping worker thread {}", index);
            eventBase.terminateLoopSoon();
        } else if (log) {
            LOG_INFO("Waiting for {} connected clients on worker thread {}",
                     connected,
                     index);
        }
    }
}

void notifyIoComplete(Cookie& cookie, cb::engine_errc status) {
    auto& thr = cookie.getConnection().getThread();
    LOG_DEBUG("notifyIoComplete: Got notify from {}, status {}",
              cookie.getConnection().getId(),
              status);

    /* kick the thread in the butt */
    if (add_conn_to_pending_io_list(cookie.getConnection(), cookie, status)) {
        notify_thread(thr);
    }
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
                              bool system,
                              in_port_t port,
                              uniqueSslPtr ssl) {
    // Which thread we assigned a connection to most recently.
    static std::atomic<size_t> last_thread = 0;
    size_t tid = (last_thread + 1) % Settings::instance().getNumWorkerThreads();
    auto& thread = threads[tid];
    last_thread = tid;

    try {
        thread.new_conn_queue.push(sfd, system, port, move(ssl));
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

        if (system) {
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
        if (!create_nonblocking_socketpair(threads[ii].notify)) {
            FATAL_ERROR(EXIT_FAILURE, "Cannot create notification pipe");
        }
        threads[ii].index = ii;

        setup_thread(threads[ii]);
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
        notify_thread(thread);
    }

    // Wait for all of them to complete
    for (auto& thread : threads) {
        // When using bufferevents we need to run a few iterations here.
        // Calling signalIfIdle won't run the event immediately, but when
        // the control goes back to libevent. That means that some of the
        // connections could be "stuck" for another round in the event loop.
        while (thread.running) {
            notify_thread(thread);
            std::this_thread::sleep_for(std::chrono::microseconds(250));
        }
        cb_join_thread(thread.thread_id);
    }
    threads.clear();
}

FrontEndThread::~FrontEndThread() {
    for (auto& sock : notify) {
        if (sock != INVALID_SOCKET) {
            safe_close(sock);
        }
    }
}

void notify_thread(FrontEndThread& thread) {
    if (cb::net::send(thread.notify[1], "", 1, 0) != 1 &&
        !cb::net::is_blocking(cb::net::get_socket_error())) {
        LOG_WARNING("Failed to notify thread: {}",
                    cb_strerror(cb::net::get_socket_error()));
    }
}

int add_conn_to_pending_io_list(Connection& c,
                                Cookie& cookie,
                                cb::engine_errc status) {
    auto& thread = c.getThread();

    std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
    auto iter = thread.pending_io.map.find(&c);
    if (iter == thread.pending_io.map.end()) {
        thread.pending_io.map.emplace(
                &c,
                std::vector<std::pair<Cookie*, cb::engine_errc>>{
                        {&cookie, status}});
        return 1;
    }

    for (const auto& pair : iter->second) {
        if (pair.first == &cookie) {
            // we've already got a pending notification for this
            // cookie.. Ignore it
            return 0;
        }
    }
    iter->second.emplace_back(&cookie, status);
    return 1;
}
