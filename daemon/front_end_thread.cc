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
#include "front_end_thread.h"
#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "listening_port.h"
#include "log_macros.h"
#include "mcaudit.h"
#include "memcached.h"
#include "settings.h"
#include "stats.h"
#include "tracing.h"
#include <hdrhistogram/hdrhistogram.h>
#include <json/syntax_validator.h>
#include <memcached/tracer.h>
#include <phosphor/phosphor.h>
#include <platform/histogram.h>
#include <platform/scope_timer.h>
#include <platform/socket.h>
#include <platform/thread.h>
#include <platform/timeutils.h>
#include <xattr/utils.h>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <queue>

void ClientConnectionDetails::onConnect() {
    ++current_connections;
    ++total_connections;
    last_used = std::chrono::steady_clock::now();
}

void ClientConnectionDetails::onDisconnect() {
    --current_connections;
    last_used = std::chrono::steady_clock::now();
}

void ClientConnectionDetails::onForcedDisconnect() {
    ++forced_disconnect;
    last_used = std::chrono::steady_clock::now();
}

nlohmann::json ClientConnectionDetails::to_json(
        std::chrono::steady_clock::time_point now) const {
    const auto duration = now - last_used;
    return nlohmann::json{
            {"current", current_connections},
            {"total", total_connections},
            {"disconnect", forced_disconnect},
            {"last_used",
             cb::time2text(std::chrono::duration_cast<std::chrono::nanoseconds>(
                     duration))}};
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
                          std::thread& thread,
                          std::string name) {
    thread = create_thread([arg, func]() { func(arg); }, std::move(name));
}

void FrontEndThread::maybeRegisterThrottleableDcpConnection(
        Connection& connection) {
    if (!connection.isUnthrottled()) {
        dcp_connections.emplace_back(connection);
    }
}

void FrontEndThread::removeThrottleableDcpConnection(Connection& connection) {
    for (auto iter = dcp_connections.begin(); iter != dcp_connections.end();
         ++iter) {
        if (&iter->get() == &connection) {
            dcp_connections.erase(iter);
            return;
        }
    }
}

void FrontEndThread::iterateThrottleableDcpConnections(
        const std::function<void(Connection&)>& callback) const {
    for (auto& c : dcp_connections) {
        callback(c);
    }
}

void FrontEndThread::forEach(std::function<void(FrontEndThread&)> callback,
                             bool wait) {
    for (auto& thr : threads) {
        if (wait) {
            thr.eventBase.runInEventBaseThreadAndWait([&callback, &thr]() {
                TRACE_LOCKGUARD_TIMED(thr.mutex,
                                      "mutex",
                                      "forEach::threadLock",
                                      SlowMutexThreshold);
                callback(thr);
            });
        } else {
            thr.eventBase.runInEventBaseThread([callback, &thr]() {
                TRACE_LOCKGUARD_TIMED(thr.mutex,
                                      "mutex",
                                      "forEach::threadLock",
                                      SlowMutexThreshold);
                callback(thr);
            });
        }
    }
}

void FrontEndThread::onConnectionCreate(Connection& connection) {
    unauthenticatedConnections.emplace_back(connection);

    const std::string ip = connection.getPeername()["ip"];
    // The common case is updating an existing entry.
    auto iter = clientConnectionMap.find(ip);
    if (iter == clientConnectionMap.end()) {
        if (maybeTrimClientConnectionMap()) {
            // there is room in the map for this entry (insert or update)
            clientConnectionMap[ip].onConnect();
        }
    } else {
        iter->second.onConnect();
    }
}

/// Erase a connection from a list and return true if it was found or false
/// otherwise
static bool eraseConnection(
        std::deque<std::reference_wrapper<Connection>>& list,
        const Connection& connection) {
    for (auto iter = list.begin(); iter != list.end(); ++iter) {
        if (&iter->get() == &connection) {
            list.erase(iter);
            return true;
        }
    }
    return false;
}

void FrontEndThread::onConnectionDestroy(const Connection& connection) {
    {
        auto iter = clientConnectionMap.find(connection.getPeername()["ip"]);
        if (iter != clientConnectionMap.end()) {
            iter->second.onDisconnect();
        }
    }

    eraseConnection(connectionsInitiatedShutdown, connection);

    // unauthenticatedConnections should only contain the connection if
    // it hasn't been authenticated, so we don't need to search the list
    // for authenticated connections.
    if (connection.isAuthenticated()) {
#ifdef CB_DEVELOPMENT_ASSERTS
        // Just to verify that the logic is correct lets search the entire
        // list for the connection and assert that the connection isn't
        // located in the unauthenticatedConnections
        Expects(!eraseConnection(unauthenticatedConnections, connection));
#endif
        return;
    }

    // A connection may currently "restart authentication" if it
    // authenticated via SASL (first authenticate as user1, then
    // authenticate as user2 etc), and as part of receiving the
    // first SASL AUTH package we set the connection to unauthenticated.
    // I'm not sure if any clients actually does this (and I'd like
    // to remove the support for it), so we cannot assert that the connection
    // is present in the list.
    eraseConnection(unauthenticatedConnections, connection);
}

void FrontEndThread::onConnectionForcedDisconnect(
        const Connection& connection) {
    auto iter = clientConnectionMap.find(connection.getPeername()["ip"]);
    if (iter != clientConnectionMap.end()) {
        iter->second.onForcedDisconnect();
    }
}

void FrontEndThread::onConnectionAuthenticated(Connection& connection) {
    eraseConnection(unauthenticatedConnections, connection);
}

void FrontEndThread::onInitiateShutdown(Connection& connection) {
    connectionsInitiatedShutdown.emplace_back(connection);
}

bool FrontEndThread::maybeTrimClientConnectionMap() {
    if (clientConnectionMap.size() <
        Settings::instance().getMaxClientConnectionDetails()) {
        return true;
    }

    for (auto i = clientConnectionMap.begin(), last = clientConnectionMap.end();
         i != last;) {
        if (i->second.current_connections == 0) {
            clientConnectionMap.erase(i);
            return true;
        }
    }

    return false;
}

std::unordered_map<std::string, ClientConnectionDetails>
FrontEndThread::getClientConnectionDetails() {
    std::unordered_map<std::string, ClientConnectionDetails> ret;
    forEach(
            [&ret](auto& thread) {
                for (const auto& [ip, info] : thread.clientConnectionMap) {
                    auto& entry = ret[ip];
                    entry.current_connections += info.current_connections;
                    entry.forced_disconnect += info.forced_disconnect;
                    entry.total_connections += info.total_connections;
                    if (entry.last_used < info.last_used) {
                        entry.last_used = info.last_used;
                    }
                }
            },
            true);
    return ret;
}

/****************************** LIBEVENT THREADS *****************************/

void iterate_all_connections(std::function<void(Connection&)> callback) {
    for (auto& thr : threads) {
        thr.eventBase.runInEventBaseThreadAndWait([&callback, &thr]() {
            TRACE_LOCKGUARD_TIMED(thr.mutex,
                                  "mutex",
                                  "iterate_all_connections::threadLock",
                                  SlowMutexThreshold);
            thr.iterate_connections(callback);
        });
    }
}

/// Worker thread: main event loop
static void worker_libevent(void* arg) {
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

void FrontEndThread::do_dispatch(SOCKET sfd,
                                 std::shared_ptr<ListeningPort> descr) {
    tryDisconnectUnauthenticatedConnections();

    if (!connectionsInitiatedShutdown.empty()) {
        const auto now = std::chrono::steady_clock::now();
        for (const auto& c : connectionsInitiatedShutdown) {
            c.get().reportIfStuckInShutdown(now);
        }
    }

    if (is_memcached_shutting_down()) {
        cb::net::closesocket(sfd);
        return;
    }

    const bool system = descr->system;
    bool success = false;
    try {
        auto connection = Connection::create(sfd, *this, std::move(descr));
        auto* c = connection.get();
        add_connection(std::move(connection));
        stats.total_conns++;
        associate_initial_bucket(*c);
        success = true;
    } catch (const std::bad_alloc&) {
        LOG_WARNING_RAW("Failed to allocate memory for connection");
    } catch (const std::exception& error) {
        LOG_WARNING_CTX("Failed to create connection", {"error", error.what()});
    } catch (...) {
        LOG_WARNING_RAW("Failed to create connection");
    }

    if (!success) {
        if (system) {
            --stats.system_conns;
        }
        close_client_socket(sfd);
    }
}

void FrontEndThread::add_connection(std::unique_ptr<Connection> connection) {
    connections.insert({connection.get(), std::move(connection)});
}

void FrontEndThread::destroy_connection(Connection& connection) {
    auto node = connections.extract(&connection);
    if (node.empty() || node.key() != &connection) {
        throw std::logic_error("destroy_connection: Connection not found");
    }
}

void FrontEndThread::tryDisconnectUnauthenticatedConnections() {
    static constexpr std::chrono::minutes limit{5};
    static const std::string message = fmt::format(
            "Client did not authenticate within {} minutes", limit.count());

    const auto now = std::chrono::steady_clock::now();
    for (auto& conn : unauthenticatedConnections) {
        if (conn.get().getCreationTimestamp() + limit > now) {
            // No need of inspecting the rest of the list (we add at the tail
            // of the list
            return;
        }
        conn.get().maybeInitiateShutdown(message, true);
    }
}

void FrontEndThread::iterate_connections(
        const std::function<void(Connection&)>& callback) const {
    for (const auto& [c, conn] : connections) {
        callback(*c);
    }
}

int FrontEndThread::signal_idle_clients(bool dumpConnection) {
    int connected = 0;
    iterate_connections(
            [this, &connected, dumpConnection](Connection& connection) {
                ++connected;
                if (!connection.signalIfIdle() && dumpConnection) {
                    auto details = connection.to_json().dump();
                    LOG_INFO_CTX("Worker thread",
                                 {"index", index},
                                 {"details", details});
                }
            });
    return connected;
}

void FrontEndThread::dispatch(SOCKET sfd,
                              std::shared_ptr<ListeningPort> descr) {
    // Which thread we assigned a connection to most recently.
    static std::atomic<size_t> last_thread = 0;
    size_t tid = (last_thread + 1) % Settings::instance().getNumWorkerThreads();
    auto& thread = threads[tid];
    last_thread = tid;

    try {
        thread.eventBase.runInEventBaseThread(
                [&thread, sock = sfd, port = std::move(descr)]() {
                    thread.do_dispatch(sock, port);
                });
    } catch (const std::bad_alloc& e) {
        LOG_WARNING_CTX("dispatch_conn_new: Failed to dispatch new connection",
                        {"error", e.what()});

        if (descr->system) {
            --stats.system_conns;
        }
        close_client_socket(sfd);
    }
}

FrontEndThread::FrontEndThread()
    : scratch_buffer(), validator(cb::json::SyntaxValidator::New()) {
}

FrontEndThread::~FrontEndThread() = default;

bool FrontEndThread::isValidJson(Cookie& cookie, std::string_view view) const {
    // Record how long JSON checking takes to both Tracer and bucket-level
    // histogram.
    using namespace cb::tracing;
    ScopeTimer2<HdrMicroSecStopwatch, SpanStopwatch<Code>> timer(
            std::forward_as_tuple(
                    cookie.getConnection().getBucket().jsonValidateTimes),
            std::forward_as_tuple(cookie, Code::JsonValidate));
    return validator->validate(view);
}

bool FrontEndThread::isXattrBlobValid(std::string_view view) {
    return cb::xattr::validate(*validator, view);
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
                      thread.thread,
                      fmt::format("mc:worker_{:02d}", uint64_t(thread.index)));
    }

    // Wait for all the threads to set themselves up before returning.
    std::unique_lock<std::mutex> lock(init_mutex);
    init_cond.wait(lock, [&nthr] { return !(init_count < nthr); });
}

void threads_shutdown() {
    for (auto& thread : threads) {
        LOG_INFO_CTX("Stopping worker thread", {"index", thread.index});
        thread.eventBase.terminateLoopSoon();
        thread.thread.join();
    }

    // Finally let all event base loops drain all pending functions
    // scheduled in its event notification queue (if any)
    for (auto& thread : threads) {
        thread.eventBase.loop();
    }
    LOG_INFO_RAW("All worker threads stopped");
    threads.clear();
    LOG_INFO_RAW("Released worker thread resources");
}

AuditEventFilter* FrontEndThread::getAuditEventFilter() {
    if (!auditEventFilter || !auditEventFilter->isValid()) {
        auditEventFilter = create_audit_event_filter();
    }

    return auditEventFilter.get();
}
