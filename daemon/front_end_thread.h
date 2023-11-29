/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "auditd/src/audit_event_filter.h"
#include "connection.h"
#include <folly/Synchronized.h>
#include <folly/io/async/EventBase.h>
#include <platform/sized_buffer.h>
#include <platform/socket.h>
#include <subdoc/operations.h>
#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

namespace cb::json {
class SyntaxValidator;
}

class Cookie;
class Connection;
class ListeningPort;
struct thread_stats;

/// For each unique IP address (we differentiate between IPv4 and IPv6)
/// we maintain a few counters to determine the overall client connection
/// details:
struct ClientConnectionDetails {
    /// The current number of connections
    size_t current_connections = 0;
    /// The total number of connections from this client
    size_t total_connections = 0;
    /// The number of times the server forced shutdown of a connection
    /// from the client
    size_t forced_disconnect = 0;
    /// Timestamp when the entry was last used
    std::chrono::steady_clock::time_point last_used;

    /// Update all of the members required when a connection gets created
    /// (current, total and timestamp)
    void onConnect();

    /// Update all of the members required when a connection gets disconnected
    /// (current and timestamp)
    void onDisconnect();

    /// Update all of the members required when a forced disconnect on a
    /// connection gets initiated (forced_disconnect and timestamp)
    void onForcedDisconnect();

    /// Get a JSON representation of this object
    /// @param now the current timestamp (to avoid having to fetch the clock
    ///            within the method in the case we want to dump hundreds
    ///            of these)
    nlohmann::json to_json(std::chrono::steady_clock::time_point now) const;
};

struct FrontEndThread {
    FrontEndThread();
    ~FrontEndThread();

    /// unique ID of this thread
    std::thread thread;

    /// The event base used by this thread
    folly::EventBase eventBase;

    /**
     * Dispatches a new connection to the worker thread by using round
     * robin.
     *
     * @param sfd the socket to use
     * @param descr The description of the port it is listening to
     */
    static void dispatch(SOCKET sfd, std::shared_ptr<ListeningPort> descr);

    /// Mutex to lock protect access to this object.
    std::mutex mutex;

    /// index of this thread in the threads array
    size_t index = 0;

    /**
     * Shared sub-document operation for all connections serviced by this
     * thread
     */
    Subdoc::Operation subdoc_op;

    /// Check to see if the data in view is valid JSON and update
    /// the bucket histogram (and cookie trace scope) with time spent
    /// for JSON validation
    bool isValidJson(Cookie& cookie, std::string_view view) const;

    /// Use the JSON SyntaxValidator to validate the XATTR blob
    bool isXattrBlobValid(std::string_view view);

    /// Is the thread running or not
    std::atomic_bool running{false};

    /// A temporary buffer the connections may utilize (never expect anything
    /// about the content of the buffer (expect it to be overwritten when your
    /// method returns) (It is currently big enough to keep a protocol
    /// header, frame extras, extras and key.. make sure that you don't
    /// change that or things will break)
    std::array<char, 2048> scratch_buffer;

    cb::char_buffer getScratchBuffer() const {
        return cb::char_buffer{const_cast<char*>(scratch_buffer.data()),
                               scratch_buffer.size()};
    }

    /// Notify the thread that a new connection was created
    void onConnectionCreate(Connection& connection);
    /// Notify the thread that a connection will be destroyed
    void onConnectionDestroy(const Connection& connection);
    /// Notify the thread that a connection will be disconnected
    void onConnectionForcedDisconnect(const Connection& connection);
    /// Notify the thread that the connection was used
    void onConnectionUse(Connection& connection);

    /// Get the (aggregated from all threads) map of client connection details
    static std::unordered_map<std::string, ClientConnectionDetails>
    getClientConnectionDetails();

    /// We have a bug where we can end up in a hang situation during shutdown
    /// and stuck in a tight loop logging (and flooding) the log files.
    /// While trying to solve that bug let's reduce the amount being logged
    /// so that we only log every 5 second (so that we can find the root cause
    /// of the problem)
    time_t shutdown_next_log = 0;

    /**
     * Iterate over all of the front end threads and run the callback
     *
     * @param callback The callback method to call in the thread context
     * @param wait set to true if the calling method should block and wait
     *             for the execution to complete
     */
    static void forEach(std::function<void(FrontEndThread&)> callback,
                        bool wait = false);

    /**
     * Register the DCP connection bound to this front end thread to
     * the list of DCP connections to notify as part of each tick (note
     * only connections subjected to throttling will be recorded)
     *
     * @param connection The connection (must be bound to this thread)
     */
    void maybeRegisterThrottleableDcpConnection(Connection& connection);

    /**
     * Remove the DCP connection from the list of DCP connections to notify
     * as part of throttling ticks.
     *
     * @param connection The connection to remove
     */
    void removeThrottleableDcpConnection(Connection& connection);

    /**
     * Iterate over all registered DCP connections and call the provided
     * callback
     *
     * @param callback The callback to call for each connection
     */
    void iterateThrottleableDcpConnections(
            std::function<void(Connection&)> callback);

    /// Get the thread local audit event filter to use by this thread
    /// (or nullptr if there was an error creating an event filter)
    AuditEventFilter* getAuditEventFilter();

    /// Destroy (delete) the connection
    void destroy_connection(Connection& c);

    /**
     * Signal all of the idle clients bound to the specified front
     * end thread
     *
     * @return The number of clients connections bound to this thread
     */
    int signal_idle_clients(bool dumpConnection);

    /**
     * Iterate over all of the connections and call the callback function
     * for each of the connections.
     *
     * @param callback the callback function to be called for each of the
     *                 connections
     */
    void iterate_connections(std::function<void(Connection&)> callback);

protected:
    void dispatch_new_connections();

    /**
     * The dispatcher accepts new clients and needs to dispatch them
     * to the worker threads. In order to do so we use the ConnectionQueue
     * where the dispatcher allocates the items and push on to the queue,
     * and the actual worker thread pop's the items off and start
     * serving them.
     */
    class ConnectionQueue {
    public:
        struct Entry {
            Entry(SOCKET sock, std::shared_ptr<ListeningPort> descr)
                : sock(sock), descr(std::move(descr)) {
            }
            SOCKET sock;
            std::shared_ptr<ListeningPort> descr;
        };
        ~ConnectionQueue();
        void push(SOCKET sock, std::shared_ptr<ListeningPort> descr);
        void swap(std::vector<Entry>& other);

    protected:
        folly::Synchronized<std::vector<Entry>, std::mutex> connections;
    } new_conn_queue;

    /// Shared validator used by all connections serviced by this thread
    /// when they need to validate a JSON document
    std::unique_ptr<cb::json::SyntaxValidator> validator;

    /// A list of all DCP connections bound this thread
    std::deque<std::reference_wrapper<Connection>> dcp_connections;

    /// The audit event filter used by this thread
    std::unique_ptr<AuditEventFilter> auditEventFilter;

    /// All connections bound to this connection
    std::unordered_map<Connection*, std::unique_ptr<Connection>> connections;

    /// A per-thread map containing the connection details for connections bound
    /// to this thread (to avoid locking for updating the map as it'll get
    /// more updates than reads)
    std::unordered_map<std::string, ClientConnectionDetails>
            clientConnectionMap;

    /// The maximum number of client ip addresses we should keep track of.
    /// When we hit the max number of IP addresses one of two things may
    /// happen:
    ///    a) If we find an entry with 0 current connections that entry
    ///       gets evicted and the new IP address get inserted
    ///    b) If we fail to find any entries with 0 current connections
    ///       no information get recorded for the client
    ///
    /// The reason for this logic is:
    ///    1) We don't want the size of the map to be able to grow to
    ///       an infinite size
    ///    2) We cannot evict an item with current items as we'll run
    ///       into problems with keeping the counters correct.
    ///
    ///  Given there is an overhead of locating the item to evict you should
    ///  tune this number to match your expected use-case when enabled.
    bool maybeTrimClientConnectionMap();

    /**
     * All of the connections pinned to a thread is stored within a LRU list
     * which gets updated (by the connection) in the command processing
     * code. This makes it easy to pick out an old connection when we need
     * to shut down connections without having to traverse "all" connections
     * to find a victim. Given that we spread out the connections to the threads
     * we can might as well just use a thread-specific LRU and pick a victim
     * there instead of using a shared structure and locks.
     *
     * List is ordered from least to most recently used (head = least,
     * tail = most).
     */
    boost::intrusive::list<Connection> connectionLruList;
};

class Hdr1sfMicroSecHistogram;

extern std::vector<Hdr1sfMicroSecHistogram> scheduler_info;
