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
#include "ssl_utils.h"
#include <folly/Synchronized.h>
#include <folly/io/async/EventBase.h>
#include <memcached/engine_error.h>
#include <platform/platform_thread.h>
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
    bool isValidJson(Cookie& cookie, std::string_view view);

    bool isValidJson(Cookie& cookie, cb::const_byte_buffer view) {
        return isValidJson(
                cookie,
                {reinterpret_cast<const char*>(view.data()), view.size()});
    }

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

    /**
     * Check to see if the provided event should be filtered out for the
     * provided user.
     *
     * @param id The event to check
     * @param user The user to check
     * @return true if the event should be dropped, false if it should be
     *              submitted to the audit daemon.
     */
    bool is_audit_event_filtered_out(uint32_t id,
                                     const cb::rbac::UserIdent& user);

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
};

class Hdr1sfMicroSecHistogram;

extern std::vector<Hdr1sfMicroSecHistogram> scheduler_info;
