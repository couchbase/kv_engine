/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "ssl_utils.h"
#include <JSON_checker.h>
#include <event.h>
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

class Cookie;
class Connection;
class ListeningPort;
struct thread_stats;

struct FrontEndThread {
    /**
     * Pending IO requests for this thread. Maps each pending Connection to
     * the IO status to be notified.
     */
    using PendingIoMap = std::unordered_map<
            Connection*,
            std::vector<std::pair<Cookie*, cb::engine_errc>>>;

    /**
     * Destructor.
     *
     * Close the notification pipe (if open)
     */
    ~FrontEndThread();

    /// unique ID of this thread
    cb_thread_t thread_id = {};

    /// The event base used by this thread
    folly::EventBase eventBase;

    /// listen event for notify pipe
    struct event notify_event = {};

    /**
     * notification pipe.
     *
     * The various worker threads are listening on index 0,
     * and in order to notify the thread other threads will
     * write data to index 1.
     */
    std::array<SOCKET, 2> notify = {{INVALID_SOCKET, INVALID_SOCKET}};

    /**
     * Dispatches a new connection to the worker thread by using round
     * robin.
     *
     * @param sfd the socket to use
     * @param system is this a system connection or not
     * @param port the port number the socket is bound to
     * @param ssl the OpenSSL SSL structure to use (if this is a connection
     *            using SSL)
     */
    static void dispatch(SOCKET sfd,
                         bool system,
                         in_port_t port,
                         uniqueSslPtr ssl);

    /// Mutex to lock protect access to this object.
    std::mutex mutex;

    /// Set of connections with pending async io ops.
    struct {
        std::mutex mutex;
        PendingIoMap map;
    } pending_io;

    /// A list of connections to signal if they're idle
    class NotificationList {
    public:
        bool push(Connection* c);
        void remove(Connection* c);
        void swap(std::vector<Connection*>& other);

    protected:
        std::mutex mutex;
        std::vector<Connection*> connections;
    } notification;

    /// index of this thread in the threads array
    size_t index = 0;

    /**
     * Shared sub-document operation for all connections serviced by this
     * thread
     */
    Subdoc::Operation subdoc_op;

    /**
     * Shared validator used by all connections serviced by this thread
     * when they need to validate a JSON document
     */
    JSON_checker::Validator validator;

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

    /// We have a bug where we can end up in a hang situation during shutdown
    /// and stuck in a tight loop logging (and flooding) the log files.
    /// While trying to solve that bug let's reduce the amount being logged
    /// so that we only log every 5 second (so that we can find the root cause
    /// of the problem)
    time_t shutdown_next_log = 0;

    static void libevent_callback(evutil_socket_t fd, short, void* arg);

protected:
    void libevent_callback();
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
            Entry(SOCKET sock, bool system, in_port_t port, uniqueSslPtr ssl)
                : sock(sock), system(system), port(port), ssl(std::move(ssl)) {
            }
            SOCKET sock;
            bool system;
            in_port_t port;
            uniqueSslPtr ssl;
        };
        ~ConnectionQueue();
        void push(SOCKET sock, bool system, in_port_t port, uniqueSslPtr ssl);
        void swap(std::vector<Entry>& other);

    protected:
        folly::Synchronized<std::vector<Entry>, std::mutex> connections;
    } new_conn_queue;
};

void notify_thread(FrontEndThread& thread);
void drain_notification_channel(evutil_socket_t fd);
