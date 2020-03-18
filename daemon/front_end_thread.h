/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#pragma once

#include <JSON_checker.h>
#include <event.h>
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

using SharedListeningPort = std::shared_ptr<ListeningPort>;

struct FrontEndThread {
    /**
     * Pending IO requests for this thread. Maps each pending Connection to
     * the IO status to be notified.
     */
    using PendingIoMap = std::unordered_map<
            Connection*,
            std::vector<std::pair<Cookie*, ENGINE_ERROR_CODE>>>;

    /**
     * Destructor.
     *
     * Close the notification pipe (if open)
     */
    ~FrontEndThread();

    /// unique ID of this thread
    cb_thread_t thread_id = {};

    /// libevent handle this thread uses
    struct event_base* base = nullptr;

    /// listen event for notify pipe
    struct event notify_event = {};

    /**
     * notification pipe.
     *
     * The various worker threads are listening on index 0,
     * and in order to notify the thread other threads will
     * write data to index 1.
     */
    SOCKET notify[2] = {INVALID_SOCKET, INVALID_SOCKET};

    /**
     * The dispatcher accepts new clients and needs to dispatch them
     * to the worker threads. In order to do so we use the ConnectionQueue
     * where the dispatcher allocates the items and push on to the queue,
     * and the actual worker thread pop's the items off and start
     * serving them.
     */
    class ConnectionQueue {
    public:
        ~ConnectionQueue();
        void push(SOCKET socket, SharedListeningPort interface);
        void swap(std::vector<std::pair<SOCKET, SharedListeningPort>>& other);

    protected:
        std::mutex mutex;
        std::vector<std::pair<SOCKET, SharedListeningPort>> connections;
    } new_conn_queue;

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
        void push(Connection* c);
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
};

void notify_thread(FrontEndThread& thread);
void notify_dispatcher();
void drain_notification_channel(evutil_socket_t fd);
