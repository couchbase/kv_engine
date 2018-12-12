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
#include <platform/socket.h>
#include <subdoc/operations.h>
#include <mutex>
#include <queue>
#include <unordered_map>

// Forward decl
namespace cb {
class Pipe;
}

class Cookie;
class Connection;
struct ConnectionQueueItem;
struct thread_stats;

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

    void push(std::unique_ptr<ConnectionQueueItem> item);

    std::unique_ptr<ConnectionQueueItem> pop();

private:
    std::mutex mutex;
    std::queue<std::unique_ptr<ConnectionQueueItem> > connections;
};

struct FrontEndThread {
    /**
     * Pending IO requests for this thread. Maps each pending Connection to
     * the IO status to be notified.
     */
    using PendingIoMap = std::unordered_map<Connection*, ENGINE_ERROR_CODE>;

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

    /// queue of new connections to handle
    ConnectionQueue new_conn_queue;

    /// Mutex to lock protect access to this object.
    std::mutex mutex;

    /// Set of connections with pending async io ops.
    struct {
        std::mutex mutex;
        PendingIoMap map;
    } pending_io;

    /// index of this thread in the threads array
    size_t index = 0;

    /// Shared read buffer for all connections serviced by this thread.
    std::unique_ptr<cb::Pipe> read;

    /// Shared write buffer for all connections serviced by this thread.
    std::unique_ptr<cb::Pipe> write;

    /**
     * Shared sub-document operation for all connections serviced by this
     * thread
     */
    Subdoc::Operation subdoc_op;

    /**
     * When we're deleting buckets we need to disconnect idle
     * clients. This variable is incremented for every delete bucket
     * thread running and decremented when it's done. When this
     * variable is set we'll try to look through all connections and
     * update them with a write event if they're in an "idle"
     * state. That should cause them to be rescheduled and cause the
     * client to disconnect.
     */
    int deleting_buckets = 0;

    /**
     * Shared validator used by all connections serviced by this thread
     * when they need to validate a JSON document
     */
    JSON_checker::Validator validator;

    /// Is the thread running or not
    std::atomic_bool running{false};
};

void notify_thread(FrontEndThread& thread);
void notify_dispatcher();
void notify_thread_bucket_deletion(FrontEndThread& me);
