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
     * @param ssl the OpenSSL SSL structure to use (if this is a connection
     *            using SSL)
     */
    static void dispatch(SOCKET sfd,
                         std::shared_ptr<ListeningPort> descr,
                         uniqueSslPtr ssl);

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
            Entry(SOCKET sock,
                  std::shared_ptr<ListeningPort> descr,
                  uniqueSslPtr ssl)
                : sock(sock), descr(std::move(descr)), ssl(std::move(ssl)) {
            }
            SOCKET sock;
            std::shared_ptr<ListeningPort> descr;
            uniqueSslPtr ssl;
        };
        ~ConnectionQueue();
        void push(SOCKET sock,
                  std::shared_ptr<ListeningPort> descr,
                  uniqueSslPtr ssl);
        void swap(std::vector<Entry>& other);

    protected:
        folly::Synchronized<std::vector<Entry>, std::mutex> connections;
    } new_conn_queue;

    /// Shared validator used by all connections serviced by this thread
    /// when they need to validate a JSON document
    std::unique_ptr<cb::json::SyntaxValidator> validator;
};

class Hdr1sfMicroSecHistogram;

extern std::vector<Hdr1sfMicroSecHistogram> scheduler_info;
