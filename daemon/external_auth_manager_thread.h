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

#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/thread.h>
#include <gsl/gsl>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

class Connection;
class StartSaslAuthTask;

/**
 * The ExternalAuthManagerThread class takes care of the scheduling between
 * the authentication requests and the responses received.
 *
 * One of the problems is that we try to communicate between multiple
 * frontend threads, which could cause deadlocks. To avoid locking
 * problems we're using a dedicated thread to communicate with the
 * other threads.
 */
class ExternalAuthManagerThread : public Couchbase::Thread {
public:
    ExternalAuthManagerThread() : Couchbase::Thread("mcd:ext_auth") {
    }
    ExternalAuthManagerThread(const ExternalAuthManagerThread&) = delete;

    /**
     * The named (external) user logged on to the system
     */
    void login(const std::string& user);

    /**
     * The named (external) user logged off the system
     */
    void logoff(const std::string& user);

    /**
     * Get the list of active users defined in the system
     */
    nlohmann::json getActiveUsers() const;

    /**
     * Add the provided connection to the list of available authentication
     * providers
     */
    void add(Connection& connection);

    /**
     * Remove the connection from the list of available providers (the
     * connection may not be registered)
     */
    void remove(Connection& connection);

    /**
     * Enqueue a SASL request to the list of requests to send to the
     * RBAC provider. NOTE: This method must _NOT_ be called from the
     * worker threads as that may result in a deadlock (should be
     * called from the tasks execute() method when running on the
     * executor threads)
     *
     * @param request the task to notify when the result comes back
     */
    void enqueueRequest(StartSaslAuthTask& request);

    /**
     * Received an authentication response on the provided connection
     *
     * @param connection the connection the message was received on
     * @param response
     */
    void responseReceived(const cb::mcbp::Response& response);

    /**
     * Request the authentication daemon to stop
     */
    void shutdown();

protected:
    /// The main loop of the thread
    void run() override;

    /**
     * Process all of the requests added by the various SaslTasks by putting
     * them into the queue of messages to send to the Authentication Provider.
     */
    void processRequestQueue();
    /**
     * Process all of the response messages enqueued and notify the SaslTask
     */
    void processResponseQueue();

    /**
     * Iterate over all of the connections marked as closed. Generate
     * error responses for all outstanding requests; decrement the reference
     * count and tell the thread to complete its shutdown logic.
     */
    void purgePendingDeadConnections();

    /// Let the daemon thread run as long as this member is set to true
    bool running = true;

    /// The next value to use in the opaque field
    uint32_t next = 0;

    /// The map between the opaque field being used and the SaslAuthTask
    /// requested the operation
    /// Second should be a std pair of a connection and a task, so that
    /// when we remove a connection we can iterate over the entire
    /// map and create aborts for the one we don't have anymore (or
    /// we could redistribute them O:)
    std::unordered_map<uint32_t, std::pair<Connection*, StartSaslAuthTask*>>
            requestMap;

    /// The mutex variable used to protect access to _all_ the internal
    /// members
    std::mutex mutex;
    /// The daemon thread will block and wait on this variable when there
    /// isn't any work to do
    std::condition_variable condition_variable;

    /// A list of available connections to use as authentication providers
    std::vector<Connection*> connections;

    /**
     * All of the various tasks enqueue their SASL request in this queue
     * to allow the authentication provider thread to inject them into
     * the worker thread (to avoid a possible deadlock)
     */
    std::queue<StartSaslAuthTask*> incomingRequests;

    /**
     * We need to pass the response information from one of the frontend
     * thread back to the sasl task, and we'd like to use the same
     * logic when we're terminating the connections.
     */
    struct AuthResponse {
        AuthResponse(uint32_t opaque, std::string payload)
            : opaque(opaque),
              status(cb::mcbp::Status::Etmpfail),
              payload(std::move(payload)) {
        }
        AuthResponse(uint32_t opaque,
                     cb::mcbp::Status status,
                     cb::const_byte_buffer value)
            : opaque(opaque),
              status(status),
              payload(reinterpret_cast<const char*>(value.data()),
                      value.size()) {
        }
        uint32_t opaque;
        cb::mcbp::Status status;
        std::string payload;
    };

    /**
     * All of the various responses is stored within this queue to
     * be handled by the daemon thread
     */
    std::queue<std::unique_ptr<AuthResponse>> incommingResponse;

    std::vector<Connection*> pendingRemoveConnection;

    class ActiveUsers {
    public:
        void login(const std::string& user);

        void logoff(const std::string& user);

        nlohmann::json to_json() const;

    private:
        mutable std::mutex mutex;
        std::unordered_map<std::string, uint32_t> users;
    } activeUsers;
};

extern std::unique_ptr<ExternalAuthManagerThread> externalAuthManager;
