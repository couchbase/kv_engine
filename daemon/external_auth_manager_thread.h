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

#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/thread.h>
#include <chrono>
#include <gsl/gsl>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

class Connection;
class AuthnAuthzServiceTask;

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
    void enqueueRequest(AuthnAuthzServiceTask& request);

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

    void setPushActiveUsersInterval(std::chrono::microseconds interval) {
        activeUsersPushInterval.store(interval);
        condition_variable.notify_one();
    }

    void setRbacCacheEpoch(std::chrono::steady_clock::time_point tp);

    /// Check to see if we've got an up to date RBAC entry for the user
    bool haveRbacEntryForUser(const std::string& user) const;

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

    /// Push the list of active users to the authentication provider
    void pushActiveUsers();

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
    std::unordered_map<uint32_t, std::pair<Connection*, AuthnAuthzServiceTask*>>
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
    std::queue<AuthnAuthzServiceTask*> incomingRequests;

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

    /**
     * The interval we'll push the current active users to the external
     * authentication service.
     *
     * The list can't be considered as "absolute" as memcached is a
     * highly multithreaded application and we may have disconnects
     * happening in multiple threads at the same time and we might
     * have login messages waiting to acquire the lock for the map.
     * The intention with the list is that we'll poll the users from
     * LDAP to check if the group membership has changed since the last
     * time we checked. Given that LDAP is an external system and it
     * can't push change notifications to our system our guarantee
     * is that the change is reflected in memcached at within 2 times
     * of the interval we push these users.
     *
     * We don't need microseconds resolution "in production", but
     * we don't want to have our unit tests wait multiple seconds
     * for this message to be sent
     */
    std::atomic<std::chrono::microseconds> activeUsersPushInterval{
            std::chrono::minutes(5)};

    /**
     * The time point we last sent the list of active users to the auth
     * provider
     */
    std::chrono::steady_clock::time_point activeUsersLastSent;

    /**
     * It should be possible for the authentication provider to
     * invalidate the entire cache, but we can't simply drop the
     * current cache as that would cause all of the connected clients
     * to be disconnected. Instead the authentication provider may
     * tell us to start requesting a new RBAC entry if the cached
     * one is older than a given time point. Combined with the logic
     * that we'll push all of the connected users the authentication
     * provider can make sure that all of the connections use the
     * correct RBAC definition. Ideally we would have stored this
     * as a std::chrono::steady_clock::TimePoint, but that don't
     * play well with std::atomic, so we're storing the raw
     * number of seconds instead.
     */
    std::atomic<uint64_t> rbacCacheEpoch{{}};
};

extern std::unique_ptr<ExternalAuthManagerThread> externalAuthManager;
