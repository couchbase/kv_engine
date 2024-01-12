/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <event2/event.h>
#include <folly/Synchronized.h>
#include <libevent/utilities.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace cb {
namespace mcbp {
class Request;
class Response;
} // namespace mcbp
namespace test {

class Cluster;

struct UserEntry {
    UserEntry() = default;
    UserEntry(std::string username, std::string password, nlohmann::json authz)
        : username(std::move(username)),
          password(std::move(password)),
          authz(std::move(authz)) {
    }

    std::string username;
    std::string password;
    /// See docs/rbac.md for the format for the authorization data
    nlohmann::json authz;
};

class AuthProviderService {
public:
    AuthProviderService() = delete;
    AuthProviderService(AuthProviderService&) = delete;
    explicit AuthProviderService(Cluster& cluster);
    ~AuthProviderService();

    void upsertUser(UserEntry entry);
    void removeUser(const std::string& user);

    std::optional<UserEntry> lookupUser(const std::string& user);

protected:
    /// Handle the authenticate request and send the reply
    void onAuthenticate(bufferevent* bev, const cb::mcbp::Request& req);
    /// Handle the GetAuthorization request and send the reply
    void onGetAuthorization(bufferevent* bev, const cb::mcbp::Request& req);
    /// Dispatch an incoming request (ignore the ones we don't know about)
    void onRequest(bufferevent* bev, const cb::mcbp::Request& req);
    /// Dispach an incomming response message (we don't expect any)
    void onResponse(bufferevent* bev, const cb::mcbp::Response& res);
    /// The callback from libevent when there is new data available
    static void read_callback(bufferevent* bev, void* ctx);
    /// The callback from libevent when the socket is closed
    static void event_callback(bufferevent* bev, short event, void* ctx);

    void sendResponse(bufferevent* bev,
                      const cb::mcbp::Request& req,
                      cb::mcbp::Status status,
                      const std::string& payload);

    Cluster& cluster;
    folly::Synchronized<std::vector<UserEntry>> users;
    std::thread thread;
    cb::libevent::unique_event_base_ptr base;
};

} // namespace test
} // namespace cb
