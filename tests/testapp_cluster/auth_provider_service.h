/*
 *     Copyright 2020 Couchbase, Inc
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
