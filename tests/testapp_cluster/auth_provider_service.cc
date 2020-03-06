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

#include "auth_provider_service.h"

#include "cluster.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <include/mcbp/protocol/framebuilder.h>
#include <platform/base64.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>

namespace cb {
namespace test {

AuthProviderService::AuthProviderService(Cluster& cluster) : cluster(cluster) {
    base.reset(event_base_new());

    for (size_t idx = 0; idx < cluster.size(); ++idx) {
        auto conn = cluster.getConnection(idx);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->setFeature(cb::mcbp::Feature::Duplex, true);
        auto rsp = conn->execute(
                BinprotGenericCommand(cb::mcbp::ClientOpcode::AuthProvider));
        if (!rsp.isSuccess()) {
            throw ConnectionError("Failed to register as auth provider", rsp);
        }

        auto sock = conn->releaseSocket();
        evutil_make_socket_nonblocking(sock);
        const auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                             BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS;
        auto* bev = bufferevent_socket_new(base.get(), sock, options);
        bufferevent_setcb(bev,
                          AuthProviderService::read_callback,
                          nullptr,
                          AuthProviderService::event_callback,
                          static_cast<void*>(this));
        bufferevent_enable(bev, EV_READ);
    }

    // I've registered all of them... lets start a thread to operate on
    // them
    thread = std::thread([&]() { event_base_loop(base.get(), 0); });
}

AuthProviderService::~AuthProviderService() {
    // Just break out of the loop
    event_base_loopbreak(base.get());
    base.reset();
    thread.join();
}

bool isPacketAvailable(evbuffer* input) {
    auto size = evbuffer_get_length(input);
    if (size < sizeof(cb::mcbp::Header)) {
        return false;
    }

    const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
    if (header == nullptr) {
        throw std::runtime_error(
                "isPacketAvailable(): Failed to reallocate event "
                "input buffer: " +
                std::to_string(sizeof(cb::mcbp::Header)));
    }

    if (!header->isValid()) {
        throw std::runtime_error(
                "isPacketAvailable(): Invalid packet header detected");
    }

    const auto framesize = sizeof(*header) + header->getBodylen();
    if (size >= framesize) {
        // We've got the entire buffer available.. make sure it is continuous
        if (evbuffer_pullup(input, framesize) == nullptr) {
            throw std::runtime_error(
                    "isPacketAvailable(): Failed to reallocate "
                    "event input buffer: " +
                    std::to_string(framesize));
        }
        return true;
    }

    return false;
}

void AuthProviderService::read_callback(bufferevent* bev, void* ctx) {
    auto& provider = *static_cast<AuthProviderService*>(ctx);
    auto* input = bufferevent_get_input(bev);
    while (isPacketAvailable(input)) {
        const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
                evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
        if (header->isRequest()) {
            provider.onRequest(bev, header->getRequest());
        } else {
            provider.onResponse(bev, header->getResponse());
        }
        evbuffer_drain(input, sizeof(cb::mcbp::Header) + header->getBodylen());
    }
}

void AuthProviderService::event_callback(bufferevent* bev, short event, void*) {
    if (((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) ||
        ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR)) {
        bufferevent_free(bev);
    }
}

void AuthProviderService::removeUser(const std::string& user) {
    users.withWLock([&user](auto& db) {
        for (auto iter = db.begin(); iter != db.end(); ++iter) {
            if (iter->username == user) {
                db.erase(iter);
                return;
            }
        }
    });
}

void AuthProviderService::upsertUser(UserEntry entry) {
    if (entry.authz.at("domain").get<std::string>() != "external") {
        throw std::logic_error(
                "AuthProviderService::upsertUser: Domain must be \"external\"");
    }

    users.withWLock([&entry](auto& db) {
        // check to see if we're replacing an entry
        for (auto& e : db) {
            if (e.username == entry.username) {
                e.password = entry.password;
                e.authz = entry.authz;
                break;
            }
        }
        db.emplace_back(entry);
    });

    // push the value to all of the connections
    nlohmann::json payload;
    payload[entry.username] = entry.authz;
    BinprotGenericCommand cmd(
            cb::mcbp::ClientOpcode::UpdateExternalUserPermissions);
    cmd.setValue(payload.dump());

    for (size_t idx = 0; idx < cluster.size(); ++idx) {
        auto conn = cluster.getConnection(idx);
        conn->authenticate("@admin", "password", "PLAIN");
        const auto rsp = conn->execute(cmd);
        if (!rsp.isSuccess()) {
            throw ConnectionError(
                    "AuthProviderService::upsertUser: failed to push update to "
                    "n_" + std::to_string(idx),
                    rsp);
        }
    }
}

void AuthProviderService::onRequest(bufferevent* bev,
                                    const mcbp::Request& req) {
    switch (req.getMagic()) {
    case mcbp::Magic::ClientRequest:
    case mcbp::Magic::AltClientRequest:
        std::cerr << "Unexpected message received: " << req.toJSON(true).dump(2)
                  << std::endl;
        break;
    case mcbp::Magic::ServerRequest:
        switch (req.getServerOpcode()) {
        case mcbp::ServerOpcode::ClustermapChangeNotification:
            // Ignore
            break;
        case mcbp::ServerOpcode::Authenticate:
            onAuthenticate(bev, req);
            break;
        case mcbp::ServerOpcode::ActiveExternalUsers:
            // ignore
            break;
        case mcbp::ServerOpcode::GetAuthorization:
            onGetAuthorization(bev, req);
        }
        break;
    case mcbp::Magic::ClientResponse:
    case mcbp::Magic::AltClientResponse:
    case mcbp::Magic::ServerResponse:
        std::abort();
    }
}

void AuthProviderService::onResponse(bufferevent*, const mcbp::Response& res) {
    std::cerr << "Unexpected message received: " << res.toJSON(true).dump(2)
              << std::endl;
}

void AuthProviderService::onAuthenticate(bufferevent* bev,
                                         const mcbp::Request& req) {
    auto val = req.getValue();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(std::string{
                reinterpret_cast<const char*>(val.data()), val.size()});
    } catch (const std::exception&) {
        sendResponse(bev,
                     req,
                     cb::mcbp::Status::Einval,
                     R"({"error":{"context":"Failed to parse JSON"}})");
        return;
    }

    if (json.at("mechanism").get<std::string>() != "PLAIN") {
        sendResponse(bev,
                     req,
                     cb::mcbp::Status::NotSupported,
                     R"({"error":{"context":"mechanism not supported"}})");
        return;
    }

    const auto ch = cb::base64::decode(json.at("challenge").get<std::string>());
    const auto challenge =
            std::string{reinterpret_cast<const char*>(ch.data()), ch.size()};

    // The syntax for the payload for plain auth is a string looking like:
    // \0username\0password
    if (challenge.empty()) {
        sendResponse(bev,
                     req,
                     cb::mcbp::Status::Einval,
                     R"({"error":{"context":"Invalid encoded challenge"}})");
        return;
    }

    // Skip everything up to the first \0
    size_t inputpos = 0;
    while (inputpos < challenge.size() && challenge[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos >= challenge.size()) {
        sendResponse(bev,
                     req,
                     cb::mcbp::Status::Einval,
                     R"({"error":{"context":"Invalid encoded challenge"}})");
        return;
    }

    const char* ptr = challenge.data() + inputpos;
    while (inputpos < challenge.size() && challenge[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos > challenge.size()) {
        sendResponse(bev,
                     req,
                     cb::mcbp::Status::Einval,
                     R"({"error":{"context":"Invalid encoded challenge"}})");
        return;
    }

    const std::string user(ptr);
    std::string password;

    if (inputpos != challenge.size()) {
        size_t pwlen = 0;
        ptr = challenge.data() + inputpos;
        while (inputpos < challenge.size() && challenge[inputpos] != '\0') {
            inputpos++;
            pwlen++;
        }
        password = std::string{ptr, pwlen};
    }

    UserEntry ue;
    users.withRLock([&user, &ue](auto& userdb) {
        for (const auto& u : userdb) {
            if (u.username == user) {
                ue = u;
                return;
            }
        }
    });

    if (ue.username.empty()) {
        sendResponse(bev, req, cb::mcbp::Status::KeyEnoent, {});
    } else if (password == ue.password) {
        nlohmann::json payload;
        payload["rbac"][ue.username] = ue.authz;
        sendResponse(bev, req, cb::mcbp::Status::Success, payload.dump());
    } else {
        // Invalid username password combo
        sendResponse(bev, req, cb::mcbp::Status::KeyEexists, {});
    }
}

void AuthProviderService::onGetAuthorization(bufferevent* bev,
                                             const mcbp::Request& req) {
    auto key = req.getKey();
    const auto user =
            std::string{reinterpret_cast<const char*>(key.data()), key.size()};
    UserEntry ue;
    users.withRLock([&user, &ue](auto& userdb) {
        for (const auto& u : userdb) {
            if (u.username == user) {
                ue = u;
                return;
            }
        }
    });

    if (ue.username.empty()) {
        sendResponse(bev, req, cb::mcbp::Status::KeyEnoent, {});
    } else {
        nlohmann::json json;
        json["rbac"][ue.username] = ue.authz;
        sendResponse(bev, req, cb::mcbp::Status::Success, json.dump());
    }
}

void AuthProviderService::sendResponse(bufferevent* bev,
                                       const mcbp::Request& req,
                                       cb::mcbp::Status status,
                                       const std::string& payload) {
    std::vector<uint8_t> backing(sizeof(cb::mcbp::Header) + payload.size());
    cb::mcbp::ResponseBuilder builder(backing);
    builder.setMagic(cb::mcbp::Magic::ServerResponse);
    builder.setStatus(status);
    builder.setOpaque(req.getOpaque());
    builder.setOpcode(req.getServerOpcode());
    builder.setValue({payload.data(), payload.size()});
    if (bufferevent_write(bev, backing.data(), backing.size()) == -1) {
        throw std::runtime_error(
                "AuthProviderService::sendResponse: Failed to spool "
                "response");
    }
}
} // namespace test
} // namespace cb
