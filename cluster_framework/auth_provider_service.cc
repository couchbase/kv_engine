/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "auth_provider_service.h"

#include "cluster.h"

#include <cbsasl/server.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <folly/ScopeGuard.h>
#include <json_web_token/token.h>
#include <mcbp/protocol/framebuilder.h>
#include <platform/base64.h>
#include <platform/uuid.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>

namespace cb::test {

static constexpr std::string_view token_signing_passphrase =
        "auth-service-secret-token";

struct AuthProviderService::PwDbEntry {
    PwDbEntry() = default;
    explicit PwDbEntry(UserEntry ue)
        : user_entry(std::move(ue)),
          user(sasl::pwdb::UserFactory::create(
                  user_entry.username,
                  std::vector{{user_entry.password}},
                  [](auto) { return true; },
                  "pbkdf2-hmac-sha512")) {
    }
    UserEntry user_entry;
    sasl::pwdb::User user;
};

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
        constexpr auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                                 BEV_OPT_CLOSE_ON_FREE |
                                 BEV_OPT_DEFER_CALLBACKS;
        auto* bev = bufferevent_socket_new(base.get(), sock, options);
        bufferevent_setcb(bev,
                          AuthProviderService::read_callback,
                          nullptr,
                          AuthProviderService::event_callback,
                          this);
        bufferevent_enable(bev, EV_READ);
    }

    // I've registered all of them... lets start a thread to operate on
    // them
    thread = std::thread([&]() { event_base_loop(base.get(), 0); });
}

AuthProviderService::~AuthProviderService() {
    // Just break out of the loop
    event_base_loopbreak(base.get());
    thread.join();
}
std::unique_ptr<cb::jwt::Builder> AuthProviderService::getTokenBuilder(
        std::string_view username) {
    auto builder = cb::jwt::Builder::create("HS256", token_signing_passphrase);
    builder->addAudience("kv_auth_service");
    builder->addClaim("user", username);
    return builder;
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
            if ((*iter)->user_entry.username == user) {
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
        for (auto iter = db.begin(); iter != db.end(); ++iter) {
            if ((*iter)->user_entry.username == entry.username) {
                db.erase(iter);
                break;
            }
        }
        db.emplace_back(std::make_unique<PwDbEntry>(entry));
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

std::optional<UserEntry> AuthProviderService::lookupUser(
        const std::string& user) {
    return users.withWLock([&user](auto& db) -> std::optional<UserEntry> {
        // check to see if we're replacing an entry
        for (auto& e : db) {
            if (e->user_entry.username == user) {
                return e->user_entry;
            }
        }
        return {};
    });
}

void AuthProviderService::onRequest(bufferevent* bev,
                                    const mcbp::Request& req) {
    switch (req.getMagic()) {
    case mcbp::Magic::ClientRequest:
    case mcbp::Magic::AltClientRequest:
        std::cerr << "Unexpected message received: "
                  << req.to_json(true).dump(2) << std::endl;
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
    std::cerr << "Unexpected message received: " << res.to_json(true).dump(2)
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

    if (json.contains("context")) {
        onAuthenticateStep(bev, req, json);
    } else {
        onAuthenticateStart(bev, req, json);
    }
}

void AuthProviderService::handleSaslResponse(
        bufferevent* bev,
        const cb::mcbp::Request& req,
        bool authentication_only,
        std::unique_ptr<cb::sasl::server::ServerContext> server_ctx,
        nlohmann::json rbac,
        std::optional<nlohmann::json> token_metadata,
        cb::sasl::Error status,
        std::string_view challenge) {
    using mcbp::Status;
    using sasl::Error;

    nlohmann::json success_payload;
    if (!challenge.empty()) {
        success_payload["response"] = base64::encode(challenge);
    }

    switch (status) {
    case Error::CONTINUE:
        success_payload["context"] = ::to_string(cb::uuid::random());
        active_auth.emplace(success_payload["context"],
                            std::make_unique<ActiveAuth>(std::move(server_ctx),
                                                         std::move(rbac)));
        sendResponse(bev, req, Status::AuthContinue, success_payload.dump());
        return;

    case Error::FAIL:
        sendResponse(bev,
                     req,
                     Status::Einternal,
                     R"({"error":{"context":"Internal error."}})");
        return;
    case Error::BAD_PARAM:
        sendResponse(bev,
                     req,
                     Status::Einval,
                     R"({"error":{"context":"Invalid challenge"}})");
        return;

    case Error::NO_MEM:
        sendResponse(
                bev,
                req,
                Status::Enomem,
                R"({"error":{"context":"Internal error. Missing rbac profile"}})");
        return;

    case Error::AUTH_PROVIDER_DIED:
    case Error::NO_RBAC_PROFILE:
        // These errors are not returned by start
        Expects(false);

    case Error::NO_MECH:
        sendResponse(bev,
                     req,
                     Status::NotSupported,
                     R"({"error":{"context":"mechanism not supported"}})");
        return;

    case Error::NO_USER:
        sendResponse(bev, req, Status::KeyEnoent, {});
        return;

    case Error::PASSWORD_EXPIRED:
        sendResponse(bev, req, Status::AuthStale, {});
        return;

    case Error::PASSWORD_ERROR:
        sendResponse(bev, req, Status::KeyEexists, {});
        return;

    case Error::OK:
        if (token_metadata) {
            success_payload["token"] = token_metadata.value();
        } else if (!authentication_only) {
            success_payload["rbac"][server_ctx->getUsername()] =
                    std::move(rbac);
        }
        sendResponse(bev, req, Status::Success, success_payload.dump());
        return;
    }

    sendResponse(bev,
                 req,
                 Status::Einternal,
                 R"({"error":{"context":"Internal error"}})");
}

/// Validate the user token and populate the token metadata with information
/// to return to the client
static sasl::Error validateUserTokenFunction(
        std::string_view user,
        std::string_view token,
        std::optional<nlohmann::json>& token_metadata) {
    try {
        auto jwt = cb::jwt::Token::parse(token, []() -> std::string {
            return std::string{token_signing_passphrase};
        });

        if (jwt->header.value("alg", "") != "HS256") {
            // Reject any tokens not signed by us
            return sasl::Error::BAD_PARAM;
        }
        const auto& claims = jwt->payload;
        if (claims.value("user", "") != user) {
            return sasl::Error::NO_USER;
        }

        nlohmann::json metadata;
        if (claims.contains("cb-rbac")) {
            metadata["rbac"][user] = nlohmann::json::parse(
                    base64url::decode(claims.value("cb-rbac", "")));
        } else {
            return sasl::Error::NO_RBAC_PROFILE;
        }
        if (claims.contains("exp")) {
            metadata["exp"] = claims["exp"];
        }
        if (claims.contains("nbf")) {
            metadata["nbf"] = claims["nbf"];
        }
        token_metadata = std::move(metadata);
        return sasl::Error::OK;
    } catch (const std::exception&) {
        return sasl::Error::BAD_PARAM;
    }
}

void AuthProviderService::onAuthenticateStart(bufferevent* bev,
                                              const cb::mcbp::Request& req,
                                              const nlohmann::json& json) {
    nlohmann::json authz;
    std::optional<nlohmann::json> token_metadata;

    PwDbEntry user_entry;
    auto lookup_user =
            [this, &user_entry, &authz](
                    const std::string& username) -> sasl::pwdb::User {
        PwDbEntry entry;
        users.withRLock([&username, &entry](auto& userdb) {
            for (const auto& e : userdb) {
                if (e->user_entry.username == username) {
                    entry = *e;
                    return;
                }
            }
        });
        if (entry.user.isDummy()) {
            return sasl::pwdb::User{};
        }

        user_entry = entry;
        authz = user_entry.user_entry.authz;
        return entry.user;
    };

    nlohmann::json success_payload;

    auto server_ctx =
            std::make_unique<cb::sasl::server::ServerContext>(lookup_user);

    server_ctx->setValidateUserTokenFunction(
            [&token_metadata](auto user, auto token) -> sasl::Error {
                return validateUserTokenFunction(user, token, token_metadata);
            });

    auto [status, challenge] = server_ctx->start(
            json["mechanism"].get<std::string>(),
            {},
            cb::base64::decode(json.at("challenge").get<std::string>()));

    handleSaslResponse(bev,
                       req,
                       json.value("authentication-only", true),
                       std::move(server_ctx),
                       std::move(authz),
                       token_metadata,
                       status,
                       challenge);
}

void AuthProviderService::onAuthenticateStep(bufferevent* bev,
                                             const cb::mcbp::Request& req,
                                             const nlohmann::json& json) {
    using namespace cb::sasl;
    using namespace cb::mcbp;

    auto iter = active_auth.find(json["context"]);
    if (iter == active_auth.end()) {
        sendResponse(bev,
                     req,
                     Status::Einval,
                     R"({"error":{"context":"Unknown context"}})");
        return;
    }

    auto& server_ctx = iter->second->server_context;
    auto [status, challenge] = server_ctx->step(
            base64::decode(json.at("challenge").get<std::string>()));

    auto prune = folly::makeGuard([this, &iter] { active_auth.erase(iter); });
    handleSaslResponse(bev,
                       req,
                       json.value("authentication-only", true),
                       std::move(server_ctx),
                       std::move(iter->second->json),
                       {},
                       status,
                       challenge);
}

void AuthProviderService::onGetAuthorization(bufferevent* bev,
                                             const mcbp::Request& req) {
    const std::string user{req.getKeyString()};
    UserEntry ue;
    users.withRLock([&user, &ue](auto& userdb) {
        for (const auto& u : userdb) {
            if (u->user_entry.username == user) {
                ue = u->user_entry;
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
                                       std::string_view payload) {
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
} // namespace cb::test
