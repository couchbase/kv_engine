/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "external_auth_manager_thread.h"

#include "connection.h"
#include "front_end_thread.h"
#include "get_authorization_task.h"
#include "platform/timeutils.h"
#include "sasl_auth_task.h"
#include "tracing.h" // SlowMutexThreshold

#include <logger/logger.h>
#include <mcbp/protocol/framebuilder.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/base64.h>
#include <algorithm>

/// The one and only handle to the external authentication manager
std::unique_ptr<ExternalAuthManagerThread> externalAuthManager;

void ExternalAuthManagerThread::add(Connection& connection) {
    std::lock_guard<std::mutex> guard(mutex);

    connection.incrementRefcount();
    connections.push_back(&connection);
}

void ExternalAuthManagerThread::remove(Connection& connection) {
    std::lock_guard<std::mutex> guard(mutex);

    auto iter = std::find(connections.begin(), connections.end(), &connection);
    if (iter != connections.end()) {
        pendingRemoveConnection.push_back(&connection);
        connections.erase(iter);
        condition_variable.notify_all();
    }
}

void ExternalAuthManagerThread::enqueueRequest(AuthnAuthzServiceTask& request) {
    std::lock_guard<std::mutex> guard(mutex);
    incomingRequests.push(&request);
    condition_variable.notify_all();
}

void ExternalAuthManagerThread::responseReceived(
        const cb::mcbp::Response& response) {
    // We need to keep the RBAC db in sync to avoid race conditions where
    // the response message is delayed and not handled until the auth
    // thread is scheduled. The reason we set it here is because
    // if we receive an update on the same connection the last one wins
    if (isStatusSuccess(response.getStatus())) {
        auto decoded = nlohmann::json::parse(response.getValueString());
        auto rbac = decoded.find("rbac");
        if (rbac != decoded.end()) {
            try {
                cb::rbac::updateExternalUser(rbac->dump());
            } catch (const std::runtime_error& exception) {
                LOG_WARNING_CTX("Failed to update RBAC entry",
                                {"payload", *rbac},
                                {"exception", exception.what()});
            }
        }
    }

    // Enqueue the response and let the auth thread deal with it
    std::lock_guard<std::mutex> guard(mutex);
    incommingResponse.emplace_back(
            std::make_unique<AuthResponse>(response.getOpaque(),
                                           response.getStatus(),
                                           response.getValueString()));
    condition_variable.notify_all();
}

void ExternalAuthManagerThread::run() {
    setRunning();
    std::unique_lock<std::mutex> lock(mutex);
    activeUsersLastSent = std::chrono::steady_clock::now();
    while (running) {
        if (incomingRequests.empty() && incommingResponse.empty()) {
            const auto now = std::chrono::steady_clock::now();

            // Calculate time required to sleep until the next active users list
            // push event
            const auto nextPushSleepTime = activeUsersPushInterval.load() -
                                           (now - activeUsersLastSent);

            if (!requestMap.empty()) {
                auto nextTimeout = pendingRequests.begin()->first;

                // Sleep until the next time dependent task is required to run
                auto sleepTime = std::min(nextPushSleepTime, nextTimeout - now);
                condition_variable.wait_for(lock, sleepTime);
            } else {
                // Sleep until we need to push the next active users list, as
                // there are no requests to timeout
                condition_variable.wait_for(lock, nextPushSleepTime);
            }

            if (!running) {
                // We're supposed to terminate
                return;
            }
        }

        // Purge the pending remove lists
        purgePendingDeadConnections();

        if (!incomingRequests.empty()) {
            processRequestQueue();
        }

        if (!incommingResponse.empty()) {
            processResponseQueue();
        }

        if (!requestMap.empty()) {
            handleTimeoutRequest();
        }

        const auto now = std::chrono::steady_clock::now();
        if ((now - activeUsersLastSent) >= activeUsersPushInterval.load()) {
            pushActiveUsers();
            activeUsersLastSent = now;
        }
    }
}

void ExternalAuthManagerThread::shutdown() {
    std::lock_guard<std::mutex> guard(mutex);
    running = false;
    condition_variable.notify_all();
}

void ExternalAuthManagerThread::pushActiveUsers() {
    if (connections.empty()) {
        return;
    }

    std::string payload = activeUsers.to_json().dump();
    auto* provider = connections.front();
    provider->getThread().eventBase.runInEventBaseThread(
            [provider, p = std::move(payload)]() {
                TRACE_LOCKGUARD_TIMED(provider->getThread().mutex,
                                      "mutex",
                                      "pushActiveUsers",
                                      SlowMutexThreshold);
                std::string buffer;
                buffer.resize(sizeof(cb::mcbp::Request) + p.size());
                cb::mcbp::RequestBuilder builder(buffer);
                builder.setMagic(cb::mcbp::Magic::ServerRequest);
                builder.setDatatype(provider->getEnabledDatatypes(
                        cb::mcbp::Datatype::JSON));
                builder.setOpcode(cb::mcbp::ServerOpcode::ActiveExternalUsers);
                builder.setValue(p);
                // Inject our packet into the stream!
                provider->copyToOutputStream(builder.getFrame()->getFrame());
            });
}

void ExternalAuthManagerThread::processRequestQueue() {
    if (connections.empty()) {
        // we don't have a provider, we need to cancel the request!
        while (!incomingRequests.empty()) {
            const std::string msg =
                    R"({"error":{"context":"External auth service is down"}})";
            incommingResponse.emplace_back(
                    std::make_unique<AuthResponse>(next, msg));
            requestMap[next++] =
                    std::make_pair(nullptr, incomingRequests.front());
            ++totalAuthRequestSent;
            incomingRequests.pop();
        }
        return;
    }

    // We'll be using the first connection in the list of connections.
    auto* provider = connections.front();

    while (!incomingRequests.empty()) {
        auto currentRequest = incomingRequests.front();
        currentRequest->recordStartTime();

        auto* saslTask = dynamic_cast<SaslAuthTask*>(currentRequest);
        if (saslTask == nullptr) {
            auto* getAuthz =
                    dynamic_cast<GetAuthorizationTask*>(currentRequest);
            if (getAuthz == nullptr) {
                LOG_CRITICAL_RAW(
                        "ExternalAuthManagerThread::processRequestQueue(): "
                        "Invalid entry found in request queue!");
                incomingRequests.pop();
                continue;
            }

            provider->getThread().eventBase.runInEventBaseThread(
                    [provider,
                     id = next,
                     user = std::string(getAuthz->getUsername())]() {
                        TRACE_LOCKGUARD_TIMED(provider->getThread().mutex,
                                              "mutex",
                                              "processRequestQueue",
                                              SlowMutexThreshold);
                        std::string buffer;
                        buffer.resize(sizeof(cb::mcbp::Request) + user.size());
                        cb::mcbp::RequestBuilder builder(buffer);
                        builder.setMagic(cb::mcbp::Magic::ServerRequest);
                        builder.setDatatype(cb::mcbp::Datatype::Raw);
                        builder.setOpcode(
                                cb::mcbp::ServerOpcode::GetAuthorization);
                        builder.setOpaque(id);
                        builder.setKey(user);

                        // Inject our packet into the stream!
                        provider->copyToOutputStream(
                                builder.getFrame()->getFrame());
                    });
        } else {
            nlohmann::json json;
            json["peer"] = saslTask->getPeer();
            if (!saslTask->getContext().empty()) {
                json["context"] = saslTask->getContext();
            }
            json["mechanism"] = saslTask->getMechanism();
            json["challenge"] =
                    cb::base64::encode(saslTask->getChallenge(), false);
            json["authentication-only"] =
                    haveRbacEntryForUser(saslTask->getUsername());
            auto payload = json.dump();
            provider->getThread().eventBase.runInEventBaseThread(
                    [provider, id = next, p = std::move(payload)]() {
                        const size_t needed =
                                sizeof(cb::mcbp::Request) + p.size();
                        std::string buffer;
                        buffer.resize(needed);
                        cb::mcbp::RequestBuilder builder(buffer);
                        builder.setMagic(cb::mcbp::Magic::ServerRequest);
                        builder.setDatatype(provider->getEnabledDatatypes(
                                cb::mcbp::Datatype::JSON));
                        builder.setOpcode(cb::mcbp::ServerOpcode::Authenticate);
                        builder.setOpaque(id);
                        builder.setValue(p);
                        // Inject our packet into the stream!
                        provider->copyToOutputStream(
                                builder.getFrame()->getFrame());
                    });
        }
        requestMap[next] = std::make_pair(provider, currentRequest);
        auto timeout = currentRequest->getStartTime() +
                       getExternalAuthRequestTimeout();
        pendingRequests.emplace(timeout, next++);
        ++totalAuthRequestSent;
        incomingRequests.pop();
    }
}

void ExternalAuthManagerThread::setRbacCacheEpoch(
        std::chrono::steady_clock::time_point tp) {
    using namespace std::chrono;
    const auto age = duration_cast<seconds>(tp.time_since_epoch()).count();
    rbacCacheEpoch.store(static_cast<uint64_t>(age), std::memory_order_release);
}

void ExternalAuthManagerThread::processResponseQueue() {
    auto responses = std::move(incommingResponse);
    while (!responses.empty()) {
        const auto& entry = responses.front();
        auto iter = requestMap.find(entry->opaque);
        if (iter == requestMap.end()) {
            // Unknown id.. ignore
            LOG_WARNING("processResponseQueue(): Ignoring unknown opaque: {}",
                        entry->opaque);
        } else {
            auto* task = iter->second.second;
            auto* startSaslTask = dynamic_cast<SaslAuthTask*>(task);
            auto responseTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() -
                            task->getStartTime());
            if (startSaslTask) {
                authorizationResponseTimes.add(responseTime);
            } else {
                authenticationResponseTimes.add(responseTime);
            }

            requestMap.erase(iter);
            auto timeout =
                    task->getStartTime() + getExternalAuthRequestTimeout();
            auto pendingRequestIter = pendingRequests.find(timeout);
            if (pendingRequestIter != pendingRequests.end()) {
                pendingRequests.erase(pendingRequestIter);
            }

            ++totalAuthRequestReceived;
            mutex.unlock();
            task->externalResponse(entry->status, entry->payload);
            mutex.lock();
        }
        responses.pop_front();
    }
}

void ExternalAuthManagerThread::handleTimeoutRequest() {
    const auto now = std::chrono::steady_clock::now();
    for (auto& [timeout, opaque] : pendingRequests) {
        if (timeout <= now) {
            // Time out response if we have not received a response after
            // externalAuthRequestTimeout duration
            // We need to fix this if we want to redistribute them over to
            // another provider
            LOG_WARNING(
                    "Request timed out, external authentication manager did "
                    "not respond in {}",
                    cb::time2text(getExternalAuthRequestTimeout()));
            const std::string msg =
                    R"({"error":{"context":"No response from external auth service"}})";
            incommingResponse.emplace_back(
                    std::make_unique<AuthResponse>(opaque, msg));
        } else {
            break;
        }
    }
}

void ExternalAuthManagerThread::purgePendingDeadConnections() {
    auto pending = std::move(pendingRemoveConnection);
    for (const auto& connection : pending) {
        LOG_WARNING_RAW(
                "External authentication manager died. Expect "
                "authentication failures");
        const std::string msg =
                R"({"error":{"context":"External auth service is down"}})";

        for (auto& req : requestMap) {
            if (req.second.first == connection) {
                // We don't need to check if we've got a response queued
                // already, as we'll ignore unknown responses..
                // We need to fix this if we want to redistribute
                // them over to another provider
                incommingResponse.emplace_back(
                        std::make_unique<AuthResponse>(req.first, msg));
                req.second.first = nullptr;
            }
        }

        // Notify the thread so that it may complete it's shutdown logic
        connection->getThread().eventBase.runInEventBaseThread([connection]() {
            TRACE_LOCKGUARD_TIMED(connection->getThread().mutex,
                                  "mutex",
                                  "purgePendingDeadConnections",
                                  SlowMutexThreshold);
            connection->decrementRefcount();
            connection->triggerCallback();
        });
    }
}

void ExternalAuthManagerThread::login(const std::string& user) {
    activeUsers.login(user);
}

void ExternalAuthManagerThread::logoff(const std::string& user) {
    activeUsers.logoff(user);
}

void ExternalAuthManagerThread::addStats(const StatCollector& collector) const {
    using namespace cb::stats;
    collector.addStat(Key::auth_external_sent, totalAuthRequestSent);
    collector.addStat(Key::auth_external_received, totalAuthRequestReceived);
}

bool ExternalAuthManagerThread::haveRbacEntryForUser(
        const std::string& user) const {
    const auto then = std::chrono::steady_clock::now() -
                      2 * activeUsersPushInterval.load();
    using namespace std::chrono;
    const auto ts = cb::rbac::getExternalUserTimestamp(user);
    const auto timestamp = ts.value_or(steady_clock::time_point{});
    const uint64_t age = static_cast<uint64_t>(
            duration_cast<seconds>(timestamp.time_since_epoch()).count());

    return (timestamp > then) &&
           (age >= rbacCacheEpoch.load(std::memory_order_acquire));
}

void ExternalAuthManagerThread::ActiveUsers::login(const std::string& user) {
    std::lock_guard<std::mutex> guard(mutex);
    users[user]++;
}

void ExternalAuthManagerThread::ActiveUsers::logoff(const std::string& user) {
    std::lock_guard<std::mutex> guard(mutex);
    auto iter = users.find(user);
    if (iter == users.end()) {
        throw std::runtime_error("ActiveUsers::logoff: Failed to find user");
    }
    iter->second--;
    if (iter->second == 0) {
        users.erase(iter);
    }
}

nlohmann::json ExternalAuthManagerThread::ActiveUsers::to_json() const {
    std::lock_guard<std::mutex> guard(mutex);
    auto ret = nlohmann::json::array();

    for (const auto& entry : users) {
        ret.push_back(entry.first);
    }

    return ret;
}
