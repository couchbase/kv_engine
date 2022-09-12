/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sasl_start_command_context.h"

#include <cbsasl/mechanism.h>
#include <daemon/connection.h>
#include <daemon/external_auth_manager_thread.h>
#include <daemon/memcached.h>
#include <daemon/one_shot_task.h>
#include <daemon/settings.h>
#include <daemon/start_sasl_auth_task.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <platform/scope_timer.h>

SaslStartCommandContext::SaslStartCommandContext(Cookie& cookie)
    : SaslAuthCommandContext(cookie) {
}

cb::engine_errc SaslStartCommandContext::initial() {
    if (!connection.isSaslAuthEnabled()) {
        return cb::engine_errc::not_supported;
    }

    if (connection.getSaslServerContext()) {
        cookie.setErrorContext(
                R"(Logic error: The server expects SASL STEP after returning CONTINUE from SASL START)");
        return cb::engine_errc::invalid_arguments;
    }

    connection.createSaslServerContext();
    connection.restartAuthentication();

    state = State::HandleSaslAuthTaskResult;

    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_SaslStartTask, "SASL Start", [this]() {
                doSaslStart();

                // If the user doesn't exist locally we may try the external
                // AUTH service
                if (error == cb::sasl::Error::NO_USER &&
                    Settings::instance().isExternalAuthServiceEnabled()) {
                    task = std::make_shared<StartSaslAuthTask>(
                            cookie,
                            *connection.getSaslServerContext(),
                            mechanism,
                            challenge);
                    externalAuthManager->enqueueRequest(*task);
                    return;
                }

                // We need to notify with success here to avoid having the
                // framework report the error
                ::notifyIoComplete(cookie, cb::engine_errc::success);
            }));

    return cb::engine_errc::would_block;
}

cb::engine_errc SaslStartCommandContext::handleSaslAuthTaskResult() {
    cb::engine_errc ret;
    if (task) {
        error = task->getError();
        payload = {};
        task.reset();
    }

    ret = doHandleSaslAuthTaskResult(error, payload);

    if (error != cb::sasl::Error::CONTINUE) {
        // we should _ONLY_ preserve the sasl server context if the underlying
        // sasl backend returns CONTINUE
        connection.releaseSaslServerContext();
    }

    return ret;
}

void SaslStartCommandContext::doSaslStart() {
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer(cookie, cb::tracing::Code::Sasl);

    LOG_DEBUG("{}: SASL START with mech: '{}' with {} bytes of data",
              connection.getId(),
              mechanism,
              challenge.size());

    auto& server = *connection.getSaslServerContext();
    try {
        auto avail = cookie.getConnection().getSaslMechanisms();
        auto [e, p] = server.start(mechanism, avail, challenge);
        error = e;
        payload = p;
    } catch (const cb::sasl::unknown_mechanism&) {
        error = cb::sasl::Error::NO_MECH;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StartSaslAuthTask::execute(): std::bad_alloc",
                    cookie.getConnectionId());
        error = cb::sasl::Error::NO_MEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        if (server.containsUuid()) {
            cookie.setEventId(server.getUuid());
        }
        LOG_WARNING(
                "{}: StartSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                cookie.getConnectionId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        error = cb::sasl::Error::FAIL;
    }
}
