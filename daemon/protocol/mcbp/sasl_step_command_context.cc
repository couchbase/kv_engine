/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sasl_step_command_context.h"

#include "daemon/external_auth_manager_thread.h"
#include "daemon/sasl_auth_task.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/memcached.h>
#include <daemon/nobucket_taskable.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <platform/scope_timer.h>

SaslStepCommandContext::SaslStepCommandContext(Cookie& cookie)
    : SaslAuthCommandContext(cookie) {
}

cb::engine_errc SaslStepCommandContext::initial() {
    if (!connection.isSaslAuthEnabled()) {
        return cb::engine_errc::not_supported;
    }

    if (!connection.getSaslServerContext()) {
        cookie.setErrorContext(
                R"(Logic error: The server expects the client to start with SASL START before sending SASL STEP)");
        return cb::engine_errc::invalid_arguments;
    }

    state = State::HandleSaslAuthTaskResult;
    ExecutorPool::get()->schedule(
            std::make_shared<OneShotLimitedConcurrencyTask>(
                    TaskId::Core_SaslStepTask,
                    "SASL Step",
                    [this]() {
                        if (connection.getSaslServerContext()->getDomain() ==
                            cb::sasl::Domain::Local) {
                            doSaslStep();
                            // We need to notify with success here to avoid
                            // having the framework report the error
                            cookie.notifyIoComplete(cb::engine_errc::success);
                            return;
                        }
                        task = std::make_shared<SaslAuthTask>(
                                cookie,
                                *connection.getSaslServerContext(),
                                mechanism,
                                challenge);
                        externalAuthManager->enqueueRequest(*task);
                    },
                    ConcurrencySemaphores::instance().authentication));
    return cb::engine_errc::would_block;
}

void SaslStepCommandContext::doSaslStep() {
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer(cookie, cb::tracing::Code::Sasl);

    auto& serverContext = *connection.getSaslServerContext();
    try {
        auto [e, p] = serverContext.step(challenge);
        error = e;
        payload = p;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StepSaslAuthTask::execute(): std::bad_alloc",
                    cookie.getConnectionId());
        error = cb::sasl::Error::NO_MEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        if (serverContext.containsUuid()) {
            cookie.setEventId(serverContext.getUuid());
        }
        LOG_WARNING(
                "{}: StepSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                cookie.getConnectionId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        error = cb::sasl::Error::FAIL;
    }
}
