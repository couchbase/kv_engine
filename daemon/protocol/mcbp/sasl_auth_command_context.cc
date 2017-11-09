/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include <daemon/mcbp.h>
#include <daemon/sasl_tasks.h>
#include <daemon/runtime.h>
#include <daemon/mcaudit.h>
#include "sasl_auth_command_context.h"

ENGINE_ERROR_CODE SaslAuthCommandContext::initial() {
    if (!connection.isSaslAuthEnabled()) {
        return ENGINE_ENOTSUP;
    }

    auto k = request.getKey();
    auto v = request.getValue();

    std::string mechanism(reinterpret_cast<const char*>(k.data()), k.size());
    std::string challenge(reinterpret_cast<const char*>(v.data()), v.size());

    LOG_DEBUG(&connection, "%u: SASL auth with mech: '%s' with %d bytes of data",
              connection.getId(), mechanism.c_str(), v.size());

    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::SaslAuth) {
        task = std::make_shared<StartSaslAuthTask>(
                cookie, connection, mechanism, challenge);
    } else if (request.getClientOpcode() == cb::mcbp::ClientOpcode::SaslStep) {
        task = std::make_shared<StepSaslAuthTask>(
                cookie, connection, mechanism, challenge);
    } else {
        throw std::logic_error(
            "SaslAuthCommandContext() used with illegal opcode");
    }

    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);

    state = State::ParseAuthTaskResult;
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE SaslAuthCommandContext::parseAuthTaskResult() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());

    switch (auth_task->getError()) {
    case CBSASL_OK:
        state = State::AuthOk;
        return ENGINE_SUCCESS;
    case CBSASL_CONTINUE:
        state = State::AuthContinue;
        return ENGINE_SUCCESS;
    case CBSASL_BADPARAM:
        state = State::AuthBadParameters;
        return ENGINE_SUCCESS;
    default:
        state = State::AuthFailure;
        return ENGINE_SUCCESS;
    }
}

ENGINE_ERROR_CODE SaslAuthCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::Initial:
            ret = initial();
            break;
        case State::ParseAuthTaskResult:
            ret = parseAuthTaskResult();
            break;
        case State::AuthOk:
            ret = authOk();
            break;
        case State::AuthContinue:
            ret = authContinue();
            break;
        case State::AuthBadParameters:
            ret = authBadParameters();
            break;
        case State::AuthFailure:
            ret = authFailure();
            break;

        case State::Done:
            return ENGINE_SUCCESS;

        }
    } while (ret == ENGINE_SUCCESS);

    return ret;
}

ENGINE_ERROR_CODE SaslAuthCommandContext::authOk() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    auto payload = auth_task->getResponse();
    mcbp_write_response(&connection, payload.data(), 0, 0, payload.size());
    get_thread_stats(&connection)->auth_cmds++;
    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE SaslAuthCommandContext::authContinue() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    auto payload = auth_task->getResponse();
    mcbp_add_header(&connection,
                    PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE,
                    0,
                    0,
                    payload.size(),
                    PROTOCOL_BINARY_RAW_BYTES);
    connection.addIov(payload.data(), payload.size());
    connection.setState(McbpStateMachine::State::send_data);
    connection.setWriteAndGo(McbpStateMachine::State::new_cmd);

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE SaslAuthCommandContext::authBadParameters() {
    auto* ts = get_thread_stats(&connection);
    ts->auth_cmds++;
    ts->auth_errors++;
    return ENGINE_EINVAL;
}

ENGINE_ERROR_CODE SaslAuthCommandContext::authFailure() {
    state = State::Done;

    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    if (auth_task->getError() == CBSASL_NOUSER ||
        auth_task->getError() == CBSASL_PWERR) {
        audit_auth_failure(&connection,
                           auth_task->getError() == CBSASL_NOUSER
                           ? "Unknown user"
                           : "Incorrect password");
    }
    cookie.sendResponse(cb::mcbp::Status::AuthError);

    auto* ts = get_thread_stats(&connection);
    ts->auth_cmds++;
    ts->auth_errors++;

    return ENGINE_SUCCESS;
}
