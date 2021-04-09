/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "session_validated_command_context.h"
#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <daemon/mcbp.h>

#include "../../session_cas.h"

SessionValidatedCommandContext::SessionValidatedCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      valid(session_cas.increment_session_counter(
              cookie.getRequest().getCas())) {
}

SessionValidatedCommandContext::~SessionValidatedCommandContext() {
    if (valid) {
        session_cas.decrement_session_counter();
    }
}
cb::engine_errc SessionValidatedCommandContext::step() {
    if (!valid) {
        return cb::engine_errc::key_already_exists;
    }

    const auto ret = sessionLockedStep();
    if (ret == cb::engine_errc::success) {
        // Send the status back to the caller!
        cookie.setCas(cookie.getRequest().getCas());
        cookie.sendResponse(cb::engine_errc::success);
    }
    return cb::engine_errc(ret);
}

static EngineParamCategory getParamCategory(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    using cb::mcbp::request::SetParamPayload;
    auto extras = req.getExtdata();
    auto* payload = reinterpret_cast<const SetParamPayload*>(extras.data());
    switch (payload->getParamType()) {
    case SetParamPayload::Type::Flush:
        return EngineParamCategory::Flush;
    case SetParamPayload::Type::Replication:
        return EngineParamCategory::Replication;
    case SetParamPayload::Type::Checkpoint:
        return EngineParamCategory::Checkpoint;
    case SetParamPayload::Type::Dcp:
        return EngineParamCategory::Dcp;
    case SetParamPayload::Type::Vbucket:
        return EngineParamCategory::Vbucket;
    }
    throw std::invalid_argument("getParamCategory(): Invalid param provided: " +
                                std::to_string(int(payload->getParamType())));
}

SetParameterCommandContext::SetParameterCommandContext(Cookie& cookie)
    : SessionValidatedCommandContext(cookie),
      category(getParamCategory(cookie)) {
}

cb::engine_errc SetParameterCommandContext::sessionLockedStep() {
    // We can't cache the key and value as a ewb would relocate them
    const auto& req = cookie.getRequest();
    auto key = req.getKey();
    auto value = req.getValue();

    return bucket_set_parameter(
            cookie,
            category,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            {reinterpret_cast<const char*>(value.data()), value.size()},
            req.getVBucket());
}

cb::engine_errc CompactDatabaseCommandContext::sessionLockedStep() {
    return bucket_compact_database(cookie);
}

cb::engine_errc GetVbucketCommandContext::step() {
    const auto [status, state] = bucket_get_vbucket(cookie);
    if (status == cb::engine_errc::success) {
        uint32_t st = ntohl(uint32_t(state));
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {reinterpret_cast<const char*>(&st), sizeof(st)},
                            cb::mcbp::Datatype::Raw,
                            mcbp::cas::Wildcard);
    }
    return cb::engine_errc(status);
}

SetVbucketCommandContext::SetVbucketCommandContext(Cookie& cookie)
    : SessionValidatedCommandContext(cookie) {
    const auto& req = cookie.getRequest();
    auto extras = req.getExtdata();

    try {
        if (extras.size() == 1) {
            // This is the new encoding for the SetVBucket state.
            state = vbucket_state_t(extras.front());
            auto val = req.getValue();
            if (!val.empty()) {
                if (state != vbucket_state_active) {
                    throw std::invalid_argument(
                            "vbucket meta may only be set on active vbuckets");
                }

                meta = nlohmann::json::parse(val);
            }
        } else {
            // This is the pre-mad-hatter encoding for the SetVBucketState
            if (extras.size() != sizeof(vbucket_state_t)) {
                // MB-31867: ns_server encodes this in the value field. Fall
                // back
                //           and check if it contains the value
                extras = req.getValue();
            }

            state = static_cast<vbucket_state_t>(
                    ntohl(*reinterpret_cast<const uint32_t*>(extras.data())));
        }
    } catch (const std::exception& exception) {
        error = exception.what();
    }
}

cb::engine_errc SetVbucketCommandContext::sessionLockedStep() {
    if (!error.empty()) {
        cookie.setErrorContext(error);
        cookie.sendResponse(cb::mcbp::Status::Einval);
        // and complete the execution of the command
        return cb::engine_errc::success;
    }

    return bucket_set_vbucket(cookie, state, meta);
}

cb::engine_errc DeleteVbucketCommandContext::sessionLockedStep() {
    const auto& req = cookie.getRequest();
    auto value = req.getValue();
    bool sync = value.size() == 7 && memcmp(value.data(), "async=0", 7) == 0;
    return bucket_delete_vbucket(cookie, req.getVBucket(), sync);
}
