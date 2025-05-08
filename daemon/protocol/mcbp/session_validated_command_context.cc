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
#include <daemon/session_cas.h>
#include <logger/logger.h>

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
    return ret;
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
                            cb::mcbp::cas::Wildcard);
    }
    return status;
}

SetVbucketCommandContext::SetVbucketCommandContext(Cookie& cookie)
    : SessionValidatedCommandContext(cookie) {
    const auto& req = cookie.getRequest();
    const auto extras = req.getExtdata();
    try {
        state = vbucket_state_t(extras.front());
        auto val = req.getValueString();
        if (!val.empty()) {
            meta = nlohmann::json::parse(val);
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
