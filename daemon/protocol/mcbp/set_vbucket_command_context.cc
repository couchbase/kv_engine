/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "set_vbucket_command_context.h"
#include "engine_wrapper.h"
#include <nlohmann/json.hpp>

static vbucket_state_t getState(const Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto extras = req.getExtdata();
    return vbucket_state_t(extras.front());
}

static nlohmann::json getMeta(const Cookie& cookie) {
    const auto& req = cookie.getRequest();
    auto val = req.getValueString();
    if (!val.empty()) {
        return nlohmann::json::parse(val);
    }
    return {};
}

SetVbucketCommandContext::SetVbucketCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      state(getState(cookie)),
      vbid(cookie.getRequest().getVBucket()),
      meta(getMeta(cookie)) {
}

cb::engine_errc SetVbucketCommandContext::step() {
    const auto ret = bucket_set_vbucket(cookie, state, meta);
    if (ret == cb::engine_errc::success) {
        cookie.sendResponse(cb::engine_errc::success);
    }
    return ret;
}
