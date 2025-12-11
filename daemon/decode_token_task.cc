/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "decode_token_task.h"
#include "connection.h"
#include "cookie.h"

DecodeTokenTask::DecodeTokenTask(Cookie& cookie_, const std::string& token)
    : cookie(cookie_) {
    challenge = "n,,";
    challenge.push_back(0x01);
    challenge.append(fmt::format("auth=Bearer {}", token));
    challenge.push_back(0x01);
    challenge.push_back(0x01);
}

void DecodeTokenTask::externalResponse(cb::mcbp::Status statusCode,
                                       std::string_view payload) {
    result = {std::string(payload), statusCode};
    cookie.notifyIoComplete(cb::engine_errc::success);
}
