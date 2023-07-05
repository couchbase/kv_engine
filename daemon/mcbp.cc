/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp.h"
#include "connection.h"
#include "cookie.h"

static void mcbp_response_handler(std::string_view key,
                                  std::string_view extras,
                                  std::string_view body,
                                  ValueIsJson json,
                                  cb::mcbp::Status status,
                                  uint64_t cas,
                                  CookieIface& cookieIface) {
    auto datatype = json == ValueIsJson::Yes ? PROTOCOL_BINARY_DATATYPE_JSON
                                             : PROTOCOL_BINARY_RAW_BYTES;

    auto& cookie = asCookie(cookieIface);
    Connection& c = cookie.getConnection();
    auto payload = body;
    const auto error_json = cookie.getErrorJson();

    switch (status) {
    case cb::mcbp::Status::Success:
    case cb::mcbp::Status::SubdocSuccessDeleted:
    case cb::mcbp::Status::SubdocMultiPathFailure:
    case cb::mcbp::Status::Rollback:
        break;
    case cb::mcbp::Status::NotMyVbucket:
        cookie.sendNotMyVBucket();
        return;
    default:
        //
        payload = error_json;
        key = {};
        extras = {};
        datatype = payload.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                   : PROTOCOL_BINARY_DATATYPE_JSON;
    }

    cookie.setCas(cas);
    c.sendResponse(cookie,
                   status,
                   extras,
                   key,
                   payload,
                   c.getEnabledDatatypes(datatype),
                   {});
    return;
}

// Expose a static std::function to wrap mcbp_response_handler, instead of
// creating a temporary object every time we need to call into an engine.
// This also avoids problems where a stack-allocated AddResponseFn could go
// out of scope if someone (e.g. ep-engine's FetchAllKeysTask) needs to take
// a copy of it and run it on another thread.
//
AddResponseFn mcbpResponseHandlerFn = mcbp_response_handler;
