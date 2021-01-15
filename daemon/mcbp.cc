/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "mcbp.h"

#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "cookie_trace_context.h"
#include "front_end_thread.h"
#include "utilities/logtags.h"
#include "xattr/utils.h"
#include <logger/logger.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
#include <platform/string_hex.h>

static bool mcbp_response_handler(std::string_view key,
                                  std::string_view extras,
                                  std::string_view body,
                                  protocol_binary_datatype_t datatype,
                                  cb::mcbp::Status status,
                                  uint64_t cas,
                                  const void* void_cookie) {
    auto* ccookie = reinterpret_cast<const Cookie*>(void_cookie);
    auto* cookie = const_cast<Cookie*>(ccookie);

    Connection* c = &cookie->getConnection();
    cb::compression::Buffer buffer;
    auto payload = body;

    if ((!c->isSnappyEnabled() && mcbp::datatype::is_snappy(datatype)) ||
        (mcbp::datatype::is_snappy(datatype) &&
         mcbp::datatype::is_xattr(datatype))) {
        // The client is not snappy-aware, and the content contains
        // snappy encoded data. Or it's xattr compressed. We need to inflate it!
        if (!cookie->inflateSnappy(payload, buffer)) {
            std::string mykey(key.data(), key.size());
            LOG_WARNING(
                    "<{} ERROR: Failed to inflate body, "
                    "Key: {} may have an incorrect datatype, "
                    "Datatype indicates that document is {}",
                    c->getId(),
                    cb::UserDataView(mykey),
                    mcbp::datatype::to_string(datatype));
            return false;
        }
        payload = buffer;
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    }

    if (mcbp::datatype::is_xattr(datatype)) {
        // We need to strip off the xattrs
        payload = cb::xattr::get_body(payload);
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    datatype = c->getEnabledDatatypes(datatype);
    auto& error_json = cookie->getErrorJson();

    switch (status) {
    case cb::mcbp::Status::Success:
    case cb::mcbp::Status::SubdocSuccessDeleted:
    case cb::mcbp::Status::SubdocMultiPathFailure:
    case cb::mcbp::Status::Rollback:
        break;
    case cb::mcbp::Status::NotMyVbucket:
        cookie->sendNotMyVBucket();
        return true;
    default:
        //
        payload = {error_json.data(), error_json.size()};
        key = {};
        extras = {};
        datatype = payload.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                   : PROTOCOL_BINARY_DATATYPE_JSON;
    }

    const auto& request = cookie->getRequest();
    cb::mcbp::Response response = {};
    response.setMagic(cb::mcbp::Magic::ClientResponse);
    response.setOpcode(request.getClientOpcode());
    response.setDatatype(cb::mcbp::Datatype(datatype));
    response.setStatus(status);
    response.setFramingExtraslen(0);
    response.setExtlen(extras.size());
    response.setKeylen(key.size());
    response.setBodylen(extras.size() + key.size() + payload.size());
    response.setOpaque(request.getOpaque());
    response.setCas(cas);
    c->copyToOutputStream(
            {reinterpret_cast<const char*>(&response), sizeof(response)});
    c->copyToOutputStream(extras);
    c->copyToOutputStream(key);
    c->copyToOutputStream(payload);

    ++c->getBucket().responseCounters[uint16_t(status)];
    return true;
}

// Expose a static std::function to wrap mcbp_response_handler, instead of
// creating a temporary object every time we need to call into an engine.
// This also avoids problems where a stack-allocated AddResponseFn could go
// out of scope if someone (e.g. ep-engine's FetchAllKeysTask) needs to take
// a copy of it and run it on another thread.
//
AddResponseFn mcbpResponseHandlerFn = mcbp_response_handler;
