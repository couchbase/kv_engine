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
#include "cookie.h"
#include "cookie_trace_context.h"
#include "memcached.h"
#include "opentracing.h"
#include "settings.h"
#include "utilities/logtags.h"
#include "xattr/utils.h"
#include <logger/logger.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/string_hex.h>

static cb::const_byte_buffer mcbp_add_header(Cookie& cookie,
                                             cb::Pipe& pipe,
                                             uint8_t opcode,
                                             cb::mcbp::Status status,
                                             uint8_t ext_len,
                                             uint16_t key_len,
                                             uint32_t body_len,
                                             uint8_t datatype,
                                             uint32_t opaque,
                                             uint64_t cas) {
    auto wbuf = pipe.wdata();
    auto* header = (protocol_binary_response_header*)wbuf.data();

    header->response.setOpcode(cb::mcbp::ClientOpcode(opcode));
    header->response.setExtlen(ext_len);
    header->response.setDatatype(cb::mcbp::Datatype(datatype));
    header->response.setStatus(status);
    header->response.setOpaque(opaque);
    header->response.setCas(cas);

    if (cookie.isTracingEnabled()) {
        // When tracing is enabled we'll be using the alternative
        // response header where we inject the framing header.
        // For now we'll just hard-code the adding of the bytes
        // for the tracing info.
        //
        // Moving forward we should get a builder for encoding the
        // framing header (but do that the next time we need to add
        // something so that we have a better understanding on how
        // we need to do that (it could be that we need to modify
        // an already existing section etc).
        header->response.setMagic(cb::mcbp::Magic::AltClientResponse);
        // The framing extras when we just include the tracing information
        // is 3 bytes. 1 byte with id and length, then the 2 bytes
        // containing the actual data.
        const uint8_t framing_extras_size = MCBP_TRACING_RESPONSE_SIZE;
        const uint8_t tracing_framing_id = 0x02;

        header->bytes[2] = framing_extras_size; // framing header extras 3 bytes
        header->bytes[3] = uint8_t(key_len);
        header->response.setBodylen(body_len + framing_extras_size);

        auto& tracer = cookie.getTracer();
        const auto val = htons(tracer.getEncodedMicros());
        auto* ptr = header->bytes + sizeof(header->bytes);
        *ptr = tracing_framing_id;
        ptr++;
        memcpy(ptr, &val, sizeof(val));
        pipe.produced(sizeof(header->bytes) + framing_extras_size);
        return {wbuf.data(), sizeof(header->bytes) + framing_extras_size};
    } else {
        header->response.setMagic(cb::mcbp::Magic::ClientResponse);
        header->response.setKeylen(key_len);
        header->response.setFramingExtraslen(0);
        header->response.setBodylen(body_len);
    }

    pipe.produced(sizeof(header->bytes));
    return {wbuf.data(), sizeof(header->bytes)};
}

void mcbp_add_header(Cookie& cookie,
                     cb::mcbp::Status status,
                     uint8_t ext_len,
                     uint16_t key_len,
                     uint32_t body_len,
                     uint8_t datatype) {
    auto& connection = cookie.getConnection();
    connection.addMsgHdr(true);
    const auto& header = cookie.getHeader();

    const auto wbuf = mcbp_add_header(cookie,
                                      *connection.write,
                                      header.getOpcode(),
                                      status,
                                      ext_len,
                                      key_len,
                                      body_len,
                                      datatype,
                                      header.getOpaque(),
                                      cookie.getCas());

    if (Settings::instance().getVerbose() > 1) {
        auto* header = reinterpret_cast<const cb::mcbp::Header*>(wbuf.data());
        try {
            LOG_TRACE("<{} Sending: {}",
                      connection.getId(),
                      header->toJSON(true).dump());
        } catch (const std::exception&) {
            // Failed.. do a raw dump instead
            LOG_TRACE("<{} Sending: {}",
                      connection.getId(),
                      cb::to_hex({wbuf.data(), sizeof(cb::mcbp::Header)}));
        }
    }

    ++connection.getBucket().responseCounters[uint16_t(status)];
    connection.addIov(wbuf.data(), wbuf.size());
}

static bool mcbp_response_handler(const void* key,
                                  uint16_t keylen,
                                  const void* ext,
                                  uint8_t extlen,
                                  const void* body,
                                  uint32_t bodylen,
                                  protocol_binary_datatype_t datatype,
                                  cb::mcbp::Status status,
                                  uint64_t cas,
                                  const void* void_cookie) {
    auto* ccookie = reinterpret_cast<const Cookie*>(void_cookie);
    auto* cookie = const_cast<Cookie*>(ccookie);

    Connection* c = &cookie->getConnection();
    cb::compression::Buffer buffer;
    cb::const_char_buffer payload(static_cast<const char*>(body), bodylen);

    if ((!c->isSnappyEnabled() && mcbp::datatype::is_snappy(datatype)) ||
        (mcbp::datatype::is_snappy(datatype) &&
         mcbp::datatype::is_xattr(datatype))) {
        // The client is not snappy-aware, and the content contains
        // snappy encoded data. Or it's xattr compressed. We need to inflate it!
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload, buffer)) {
            std::string mykey(reinterpret_cast<const char*>(key), keylen);
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
        keylen = 0;
        extlen = 0;
        datatype = payload.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                   : PROTOCOL_BINARY_DATATYPE_JSON;
    }

    const size_t needed = payload.len + keylen + extlen +
                          sizeof(protocol_binary_response_header);

    auto& dbuf = cookie->getDynamicBuffer();
    if (!dbuf.grow(needed)) {
        LOG_WARNING("<{} ERROR: Failed to allocate memory for response",
                    c->getId());
        return false;
    }

    auto* buf = reinterpret_cast<uint8_t*>(dbuf.getCurrent());
    const auto& header = cookie->getHeader();

    cb::mcbp::ResponseBuilder builder({buf, needed});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(header.getRequest().getClientOpcode());
    builder.setDatatype(cb::mcbp::Datatype(datatype));
    builder.setStatus(status);
    builder.setExtras({static_cast<const uint8_t*>(ext), extlen});
    builder.setKey({static_cast<const uint8_t*>(key), keylen});
    builder.setValue(
            {reinterpret_cast<const uint8_t*>(payload.data()), payload.size()});
    builder.setOpaque(header.getOpaque());
    builder.setCas(cas);
    builder.validate();

    ++c->getBucket().responseCounters[uint16_t(status)];
    dbuf.moveOffset(needed);
    return true;
}

// Expose a static std::function to wrap mcbp_response_handler, instead of
// creating a temporary object every time we need to call into an engine.
// This also avoids problems where a stack-allocated AddResponseFn could go
// out of scope if someone (e.g. ep-engine's FetchAllKeysTask) needs to take
// a copy of it and run it on another thread.
//
AddResponseFn mcbpResponseHandlerFn = mcbp_response_handler;

void mcbp_collect_timings(Cookie& cookie) {
    // The state machinery cause this method to be called for all kinds
    // of packets, but the header musts be a client request for the timings
    // to make sense (and not when we handled a ServerResponse message etc ;)
    const auto& header = cookie.getHeader();
    if (!header.isRequest()) {
        return;
    }

    auto* c = &cookie.getConnection();
    if (c->isDCP()) {
        // The state machinery works differently for the DCP connections
        // so these timings isn't accurate!
        //
        // For now disable the timings, and add them back once they're
        // correct
        return;
    }
    const auto opcode = header.getRequest().getClientOpcode();
    const auto endTime = std::chrono::steady_clock::now();
    const auto elapsed = endTime - cookie.getStart();
    cookie.getTracer().end(cb::tracing::SpanId{0}, endTime);

    // aggregated timing for all buckets
    all_buckets[0].timings.collect(opcode, elapsed);

    // timing for current bucket
    const auto bucketid = c->getBucketIndex();
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(opcode, elapsed);
    }

    // Log operations taking longer than the "slow" threshold for the opcode.
    cookie.maybeLogSlowCommand(elapsed);

    if (cookie.isOpenTracingEnabled()) {
        OpenTracing::pushTraceLog(cookie.extractTraceContext());
    }
}
