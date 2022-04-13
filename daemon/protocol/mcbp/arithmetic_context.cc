/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "arithmetic_context.h"

#include "engine_wrapper.h"
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <mcbp/protocol/header.h>
#include <memcached/durability_spec.h>
#include <memcached/util.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

using cb::mcbp::request::ArithmeticPayload;

ArithmeticCommandContext::ArithmeticCommandContext(Cookie& cookie,
                                                   const cb::mcbp::Request& req)
    : SteppableCommandContext(cookie),
      extras(req.getCommandSpecifics<ArithmeticPayload>()),
      cas(req.getCas()),
      vbucket(req.getVBucket()),
      increment(req.getClientOpcode() == cb::mcbp::ClientOpcode::Increment ||
                req.getClientOpcode() == cb::mcbp::ClientOpcode::Incrementq) {
}

cb::engine_errc ArithmeticCommandContext::getItem() {
    auto ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    if (ret.first == cb::engine_errc::success) {
        olditem = std::move(ret.second);

        if (!bucket_get_item_info(connection, olditem.get(), &oldItemInfo)) {
            return cb::engine_errc::failed;
        }

        uint64_t oldcas = oldItemInfo.cas;
        if (cas != 0 && cas != oldcas) {
            return cb::engine_errc::key_already_exists;
        }

        if (mcbp::datatype::is_snappy(oldItemInfo.datatype)) {
            try {
                std::string_view payload(
                        static_cast<const char*>(oldItemInfo.value[0].iov_base),
                        oldItemInfo.value[0].iov_len);
                if (!cookie.inflateSnappy(payload, buffer)) {
                    return cb::engine_errc::failed;
                }
            } catch (const std::bad_alloc&) {
                return cb::engine_errc::no_memory;
            }
        }

        // Move on to the next state
        state = State::AllocateNewItem;
    } else if (ret.first == cb::engine_errc::no_such_key) {
        if (extras.getExpiration() != 0xffffffff) {
            state = State::CreateNewItem;
            ret.first = cb::engine_errc::success;
        } else {
            if (increment) {
                STATS_INCR(&connection, incr_misses);
            } else {
                STATS_INCR(&connection, decr_misses);
            }
        }
    }

    return cb::engine_errc(ret.first);
}

cb::engine_errc ArithmeticCommandContext::createNewItem() {
    const std::string value{std::to_string(extras.getInitial())};
    result = extras.getInitial();

    auto pair = bucket_allocate_ex(cookie,
                                   cookie.getRequestKey(),
                                   value.size(),
                                   0, // no privileged bytes
                                   0, // Empty flags
                                   extras.getExpiration(),
                                   PROTOCOL_BINARY_DATATYPE_JSON,
                                   vbucket);
    newitem = std::move(pair.first);

    memcpy(pair.second.value[0].iov_base, value.data(), value.size());
    state = State::StoreNewItem;

    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::storeNewItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            newitem.get(),
                            ncas,
                            StoreSemantics::Add,
                            cookie.getRequest().getDurabilityRequirements(),
                            DocumentState::Alive,
                            false);

    if (ret == cb::engine_errc::success) {
        cookie.setCas(ncas);
        state = State::SendResult;
    } else if (ret == cb::engine_errc::key_already_exists && cas == 0) {
        state = State::Reset;
        ret = cb::engine_errc::success;
    } else if (ret == cb::engine_errc::not_stored) {
        // hit race condition, item with our key was created between our call
        // to bucket_store() and when we checked to see if it existed, by
        // calling bucket_get() so just re-try by resetting our state to the
        // start again.
        state = State::Reset;
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc ArithmeticCommandContext::allocateNewItem() {
    // Set ptr to point to the beginning of the input buffer.
    size_t oldsize = oldItemInfo.nbytes;
    auto* ptr = static_cast<char*>(oldItemInfo.value[0].iov_base);
    // If the input buffer was compressed we should use the temporary
    // allocated buffer instead
    if (!buffer.empty()) {
        ptr = buffer.data();
        oldsize = buffer.size();
    }

    // Preserve the XATTRs of the existing item if it had any
    size_t xattrsize = 0;
    size_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(oldItemInfo.datatype)) {
        cb::xattr::Blob blob({ptr, oldsize},
                             mcbp::datatype::is_snappy(oldItemInfo.datatype));
        priv_bytes = blob.get_system_size();
        xattrsize = blob.size();
    }
    ptr += xattrsize;
    const std::string payload(ptr, oldsize - xattrsize);

    uint64_t oldval;
    if (!safe_strtoull(payload, oldval)) {
        return cb::engine_errc::delta_badval;
    }

    auto delta = extras.getDelta();

    // perform the op ;)
    if (increment) {
        oldval += delta;
    } else {
        if (oldval < delta) {
            oldval = 0;
        } else {
            oldval -= delta;
        }
    }
    result = oldval;
    const std::string value = std::to_string(result);

    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (xattrsize > 0) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    // In order to be backwards compatible with old Couchbase server we
    // continue to use the old expiry time:
    auto pair = bucket_allocate_ex(cookie,
                                   cookie.getRequestKey(),
                                   xattrsize + value.size(),
                                   priv_bytes,
                                   oldItemInfo.flags,
                                   rel_time_t(oldItemInfo.exptime),
                                   datatype,
                                   vbucket);

    newitem = std::move(pair.first);
    cb::byte_buffer body{static_cast<uint8_t*>(pair.second.value[0].iov_base),
                         pair.second.value[0].iov_len};

    // copy the data over..
    const auto* src = (const char*)oldItemInfo.value[0].iov_base;
    if (!buffer.empty()) {
        src = buffer.data();
    }

    // copy the xattr over;
    memcpy(body.data(), src, xattrsize);
    memcpy(body.data() + xattrsize, value.data(), value.size());
    bucket_item_set_cas(connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;
    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            newitem.get(),
                            ncas,
                            StoreSemantics::CAS,
                            cookie.getRequest().getDurabilityRequirements(),
                            DocumentState::Alive,
                            false);

    if (ret == cb::engine_errc::success) {
        cookie.setCas(ncas);
        state = State::SendResult;
    } else if (ret == cb::engine_errc::key_already_exists && cas == 0) {
        state = State::Reset;
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc ArithmeticCommandContext::sendResult() {
    state = State::Done;
    const auto opcode = cookie.getHeader().getRequest().getClientOpcode();
    if ((opcode == cb::mcbp::ClientOpcode::Increment) ||
        (opcode == cb::mcbp::ClientOpcode::Incrementq)) {
        STATS_INCR(&connection, incr_hits);
    } else {
        STATS_INCR(&connection, decr_hits);
    }

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::Success)];
        return cb::engine_errc::success;
    }

    mutation_descr_t mutation_descr{};
    std::string_view extras = {reinterpret_cast<const char*>(&mutation_descr),
                               sizeof(mutation_descr_t)};
    result = ntohll(result);
    std::string_view value = {reinterpret_cast<const char*>(&result),
                              sizeof(result)};

    if (connection.isSupportsMutationExtras()) {
        item_info newItemInfo;
        if (!bucket_get_item_info(connection, newitem.get(), &newItemInfo)) {
            return cb::engine_errc::failed;
        }

        // Response includes vbucket UUID and sequence number
        // (in addition to value)
        mutation_descr.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
        mutation_descr.seqno = htonll(newItemInfo.seqno);
    } else {
        extras = {};
    }

    cookie.addDocumentReadBytes(extras.size() + value.size());
    cookie.sendResponse(cb::mcbp::Status::Success,
                        extras,
                        {},
                        value,
                        cb::mcbp::Datatype::Raw,
                        cookie.getCas());

    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::reset() {
    olditem.reset();
    newitem.reset();
    buffer.reset();
    state = State::GetItem;
    return cb::engine_errc::success;
}
