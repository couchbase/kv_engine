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
#include "item_dissector.h"
#include <daemon/buckets.h>
#include <daemon/cookie.h>
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
        if (cas != 0 && cas != ret.second->getCas()) {
            return cb::engine_errc::key_already_exists;
        }

        old_item = std::make_unique<ItemDissector>(
                cookie, std::move(ret.second), true);

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

    return ret.first;
}

cb::engine_errc ArithmeticCommandContext::createNewItem() {
    const std::string value{std::to_string(extras.getInitial())};
    result = extras.getInitial();

    newitem = bucket_allocate(cookie,
                              cookie.getRequestKey(),
                              value.size(),
                              0, // no privileged bytes
                              0, // Empty flags
                              extras.getExpiration(),
                              PROTOCOL_BINARY_DATATYPE_JSON,
                              vbucket);
    std::copy(value.begin(), value.end(), newitem->getValueBuffer().begin());
    state = State::StoreNewItem;
    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::storeNewItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            *newitem,
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
    std::string string_value(old_item->getValue());
    uint64_t oldval;
    if (!safe_strtoull(string_value, oldval)) {
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
    auto xattrs = old_item->getExtendedAttributes();
    size_t priv_bytes = 0;
    if (!xattrs.empty()) {
        cb::xattr::Blob blob({const_cast<char*>(xattrs.data()), xattrs.size()},
                             false);
        priv_bytes = blob.get_system_size();
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    const auto& document = old_item->getItem();

    // In order to be backwards compatible with old Couchbase server we
    // continue to use the old expiry time:
    newitem = bucket_allocate(cookie,
                              cookie.getRequestKey(),
                              value.size() + xattrs.size(),
                              priv_bytes,
                              document.getFlags(),
                              rel_time_t(document.getExptime()),
                              datatype,
                              vbucket);
    auto body = newitem->getValueBuffer();

    // Copy the data over.
    std::copy(xattrs.begin(), xattrs.end(), body.begin());
    std::copy(value.begin(), value.end(), body.data() + xattrs.size());
    newitem->setCas(document.getCas());

    state = State::StoreItem;
    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            *newitem,
                            ncas,
                            StoreSemantics::CAS,
                            cookie.getRequest().getDurabilityRequirements(),
                            DocumentState::Alive,
                            false);

    if (ret == cb::engine_errc::success) {
        cookie.setCas(ncas);
        state = State::SendResult;
    } else if (cas == 0 && (ret == cb::engine_errc::key_already_exists ||
                            ret == cb::engine_errc::no_such_key)) {
        // CAS mismatch case (either doc exists with different cas or no longer
        // exists).
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
    std::string_view extdata = {reinterpret_cast<const char*>(&mutation_descr),
                                sizeof(mutation_descr_t)};
    result = ntohll(result);
    std::string_view value = {reinterpret_cast<const char*>(&result),
                              sizeof(result)};

    if (connection.isSupportsMutationExtras()) {
        item_info newItemInfo;
        if (!bucket_get_item_info(connection, *newitem, newItemInfo)) {
            return cb::engine_errc::failed;
        }

        // Response includes vbucket UUID and sequence number
        // (in addition to value)
        mutation_descr.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
        mutation_descr.seqno = htonll(newItemInfo.seqno);
    } else {
        extdata = {};
    }

    cookie.sendResponse(cb::mcbp::Status::Success,
                        extdata,
                        {},
                        value,
                        cb::mcbp::Datatype::Raw,
                        cookie.getCas());

    return cb::engine_errc::success;
}

cb::engine_errc ArithmeticCommandContext::reset() {
    old_item.reset();
    newitem.reset();
    state = State::GetItem;
    return cb::engine_errc::success;
}
