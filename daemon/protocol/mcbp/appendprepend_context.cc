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
#include "appendprepend_context.h"
#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <daemon/stats.h>
#include <memcached/durability_spec.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

AppendPrependCommandContext::AppendPrependCommandContext(
        Cookie& cookie, const cb::mcbp::Request& req)
    : SteppableCommandContext(cookie),
      mode((req.getClientOpcode() == cb::mcbp::ClientOpcode::Append ||
            req.getClientOpcode() == cb::mcbp::ClientOpcode::Appendq)
                   ? Mode::Append
                   : Mode::Prepend),
      vbucket(req.getVBucket()),
      cas(req.getCas()),
      state(State::GetItem) {
}

cb::engine_errc AppendPrependCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
            break;
        case State::AllocateNewItem:
            ret = allocateNewItem();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::Reset:
            ret = reset();
            break;
        case State::Done:
	    SLAB_INCR(&connection, cmd_set);
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    if (ret == cb::engine_errc::no_such_key) {
        // for some reason the return code for no key is not stored so we need
        // to remap that error code..
        ret = cb::engine_errc::not_stored;
    }

    if (ret != cb::engine_errc::would_block) {
        SLAB_INCR(&connection, cmd_set);
    }

    return ret;
}

cb::engine_errc AppendPrependCommandContext::getItem() {
    auto ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    if (ret.first == cb::engine_errc::success) {
        olditem = std::move(ret.second);
        if (!bucket_get_item_info(connection, olditem.get(), &oldItemInfo)) {
            return cb::engine_errc::failed;
        }

        if (cas != 0) {
            if (oldItemInfo.cas == uint64_t(-1)) {
                // The object in the cache is locked... lets try to use
                // the cas provided by the user to override this
                oldItemInfo.cas = cas;
            } else if (cas != oldItemInfo.cas) {
                return cb::engine_errc::key_already_exists;
            }
        } else if (oldItemInfo.cas == uint64_t(-1)) {
            return cb::engine_errc::locked;
        }

        if (cb::mcbp::datatype::is_snappy(oldItemInfo.datatype)) {
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
    }

    return cb::engine_errc(ret.first);
}

cb::engine_errc AppendPrependCommandContext::allocateNewItem() {
    cb::char_buffer old{static_cast<char*>(oldItemInfo.value[0].iov_base),
                        oldItemInfo.nbytes};

    if (!buffer.empty()) {
        old = {buffer.data(), buffer.size()};
    }

    // If we're operating on a document containing xattr's we need to
    // tell the underlying engine about how much of the data which
    // should be accounted for in the privileged segment.
    size_t priv_size = 0;

    // The offset into the old item where the actual body start.
    size_t body_offset = 0;

    // If the existing item had XATTRs we need to preserve the xattrs
    auto datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (cb::mcbp::datatype::is_xattr(oldItemInfo.datatype)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;

        // Calculate the size of the system xattr's. We know they arn't
        // compressed as we are already using the decompression buffer as
        // input (see head of function).
        cb::xattr::Blob blob(old, false);
        body_offset = blob.size();
        priv_size = blob.get_system_size();
    }

    // If the client sent a compressed value we should use the one
    // we inflated
    const auto value = cookie.getInflatedInputPayload();
    auto pair = bucket_allocate_ex(cookie,
                                   cookie.getRequestKey(),
                                   old.size() + value.size(),
                                   priv_size,
                                   oldItemInfo.flags,
                                   (rel_time_t)oldItemInfo.exptime,
                                   datatype,
                                   vbucket);

    newitem = std::move(pair.first);
    cb::byte_buffer body{static_cast<uint8_t*>(pair.second.value[0].iov_base),
                         pair.second.value[0].iov_len};

    // copy the data over..
    if (mode == Mode::Append) {
        memcpy(body.data(), old.data(), old.size());
        memcpy(body.data() + old.size(), value.data(), value.size());
    } else {
        // The xattrs should go first (body_offset == 0 if the object
        // don't have any xattrs)
        memcpy(body.data(), old.data(), body_offset);
        memcpy(body.data() + body_offset, value.data(), value.size());
        memcpy(body.data() + body_offset + value.size(),
               old.data() + body_offset,
               old.size() - body_offset);
    }
    // If the resulting document's data is valid JSON, set the datatype flag
    // to reflect this.
    cb::const_byte_buffer buf{
            reinterpret_cast<const uint8_t*>(body.data() + body_offset),
            old.size() + value.size()};
    // Update the documents's datatype and CAS values
    setDatatypeJSONFromValue(buf, datatype);
    bucket_item_set_datatype(connection, newitem.get(), datatype);
    bucket_item_set_cas(connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;

    return cb::engine_errc::success;
}

cb::engine_errc AppendPrependCommandContext::storeItem() {
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
        if (connection.isSupportsMutationExtras()) {
            item_info newItemInfo;
            if (!bucket_get_item_info(
                        connection, newitem.get(), &newItemInfo)) {
                return cb::engine_errc::failed;
            }
            mutation_descr_t extras = {};
            extras.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
            extras.seqno = htonll(newItemInfo.seqno);
            cookie.sendResponse(
                    cb::mcbp::Status::Success,
                    {reinterpret_cast<const char*>(&extras), sizeof(extras)},
                    {},
                    {},
                    cb::mcbp::Datatype::Raw,
                    ncas);
        } else {
            cookie.sendResponse(cb::mcbp::Status::Success);
        }
        state = State::Done;
    } else if (ret == cb::engine_errc::key_already_exists && cas == 0) {
        state = State::Reset;
        // We need to return cb::engine_errc::success in order to continue
        // processing
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc AppendPrependCommandContext::reset() {
    olditem.reset();
    newitem.reset();
    buffer.reset();

    state = State::GetItem;
    return cb::engine_errc::success;
}
