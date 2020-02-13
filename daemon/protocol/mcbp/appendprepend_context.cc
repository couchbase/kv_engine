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

ENGINE_ERROR_CODE AppendPrependCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
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
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    if (ret == ENGINE_KEY_ENOENT) {
        // for some reason the return code for no key is not stored so we need
        // to remap that error code..
        ret = ENGINE_NOT_STORED;
    }

    if (ret != ENGINE_EWOULDBLOCK) {
        SLAB_INCR(&connection, cmd_set);
    }

    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::getItem() {
    auto ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    if (ret.first == cb::engine_errc::success) {
        olditem = std::move(ret.second);
        if (!bucket_get_item_info(connection, olditem.get(), &oldItemInfo)) {
            return ENGINE_FAILED;
        }

        if (cas != 0) {
            if (oldItemInfo.cas == uint64_t(-1)) {
                // The object in the cache is locked... lets try to use
                // the cas provided by the user to override this
                oldItemInfo.cas = cas;
            } else if (cas != oldItemInfo.cas) {
                return ENGINE_KEY_EEXISTS;
            }
        } else if (oldItemInfo.cas == uint64_t(-1)) {
            return ENGINE_LOCKED;
        }

        if (mcbp::datatype::is_snappy(oldItemInfo.datatype)) {
            try {
                cb::const_char_buffer payload(static_cast<const char*>(
                                              oldItemInfo.value[0].iov_base),
                                              oldItemInfo.value[0].iov_len);
                if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                              payload,
                                              buffer)) {
                    return ENGINE_FAILED;
                }
            } catch (const std::bad_alloc&) {
                return ENGINE_ENOMEM;
            }
        }

        // Move on to the next state
        state = State::AllocateNewItem;
    }

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE AppendPrependCommandContext::allocateNewItem() {
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
    if (mcbp::datatype::is_xattr(oldItemInfo.datatype)) {
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
        memcpy(body.buf, old.buf, old.len);
        memcpy(body.buf + old.len, value.buf, value.len);
    } else {
        // The xattrs should go first (body_offset == 0 if the object
        // don't have any xattrs)
        memcpy(body.buf, old.buf, body_offset);
        memcpy(body.buf + body_offset, value.buf, value.len);
        memcpy(body.buf + body_offset + value.len, old.buf + body_offset,
               old.len - body_offset);
    }
    // If the resulting document's data is valid JSON, set the datatype flag
    // to reflect this.
    cb::const_byte_buffer buf{
            reinterpret_cast<const uint8_t*>(body.buf + body_offset),
            old.len + value.len};
    // Update the documents's datatype and CAS values
    setDatatypeJSONFromValue(buf, datatype);
    bucket_item_set_datatype(connection, newitem.get(), datatype);
    bucket_item_set_cas(connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            newitem.get(),
                            ncas,
                            OPERATION_CAS,
                            cookie.getRequest().getDurabilityRequirements(),
                            DocumentState::Alive,
                            false);

    if (ret == ENGINE_SUCCESS) {
        update_topkeys(cookie);
        cookie.setCas(ncas);
        if (connection.isSupportsMutationExtras()) {
            item_info newItemInfo;
            if (!bucket_get_item_info(
                        connection, newitem.get(), &newItemInfo)) {
                return ENGINE_FAILED;
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
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
        // We need to return ENGINE_SUCCESS in order to continue processing
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::reset() {
    olditem.reset();
    newitem.reset();
    buffer.reset();

    state = State::GetItem;
    return ENGINE_SUCCESS;
}
