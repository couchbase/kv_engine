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
#include "remove_context.h"
#include "engine_wrapper.h"
#include <daemon/mcbp.h>
#include <xattr/utils.h>
#include <xattr/blob.h>

ENGINE_ERROR_CODE RemoveCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
            break;
        case State::RebuildXattr:
            ret = rebuildXattr();
            break;
        case State::AllocateDeletedItem:
            ret = allocateDeletedItem();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::RemoveItem:
            ret = removeItem();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Reset:
            ret = reset();
            break;
        case State::Done:
            SLAB_INCR(&connection, delete_hits);
            update_topkeys(cookie);
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);


    if (ret == ENGINE_KEY_ENOENT) {
        STATS_INCR(&connection, delete_misses);
    }

    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::getItem() {
    auto ret = bucket_get(cookie, key, vbucket);
    if (ret.first == cb::engine_errc::success) {
        existing = std::move(ret.second);
        if (!bucket_get_item_info(cookie, existing.get(), &existing_info)) {
            return ENGINE_FAILED;
        }

        if (input_cas != 0) {
            if (existing_info.cas == uint64_t(-1)) {
                // The object in the cache is locked... lets try to use
                // the cas provided by the user to override this
                existing_info.cas = input_cas;
            } else if (input_cas != existing_info.cas) {
                return ENGINE_KEY_EEXISTS;
            }
        } else if (existing_info.cas == uint64_t(-1)) {
            return ENGINE_LOCKED;
        }

        if (mcbp::datatype::is_xattr(existing_info.datatype)) {
            state = State::RebuildXattr;
        } else {
            state = State::RemoveItem;
        }
    }
    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE RemoveCommandContext::allocateDeletedItem() {
    protocol_binary_datatype_t datatype;
    if (xattr.size() == 0) {
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    auto pair =
            bucket_allocate_ex(cookie,
                               key,
                               xattr.size(),
                               xattr.size(), // Only system xattrs
                               0, // MB-25273: 0 flags when deleting the body
                               existing_info.exptime,
                               datatype,
                               vbucket);

    deleted = std::move(pair.first);
    if (input_cas == 0) {
        bucket_item_set_cas(cookie, deleted.get(), existing_info.cas);
    } else {
        bucket_item_set_cas(cookie, deleted.get(), input_cas);
    }

    if (xattr.size() > 0) {
        std::memcpy(pair.second.value[0].iov_base, xattr.buf, xattr.size());
    }

    state = State::StoreItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE RemoveCommandContext::storeItem() {
    uint64_t new_cas;
    auto ret = bucket_store(cookie,
                            deleted.get(),
                            new_cas,
                            OPERATION_CAS,
                            DocumentState::Deleted);

    if (ret == ENGINE_SUCCESS) {

        item_info info;
        if (!bucket_get_item_info(cookie, deleted.get(), &info)) {
            return ENGINE_FAILED;
        }

        // Response includes vbucket UUID and sequence number
        mutation_descr.vbucket_uuid = info.vbucket_uuid;
        mutation_descr.seqno = info.seqno;
        cookie.setCas(info.cas);

        state = State::SendResponse;
    } else if (ret == ENGINE_KEY_EEXISTS && input_cas == 0) {
        // Cas collision and the caller specified the CAS wildcard.. retry
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::removeItem() {
    uint64_t new_cas = input_cas;
    auto ret = bucket_remove(cookie, key, new_cas, vbucket, mutation_descr);

    if (ret == ENGINE_SUCCESS) {
        cookie.setCas(new_cas);
        state = State::SendResponse;
    } else if (ret == ENGINE_KEY_EEXISTS && input_cas == 0) {
        // Cas collision and the caller specified the CAS wildcard.. retry
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::reset() {
    deleted.reset();
    existing.reset();

    xattr = {nullptr, 0};

    state = State::GetItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE RemoveCommandContext::sendResponse() {
    state = State::Done;

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[PROTOCOL_BINARY_RESPONSE_SUCCESS];
        connection.setState(McbpStateMachine::State::new_cmd);
        return ENGINE_SUCCESS;
    }

    if (connection.isSupportsMutationExtras()) {
        // Response includes vbucket UUID and sequence number
        // Make the byte ordering in the mutation descriptor
        mutation_descr.vbucket_uuid = htonll(mutation_descr.vbucket_uuid);
        mutation_descr.seqno = htonll(mutation_descr.seqno);

        cb::const_char_buffer extras = {
                reinterpret_cast<const char*>(&mutation_descr),
                sizeof(mutation_descr_t)};

        cookie.sendResponse(cb::mcbp::Status::Success,
                            extras,
                            {},
                            {},
                            cb::mcbp::Datatype::Raw,
                            cookie.getCas());
    } else {
        cookie.sendResponse(cb::mcbp::Status::Success);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE RemoveCommandContext::rebuildXattr() {
    if (mcbp::datatype::is_xattr(existing_info.datatype)) {
        const auto size = cb::xattr::get_body_offset({
                static_cast<const char*>(existing_info.value[0].iov_base),
                existing_info.value[0].iov_len
            });

        // We can't modify the item as when we try to replace the item it
        // may fail due to a race condition. Create a temporary copy of the
        // current value. Given that we're only going to (potentially) remove
        // data in the xattr blob, it will only _shrink_ in size so we
        // don't need to pass on the allocator to the blob
        auto* ptr = static_cast<char*>(existing_info.value[0].iov_base);
        xattr_buffer.reset(new char[size]);
        std::copy(ptr, ptr + size, xattr_buffer.get());
        cb::xattr::Blob blob({xattr_buffer.get(), size});
        blob.prune_user_keys();
        xattr = blob.finalize();
        if (xattr.data() != xattr_buffer.get()) {
            throw std::logic_error(
                    "RemoveCommandContext::rebuildXattr: Internal error. No "
                    "reallocations should happend when pruning user "
                    "attributes");
        }
    }

    if (xattr.size() > 0) {
        state = State::AllocateDeletedItem;
    } else {
        // All xattrs should be nuked
        state = State::RemoveItem;
    }

    return ENGINE_SUCCESS;
}
