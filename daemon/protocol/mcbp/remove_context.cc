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
    ENGINE_ERROR_CODE ret;
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
            update_topkeys(key, &connection);
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);


    if (ret == ENGINE_KEY_ENOENT) {
        STATS_INCR(&connection, delete_misses);
    }

    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::getItem() {
    item* it;
    auto ret = bucket_get(&connection, &it, key, vbucket);
    if (ret == ENGINE_SUCCESS) {
        existing.reset(it);
        if (!bucket_get_item_info(&connection, existing.get(),
                                  &existing_info)) {
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
    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::allocateDeletedItem() {
    ENGINE_ERROR_CODE ret;
    protocol_binary_datatype_t datatype;
    if (xattr.size() == 0) {
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    item* it;
    ret = bucket_allocate(&connection, &it, key, xattr.size(),
                          existing_info.flags,
                          existing_info.exptime,
                          datatype,
                          vbucket);
    if (ret == ENGINE_SUCCESS) {
        deleted.reset(it);
        if (input_cas == 0) {
            bucket_item_set_cas(&connection, deleted.get(), existing_info.cas);
        } else {
            bucket_item_set_cas(&connection, deleted.get(), input_cas);
        }
        if (xattr.size() > 0) {
            item_info info;
            if (!bucket_get_item_info(&connection, deleted.get(), &info)) {
                return ENGINE_FAILED;
            }

            std::memcpy(info.value[0].iov_base, xattr.buf, xattr.size());
        }

        state = State::StoreItem;
    }
    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::storeItem() {
    uint64_t new_cas;
    auto ret = bucket_store(&connection, deleted.get(), &new_cas, OPERATION_CAS,
                            DocumentState::Deleted);

    if (ret == ENGINE_SUCCESS) {
        connection.setCAS(new_cas);

        item_info info;
        if (!bucket_get_item_info(&connection, deleted.get(), &info)) {
            return ENGINE_FAILED;
        }

        // Response includes vbucket UUID and sequence number
        mutation_descr.vbucket_uuid = htonll(info.vbucket_uuid);
        mutation_descr.seqno = htonll(info.seqno);

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
    auto ret = bucket_remove(&connection, key, &new_cas,
                             vbucket, &mutation_descr);

    if (ret == ENGINE_SUCCESS) {
        connection.setCAS(new_cas);
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

    if (connection.isNoReply()) {
        connection.setState(conn_new_cmd);
        return ENGINE_SUCCESS;
    }

    if (connection.isSupportsMutationExtras()) {
        // Response includes vbucket UUID and sequence number
        if (!mcbp_response_handler(nullptr, 0,
                                   &mutation_descr, sizeof(mutation_descr),
                                   nullptr, 0,
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   connection.getCAS(),
                                   connection.getCookie())) {
            return ENGINE_FAILED;
        }

        mcbp_write_and_free(&connection, &connection.getDynamicBuffer());
    } else {
        mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE RemoveCommandContext::rebuildXattr() {
    if (mcbp::datatype::is_xattr(existing_info.datatype)) {
        const auto size = cb::xattr::get_body_offset({
                static_cast<const char*>(existing_info.value[0].iov_base),
                existing_info.value[0].iov_len
            });

        cb::xattr::Blob blob({static_cast<uint8_t*>(existing_info.value[0].iov_base),
                             size}, xattr_buffer);

        blob.prune_user_keys();
        xattr = blob.finalize();
    }

    if (xattr.size() > 0) {
        state = State::AllocateDeletedItem;
    } else {
        // All xattrs should be nuked
        state = State::RemoveItem;
    }

    return ENGINE_SUCCESS;
}
