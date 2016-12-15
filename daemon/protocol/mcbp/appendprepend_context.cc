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
#include "../../mcbp.h"
#include <xattr/utils.h>

ENGINE_ERROR_CODE AppendPrependCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::InflateInputData:
            ret = inflateInputData();
            break;
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
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    if (ret != ENGINE_EWOULDBLOCK) {
        SLAB_INCR(&connection, cmd_set, key, nkey);
    }

    if (ret == ENGINE_KEY_ENOENT) {
        // for some reason the return code for no key is not stored so we need
        // to remap that error code..
        ret = ENGINE_NOT_STORED;
    }

    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::inflateInputData() {
    try {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      value.buf, value.len, inputbuffer)) {
            return ENGINE_EINVAL;
        }
        value.buf = inputbuffer.data.get();
        value.len = inputbuffer.len;
        state = State::GetItem;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::getItem() {
    item* it;
    auto ret = bucket_get(&connection, &it, key, vbucket);
    if (ret == ENGINE_SUCCESS) {
        olditem.reset(it);
        oldItemInfo.info.nvalue = 1;

        if (!bucket_get_item_info(&connection, olditem.get(),
                                  &oldItemInfo.info)) {
            return ENGINE_FAILED;
        }

        uint64_t oldcas = oldItemInfo.info.cas;
        if (cas != 0 && cas != oldcas) {
            return ENGINE_KEY_EEXISTS;
        }

        if (mcbp::datatype::is_compressed(oldItemInfo.info.datatype)) {
            try {
                if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                              (const char*)oldItemInfo.info.value[0].iov_base,
                                              oldItemInfo.info.value[0].iov_len,
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

    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::allocateNewItem() {
    size_t oldsize = oldItemInfo.info.nbytes;
    if (buffer.len != 0) {
        oldsize = buffer.len;
    }

    // If the existing item had XATTRs we need to preserve the xattrs
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (mcbp::datatype::is_xattr(oldItemInfo.info.datatype)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    item* it;
    ENGINE_ERROR_CODE ret;
    ret = bucket_allocate(&connection, &it, key, oldsize + value.len,
                          oldItemInfo.info.flags, 0, datatype, vbucket);
    if (ret == ENGINE_SUCCESS) {
        newitem.reset(it);
        // copy the data over..
        newItemInfo.info.nvalue = 1;

        if (!bucket_get_item_info(&connection, newitem.get(),
                                  &newItemInfo.info)) {
            return ENGINE_FAILED;
        }

        const char* src = (const char*)oldItemInfo.info.value[0].iov_base;
        size_t oldsize = oldItemInfo.info.value[0].iov_len;
        if (buffer.len != 0) {
            src = buffer.data.get();
            oldsize = buffer.len;
        }

        char* newdata = (char*)newItemInfo.info.value[0].iov_base;

        // do the op
        if (mode == Mode::Append) {
            memcpy(newdata, src, oldsize);
            memcpy(newdata + oldsize, value.buf, value.len);
        } else {
            // The xattrs should go first
            size_t offset = 0;
            if (mcbp::datatype::is_xattr(oldItemInfo.info.datatype)) {
                offset = cb::xattr::get_body_offset({src, oldsize});
                memcpy(newdata, src, offset);
            }

            memcpy(newdata + offset, value.buf, value.len);
            memcpy(newdata + offset + value.len, src + offset,
                   oldsize - offset);
        }
        bucket_item_set_cas(&connection, newitem.get(), oldItemInfo.info.cas);

        state = State::StoreItem;
    }
    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(&connection, newitem.get(), &ncas, OPERATION_CAS);

    if (ret == ENGINE_SUCCESS) {
        update_topkeys(key, &connection);
        connection.setCAS(ncas);
        if (connection.isSupportsMutationExtras()) {
            newItemInfo.info.nvalue = 1;
            if (!bucket_get_item_info(&connection, newitem.get(),
                                      &newItemInfo.info)) {
                return ENGINE_FAILED;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                (connection.write.buf +
                 sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(newItemInfo.info.vbucket_uuid);
            extras->seqno = htonll(newItemInfo.info.seqno);
            mcbp_write_response(&connection, extras, sizeof(*extras), 0,
                                sizeof(*extras));
        } else {
            mcbp_write_packet(&connection,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
        state = State::Done;
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
    }

    return ret;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::reset() {
    olditem.reset();
    newitem.reset();

    if (buffer.len > 0) {
        buffer.len = 0;
        buffer.data.reset();
    }
    state = State::GetItem;
    return ENGINE_SUCCESS;
}
