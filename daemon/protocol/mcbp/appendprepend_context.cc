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
#include "../../mcbp.h"
#include <xattr/blob.h>
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
        SLAB_INCR(&connection, cmd_set);
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
        if (!bucket_get_item_info(&connection, olditem.get(),
                                  &oldItemInfo)) {
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

        if (mcbp::datatype::is_compressed(oldItemInfo.datatype)) {
            try {
                if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                              (const char*)oldItemInfo.value[0].iov_base,
                                              oldItemInfo.value[0].iov_len,
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
    cb::byte_buffer old{(uint8_t*)oldItemInfo.value[0].iov_base,
                        oldItemInfo.nbytes};

    if (buffer.len != 0) {
        old = {(uint8_t*)buffer.data.get(), buffer.len};
    }

    // If we're operating on a document containing xattr's we need to
    // tell the underlying engine about how much of the data which
    // should be accounted for in the privileged segment.
    size_t priv_size = 0;

    // The offset into the old item where the actual body start.
    size_t body_offset = 0;

    // If the existing item had XATTRs we need to preserve the xattrs
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (mcbp::datatype::is_xattr(oldItemInfo.datatype)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;

        // Calculate the size of the system xattr's.
        body_offset = cb::xattr::get_body_offset({(const char*)old.buf,
                                                  old.len});
        cb::byte_buffer xattr_blob{old.buf, body_offset};
        cb::xattr::Blob blob(xattr_blob);
        priv_size = blob.get_system_size();
    }

    auto pair = bucket_allocate_ex(connection, key,
                                   old.len + value.len,
                                   priv_size, oldItemInfo.flags,
                                   0, datatype, vbucket);

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
    bucket_item_set_cas(&connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE AppendPrependCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(&connection, newitem.get(), &ncas, OPERATION_CAS);

    if (ret == ENGINE_SUCCESS) {
        update_topkeys(key, &connection);
        connection.setCAS(ncas);
        if (connection.isSupportsMutationExtras()) {
            item_info newItemInfo;
            if (!bucket_get_item_info(&connection, newitem.get(),
                                      &newItemInfo)) {
                return ENGINE_FAILED;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                (connection.write.buf +
                 sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
            extras->seqno = htonll(newItemInfo.seqno);
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
