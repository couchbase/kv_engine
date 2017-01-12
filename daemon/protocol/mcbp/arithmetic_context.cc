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
#include "arithmetic_context.h"
#include "engine_wrapper.h"
#include "../../mcbp.h"
#include <xattr/blob.h>
#include <xattr/utils.h>

ENGINE_ERROR_CODE ArithmeticCommandContext::getItem() {
    item* it;
    auto ret = bucket_get(&connection, &it, key, vbucket);
    if (ret == ENGINE_SUCCESS) {
        olditem.reset(it);

        if (!bucket_get_item_info(&connection, olditem.get(),
                                  &oldItemInfo)) {
            return ENGINE_FAILED;
        }

        uint64_t oldcas = oldItemInfo.cas;
        if (cas != 0 && cas != oldcas) {
            return ENGINE_KEY_EEXISTS;
        }

        if (mcbp::datatype::is_compressed(oldItemInfo.datatype)) {
            try {
                if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy,
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
    } else if (ret == ENGINE_KEY_ENOENT) {
        if (ntohl(request.message.body.expiration) != 0xffffffff) {
            state = State::CreateNewItem;
            ret = ENGINE_SUCCESS;
        } else {
            if ((connection.getCmd() == PROTOCOL_BINARY_CMD_INCREMENT) ||
                (connection.getCmd() == PROTOCOL_BINARY_CMD_INCREMENTQ)) {
                STATS_INCR(&connection, incr_misses);
            } else {
                STATS_INCR(&connection, decr_misses);
            }
        }
    }

    return ret;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::createNewItem() {
    const std::string value{std::to_string(ntohll(request.message.body.initial))};
    result = ntohll(request.message.body.initial);

    auto pair = bucket_allocate_ex(connection, key, value.size(),
                                   0, // no privileged bytes
                                   0, // Empty flags
                                   ntohl(request.message.body.expiration),
                                   PROTOCOL_BINARY_RAW_BYTES, vbucket);
    newitem = std::move(pair.first);

    memcpy(pair.second.value[0].iov_base, value.data(), value.size());
    state = State::StoreNewItem;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::storeNewItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(&connection, newitem.get(), &ncas, OPERATION_ADD);

    if (ret == ENGINE_SUCCESS) {
        connection.setCAS(ncas);
        state = State::SendResult;
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::allocateNewItem() {
    // Set ptr to point to the beginning of the input buffer.
    size_t oldsize = oldItemInfo.nbytes;
    char* ptr = static_cast<char*>(oldItemInfo.value[0].iov_base);
    // If the input buffer was compressed we should use the temporary
    // allocated buffer instead
    if (buffer.len != 0) {
        ptr = static_cast<char*>(buffer.data.get());
        oldsize = buffer.len;
    }

    // Preserve the XATTRs of the existing item if it had any
    size_t xattrsize = 0;
    size_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(oldItemInfo.datatype)) {
        xattrsize = cb::xattr::get_body_offset({ptr, oldsize});
        cb::byte_buffer xattr_blob{reinterpret_cast<uint8_t*>(ptr),
                                   xattrsize};
        cb::xattr::Blob blob(xattr_blob);
        priv_bytes = blob.get_system_size();
    }
    ptr += xattrsize;
    const std::string payload(ptr, oldsize - xattrsize);

    uint64_t oldval;
    if (!safe_strtoull(payload.c_str(), &oldval)) {
        return ENGINE_DELTA_BADVAL;
    }

    uint64_t delta = ntohll(request.message.body.delta);

    // perform the op ;)
    if (request.message.header.request.opcode == PROTOCOL_BINARY_CMD_INCREMENT ||
        request.message.header.request.opcode == PROTOCOL_BINARY_CMD_INCREMENTQ) {
        // increment
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

    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (xattrsize > 0) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    auto pair = bucket_allocate_ex(connection, key,
                                   xattrsize + value.size(),
                                   priv_bytes,
                                   oldItemInfo.flags,
                                   ntohl(request.message.body.expiration),
                                   datatype, vbucket);

    newitem = std::move(pair.first);
    cb::byte_buffer body{static_cast<uint8_t*>(pair.second.value[0].iov_base),
                         pair.second.value[0].iov_len};

    // copy the data over..
    const char* src = (const char*)oldItemInfo.value[0].iov_base;
    if (buffer.len != 0) {
        src = buffer.data.get();
    }

    // copy the xattr over;
    memcpy(body.buf, src, xattrsize);
    memcpy(body.buf + xattrsize, value.data(), value.size());
    bucket_item_set_cas(&connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(&connection, newitem.get(), &ncas, OPERATION_CAS);

    if (ret == ENGINE_SUCCESS) {
        connection.setCAS(ncas);
        state = State::SendResult;
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::sendResult() {
    update_topkeys(key, &connection);
    state = State::Done;

    if ((connection.getCmd() == PROTOCOL_BINARY_CMD_INCREMENT) ||
        (connection.getCmd() == PROTOCOL_BINARY_CMD_INCREMENTQ)) {
        STATS_INCR(&connection, incr_hits);
    } else {
        STATS_INCR(&connection, decr_hits);
    }

    if (connection.isNoReply()) {
        connection.setState(conn_new_cmd);
        return ENGINE_SUCCESS;
    }

    if (connection.isSupportsMutationExtras()) {
        item_info newItemInfo;
        if (!bucket_get_item_info(&connection, newitem.get(),
                                  &newItemInfo)) {
            return ENGINE_FAILED;
        }

        // Response includes vbucket UUID and sequence number
        // (in addition to value)
        mutation_descr_t extras;
        extras.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
        extras.seqno = htonll(newItemInfo.seqno);
        result = ntohll(result);

        if (!mcbp_response_handler(nullptr, 0,
                                   &extras, sizeof(extras),
                                   &result, sizeof(result),
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   connection.getCAS(),
                                   connection.getCookie())) {
            return ENGINE_FAILED;
        }
    } else {
        result = htonll(result);
        if (!mcbp_response_handler(nullptr, 0,
                                   nullptr, 0,
                                   &result, sizeof(result),
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   connection.getCAS(),
                                   connection.getCookie())) {
            return ENGINE_FAILED;
        }
    }

    mcbp_write_and_free(&connection, &connection.getDynamicBuffer());
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::reset() {
    olditem.reset();
    newitem.reset();

    if (buffer.len > 0) {
        buffer.len = 0;
        buffer.data.reset();
    }
    state = State::GetItem;
    return ENGINE_SUCCESS;
}
