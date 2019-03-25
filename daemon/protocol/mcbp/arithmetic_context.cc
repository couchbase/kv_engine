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
      extras(*reinterpret_cast<const ArithmeticPayload*>(
              req.getExtdata().data())),
      cas(req.getCas()),
      vbucket(req.getVBucket()),
      increment(req.getClientOpcode() == cb::mcbp::ClientOpcode::Increment ||
                req.getClientOpcode() == cb::mcbp::ClientOpcode::Incrementq) {
}

ENGINE_ERROR_CODE ArithmeticCommandContext::getItem() {
    auto ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    if (ret.first == cb::engine_errc::success) {
        olditem = std::move(ret.second);

        if (!bucket_get_item_info(connection, olditem.get(), &oldItemInfo)) {
            return ENGINE_FAILED;
        }

        uint64_t oldcas = oldItemInfo.cas;
        if (cas != 0 && cas != oldcas) {
            return ENGINE_KEY_EEXISTS;
        }

        if (mcbp::datatype::is_snappy(oldItemInfo.datatype)) {
            try {
                cb::const_char_buffer payload(static_cast<const char*>(
                                              oldItemInfo.value[0].iov_base),
                                              oldItemInfo.value[0].iov_len);
                if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy,
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

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE ArithmeticCommandContext::createNewItem() {
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

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::storeNewItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            newitem.get(),
                            ncas,
                            OPERATION_ADD,
                            cookie.getRequest().getDurabilityRequirements());

    if (ret == ENGINE_SUCCESS) {
        cookie.setCas(ncas);
        state = State::SendResult;
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    } else if(ret == ENGINE_NOT_STORED) {
        // hit race condition, item with our key was created between our call
        // to bucket_store() and when we checked to see if it existed, by
        // calling bucket_get() so just re-try by resetting our state to the
        // start again.
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::allocateNewItem() {
    // Set ptr to point to the beginning of the input buffer.
    size_t oldsize = oldItemInfo.nbytes;
    auto* ptr = static_cast<char*>(oldItemInfo.value[0].iov_base);
    // If the input buffer was compressed we should use the temporary
    // allocated buffer instead
    if (buffer.size() != 0) {
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
    if (!safe_strtoull(payload.c_str(), oldval)) {
        return ENGINE_DELTA_BADVAL;
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
    if (buffer.size() != 0) {
        src = buffer.data();
    }

    // copy the xattr over;
    memcpy(body.buf, src, xattrsize);
    memcpy(body.buf + xattrsize, value.data(), value.size());
    bucket_item_set_cas(connection, newitem.get(), oldItemInfo.cas);

    state = State::StoreItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            newitem.get(),
                            ncas,
                            OPERATION_CAS,
                            cookie.getRequest().getDurabilityRequirements());

    if (ret == ENGINE_SUCCESS) {
        cookie.setCas(ncas);
        state = State::SendResult;
    } else if (ret == ENGINE_KEY_EEXISTS && cas == 0) {
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::sendResult() {
    update_topkeys(cookie);
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
        connection.setState(StateMachine::State::new_cmd);
        return ENGINE_SUCCESS;
    }

    mutation_descr_t mutation_descr{};
    cb::const_char_buffer extras = {
            reinterpret_cast<const char*>(&mutation_descr),
            sizeof(mutation_descr_t)};
    result = ntohll(result);
    cb::const_char_buffer value = {reinterpret_cast<const char*>(&result),
                                   sizeof(result)};

    if (connection.isSupportsMutationExtras()) {
        item_info newItemInfo;
        if (!bucket_get_item_info(connection, newitem.get(), &newItemInfo)) {
            return ENGINE_FAILED;
        }

        // Response includes vbucket UUID and sequence number
        // (in addition to value)
        mutation_descr.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
        mutation_descr.seqno = htonll(newItemInfo.seqno);
    } else {
        extras = {};
    }

    cookie.sendResponse(cb::mcbp::Status::Success,
                        extras,
                        {},
                        value,
                        cb::mcbp::Datatype::Raw,
                        cookie.getCas());

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ArithmeticCommandContext::reset() {
    olditem.reset();
    newitem.reset();
    buffer.reset();
    state = State::GetItem;
    return ENGINE_SUCCESS;
}
