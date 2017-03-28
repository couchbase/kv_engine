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
#include "daemon/mcbp.h"
#include "daemon/memcached.h"
#include "engine_wrapper.h"
#include "mutation_context.h"

#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <xattr/utils.h>

MutationCommandContext::MutationCommandContext(McbpConnection& c,
                                               protocol_binary_request_set* req,
                                               const ENGINE_STORE_OPERATION op_)
    : SteppableCommandContext(c),
      operation(req->message.header.request.cas == 0 ? op_ : OPERATION_CAS),
      key(req->bytes + sizeof(req->bytes),
          ntohs(req->message.header.request.keylen),
          c.getDocNamespace()),
      value(reinterpret_cast<const char*>(key.data() + key.size()),
            ntohl(req->message.header.request.bodylen) - key.size() -
            req->message.header.request.extlen),
      vbucket(ntohs(req->message.header.request.vbucket)),
      input_cas(ntohll(req->message.header.request.cas)),
      expiration(ntohl(req->message.body.expiration)),
      flags(req->message.body.flags),
      datatype(req->message.header.request.datatype),
      state(State::ValidateInput),
      newitem(nullptr, cb::ItemDeleter{c.getBucketEngineAsV0()}),
      existing(nullptr, cb::ItemDeleter{c.getBucketEngineAsV0()}),
      xattr_size(0) {
}

ENGINE_ERROR_CODE MutationCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::ValidateInput:
            ret = validateInput();
            break;
        case State::GetExistingItemToPreserveXattr:
            ret = getExistingItemToPreserveXattr();
            break;
        case State::AllocateNewItem:
            ret = allocateNewItem();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Reset:
            ret = reset();
            break;
        case State::Done:
            if (operation == OPERATION_CAS) {
                SLAB_INCR(&connection, cas_hits);
            } else {
                SLAB_INCR(&connection, cmd_set);
            }
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    if (ret != ENGINE_EWOULDBLOCK) {
        if (operation == OPERATION_CAS) {
            switch (ret) {
            case ENGINE_KEY_EEXISTS:
                SLAB_INCR(&connection, cas_badval);
                break;
            case ENGINE_KEY_ENOENT:
                get_thread_stats(&connection)->cas_misses++;
                break;
            default:;
            }
        } else {
            SLAB_INCR(&connection, cmd_set);
        }
    }

    return ret;
}

ENGINE_ERROR_CODE MutationCommandContext::validateInput() {
    if (!connection.isDatatypeEnabled(datatype)) {
        return ENGINE_EINVAL;
    }

    if (!connection.isJsonEnabled()) {
        auto* validator = connection.getThread()->validator;
        try {
            auto* ptr = reinterpret_cast<const uint8_t*>(value.buf);
            if (validator->validate(ptr, value.len)) {
                datatype = PROTOCOL_BINARY_DATATYPE_JSON;
            }
        } catch (std::bad_alloc&) {
            return ENGINE_ENOMEM;
        }
    }

    if (operation == OPERATION_ADD || !settings.isXattrEnabled()) {
        state = State::AllocateNewItem;
    } else {
        // We might want to preserve XATTRs
        state = State::GetExistingItemToPreserveXattr;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MutationCommandContext::getExistingItemToPreserveXattr() {
    // Try to fetch the previous version of the document _iff_ it contains
    // any xattrs so that we can preserve those by copying them over to
    // the new document. Documents without any xattrs can safely be
    // ignored. The motivation to use get_if over a normal get is for the
    // value eviction case where the underlying engine would have to read
    // the value off disk in order to return it via get() even if we don't
    // need it (and would throw it away in the frontend).
    auto pair = bucket_get_if(&connection, key, vbucket,
                             [](const item_info& info) {
                                 return mcbp::datatype::is_xattr(info.datatype);
                             });
    if (pair.first != cb::engine_errc::no_such_key &&
        pair.first != cb::engine_errc::success) {
        return ENGINE_ERROR_CODE(pair.first);
    }

    existing = std::move(pair.second);
    if (!existing) {
        state = State::AllocateNewItem;
        return ENGINE_SUCCESS;
    }

    if (!bucket_get_item_info(&connection, existing.get(), &existing_info)) {
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

    cb::const_char_buffer payload{
        static_cast<const char*>(existing_info.value[0].iov_base),
        existing_info.value[0].iov_len};
    xattr_size = cb::xattr::get_body_offset(payload);

    state = State::AllocateNewItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MutationCommandContext::allocateNewItem() {
    item* it = nullptr;
    auto dtype = datatype;
    if (xattr_size > 0) {
        dtype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    auto ret = bucket_allocate(&connection, &it, key, value.len + xattr_size,
                               flags, expiration, dtype, vbucket);

    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    newitem.reset(it);

    if (operation == OPERATION_ADD || input_cas != 0) {
        bucket_item_set_cas(&connection, newitem.get(), input_cas);
    } else {
        if (existing) {
            bucket_item_set_cas(&connection, newitem.get(), existing_info.cas);
        } else {
            bucket_item_set_cas(&connection, newitem.get(), input_cas);
        }
    }

    item_info newitem_info;
    if (!bucket_get_item_info(&connection, newitem.get(), &newitem_info)) {
        return ENGINE_FAILED;
    }

    uint8_t* root = reinterpret_cast<uint8_t*>(newitem_info.value[0].iov_base);
    if (xattr_size > 0) {
        // Preserve the xattrs
        auto* ex = reinterpret_cast<uint8_t*>(existing_info.value[0].iov_base);
        std::copy(ex, ex + xattr_size, root);
        root += xattr_size;
    }

    // Copy the user supplied value over
    std::copy(value.buf, value.buf + value.len, root);
    state = State::StoreItem;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MutationCommandContext::storeItem() {
    uint64_t new_cas = input_cas;
    auto ret = bucket_store(&connection, newitem.get(), &new_cas, operation);
    if (ret == ENGINE_SUCCESS) {
        connection.setCAS(new_cas);
        state = State::SendResponse;
    } else if (ret == ENGINE_NOT_STORED) {
        // Need to remap error for add and replace
        if (operation == OPERATION_ADD) {
            ret = ENGINE_KEY_EEXISTS;
        } else if (operation == OPERATION_REPLACE) {
            ret = ENGINE_KEY_ENOENT;
        }
    } else if (ret == ENGINE_KEY_EEXISTS && input_cas == 0) {
        // We failed due to CAS mismatch, and the user did not specify
        // the CAS, retry the operation.
        state = State::Reset;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE MutationCommandContext::sendResponse() {
    update_topkeys(key, &connection);
    state = State::Done;

    if (connection.isNoReply()) {
        ++connection.getBucket()
                  .responseCounters[PROTOCOL_BINARY_RESPONSE_SUCCESS];
        connection.setState(conn_new_cmd);
        return ENGINE_SUCCESS;
    }

    if (connection.isSupportsMutationExtras()) {
        item_info newitem_info;
        if (!bucket_get_item_info(&connection, newitem.get(), &newitem_info)) {
            return ENGINE_FAILED;
        }

        // Response includes vbucket UUID and sequence number
        // (in addition to value)
        mutation_descr_t extras;
        extras.vbucket_uuid = htonll(newitem_info.vbucket_uuid);
        extras.seqno = htonll(newitem_info.seqno);

        if (!mcbp_response_handler(nullptr, 0,
                                   &extras, sizeof(extras),
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

ENGINE_ERROR_CODE MutationCommandContext::reset() {
    newitem.reset();
    existing.reset();
    xattr_size = 0;
    state = State::GetExistingItemToPreserveXattr;
    return ENGINE_SUCCESS;
}