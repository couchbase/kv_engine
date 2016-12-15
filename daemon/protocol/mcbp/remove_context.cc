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
#include <daemon/mcbp.h>

ENGINE_ERROR_CODE RemoveCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
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
        case State::Done: SLAB_INCR(&connection, delete_hits, key.data(),
                                    key.size());
            update_topkeys(key, &connection);
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);


    if (ret == ENGINE_KEY_ENOENT) {
        STATS_INCR(&connection, delete_misses, key.data(), key.size());
    }

    return ret;
}

ENGINE_ERROR_CODE RemoveCommandContext::getItem() {
    auto ret = bucket_get(&connection, &existing, key, vbucket);
    if (ret == ENGINE_SUCCESS) {
        existing_info.nvalue = 1;
        if (!bucket_get_item_info(&connection, existing, &existing_info)) {
            return ENGINE_FAILED;
        }

        if (input_cas != 0 && input_cas != existing_info.cas) {
            return ENGINE_KEY_EEXISTS;
        }

        state = State::RemoveItem;
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
    if (zombie != nullptr) {
        bucket_release_item(&connection, zombie);
        zombie = nullptr;
    }

    if (existing != nullptr) {
        bucket_release_item(&connection, existing);
        existing = nullptr;
    }

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
