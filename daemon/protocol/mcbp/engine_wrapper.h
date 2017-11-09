/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#pragma once

/**
 * This file contains wrapper functions on top of the engine interface.
 * If you want to know more information of a function or the arguments
 * it takes, you should look in `memcached/engine.h`.
 *
 * The `handle` and `cookie` parameter in the engine interface methods is
 * replaced by the connection object.
 *
 * Note: We're working on cleaning up this API from a C api to a C++ API, so
 * it is no longer consistent (some methods takes pointers, whereas others
 * take reference to objects). We might do a full scrub of the API at
 * some point.
 */

#include <memcached/engine_error.h>
#include <daemon/connection_mcbp.h>

ENGINE_ERROR_CODE bucket_unknown_command(McbpConnection* c,
                                         protocol_binary_request_header* request,
                                         ADD_RESPONSE response);

void bucket_item_set_cas(Cookie& cookie, item* it, uint64_t cas)
        CB_ATTR_NONNULL(2);

void bucket_reset_stats(Cookie& cookie);

bool bucket_get_item_info(Cookie& cookie,
                          const item* item_,
                          item_info* item_info_) CB_ATTR_NONNULL(2, 3);

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKey& key,
                                            uint16_t vbucket);

ENGINE_ERROR_CODE bucket_store(
        Cookie& cookie,
        item* item_,
        uint64_t* cas,
        ENGINE_STORE_OPERATION operation,
        DocumentState document_state = DocumentState::Alive)
        CB_ATTR_NONNULL(2, 3);

cb::EngineErrorCasPair bucket_store_if(
        Cookie& cookie,
        item* item_,
        uint64_t cas,
        ENGINE_STORE_OPERATION operation,
        cb::StoreIfPredicate predicate,
        DocumentState document_state = DocumentState::Alive) CB_ATTR_NONNULL(2);

ENGINE_ERROR_CODE bucket_remove(Cookie& cookie,
                                const DocKey& key,
                                uint64_t* cas,
                                uint16_t vbucket,
                                mutation_descr_t* mut_info)
        CB_ATTR_NONNULL(3, 5);

cb::EngineErrorItemPair bucket_get(
        Cookie& cookie,
        const DocKey& key,
        uint16_t vbucket,
        DocStateFilter documentStateFilter = DocStateFilter::Alive);

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKey& key,
        uint16_t vbucket,
        std::function<bool(const item_info&)> filter);

cb::EngineErrorItemPair bucket_get_and_touch(Cookie& cookie,
                                             const DocKey& key,
                                             uint16_t vbucket,
                                             uint32_t expiration);

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKey& key,
                                          uint16_t vbucket,
                                          uint32_t lock_timeout);

ENGINE_ERROR_CODE bucket_unlock(Cookie& cookie,
                                const DocKey& key,
                                uint16_t vbucket,
                                uint64_t cas);

std::pair<cb::unique_item_ptr, item_info> bucket_allocate_ex(Cookie& cookie,
                                                             const DocKey& key,
                                                             size_t nbytes,
                                                             size_t priv_nbytes,
                                                             int flags,
                                                             rel_time_t exptime,
                                                             uint8_t datatype,
                                                             uint16_t vbucket);
