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

void bucket_item_set_cas(McbpConnection* c, item* it, uint64_t cas);

void bucket_reset_stats(McbpConnection* c);

ENGINE_ERROR_CODE bucket_get_engine_vb_map(McbpConnection* c,
                                           engine_get_vb_map_cb callback);

bool bucket_get_item_info(McbpConnection* c, const item* item_,
                          item_info* item_info_);

bool bucket_set_item_info(McbpConnection* c, item* item_,
                          const item_info* item_info_);

ENGINE_ERROR_CODE bucket_store(McbpConnection* c,
                               item* item_,
                               uint64_t* cas,
                               ENGINE_STORE_OPERATION operation,
                               DocumentState document_state = DocumentState::Alive);

ENGINE_ERROR_CODE bucket_remove(McbpConnection* c,
                                const DocKey& key,
                                uint64_t* cas,
                                uint16_t vbucket,
                                mutation_descr_t* mut_info);

ENGINE_ERROR_CODE bucket_get(McbpConnection* c,
                             item** item_,
                             const DocKey& key,
                             uint16_t vbucket,
                             DocumentState document_state = DocumentState::Alive);

ENGINE_ERROR_CODE bucket_get_locked(McbpConnection& c,
                                    item** item_,
                                    const DocKey& key,
                                    uint16_t vbucket,
                                    uint32_t lock_timeout);

ENGINE_ERROR_CODE bucket_unlock(McbpConnection& c,
                                const DocKey& key,
                                uint16_t vbucket,
                                uint64_t cas);

void bucket_release_item(McbpConnection* c, item* it);

ENGINE_ERROR_CODE bucket_allocate(McbpConnection* c,
                                  item** it,
                                  const DocKey& key,
                                  const size_t nbytes,
                                  const int flags,
                                  const rel_time_t exptime,
                                  uint8_t datatype,
                                  uint16_t vbucket);