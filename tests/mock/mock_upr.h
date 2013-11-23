/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef TESTS_MOCK_MOCH_UPR_H_
#define TESTS_MOCK_MOCH_UPR_H_ 1

#include "config.h"

#include <memcached/engine.h>
#include <memcached/upr.h>

typedef enum {
    UPR_OP_GET_FAILOVER_LOG,
    UPR_OP_STREAM_REQ,
    UPR_OP_ADD_STREAM_REQ,
    UPR_OP_STREAM_END,
    UPR_OP_MARKER,
    UPR_OP_MUTATION,
    UPR_OP_DELETION,
    UPR_OP_EXPIRATION,
    UPR_OP_FLUSH,
    UPR_OP_SET_VBUCKET_STATE
} upr_op_t;

#ifdef __cplusplus
extern "C" {
#endif

ENGINE_ERROR_CODE mock_upr_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            const void *cookie);
struct upr_message_producers* get_upr_producers();

#ifdef __cplusplus
}
#endif

#endif  // TESTS_MOCK_MOCH_UPR_H_
