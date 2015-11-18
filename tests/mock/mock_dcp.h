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

#ifndef TESTS_MOCK_MOCH_dcp_H_
#define TESTS_MOCK_MOCH_dcp_H_ 1

#include "config.h"

#include <memcached/engine.h>
#include <memcached/dcp.h>

#ifdef __cplusplus
extern "C" {
#endif

extern std::vector<std::pair<uint64_t, uint64_t> > dcp_failover_log;

ENGINE_ERROR_CODE mock_dcp_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            const void *cookie);

void clear_dcp_data();

struct dcp_message_producers* get_dcp_producers(ENGINE_HANDLE *_h,
                                                ENGINE_HANDLE_V1 *_h1);



#ifdef __cplusplus
}
#endif

#endif  // TESTS_MOCK_MOCH_dcp_H_
