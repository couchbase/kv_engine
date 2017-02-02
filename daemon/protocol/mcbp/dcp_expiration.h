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

#include "../../memcached.h"

void dcp_expiration_executor(McbpConnection* c, void* packet);

ENGINE_ERROR_CODE dcp_message_expiration(const void* void_cookie,
                                         uint32_t opaque,
                                         item* it,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         const void* meta,
                                         uint16_t nmeta);