/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "config.h"

#include <memcached/protocol_binary.h>

#include <string>
#include <vector>

/*
 * Sub-document API encoding helpers
 */

/* Sub-document API MULTI_LOOKUP command */
struct SubdocMultiLookupCmd {
public:
    std::string key;

    struct LookupSpec {
        protocol_binary_command opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
    };
    std::vector<LookupSpec> specs;

    /* Takes the current state of object and encodes a
     * protocol_binary_request_subdocument_multi_lookup packet in network order.
     */
    std::vector<char> encode() const;
};
