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

#include <cstdint>
#include <string>

namespace Greenstack {
    /**
     * The datatype used for CAS values
     */
    typedef uint64_t cas_t;

    namespace CAS {
        /**
         * The special value used as a wildcard and match all CAS values
         */
        const cas_t Wildcard = 0x0;
    }

    /**
     * Get a textual representation of a CAS value
     */
    std::string to_string(cas_t cas);
}