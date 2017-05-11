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

#include <string>
#include <cstdint>
#include <memory>

namespace Greenstack {
    class Message;

    typedef uint8_t mutation_type_t;
    namespace MutationType {
        const mutation_type_t Add = 0;
        const mutation_type_t Set = 1;
        const mutation_type_t Replace = 2;
        const mutation_type_t Append = 3;
        const mutation_type_t Prepend = 4;
        const mutation_type_t Patch = 5;

        std::string to_string(mutation_type_t mutation);

        mutation_type_t from_string(const std::string& str);
    }
}
