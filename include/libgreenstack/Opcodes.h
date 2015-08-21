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
    enum class Opcode : uint16_t {
        // Generic packet opcodes (0x0000 - 0x03ff)
        Hello = 0x0001,
        SaslAuth = 0x0002,
        Noop = 0x0003,
        Keepalive = 0x0004,

        // Memcached specific opcodes (0x0000 - 0x03ff)
        SelectBucket = 0x0400,
        ListBuckets = 0x0401,
        CreateBucket = 0x0402,
        DeleteBucket = 0x0403,
        AssumeRole = 0x0404,
        Mutation = 0x0405,
        Get = 0x0406,

        InvalidOpcode = 0xffff
    };

    /**
     * Get a textual representation for an opcode
     */
    std::string to_string(const Opcode& opcode);

    /**
     * Get the opcode for a textual string
     */
    Opcode from_string(const std::string& str);

    /**
     * Try to convert a uint16_t to an opcode
     */
    Opcode to_opcode(uint16_t opc);
}
