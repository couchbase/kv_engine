/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include <memcached/protocol_binary.h>
#include <platform/sized_buffer.h>

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
class SubdocPayloadParser {
public:
    explicit SubdocPayloadParser(cb::const_byte_buffer extras)
        : extras(extras) {
    }

    uint16_t getPathlen() const {
        return ntohs(*reinterpret_cast<const uint16_t*>(extras.data()));
    }

    protocol_binary_subdoc_flag getSubdocFlags() const {
        return static_cast<protocol_binary_subdoc_flag>(
                extras[sizeof(uint16_t)]);
    }

    uint32_t getExpiry() const {
        if (extras.size() == 7 || extras.size() == 8) {
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data() + 3));
        }
        return 0;
    }

    ::mcbp::subdoc::doc_flag getDocFlag() const {
        if (extras.size() == 4 || extras.size() == 8) {
            return static_cast<::mcbp::subdoc::doc_flag>(extras.back());
        }
        return ::mcbp::subdoc::doc_flag::None;
    }

    bool isValid() const {
        using ::mcbp::subdoc::doc_flag;
        switch (extras.size()) {
        case 3: // just the mandatory fields
        case 4: // including doc flags
        case 7: // including expiry
        case 8: // including expiry and doc flags
            if ((uint8_t(0xf8) & uint8_t(getDocFlag())) != 0) {
                return false;
            }
            return true;
        default:
            return false;
        }
    }

protected:
    cb::const_byte_buffer extras;
};

class SubdocMultiPayloadParser {
public:
    explicit SubdocMultiPayloadParser(cb::const_byte_buffer extras)
        : extras(extras) {
    }

    uint32_t getExpiry() const {
        if (extras.size() == 4 || extras.size() == 5) {
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
        }
        return 0;
    }

    ::mcbp::subdoc::doc_flag getDocFlag() const {
        if (extras.size() == 1 || extras.size() == 5) {
            return static_cast<::mcbp::subdoc::doc_flag>(extras.back());
        }
        return ::mcbp::subdoc::doc_flag::None;
    }

    bool isValid() const {
        using ::mcbp::subdoc::doc_flag;
        switch (extras.size()) {
        case 0: // None specified
        case 1: // Only doc flag
        case 4: // Expiry time
        case 5: // Expiry time and doc flag
            if ((uint8_t(0xf8) & uint8_t(getDocFlag())) != 0) {
                return false;
            }
            return true;
        default:
            return false;
        }
    }

protected:
    cb::const_byte_buffer extras;
};

#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb
