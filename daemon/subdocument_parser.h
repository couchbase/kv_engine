/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/protocol_binary.h>
#include <platform/sized_buffer.h>

namespace cb::mcbp::request {
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
        switch (extras.size()) {
        case 3: // just the mandatory fields
        case 4: // including doc flags
        case 7: // including expiry
        case 8: // including expiry and doc flags
            if ((::mcbp::subdoc::extrasDocFlagMask & uint8_t(getDocFlag())) !=
                0) {
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
        switch (extras.size()) {
        case 0: // None specified
        case 1: // Only doc flag
        case 4: // Expiry time
        case 5: // Expiry time and doc flag
            if ((::mcbp::subdoc::extrasDocFlagMask & uint8_t(getDocFlag())) !=
                0) {
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
} // namespace cb::mcbp::request
