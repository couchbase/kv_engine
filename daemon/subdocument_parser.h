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

    [[nodiscard]] uint16_t getPathlen() const {
        return ntohs(*reinterpret_cast<const uint16_t*>(extras.data()));
    }

    [[nodiscard]] subdoc::PathFlag getSubdocFlags() const {
        return static_cast<subdoc::PathFlag>(extras[sizeof(uint16_t)]);
    }

    [[nodiscard]] uint32_t getExpiry() const {
        if (extras.size() == 7 || extras.size() == 8 || extras.size() == 11) {
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data() + 3));
        }
        return 0;
    }

    [[nodiscard]] subdoc::DocFlag getDocFlag() const {
        if (extras.size() == 4 || extras.size() == 8 || extras.size() == 12) {
            return static_cast<subdoc::DocFlag>(extras.back());
        }
        return subdoc::DocFlag::None;
    }

    [[nodiscard]] std::optional<uint32_t> getUserFlags() const {
        if (extras.size() == 11 || extras.size() == 12) {
            auto* ptr = extras.data();
            ptr += 3; // path length and flags
            ptr += sizeof(uint32_t); // Exptime
            return *reinterpret_cast<const uint32_t*>(ptr);
        }
        return {};
    }

    [[nodiscard]] bool isValidLookup() const {
        // only mandatory and doc flags legal
        const auto size = extras.size();
        return size == 3 || size == 4;
    }

    [[nodiscard]] bool isValidMutation() const {
        // all combinations legal
        return isValid();
    }

    [[nodiscard]] bool isValid() const {
        switch (extras.size()) {
        case 3: // just the mandatory fields
        case 4: // including doc flags
        case 7: // including expiry
        case 8: // including expiry and doc flags
        case 11: // Including Expiry and User Flags
        case 12: // Including Expiry, User Flags and Doc Flags
            if ((subdoc::extrasDocFlagMask & getDocFlag()) !=
                subdoc::DocFlag::None) {
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

    [[nodiscard]] uint32_t getExpiry() const {
        if (extras.size() >= sizeof(uint32_t)) {
            // 4 byte exp time
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
        }

        return 0;
    }

    [[nodiscard]] subdoc::DocFlag getDocFlag() const {
        if (extras.size() == 1 || // alone
            extras.size() == 5 || // Exp time and doc flag
            extras.size() == 9) { // Exp time, User flags and doc flag
            return static_cast<subdoc::DocFlag>(extras.back());
        }
        return subdoc::DocFlag::None;
    }

    [[nodiscard]] std::optional<uint32_t> getUserFlags() const {
        if (extras.size() == 8 || extras.size() == 9) {
            auto* ptr = extras.data();
            ptr += sizeof(uint32_t); // Exptime
            return *reinterpret_cast<const uint32_t*>(ptr);
        }
        return {};
    }

    [[nodiscard]] bool isValidLookup() const {
        // only doc flags legal
        const auto size = extras.size();
        return size == 0 || size == 1;
    }

    [[nodiscard]] bool isValidMutation() const {
        // all combinations legal
        return isValid();
    }

    [[nodiscard]] bool isValid() const {
        switch (extras.size()) {
        case 0: // None specified
        case 1: // Only doc flag
        case 4: // Expiry time
        case 5: // Expiry time and doc flag
        case 8: // Expiry time and User flags
        case 9: // Expiry time and User flags and Doc flag
            if ((subdoc::extrasDocFlagMask & getDocFlag()) !=
                subdoc::DocFlag::None) {
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
