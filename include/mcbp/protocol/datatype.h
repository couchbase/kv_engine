/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <iosfwd>

namespace cb::mcbp {

/**
 * Definition of the data types in the packet
 * See section 3.4 Data Types
 */

enum class Datatype : uint8_t {
    /// "unknown" data
    Raw = 0,
    /// The data contains JSON
    JSON = 1,
    /// The data is compressed with Snappy compression
    Snappy = 2,
    /// Convenience for Snappy Compressed JSON data
    SnappyCompressedJson = JSON | Snappy,
    /// The data contains (at least one) XAttr section
    Xattr = 4
};

static_assert((int(Datatype::Raw) | int(Datatype::JSON) |
               int(Datatype::Snappy) | int(Datatype::Xattr)) <= 0b111,
              "We expect datatype to require 3 bits to encode for a layout "
              "optimisation in StoredValue.");

} // namespace cb::mcbp

/**
 * Legacy 'datatype' type - not strongly typed like cb::mcbp::Datatype.
 * Prefer cb::mcbp::Datatype where possible for new code.
 */
using protocol_binary_datatype_t = uint8_t;
#define PROTOCOL_BINARY_RAW_BYTES uint8_t(cb::mcbp::Datatype::Raw)
#define PROTOCOL_BINARY_DATATYPE_JSON uint8_t(cb::mcbp::Datatype::JSON)
#define PROTOCOL_BINARY_DATATYPE_SNAPPY uint8_t(cb::mcbp::Datatype::Snappy)
#define PROTOCOL_BINARY_DATATYPE_XATTR uint8_t(cb::mcbp::Datatype::Xattr)

/*
 * Bitmask that defines datatypes that can only be valid when a document body
 * exists. i.e. When the document is not soft-deleted
 */
#define BODY_ONLY_DATATYPE_MASK \
    uint8_t(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY);

std::string to_string(cb::mcbp::Datatype datatype);
nlohmann::json to_json(cb::mcbp::Datatype datatype);

// Create a namespace to handle the Datatypes
namespace cb::mcbp::datatype {
const uint8_t highest = PROTOCOL_BINARY_DATATYPE_XATTR |
                        PROTOCOL_BINARY_DATATYPE_SNAPPY |
                        PROTOCOL_BINARY_DATATYPE_JSON;
inline bool is_raw(const protocol_binary_datatype_t datatype) {
    return datatype == PROTOCOL_BINARY_RAW_BYTES;
}

inline bool is_json(const protocol_binary_datatype_t datatype) {
    return (datatype & PROTOCOL_BINARY_DATATYPE_JSON) ==
           PROTOCOL_BINARY_DATATYPE_JSON;
}

inline bool is_snappy(const protocol_binary_datatype_t datatype) {
    return (datatype & PROTOCOL_BINARY_DATATYPE_SNAPPY) ==
           PROTOCOL_BINARY_DATATYPE_SNAPPY;
}

inline bool is_xattr(const protocol_binary_datatype_t datatype) {
    return (datatype & PROTOCOL_BINARY_DATATYPE_XATTR) ==
           PROTOCOL_BINARY_DATATYPE_XATTR;
}

inline bool is_valid(const protocol_binary_datatype_t datatype) {
    return datatype <= highest;
}

std::string to_string(protocol_binary_datatype_t datatype);
} // namespace cb::mcbp::datatype
