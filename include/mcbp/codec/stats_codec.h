/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/response.h>
#include <cstdint>

namespace cb::mcbp::response {

class StatsResponse : public Response {
public:
    StatsResponse(size_t keyLen, size_t valueLen) {
        setMagic(cb::mcbp::Magic::ClientResponse);
        setOpcode(cb::mcbp::ClientOpcode::Stat);
        setDatatype(cb::mcbp::Datatype::Raw);
        setStatus(cb::mcbp::Status::Success);
        setFramingExtraslen(0);
        setExtlen(0);
        setKeylen(gsl::narrow_cast<uint16_t>(keyLen));
        setBodylen(gsl::narrow_cast<uint32_t>(keyLen + valueLen));
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }
};

static_assert(sizeof(StatsResponse) == sizeof(cb::mcbp::Header),
              "Unexpected struct size");

} // namespace cb::mcbp::response
