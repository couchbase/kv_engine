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

#include <mcbp/protocol/response.h>

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
        setKeylen(keyLen);
        setBodylen(keyLen + valueLen);
    }
};

} // namespace cb::mcbp::response
