/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/response.h>

#include <mcbp/protocol/header.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>

namespace cb::mcbp {

nlohmann::json Response::toJSON(bool validated) const {
    if (!validated && !isValid()) {
        throw std::logic_error("Response::toJSON(): Invalid packet");
    }

    nlohmann::json ret;
    auto m = cb::mcbp::Magic(magic);
    ret["magic"] = ::to_string(m);

    if (is_client_magic(m)) {
        ret["opcode"] = ::to_string(getClientOpcode());

    } else {
        ret["opcode"] = ::to_string(getServerOpcode());
    }

    ret["keylen"] = getKeylen();
    ret["extlen"] = getExtlen();

    if (m == Magic::AltClientResponse) {
        ret["framingextra"] = getFramingExtras().size();
    }

    ret["datatype"] = ::toJSON(getDatatype());
    ret["status"] = ::to_string(Status(getStatus()));
    ret["bodylen"] = getBodylen();
    ret["opaque"] = getOpaque();
    ret["cas"] = getCas();

    return ret;
}

bool Response::isValid() const {
    auto m = Magic(magic);
    if (!is_legal(m) || !is_response(m)) {
        return false;
    }

    return (size_t(getExtlen()) + size_t(getKeylen() + getFramingExtraslen()) <=
            size_t(getBodylen()));
}

void Response::parseFrameExtras(FrameInfoCallback callback) const {
    auto fe = getFramingExtras();
    if (fe.empty()) {
        return;
    }
    size_t offset = 0;
    while (offset < fe.size()) {
        using cb::mcbp::response::FrameInfoId;

        auto idbits = size_t(fe[offset] >> 4);
        auto size = size_t(fe[offset] & 0x0f);
        ++offset;

        if (idbits == 0x0f) {
            // This is the escape byte
            if ((offset + 1) > fe.size()) {
                throw std::overflow_error(
                        "parseFrameExtras: outside frame extras");
            }
            idbits += fe[offset++];
        }

        if (size == 0x0f) {
            // This is the escape value
            if ((offset + 1) > fe.size()) {
                throw std::overflow_error(
                        "parseFrameExtras: outside frame extras");
            }
            size += fe[offset++];
        }

        const auto id = FrameInfoId(idbits);
        if ((offset + size) > fe.size()) {
            throw std::overflow_error("parseFrameExtras: outside frame extras");
        }

        cb::const_byte_buffer content{fe.data() + offset, size};
        offset += size;

        if (!callback(id, content)) {
            return;
        }
    }
}

} // namespace cb::mcbp
