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

#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>

namespace cb::mcbp {

bool Header::isValid() const {
    if (is_legal(Magic(magic))) {
        if (isRequest()) {
            return getRequest().isValid();
        }

        if (isResponse()) {
            return getResponse().isValid();
        }
    }
    return false;
}

nlohmann::json Header::to_json(bool validated) const {
    if (isRequest()) {
        return getRequest().to_json(validated);
    }

    if (isResponse()) {
        return getResponse().to_json(validated);
    }

    throw std::logic_error("Header::to_json(): Invalid packet");
}

std::ostream& operator<<(std::ostream& os, const Header& header) {
    const auto m = header.getMagic();
    os << "mcbp::header:"
       << " magic:0x" << std::hex << int(m) << ", opcode:0x" << std::hex
       << int(header.getOpcode());
    if (is_legal(Magic(m)) && is_alternative_encoding(Magic(m))) {
        os << ", frameExtras:" << std::dec << int(header.getFramingExtraslen());
    }
    os << ", keylen:" << std::dec << header.getKeylen()
       << ", extlen:" << std::dec << int(header.getExtlen()) << ", datatype:0x"
       << std::hex << int(header.getDatatype()) << ", specific:" << std::dec
       << header.getSpecific() << ", bodylen:" << std::dec
       << header.getBodylen() << ", opaque:0x" << std::hex << header.getOpaque()
       << ", cas:0x" << std::hex << header.getCas();

    return os;
}

} // namespace cb::mcbp
