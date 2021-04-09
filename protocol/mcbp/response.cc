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

} // namespace cb::mcbp
