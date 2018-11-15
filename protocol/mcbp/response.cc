/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>

namespace cb {
namespace mcbp {

uint32_t Response::getValuelen() const {
    return getBodylen() - (getKeylen() + getExtlen() + getFramingExtraslen());
}

size_t Response::getHeaderlen() const {
    return sizeof(*this);
}

// offsets from payload begin
const uint8_t* Response::begin() const {
    return reinterpret_cast<const uint8_t*>(this);
}

size_t Response::getFramingExtrasOffset() const {
    return getHeaderlen();
}

size_t Response::getExtOffset() const {
    return getFramingExtrasOffset() + getFramingExtraslen();
}

size_t Response::getKeyOffset() const {
    return getExtOffset() + getExtlen();
}

size_t Response::getValueOffset() const {
    return getKeyOffset() + getKeylen();
}

cb::const_byte_buffer Response::getFramingExtras() const {
    return {begin() + getFramingExtrasOffset(), getFramingExtraslen()};
}

cb::const_byte_buffer Response::getKey() const {
    return {begin() + getKeyOffset(), getKeylen()};
}

cb::const_byte_buffer Response::getExtdata() const {
    return {begin() + getExtOffset(), getExtlen()};
}

cb::const_byte_buffer Response::getValue() const {
    return {begin() + getValueOffset(), getValuelen()};
}

nlohmann::json Response::toJSON() const {
    if (!isValid()) {
        throw std::logic_error("Response::toJSON(): Invalid packet");
    }

    nlohmann::json ret;
    auto m = cb::mcbp::Magic(magic);
    ret["magic"] = ::to_string(m);

    if (m == Magic::ClientResponse || m == Magic::AltClientResponse) {
        ret["opcode"] = ::to_string(getClientOpcode());

    } else {
        ret["opcode"] = ::to_string(getServerOpcode());
    }

    ret["keylen"] = getKeylen();
    ret["extlen"] = getExtlen();

    if (m == Magic::AltClientResponse) {
        ret["framingextra"] = getFramingExtraslen();
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
    if (m != Magic::ClientResponse && m != Magic::ServerResponse &&
        m != Magic::AltClientResponse) {
        return false;
    }

    return (size_t(getExtlen()) + size_t(getKeylen() + getFramingExtraslen()) <=
            size_t(getBodylen()));
}

} // namespace mcbp
} // namespace cb
