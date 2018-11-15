/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>
#include <platform/platform.h>

namespace cb {
namespace mcbp {

bool Header::isRequest() const {
    const auto m = Magic(magic);
    return (m == Magic::ClientRequest || m == Magic::ServerRequest);
}

const Request& Header::getRequest() const {
    if (isRequest()) {
        return *reinterpret_cast<const Request*>(this);
    }
    throw std::logic_error("Header::getRequest(): Header is not a request");
}

bool Header::isResponse() const {
    const auto m = Magic(magic);
    return (m == Magic::ClientResponse || m == Magic::ServerResponse ||
            m == Magic::AltClientResponse);
}

const Response& Header::getResponse() const {
    if (isResponse()) {
        return *reinterpret_cast<const Response*>(this);
    }
    throw std::logic_error("Header::getResponse(): Header is not a response");
}

bool Header::isValid() const {
    if (isRequest()) {
        return getRequest().isValid();
    }

    if (isResponse()) {
        return getResponse().isValid();
    }

    return false;
}

nlohmann::json Header::toJSON() const {
    if (isRequest()) {
        return getRequest().toJSON();
    }

    if (isResponse()) {
        return getResponse().toJSON();
    }

    throw std::logic_error("Header::toJSON(): Invalid packet");
}

std::ostream& operator<<(std::ostream& os, const Header& header) {
    os << "mcbp::header:"
       << " magic:0x" << std::hex << int(header.getMagic()) << ", opcode:0x"
       << std::hex << int(header.getOpcode()) << ", keylen:" << std::dec
       << header.getKeylen() << ", extlen:" << std::dec
       << int(header.getExtlen()) << ", datatype:0x" << std::hex
       << int(header.getDatatype()) << ", specific:" << std::dec
       << header.getSpecific() << ", bodylen:" << std::dec
       << header.getBodylen() << ", opaque:0x" << std::hex
       << header.getOpaque();

    return os;
}

} // namespace mcbp
} // namespace cb
