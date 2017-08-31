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
#pragma once

#include "config.h"
#include "datatype.h"

#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <platform/sized_buffer.h>

#include <cstdint>

namespace cb {
namespace mcbp {

/**
 * Definition of the header structure for a response packet.
 * See section 2
 */
struct Response {
    uint8_t magic;
    uint8_t opcode;
    uint16_t keylen;
    uint8_t extlen;
    uint8_t datatype;
    uint16_t status;
    uint32_t bodylen;
    uint32_t opaque;
    uint64_t cas;

    // Convenience methods to get/set the various fields in the header (in
    // the correct byteorder)

    void setMagic(Magic magic) {
        if (magic == Magic::ClientResponse || magic == Magic::ServerResponse) {
            Response::magic = uint8_t(magic);
        } else {
            throw std::invalid_argument(
                    "Response::setMagic: Invalid magic specified: " +
                    std::to_string(uint8_t(magic)));
        }
    }

    Magic getMagic() const {
        return Magic(magic);
    }

    void setOpcode(Opcode opcode) {
        Response::opcode = uint8_t(opcode);
    }

    Opcode getOpcode() const {
        return Opcode(opcode);
    }

    uint16_t getKeylen() const {
        return ntohs(keylen);
    }

    void setKeylen(uint16_t value) {
        keylen = htons(value);
    }

    uint8_t getExtlen() const {
        return extlen;
    }

    void setExtlen(uint8_t extlen) {
        Response::extlen = extlen;
    }

    void setDatatype(Datatype datatype) {
        Response::datatype = uint8_t(datatype);
    }

    Datatype getDatatype() const {
        return Datatype(datatype);
    }

    uint16_t getStatus() const {
        return ntohs(status);
    }

    void setStatus(uint16_t value) {
        status = htons(value);
    }

    uint32_t getBodylen() const {
        return ntohl(bodylen);
    }

    void setBodylen(uint32_t value) {
        bodylen = htonl(value);
    }

    void setOpaque(uint32_t opaque) {
        Response::opaque = opaque;
    }

    uint32_t getOpaque() const {
        return opaque;
    }

    uint64_t getCas() const {
        return ntohll(cas);
    }

    void setCas(uint64_t val) {
        cas = htonll(val);
    }

    cb::const_byte_buffer getKey() const {
        return {reinterpret_cast<const uint8_t*>(this) + sizeof(*this) + extlen,
                getKeylen()};
    }

    cb::const_byte_buffer getExtdata() const {
        return {reinterpret_cast<const uint8_t*>(this) + sizeof(*this), extlen};
    }

    cb::const_byte_buffer getValue() const {
        const auto buf = getKey();
        return {buf.data() + buf.size(), getBodylen() - getKeylen() - extlen};
    }

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool validate() {
        auto m = Magic(magic);
        if (m != Magic::ClientResponse && m != Magic::ServerResponse) {
            return false;
        }

        return (size_t(extlen) + size_t(getKeylen()) <= size_t(getBodylen()));
    }
};

static_assert(sizeof(Response) == 24, "Incorrect compiler padding");

using DropPrivilegeResponse = Response;

} // namespace mcbp
} // namespace cb
