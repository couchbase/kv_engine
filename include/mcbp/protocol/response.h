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
#include <mcbp/protocol/status.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>
#include <cstdint>
#ifndef WIN32
#include <arpa/inet.h>
#endif

namespace cb {
namespace mcbp {

/**
 * Definition of the header structure for a response packet.
 * See section 2
 */
struct Response {
    uint8_t magic;
    uint8_t opcode;
    uint8_t frame_extlen;
    uint8_t keylen;
    uint8_t extlen;
    uint8_t datatype;
    uint16_t status;
    uint32_t bodylen;
    uint32_t opaque;
    uint64_t cas;

    // Convenience methods to get/set the various fields in the header (in
    // the correct byteorder)

    void setMagic(Magic magic) {
        if (magic == Magic::ClientResponse ||
            magic == Magic::AltClientResponse ||
            magic == Magic::ServerResponse) {
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

    void setOpcode(ClientOpcode opcode) {
        setMagic(Magic::ClientResponse);
        Response::opcode = uint8_t(opcode);
    }

    ClientOpcode getClientOpcode() const {
        if (getMagic() != Magic::ClientResponse &&
            getMagic() != Magic::AltClientResponse) {
            throw std::logic_error(
                    "getClientOpcode: magic != [Alt]client response");
        }
        return ClientOpcode(opcode);
    }

    void setOpcode(ServerOpcode opcode) {
        setMagic(Magic::ServerResponse);
        Response::opcode = uint8_t(opcode);
    }

    ServerOpcode getServerOpcode() const {
        if (getMagic() != Magic::ServerResponse) {
            throw std::logic_error("getServerOpcode: magic != server response");
        }
        return ServerOpcode(opcode);
    }

    bool isAltClientResponse() const {
        return getMagic() == Magic::AltClientResponse;
    }

    uint8_t getFramingExtraslen() const {
        if (!isAltClientResponse()) {
            return 0;
        }
        return begin()[2];
    }

    uint16_t getKeylen() const {
        return keylen;
    }

    void setKeylen(uint16_t value) {
        if (value > 0xff) {
            throw std::invalid_argument(
                    "Response::setKeylen: key cannot exceed 1 byte");
        }
        keylen = uint8_t(value);
    }

    uint8_t getFrameExtlen() const {
        return frame_extlen;
    }

    void setFrameExtlen(uint8_t len) {
        frame_extlen = len;
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

    cb::mcbp::Status getStatus() const {
        return cb::mcbp::Status(ntohs(status));
    }

    void setStatus(cb::mcbp::Status value) {
        status = htons(uint16_t(value));
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

    uint32_t getValuelen() const {
        return getBodylen() -
               (getKeylen() + getExtlen() + getFramingExtraslen());
    }

    size_t getHeaderlen() const {
        return sizeof(*this);
    }

    // offsets from payload begin
    const uint8_t* begin() const {
        return reinterpret_cast<const uint8_t*>(this);
    }

    size_t getFramingExtrasOffset() const {
        return getHeaderlen();
    }

    size_t getExtOffset() const {
        return getFramingExtrasOffset() + getFramingExtraslen();
    }

    size_t getKeyOffset() const {
        return getExtOffset() + getExtlen();
    }

    size_t getValueOffset() const {
        return getKeyOffset() + getKeylen();
    }

    cb::const_byte_buffer getFramingExtras() const {
        return {begin() + getFramingExtrasOffset(), getFramingExtraslen()};
    }

    cb::const_byte_buffer getKey() const {
        return {begin() + getKeyOffset(), getKeylen()};
    }

    cb::const_byte_buffer getExtdata() const {
        return {begin() + getExtOffset(), getExtlen()};
    }

    cb::const_byte_buffer getValue() const {
        return {begin() + getValueOffset(), getValuelen()};
    }

    nlohmann::json toJSON() const;

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool isValid() const {
        auto m = Magic(magic);
        if (m != Magic::ClientResponse && m != Magic::ServerResponse &&
            m != Magic::AltClientResponse) {
            return false;
        }

        return (size_t(getExtlen()) +
                        size_t(getKeylen() + getFramingExtraslen()) <=
                size_t(getBodylen()));
    }
};

static_assert(sizeof(Response) == 24, "Incorrect compiler padding");

using DropPrivilegeResponse = Response;

} // namespace mcbp
} // namespace cb
