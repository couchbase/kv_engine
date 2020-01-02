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

#include "datatype.h"

#include <mcbp/protocol/header.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <cstdint>

namespace cb {
namespace mcbp {

/**
 * Definition of the header structure for a response packet.
 * See section 2
 */
class Response {
public:
    void setMagic(Magic magic) {
        if (is_response(magic)) {
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
        Response::opcode = uint8_t(opcode);
    }

    ClientOpcode getClientOpcode() const {
        if (is_server_magic(getMagic())) {
            throw std::logic_error(
                    "getClientOpcode: magic != [Alt]client response");
        }
        return ClientOpcode(opcode);
    }

    void setOpcode(ServerOpcode opcode) {
        Response::opcode = uint8_t(opcode);
    }

    ServerOpcode getServerOpcode() const {
        if (getMagic() != Magic::ServerResponse) {
            throw std::logic_error(
                    "getServerOpcode: magic != server response: " +
                    std::to_string(uint8_t(getMagic())));
        }
        return ServerOpcode(opcode);
    }

    uint8_t getFramingExtraslen() const {
        return reinterpret_cast<const Header*>(this)->getFramingExtraslen();
    }

    uint16_t getKeylen() const {
        return reinterpret_cast<const Header*>(this)->getKeylen();
    }

    void setKeylen(uint16_t value) {
        if (value > 0xff) {
            throw std::invalid_argument(
                    "Response::setKeylen: key cannot exceed 1 byte");
        }
        keylen = uint8_t(value);
    }

    void setFramingExtraslen(uint8_t len) {
        frame_extlen = len;
    }

    uint8_t getExtlen() const {
        return reinterpret_cast<const Header*>(this)->getExtlen();
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
        return reinterpret_cast<const Header*>(this)->getBodylen();
    }

    void setBodylen(uint32_t value) {
        bodylen = htonl(value);
    }

    void setOpaque(uint32_t opaque) {
        Response::opaque = opaque;
    }

    uint32_t getOpaque() const {
        return reinterpret_cast<const Header*>(this)->getOpaque();
    }

    uint64_t getCas() const {
        return reinterpret_cast<const Header*>(this)->getCas();
    }

    void setCas(uint64_t val) {
        cas = htonll(val);
    }

    cb::const_byte_buffer getFramingExtras() const {
        return reinterpret_cast<const Header*>(this)->getFramingExtras();
    }

    cb::const_byte_buffer getExtdata() const {
        return reinterpret_cast<const Header*>(this)->getExtdata();
    }

    cb::const_byte_buffer getKey() const {
        return reinterpret_cast<const Header*>(this)->getKey();
    }

    cb::const_byte_buffer getValue() const {
        return reinterpret_cast<const Header*>(this)->getValue();
    }

    cb::const_byte_buffer getFrame() const {
        return reinterpret_cast<const Header*>(this)->getFrame();
    }

    nlohmann::json toJSON(bool validated) const;

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool isValid() const;

protected:
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
};

static_assert(sizeof(Response) == 24, "Incorrect compiler padding");

using DropPrivilegeResponse = Response;

} // namespace mcbp
} // namespace cb
