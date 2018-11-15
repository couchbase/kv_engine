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

#include <cJSON_utils.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/vbucket.h>
#include <platform/platform.h>
#include <platform/sized_buffer.h>

#ifndef WIN32
#include <arpa/inet.h>
#endif

#include <cstdint>

namespace cb {
namespace mcbp {

/**
 * Definition of the header structure for a request packet.
 * See section 2
 */
class Request {
public:
    // Convenience methods to get/set the various fields in the header (in
    // the correct byteorder)

    void setMagic(Magic magic) {
        if (magic == Magic::ClientRequest || magic == Magic::ServerRequest) {
            Request::magic = uint8_t(magic);
        } else {
            throw std::invalid_argument(
                    "Request::setMagic: Invalid magic specified: " +
                    std::to_string(uint8_t(magic)));
        }
    }

    Magic getMagic() const {
        return Magic(magic);
    }

    void setOpcode(ClientOpcode opcode) {
        setMagic(Magic::ClientRequest);
        Request::opcode = uint8_t(opcode);
    }

    ClientOpcode getClientOpcode() const {
        if (getMagic() != Magic::ClientRequest) {
            throw std::logic_error("getClientOpcode: magic != client request");
        }
        return ClientOpcode(opcode);
    }

    void setOpcode(ServerOpcode opcode) {
        setMagic(Magic::ServerRequest);
        Request::opcode = uint8_t(opcode);
    }

    ServerOpcode getServerOpcode() const {
        if (getMagic() != Magic::ServerRequest) {
            throw std::logic_error("getServerOpcode: magic != server request");
        }
        return ServerOpcode(opcode);
    }

    void setKeylen(uint16_t value) {
        keylen = htons(value);
    }

    uint16_t getKeylen() const {
        return ntohs(keylen);
    }

    void setExtlen(uint8_t extlen) {
        Request::extlen = extlen;
    }

    uint8_t getExtlen() const {
        return extlen;
    }

    void setDatatype(Datatype datatype) {
        Request::datatype = uint8_t(datatype);
    }

    Datatype getDatatype() const {
        return Datatype(datatype);
    }

    void setVBucket(Vbid value) {
        vbucket = value.hton();
    }

    Vbid getVBucket() const {
        return vbucket.ntoh();
    }

    uint32_t getBodylen() const {
        return ntohl(bodylen);
    }

    void setBodylen(uint32_t value) {
        bodylen = htonl(value);
    }

    void setOpaque(uint32_t opaque) {
        Request::opaque = opaque;
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

    cb::const_byte_buffer getKey() const;

    /**
     * Get a printable version of the key (non-printable characters replaced
     * with a '.'
     */
    std::string getPrintableKey() const;

    cb::const_byte_buffer getExtdata() const;

    cb::const_byte_buffer getValue() const;

    /**
     * Is this a quiet command or not
     */
    bool isQuiet() const;

    nlohmann::json toJSON() const;

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool isValid() const;

    uint8_t magic;
    uint8_t opcode;
    uint16_t keylen;
    uint8_t extlen;
    uint8_t datatype;
    Vbid vbucket;
    uint32_t bodylen;
    uint32_t opaque;
    uint64_t cas;
};

static_assert(sizeof(Request) == 24, "Incorrect compiler padding");

using DropPrivilegeRequest = Request;

} // namespace mcbp
} // namespace cb
