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
#pragma once

#include "magic.h"

#include <nlohmann/json_fwd.hpp>
#include <platform/platform_socket.h>
#include <platform/sized_buffer.h>

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <stdexcept>

namespace cb::mcbp {

#define MCBP_TRACING_RESPONSE_SIZE 0x03

class Request;
class Response;

/**
 * The header class is a representation of the header in a binary protocol.
 * It is (slightly) different for a request and a response packet, but the
 * size is the same.
 *
 * The header class allows us to pick out the header, and inspect the
 * common fields (without first determining if the packet is a request
 * or a response).
 *
 * If you receive a packet from an external source you should call
 * isValid() on the packet first to check if the magic is valid and
 * that the length fields in the packet adds up.
 */
class Header {
public:
    uint8_t getMagic() const {
        return magic;
    }

    uint8_t getOpcode() const {
        return opcode;
    }

    uint16_t getKeylen() const {
        if (is_alternative_encoding(Magic(magic))) {
            return uint16_t(reinterpret_cast<const uint8_t*>(this)[3]);
        }
        return ntohs(keylen);
    }

    uint8_t getFramingExtraslen() const {
        if (is_alternative_encoding(Magic(magic))) {
            return reinterpret_cast<const uint8_t*>(this)[2];
        }

        return uint8_t(0);
    }

    uint8_t getExtlen() const {
        return extlen;
    }

    uint8_t getDatatype() const {
        return datatype;
    }

    uint16_t getSpecific() const {
        return ntohs(specific);
    }

    uint32_t getBodylen() const {
        return ntohl(bodylen);
    }

    uint32_t getOpaque() const {
        return opaque;
    }

    uint64_t getCas() const {
        return ntohll(cas);
    }

    /**
     * Does this packet represent a request packet?
     *
     * @throws std::invalid_argument if the magic byte isn't a legal
     *         value
     */
    bool isRequest() const {
        return is_request(Magic(magic));
    }

    /**
     * Get a request object from this packet. Note that the entire
     * object may not be present (if called while we're still spooling
     * data for the object). The entire header is however available
     */
    const cb::mcbp::Request& getRequest() const {
        if (isRequest()) {
            return *reinterpret_cast<const Request*>(this);
        }
        throw std::logic_error("Header::getRequest(): Header is not a request");
    }

    /**
     * Does this packet represent a response packet?
     *
     * @throws std::invalid_argument if the magic byte isn't a legal
     *         value
     */
    bool isResponse() const {
        return is_response(Magic(magic));
    }

    /**
     * Get a response object from this packet. Note that the entire
     * object may not be present (if called while we're still spooling
     * data for the object). The entire header is however available
     */
    const cb::mcbp::Response& getResponse() const {
        if (isResponse()) {
            return *reinterpret_cast<const Response*>(this);
        }
        throw std::logic_error(
                "Header::getResponse(): Header is not a response");
    }

    /**
     * Get the byte buffer containing the framing extras for this
     * packet.
     *
     * To simplify the use of the code an empty buffer is returned
     * for packets which isn't using the framing extras.
     */
    cb::const_byte_buffer getFramingExtras() const {
        const auto* base = reinterpret_cast<const uint8_t*>(this);
        size_t length = 0;
        if (is_alternative_encoding(Magic(magic))) {
            length = base[2];
        }
        // This packet does not have any Framing Extras
        return {base + sizeof(*this), length};
    }

    cb::byte_buffer getFramingExtras() {
        auto cbb = const_cast<const Header&>(*this).getFramingExtras();
        return {const_cast<uint8_t*>(cbb.data()), cbb.size()};
    }

    /**
     * Get the byte buffer containing the extras
     */
    cb::const_byte_buffer getExtdata() const {
        auto loc = getFramingExtras();
        return {loc.end(), getExtlen()};
    }

    cb::byte_buffer getExtdata() {
        auto loc = getFramingExtras();
        return {const_cast<uint8_t*>(loc.end()), getExtlen()};
    }

    /**
     * Get the byte buffer containing the key
     */
    cb::const_byte_buffer getKey() const {
        auto loc = getExtdata();
        return {loc.end(), getKeylen()};
    }

    cb::byte_buffer getKey() {
        auto cbb = const_cast<const Header&>(*this).getKey();
        return {const_cast<uint8_t*>(cbb.data()), cbb.size()};
    }

    std::string_view getKeyString() const {
        const auto key = getKey();
        return {reinterpret_cast<const char*>(key.data()), key.size()};
    }

    /**
     * Get the byte buffer containing the value (this is not the entire
     * payload of the message, but the portion after the key in the packet)
     */
    cb::const_byte_buffer getValue() const {
        auto fe = getFramingExtras();
        auto ex = getExtdata();
        auto key = getKey();
        return {key.end(), getBodylen() - key.size() - ex.size() - fe.size()};
    }

    cb::byte_buffer getValue() {
        auto cbb = const_cast<const Header&>(*this).getValue();
        return {const_cast<uint8_t*>(cbb.data()), cbb.size()};
    }

    std::string_view getValueString() const {
        const auto val = getValue();
        return {reinterpret_cast<const char*>(val.data()), val.size()};
    }

    /**
     * Get the byte buffer of the entire packet where this header and body
     * spans
     */
    cb::const_byte_buffer getFrame() const {
        return {reinterpret_cast<const uint8_t*>(this),
                sizeof(*this) + getBodylen()};
    }

    /**
     * Do basic validation of the packet by verifying that the magic is
     * legal and that the length fields adds up.
     * @return
     */
    bool isValid() const;

    /**
     * Create a JSON representation of the header
     */
    nlohmann::json toJSON(bool validated) const;

protected:
    /*
     * This is the binary representation of the packet as described in
     * the binary protocol (see Packet Structure in docs/BinaryProtocol.md).
     *
     * All of the fields is stored in network byte order, and for all of the
     * "multibyte" fields there is an accessory function which perform the
     * "correct" translation (if needed). (Some fields (like opaque) isn't
     * being translated, so it does not do any conversion).
     */
    uint8_t magic;
    uint8_t opcode;
    // In the alternative encoding the first byte is the frame extras,
    // followed by the encoding. getKeylen() hides the difference
    uint16_t keylen;
    uint8_t extlen;
    uint8_t datatype;
    uint16_t specific;
    uint32_t bodylen;
    uint32_t opaque;
    uint64_t cas;
};

static_assert(sizeof(Header) == 24, "Incorrect compiler padding");

std::ostream& operator<<(std::ostream& os, const Header& header);

} // namespace cb::mcbp
