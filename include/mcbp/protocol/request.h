/*
 * Portions Copyright (c) 2015-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#pragma once

#include "datatype.h"

#include <mcbp/protocol/header.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/rbac/privilege_database.h>
#include <memcached/vbucket.h>
#include <optional>
#ifndef WIN32
#include <arpa/inet.h>
#endif

#include <cstdint>
#include <functional>

namespace cb::durability {
class Requirements;
} // namespace cb::durability

namespace cb::mcbp {

namespace request {
enum class FrameInfoId {
    Barrier = 0,
    DurabilityRequirement = 1,
    DcpStreamId = 2,
    OpenTracingContext = 3,
    Impersonate = 4,
    PreserveTtl = 5,
};
}

/**
 * Definition of the header structure for a request packet.
 * See section 2
 */
class Request {
public:
    void setMagic(Magic magic) {
        if (is_request(magic)) {
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
        Request::opcode = uint8_t(opcode);
    }

    ClientOpcode getClientOpcode() const {
        if (is_server_magic(getMagic())) {
            throw std::logic_error("getClientOpcode: magic != client request");
        }
        return ClientOpcode(opcode);
    }

    void setOpcode(ServerOpcode opcode) {
        Request::opcode = uint8_t(opcode);
    }

    ServerOpcode getServerOpcode() const {
        if (is_client_magic(getMagic())) {
            throw std::logic_error(
                    "getServerOpcode: magic != server request: " +
                    std::to_string(uint8_t(getMagic())));
        }
        return ServerOpcode(opcode);
    }

    void setKeylen(uint16_t value);

    uint16_t getKeylen() const {
        return reinterpret_cast<const Header*>(this)->getKeylen();
    }

    uint8_t getFramingExtraslen() const {
        return reinterpret_cast<const Header*>(this)->getFramingExtraslen();
    }

    void setFramingExtraslen(uint8_t len);

    void setExtlen(uint8_t extlen) {
        Request::extlen = extlen;
    }

    uint8_t getExtlen() const {
        return reinterpret_cast<const Header*>(this)->getExtlen();
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
        return reinterpret_cast<const Header*>(this)->getBodylen();
    }

    void setBodylen(uint32_t value) {
        bodylen = htonl(value);
    }

    void setOpaque(uint32_t opaque) {
        Request::opaque = opaque;
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

    /**
     * Get a printable version of the key (non-printable characters replaced
     * with a '.'
     */
    std::string getPrintableKey() const;

    cb::const_byte_buffer getFramingExtras() const {
        return reinterpret_cast<const Header*>(this)->getFramingExtras();
    }

    cb::byte_buffer getFramingExtras() {
        return reinterpret_cast<Header*>(this)->getFramingExtras();
    }

    cb::const_byte_buffer getExtdata() const {
        return reinterpret_cast<const Header*>(this)->getExtdata();
    }

    cb::byte_buffer getExtdata() {
        return reinterpret_cast<Header*>(this)->getExtdata();
    }

    cb::const_byte_buffer getKey() const {
        return reinterpret_cast<const Header*>(this)->getKey();
    }

    cb::byte_buffer getKey() {
        return reinterpret_cast<Header*>(this)->getKey();
    }

    cb::const_byte_buffer getValue() const {
        return reinterpret_cast<const Header*>(this)->getValue();
    }

    cb::byte_buffer getValue() {
        return reinterpret_cast<Header*>(this)->getValue();
    }

    cb::const_byte_buffer getFrame() const {
        return reinterpret_cast<const Header*>(this)->getFrame();
    }

    /**
     * Callback function to use while parsing the FrameExtras section
     *
     * The first parameter is the identifier for the frame info, the
     * second parameter is the content of the frame info.
     *
     * If the callback function should return false if it wants to stop
     * further parsing of the FrameExtras
     */
    using FrameInfoCallback = std::function<bool(cb::mcbp::request::FrameInfoId,
                                                 cb::const_byte_buffer)>;
    /**
     * Iterate over the provided frame extras
     *
     * @param callback The callback function to call for each frame info found.
     *                 if the callback returns false we stop parsing the
     *                 frame extras. Provided to the callback is the
     *                 Frame Info identifier and the content
     * @throws std::overflow_error if we overflow the encoded buffer.
     */
    void parseFrameExtras(FrameInfoCallback callback) const;

    /**
     * Parse the Frame Extras section and pick out the optional Durability
     * spec associated with the command.
     *
     * This method may throw exceptions iff the request object has not been
     * inspected by the packet validators.
     */
    std::optional<cb::durability::Requirements> getDurabilityRequirements()
            const;

    /**
     * Is this a quiet command or not
     */
    bool isQuiet() const;

    /**
     * Create a JSON dump of the provided object.
     *
     * If the object is validated the dump includes stuff present outside
     * the header object, otherwise we'll only touch the header fields (
     * as we can't trust all of the offsets etc).
     *
     * @param validated If the request is validated or not
     * @return A JSON representation of the packet
     */
    nlohmann::json toJSON(bool validated) const;

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool isValid() const;

protected:
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

} // namespace cb::mcbp

std::string to_string(cb::mcbp::request::FrameInfoId id);
