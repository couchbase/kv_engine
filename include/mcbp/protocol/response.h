/*
 * Portions Copyright (c) 2015-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#pragma once

#include "datatype.h"

#include <mcbp/protocol/header.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json_fwd.hpp>
#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <cstdint>
#include <functional>

namespace cb::mcbp {

namespace response {
/// See BinaryProtocol.md for a description of the available
/// frame identifiers and their encoding
enum class FrameInfoId : uint8_t {
    ServerRecvSendDuration = 0,
    ReadUnits = 1,
    WriteUnits = 2,
    ThrottleDuration = 3,
};

/// ServerRecvSendDuration use 1 byte header and 2 byte value
static constexpr size_t ServerRecvSendDurationFrameInfoSize = 3;
/// The magic (id and size) for a ServerRecvSendDuration
static constexpr uint8_t ServerRecvSendDurationFrameInfoMagic = 0x02;

/// ReadUnits use 1 byte header and 2 byte value
static constexpr size_t ReadUnitsFrameInfoSize = 3;
static constexpr uint8_t ReadUnitsFrameInfoMagic = 0x12;

/// WriteUnits use 1 byte header and 2 byte value
static constexpr size_t WriteUnitsFrameInfoSize = 3;
static constexpr uint8_t WriteUnitsFrameInfoMagic = 0x22;

/// ThrottleDuration use 1 byte header and 2 byte value
static constexpr size_t ThrottleDurationFrameInfoSize = 3;
static constexpr uint8_t ThrottleDurationFrameInfoMagic = 0x32;
} // namespace response

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
        Response::datatype = static_cast<uint8_t>(datatype);
    }

    void setDatatype(protocol_binary_datatype_t datatype_) {
        datatype = datatype_;
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
    std::string_view getKeyString() const {
        auto ret = getKey();
        return {reinterpret_cast<const char*>(ret.data()), ret.size()};
    }

    cb::const_byte_buffer getValue() const {
        return reinterpret_cast<const Header*>(this)->getValue();
    }
    cb::byte_buffer getValue() {
        return reinterpret_cast<Header*>(this)->getValue();
    }

    std::string_view getValueString() const {
        return reinterpret_cast<const Header*>(this)->getValueString();
    }

    cb::const_byte_buffer getFrame() const {
        return reinterpret_cast<const Header*>(this)->getFrame();
    }

    nlohmann::json to_json(bool validated) const;

    /**
     * Validate that the header is "sane" (correct magic, and extlen+keylen
     * doesn't exceed the body size)
     */
    bool isValid() const;

    /**
     * Callback function to use while parsing the FrameExtras section
     *
     * The first parameter is the identifier for the frame info, the
     * second parameter is the content of the frame info.
     *
     * If the callback function should return false if it wants to stop
     * further parsing of the FrameExtras
     */
    using FrameInfoCallback = std::function<bool(
            cb::mcbp::response::FrameInfoId, cb::const_byte_buffer)>;
    /**
     * Iterate over the provided frame extras
     *
     * @param callback The callback function to call for each frame info found.
     *                 if the callback returns false we stop parsing the
     *                 frame extras. Provided to the callback is the
     *                 Frame Info identifier and the content
     * @throws std::overflow_error if we overflow the encoded buffer.
     */
    void parseFrameExtras(const FrameInfoCallback& callback) const;

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

} // namespace cb::mcbp
