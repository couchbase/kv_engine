/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <libgreenstack/Message.h>
#include <libgreenstack/Frame.h>
#include <libgreenstack/Opcodes.h>
#include <libgreenstack/Response.h>
#include <libgreenstack/Request.h>
#include <libgreenstack/core/Hello.h>
#include <libgreenstack/Reader.h>
#include <libgreenstack/core/SaslAuth.h>
#include <libgreenstack/Writer.h>

#include <iostream>
#include <iomanip>
#include <cassert>
#include <stdexcept>
#include <libgreenstack/memcached/Mutation.h>
#include <libgreenstack/memcached/AssumeRole.h>
#include <libgreenstack/memcached/CreateBucket.h>
#include <libgreenstack/memcached/DeleteBucket.h>
#include <libgreenstack/memcached/ListBuckets.h>
#include <libgreenstack/memcached/SelectBucket.h>
#include <libgreenstack/memcached/Get.h>

Greenstack::Message::Message(bool response)
    : opaque(std::numeric_limits<uint32_t>::max()),
      opcode(Opcode::InvalidOpcode) {
    memset(&flags, 0, sizeof(flags));
    flags.response = response;
}

Greenstack::Message::~Message() {

}

void Greenstack::Message::setOpaque(uint32_t value) {
    opaque = value;
}

uint32_t Greenstack::Message::getOpaque() const {
    return opaque;
}

void Greenstack::Message::setOpcode(const Opcode& value) {
    opcode = value;
}

Greenstack::Opcode Greenstack::Message::getOpcode() const {
    return opcode;
}

void Greenstack::Message::setFenceBit(bool enable) {
    flags.fence = enable;
}

bool Greenstack::Message::isFenceBitSet() const {
    return flags.fence;
}

void Greenstack::Message::setMoreBit(bool enable) {
    flags.more = enable;
}

bool Greenstack::Message::isMoreBitSet() const {
    return flags.more;
}

void Greenstack::Message::setQuietBit(bool enable) {
    flags.quiet = enable;
}

bool Greenstack::Message::isQuietBitSet() const {
    return flags.quiet;
}

Greenstack::FlexHeader& Greenstack::Message::getFlexHeader() {
    return flexHeader;
}

const Greenstack::FlexHeader& Greenstack::Message::getFlexHeader() const {
    return flexHeader;
}

void Greenstack::Message::setPayload(std::vector<uint8_t>& data) {
    payload.assign(data.begin(), data.end());
}

void Greenstack::Message::validate() {
    // Empty
}

/**
* Encode the message into the byte array. The format of the packet is
*
*        0         1         2         3
*   +---------+---------+---------+---------+
*   |  Opaque                               |
*   +---------+---------+---------+---------+
*   |  Opcode           |  Flags  |
*   +---------+---------+---------+
*   7 bytes
*
*   If the response bit in the flag section is set then the
*   following layout follows
*
*        0         1
*   +---------+---------+
*   |  Status           |
*   +---------+---------+
*
*   If the flex header bit in the flag section is set the following
*   layout follows:
*
*        0         1         2         3
*   +---------+---------+---------+---------+
*   |  Flex header length                   |
*   +---------+---------+---------+---------+
*   |  N bytes with flex header data        |
*   +---------+---------+---------+---------+
*   4 + n bytes
*
*   Finally the actual data payload for the command follows
*/
size_t Greenstack::Message::encode(std::vector<uint8_t>& vector,
                                   size_t offset) const {
    VectorWriter writer(vector, offset);

    writer.write(opaque);
    writer.write(uint16_t(opcode));

    Flags theFlags = flags;
    theFlags.flexHeaderPresent = !flexHeader.isEmpty();

    // This is a hack to convert my bitfield to a byte
    writer.write(reinterpret_cast<uint8_t*>(&theFlags), 1);

    // move offset past the fixed size header:
    offset += 7;

    if (flags.response) {
        const Response* r = dynamic_cast<const Response*>(this);
        assert(r);
        writer.write(uint16_t(r->getStatus()));
        offset += 2;
    }

    if (theFlags.flexHeaderPresent) {
        // the header should be written after the length field
        offset += 4;
        VectorWriter out(vector, offset);
        size_t nbytes = flexHeader.encode(out);
        writer.write(static_cast<uint32_t>(nbytes));
        writer.skip(nbytes);
    }

    if (payload.size() > 0) {
        writer.write(payload.data(), payload.size());
    }

    return writer.getOffset();
}

Greenstack::Message* Greenstack::Message::create(Greenstack::Reader& reader,
                                                 size_t size) {
    Message* ret;
    size_t endOffset = reader.getOffset() + size;

    uint32_t opaque;
    uint16_t opc;
    struct Flags flags;

    reader.read(opaque);
    reader.read(opc);

    Opcode opcode = to_opcode(opc);
    if (opcode == Opcode::InvalidOpcode) {
        throw std::runtime_error("Unknown opcode: " + std::to_string(opc));
    }

    reader.read(reinterpret_cast<uint8_t*>(&flags), 1);

    if (flags.next) {
        throw std::runtime_error("Only one flag byte is defined");
    }

    if (flags.unassigned) {
        throw std::runtime_error("Do not use unassigned bits");
    }

    ret = createInstance(flags.response, opcode);
    ret->opaque = opaque;
    ret->opcode = opcode;
    ret->flags = flags;

    if (flags.response) {
        Response* r = dynamic_cast<Response*>(ret);
        uint16_t status;
        reader.read(status);
        r->setStatus(to_status(status));
        if (r->getStatus() == Status::InvalidStatusCode) {
            delete ret;
            throw std::runtime_error(
                "Invalid status code " + std::to_string(status));
        }
    }

    if (flags.flexHeaderPresent) {
        uint32_t nbytes;
        reader.read(nbytes);

        std::vector<uint8_t> data;
        data.resize(nbytes);
        reader.read(data.data(), nbytes);
        ByteArrayReader in(data);
        auto flex = FlexHeader::create(in);
        ret->flexHeader = flex;
    }

    // Set the content
    size_t payloadSize = endOffset - reader.getOffset();
    ret->payload.resize(payloadSize);
    reader.read(ret->payload.data(), payloadSize);

    try {
        ret->validate();
    } catch (std::runtime_error& err) {
        delete ret;
        throw err;
    }

    return ret;
}

Greenstack::Message* Greenstack::Message::createInstance(bool response,
                                                         const Greenstack::Opcode& opcode) {
    switch (opcode) {
    case Greenstack::Opcode::Hello:
        if (response) {
            return new HelloResponse;
        } else {
            return new HelloRequest;
        }
    case Greenstack::Opcode::SaslAuth:
        if (response) {
            return new SaslAuthResponse;
        } else {
            return new SaslAuthRequest;
        }
    case Greenstack::Opcode::Mutation:
        if (response) {
            return new MutationResponse;
        } else {
            return new MutationRequest;
        }
    case Greenstack::Opcode::AssumeRole:
        if (response) {
            return new AssumeRoleResponse;
        } else {
            return new AssumeRoleRequest;
        }
    case Greenstack::Opcode::CreateBucket:
        if (response) {
            return new CreateBucketResponse;
        } else {
            return new CreateBucketRequest;
        }
    case Greenstack::Opcode::DeleteBucket:
        if (response) {
            return new DeleteBucketResponse;
        } else {
            return new DeleteBucketRequest;
        }
    case Greenstack::Opcode::SelectBucket:
        if (response) {
            return new SelectBucketResponse;
        } else {
            return new SelectBucketRequest;
        }
    case Greenstack::Opcode::ListBuckets:
        if (response) {
            return new ListBucketsResponse;
        } else {
            return new ListBucketsRequest;
        }
    case Greenstack::Opcode::Get:
        if (response) {
            return new GetResponse;
        } else {
            return new GetRequest;
        }

    default:
        if (response) {
            return new Response;
        } else {
            return new Request;
        }
    }
}
