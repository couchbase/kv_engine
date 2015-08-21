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

#include <libgreenstack/FlexHeader.h>
#include <libgreenstack/Writer.h>
#include <libgreenstack/Reader.h>
#include <iostream>
#include <sstream>
#include <cstddef>
#include <stdexcept>

// @todo.. Should I refactor this to something like..
class Option {
public:
    virtual uint16_t getId() = 0;

    virtual const char* getName() = 0;

    virtual void write(Greenstack::Writer& writer) = 0;
};

static const uint16_t LANE_ID = 0x0000;
static const uint16_t COMPRESSION = 0x0001;
static const uint16_t DATATYPE = 0x0002;
static const uint16_t CAS = 0x003;
static const uint16_t TXID = 0x0004;
static const uint16_t PRIORITY = 0x0005;
static const uint16_t DCPID = 0x0006;
static const uint16_t VBUCKETID = 0x0007;
static const uint16_t HASH = 0x0008;
static const uint16_t CMD_EXPIRY = 0x0009;
static const uint16_t COMMAND_TIMINGS = 0x000a;

size_t Greenstack::FlexHeader::encode(Greenstack::Writer& writer) const {
    size_t nw = 0;
    for (auto& ii : header) {
        writer.write(ii.first);
        writer.write(static_cast<uint16_t>(ii.second.size()));
        writer.write(ii.second.data(), ii.second.size());
        nw += 4 + ii.second.size();
    }

    return nw;
}

Greenstack::FlexHeader Greenstack::FlexHeader::create(
    Greenstack::Reader& reader) {
    FlexHeader ret;

    while (reader.getRemainder() > 0) {
        try {
            // each field is at least 2 byte key, 2 byte length
            uint16_t opcode;
            uint16_t length;
            reader.read(opcode);
            reader.read(length);
            std::vector<uint8_t> value;
            value.resize(length);
            reader.read(value.data(), length);

            ret.insertField(opcode, value.data(), value.size());
        } catch (std::out_of_range&) {
            throw std::runtime_error("Incorrect encoding of Flex Header");
        }
    }

    return ret;
}

static std::string headerIdToString(uint16_t id) {
    switch (id) {
    case LANE_ID:
        return "Lane ID";
    case COMPRESSION:
        return "Compression";
    case DATATYPE:
        return "Datatype";
    case CAS:
        return "CAS";
    case TXID:
        return "TXID";
    case PRIORITY:
        return "Priority";
    case DCPID:
        return "DCP-ID";
    case VBUCKETID:
        return "VBucket ID";
    case HASH:
        return "Hash";
    case CMD_EXPIRY:
        return "Command expiry";
    case COMMAND_TIMINGS:
        return "Command timings";
    default: {
        std::stringstream ss;
        ss << "0x" << std::hex << id;
        return ss.str();
    }
    }
}

void Greenstack::FlexHeader::setLaneId(const std::string& lane) {
    insertField(LANE_ID, reinterpret_cast<const uint8_t*>(lane.data()),
                lane.length());
}

bool Greenstack::FlexHeader::haveLaneId() const {
    return header.find(LANE_ID) != header.end();
}

std::string Greenstack::FlexHeader::getLaneId() const {
    auto iter = header.find(LANE_ID);

    if (iter == header.end()) {
        throw std::runtime_error("Lane ID not present");
    }

    std::string ret(reinterpret_cast<const char*>(iter->second.data()),
                    iter->second.size());
    return ret;
}

void Greenstack::FlexHeader::setTXID(const std::string& txid) {
    insertField(TXID, reinterpret_cast<const uint8_t*>(txid.data()),
                txid.length());

}

bool Greenstack::FlexHeader::haveTXID() const {
    return header.find(TXID) != header.end();
}

std::string Greenstack::FlexHeader::getTXID() const {
    auto iter = header.find(TXID);

    if (iter == header.end()) {
        throw std::runtime_error("TXID not present");
    }

    std::string ret(reinterpret_cast<const char*>(iter->second.data()),
                    iter->second.size());
    return ret;
}

void Greenstack::FlexHeader::setPriority(uint8_t priority) {
    insertField(PRIORITY, reinterpret_cast<const uint8_t*>(&priority), 1);
}

bool Greenstack::FlexHeader::havePriority() const {
    return header.find(PRIORITY) != header.end();
}

uint8_t Greenstack::FlexHeader::getPriority() const {
    auto iter = header.find(PRIORITY);

    if (iter == header.end()) {
        throw std::runtime_error("Priority not present");
    }

    return iter->second[0];
}

void Greenstack::FlexHeader::setDcpId(const std::string& dcpid) {
    insertField(DCPID, reinterpret_cast<const uint8_t*>(dcpid.data()),
                dcpid.length());
}

bool Greenstack::FlexHeader::haveDcpId() const {
    return header.find(DCPID) != header.end();
}

std::string Greenstack::FlexHeader::getDcpId() const {
    auto iter = header.find(DCPID);

    if (iter == header.end()) {
        throw std::runtime_error("DCP id not present");
    }

    std::string ret(reinterpret_cast<const char*>(iter->second.data()),
                    iter->second.size());
    return ret;
}

void Greenstack::FlexHeader::setVbucketId(uint16_t vbid) {
    uint16_t value = htons(vbid);
    insertField(VBUCKETID, reinterpret_cast<const uint8_t*>(&value), 2);
}

bool Greenstack::FlexHeader::haveVbucketId() const {
    return header.find(VBUCKETID) != header.end();
}

uint16_t Greenstack::FlexHeader::getVbucketId() const {
    auto iter = header.find(VBUCKETID);

    if (iter == header.end()) {
        throw std::runtime_error("VBucket id not present");
    }

    uint16_t value;
    memcpy(&value, iter->second.data(), 2);
    return ntohs(value);
}

void Greenstack::FlexHeader::setHash(uint32_t hash) {
    uint32_t value = ntohl(hash);
    insertField(HASH, reinterpret_cast<const uint8_t*>(&value), 4);
}

bool Greenstack::FlexHeader::haveHash() const {
    return header.find(HASH) != header.end();
}

uint32_t Greenstack::FlexHeader::getHash() const {
    auto iter = header.find(HASH);

    if (iter == header.end()) {
        throw std::runtime_error("Hash not present");
    }

    uint32_t value;
    memcpy(&value, iter->second.data(), 4);
    return ntohl(value);
}

void Greenstack::FlexHeader::setTimeout(uint32_t timeout) {
    uint32_t value = ntohl(timeout);
    insertField(CMD_EXPIRY, reinterpret_cast<const uint8_t*>(&value), 4);
}

bool Greenstack::FlexHeader::haveTimeout() const {
    return header.find(CMD_EXPIRY) != header.end();
}

uint32_t Greenstack::FlexHeader::getTimeout() const {
    auto iter = header.find(CMD_EXPIRY);

    if (iter == header.end()) {
        throw std::runtime_error("Command timeout not present");
    }

    uint32_t value;
    memcpy(&value, iter->second.data(), 4);
    return ntohl(value);
}

void Greenstack::FlexHeader::setCommandTimings(const std::string& value) {
    insertField(COMMAND_TIMINGS, reinterpret_cast<const uint8_t*>(value.data()),
                value.length());
}

bool Greenstack::FlexHeader::haveCommandTimings() const {
    return header.find(COMMAND_TIMINGS) != header.end();
}

std::string Greenstack::FlexHeader::getCommandTimings() const {
    auto iter = header.find(COMMAND_TIMINGS);

    if (iter == header.end()) {
        throw std::runtime_error("Command timings not present");
    }

    std::string ret(reinterpret_cast<const char*>(iter->second.data()),
                    iter->second.size());
    return ret;
}

void Greenstack::FlexHeader::insertField(uint16_t opcode, const uint8_t* value,
                                         size_t length) {
    if (header.find(opcode) != header.end()) {
        std::stringstream ss;
        ss << "Header " << headerIdToString(opcode) << " already specified";
        throw std::runtime_error(ss.str());
    }

    // @todo insert validation of the different keys (length etc)
    header[opcode].resize(length);
    memcpy(header[opcode].data(), value, length);
}
