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

#include "cookie.h"
#include "connection_mcbp.h"
#include "mcbp.h"

#include <mcbp/mcbp.h>
#include <phosphor/phosphor.h>
#include <platform/checked_snprintf.h>
#include <platform/timeutils.h>

#include <chrono>

unique_cJSON_ptr Cookie::toJSON() const {
    unique_cJSON_ptr ret(cJSON_CreateObject());

    if (packet.empty()) {
        cJSON_AddItemToObject(ret.get(), "packet", cJSON_CreateObject());
    } else {
        const auto& header = getHeader();
        cJSON_AddItemToObject(ret.get(), "packet", header.toJSON().release());
    }

    if (!event_id.empty()) {
        cJSON_AddStringToObject(ret.get(), "event_id", event_id.c_str());
    }

    if (!error_context.empty()) {
        cJSON_AddStringToObject(
                ret.get(), "error_context", error_context.c_str());
    }

    if (cas != 0) {
        std::string str = std::to_string(cas);
        cJSON_AddStringToObject(ret.get(), "cas", str.c_str());
    }

    return ret;
}

const std::string& Cookie::getErrorJson() {
    json_message.clear();
    if (error_context.empty() && event_id.empty()) {
        return json_message;
    }

    unique_cJSON_ptr root(cJSON_CreateObject());
    unique_cJSON_ptr error(cJSON_CreateObject());
    if (!error_context.empty()) {
        cJSON_AddStringToObject(error.get(), "context", error_context.c_str());
    }
    if (!event_id.empty()) {
        cJSON_AddStringToObject(error.get(), "ref", event_id.c_str());
    }
    cJSON_AddItemToObject(root.get(), "error", error.release());
    json_message = to_string(root, false);
    return json_message;
}

void Cookie::setPacket(PacketContent content, cb::const_byte_buffer buffer) {
    switch (content) {
    case PacketContent::Header:
        if (buffer.size() != sizeof(cb::mcbp::Request)) {
            throw std::invalid_argument(
                    "Cookie::setPacket(): Incorrect packet size");
        }
        packet = buffer;
        return;
    case PacketContent::Full:
        if (buffer.size() < sizeof(cb::mcbp::Request)) {
            // we don't have the header, so we can't even look at the body
            // length
            throw std::logic_error(
                    "Cookie::setPacket(): packet must contain header");
        }

        const auto* req =
                reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
        const size_t packetsize = sizeof(cb::mcbp::Request) + req->getBodylen();

        if (buffer.size() != packetsize) {
            throw std::logic_error("Cookie::setPacket(): Body not available");
        }

        packet = buffer;
        return;
    }
    throw std::logic_error("Cookie::setPacket(): Invalid content provided");
}

cb::const_byte_buffer Cookie::getPacket(PacketContent content) const {
    if (packet.empty()) {
        throw std::logic_error("Cookie::getPacket(): packet not available");
    }

    switch (content) {
    case PacketContent::Header:
        return cb::const_byte_buffer{packet.data(), sizeof(cb::mcbp::Request)};
    case PacketContent::Full:
        const auto* req =
                reinterpret_cast<const cb::mcbp::Request*>(packet.data());
        const size_t packetsize = sizeof(cb::mcbp::Request) + req->getBodylen();

        if (packet.size() != packetsize) {
            throw std::logic_error("Cookie::getPacket(): Body not available");
        }

        return packet;
    }

    throw std::invalid_argument(
            "Cookie::getPacket(): Invalid content requested");
}

const cb::mcbp::Header& Cookie::getHeader() const {
    const auto packet = getPacket(PacketContent::Header);
    return *reinterpret_cast<const cb::mcbp::Header*>(packet.data());
}

const cb::mcbp::Request& Cookie::getRequest(PacketContent content) const {
    cb::const_byte_buffer packet = getPacket(content);
    const auto* ret = reinterpret_cast<const cb::mcbp::Request*>(packet.data());
    switch (ret->getMagic()) {
    case cb::mcbp::Magic::ClientRequest:
    case cb::mcbp::Magic::ServerRequest:
        return *ret;
    case cb::mcbp::Magic::ClientResponse:
    case cb::mcbp::Magic::ServerResponse:
        throw std::logic_error("Cookie::getRequest(): Packet is response");
    }

    throw std::invalid_argument("Cookie::getRequest(): Invalid packet type");
}

const cb::mcbp::Response& Cookie::getResponse(PacketContent content) const {
    cb::const_byte_buffer packet = getPacket(content);
    const auto* ret =
            reinterpret_cast<const cb::mcbp::Response*>(packet.data());
    switch (ret->getMagic()) {
    case cb::mcbp::Magic::ClientRequest:
    case cb::mcbp::Magic::ServerRequest:
        throw std::logic_error("Cookie::getRequest(): Packet is resquest");
    case cb::mcbp::Magic::ClientResponse:
    case cb::mcbp::Magic::ServerResponse:
        return *ret;
    }

    throw std::invalid_argument("Cookie::getResponse(): Invalid packet type");
}

const ENGINE_ERROR_CODE Cookie::getAiostat() const {
    return connection.getAiostat();
}

void Cookie::setAiostat(const ENGINE_ERROR_CODE& aiostat) {
    connection.setAiostat(aiostat);
}

bool Cookie::isEwouldblock() const {
    return connection.isEwouldblock();
}

void Cookie::setEwouldblock(bool ewouldblock) {
    connection.setEwouldblock(ewouldblock);
}

void Cookie::sendResponse(cb::mcbp::Status status) {
    mcbp_write_packet(&connection, uint16_t(status));
}

void Cookie::sendResponse(cb::engine_errc code) {
    sendResponse(cb::mcbp::to_status(code));
}

const DocKey Cookie::getRequestKey() const {
    auto key = getRequest().getKey();
    return DocKey{key.data(), key.size(), connection.getDocNamespace()};
}

std::string Cookie::getPrintableRequestKey() const {
    const auto key = getRequest().getKey();

    std::string buffer{reinterpret_cast<const char*>(key.data()), key.size()};
    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    return buffer;
}

void Cookie::logCommand() const {
    if (settings.getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    const auto opcode = getRequest().getClientOpcode();
    LOG_INFO(this,
             "%u> %s %s",
             connection.getId(),
             to_string(opcode).c_str(),
             getPrintableRequestKey().c_str());
}

void Cookie::logResponse(const char* reason) const {
    const auto opcode = getRequest().getClientOpcode();
    LOG_INFO(this,
             "%u< %s %s - %s",
             connection.getId(),
             to_string(opcode).c_str(),
             getPrintableRequestKey().c_str(),
             reason);
}

void Cookie::logResponse(ENGINE_ERROR_CODE code) const {
    if (settings.getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    if (code == ENGINE_EWOULDBLOCK || code == ENGINE_WANT_MORE) {
        // These are temporary states
        return;
    }

    logResponse(cb::to_string(cb::engine_errc(code)).c_str());
}

void Cookie::maybeLogSlowCommand(
        const std::chrono::milliseconds& elapsed) const {
    const auto opcode = getRequest().getClientOpcode();
    const auto limit = cb::mcbp::sla::getSlowOpThreshold(opcode);

    if (elapsed > limit) {
        const auto& header = getHeader();
        std::chrono::nanoseconds timings(elapsed);
        std::string command;
        try {
            command = to_string(opcode);
        } catch (const std::exception& e) {
            char opcode_s[16];
            checked_snprintf(
                    opcode_s, sizeof(opcode_s), "0x%X", header.getOpcode());
            command.assign(opcode_s);
        }

        std::string details;
        if (opcode == cb::mcbp::ClientOpcode::Stat) {
            // Log which stat command took a long time
            details.append(", key: ");
            auto key = getPrintableRequestKey();

            if (key.find("key ") == 0) {
                // stat key username1324423e; truncate the actual item key
                details.append("key <TRUNCATED>");
            } else if (key.empty()) {
                // requests all stats
                details.append("<EMPTY>");
            } else {
                details.append(key);
            }
        }

        auto& c = getConnection();

        TRACE_INSTANT2("memcached/slow",
                       "Slow cmd",
                       "opcode",
                       getHeader().getOpcode(),
                       "connection_id",
                       c.getId());
        LOG_WARNING(nullptr,
                    "%u: Slow %s operation on connection: %s (%s)%s"
                    " opaque:0x%08x",
                    c.getId(),
                    command.c_str(),
                    cb::time2text(timings).c_str(),
                    c.getDescription().c_str(),
                    details.c_str(),
                    header.getOpaque());
    }
}
