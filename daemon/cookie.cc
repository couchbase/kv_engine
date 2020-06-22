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

#include "buckets.h"
#include "connection.h"
#include "cookie_trace_context.h"
#include "mcbp.h"
#include "mcbp_executors.h"
#include "settings.h"

#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/checked_snprintf.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <platform/uuid.h>
#include <utilities/logtags.h>
#include <chrono>

nlohmann::json Cookie::toJSON() const {
    nlohmann::json ret;

    if (packet.empty()) {
        ret["packet"] = nlohmann::json();
    } else {
        const auto& header = getHeader();
        ret["packet"] = header.toJSON(validated);
    }

    if (!event_id.empty()) {
        ret["event_id"] = event_id;
    }

    if (!error_context.empty()) {
        ret["error_context"] = error_context;
    }

    if (cas != 0) {
        ret["cas"] = std::to_string(cas);
    }

    ret["connection"] = connection.getDescription();
    ret["ewouldblock"] = ewouldblock;
    ret["aiostat"] = to_string(cb::engine_errc(aiostat));
    ret["refcount"] = uint32_t(refcount);
    ret["engine_storage"] = cb::to_hex(uint64_t(engine_storage));
    return ret;
}

const std::string& Cookie::getEventId() const {
    if (event_id.empty()) {
        event_id = to_string(cb::uuid::random());
    }

    return event_id;
}

void Cookie::setErrorJsonExtras(const nlohmann::json& json) {
    if (json.find("error") != json.end()) {
        throw std::invalid_argument(
                "Cookie::setErrorJsonExtras: cannot use \"error\" as a key, "
                "json:" +
                json.dump());
    }

    error_extra_json = json;
}

const std::string& Cookie::getErrorJson() {
    json_message.clear();
    if (error_context.empty() && event_id.empty() && error_extra_json.empty()) {
        return json_message;
    }

    nlohmann::json error;
    if (!error_context.empty()) {
        error["context"] = error_context;
    }
    if (!event_id.empty()) {
        error["ref"] = event_id;
    }

    nlohmann::json root;

    if (!error.empty()) {
        root["error"] = error;
    }

    if (!error_extra_json.empty()) {
        root.update(error_extra_json);
    }
    json_message = root.dump();
    return json_message;
}

bool Cookie::execute() {
    // Reset ewouldblock state!
    setEwouldblock(false);
    const auto& header = getHeader();
    if (header.isResponse()) {
        execute_response_packet(*this, header.getResponse());
    } else {
        // We've already verified that the packet is a legal packet
        // so it must be a request
        execute_request_packet(*this, header.getRequest());
    }

    return !isEwouldblock();
}

void Cookie::setPacket(PacketContent content,
                       cb::const_byte_buffer buffer,
                       bool copy) {
    if (buffer.size() < sizeof(cb::mcbp::Request)) {
        // we don't have the header, so we can't even look at the body
        // length
        throw std::invalid_argument(
                "Cookie::setPacket(): packet must contain header");
    }

    switch (content) {
    case PacketContent::Header:
        if (buffer.size() != sizeof(cb::mcbp::Request)) {
            throw std::invalid_argument(
                    "Cookie::setPacket(): Incorrect packet size");
        }

        if (copy) {
            throw std::logic_error(
                    "Cookie::setPacket(): copy should only be set for full "
                    "content");
        }
        packet = buffer;
        return;
    case PacketContent::Full:
        const auto* req =
                reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
        const size_t packetsize = sizeof(cb::mcbp::Request) + req->getBodylen();

        if (buffer.size() != packetsize) {
            throw std::logic_error("Cookie::setPacket(): Body not available");
        }

        if (copy) {
            received_packet.reset(new uint8_t[buffer.size()]);
            std::copy(buffer.begin(), buffer.end(), received_packet.get());
            packet = {received_packet.get(), buffer.size()};
            return;
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
    const auto* ret = reinterpret_cast<const cb::mcbp::Header*>(packet.data());
    return ret->getRequest();
}

const cb::mcbp::Response& Cookie::getResponse(PacketContent content) const {
    const auto* ret = reinterpret_cast<const cb::mcbp::Header*>(packet.data());
    return ret->getResponse();
}

ENGINE_ERROR_CODE Cookie::swapAiostat(ENGINE_ERROR_CODE value) {
    auto ret = getAiostat();
    setAiostat(value);
    return ret;
}

ENGINE_ERROR_CODE Cookie::getAiostat() const {
    return aiostat;
}

void Cookie::setAiostat(ENGINE_ERROR_CODE aiostat) {
    Cookie::aiostat = aiostat;
}

void Cookie::setEwouldblock(bool ewouldblock) {
    if (ewouldblock && !connection.isDCP()) {
        setAiostat(ENGINE_EWOULDBLOCK);
    }

    Cookie::ewouldblock = ewouldblock;
}

void Cookie::sendDynamicBuffer() {
    if (dynamicBuffer.getRoot() == nullptr) {
        throw std::logic_error(
                "Cookie::sendDynamicBuffer(): Dynamic buffer not created");
    } else {
        connection.addIov(dynamicBuffer.getRoot(), dynamicBuffer.getOffset());
        connection.setState(StateMachine::State::send_data);
        connection.setWriteAndGo(StateMachine::State::new_cmd);
        connection.pushTempAlloc(dynamicBuffer.getRoot());
        dynamicBuffer.takeOwnership();
    }
}

void Cookie::sendNotMyVBucket() {
    auto pair = connection.getBucket().clusterConfiguration.getConfiguration();
    if (pair.first == -1 || (pair.first == connection.getClustermapRevno() &&
                             Settings::instance().isDedupeNmvbMaps())) {
        // We don't have a vbucket map, or we've already sent it to the
        // client
        mcbp_add_header(*this,
                        cb::mcbp::Status::NotMyVbucket,
                        0,
                        0,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES);
        connection.setState(StateMachine::State::send_data);
        connection.setWriteAndGo(StateMachine::State::new_cmd);
        return;
    }

    const size_t needed = sizeof(cb::mcbp::Response) + pair.second->size();
    if (!growDynamicBuffer(needed)) {
        throw std::bad_alloc();
    }
    auto& buffer = getDynamicBuffer();
    auto* buf = reinterpret_cast<uint8_t*>(buffer.getCurrent());
    const auto& header = getHeader();
    cb::mcbp::ResponseBuilder builder({buf, needed});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(header.getRequest().getClientOpcode());
    builder.setStatus(cb::mcbp::Status::NotMyVbucket);
    builder.setOpaque(header.getOpaque());
    builder.setValue({reinterpret_cast<const uint8_t*>(pair.second->data()),
                      pair.second->size()});
    builder.validate();

    buffer.moveOffset(needed);
    sendDynamicBuffer();
    connection.setClustermapRevno(pair.first);
}

void Cookie::sendResponse(cb::mcbp::Status status) {
    if (status == cb::mcbp::Status::Success) {
        const auto& request = getHeader().getRequest();
        const auto quiet = request.isQuiet();
        if (quiet) {
            // The responseCounter is updated here as this is non-responding
            // code hence mcbp_add_header will not be called (which is what
            // normally updates the responseCounters).
            auto& bucket = connection.getBucket();
            ++bucket.responseCounters[int(cb::mcbp::Status::Success)];
            connection.setState(StateMachine::State::new_cmd);
            return;
        }

        mcbp_add_header(*this, status, 0, 0, 0, PROTOCOL_BINARY_RAW_BYTES);
        connection.setState(StateMachine::State::send_data);
        connection.setWriteAndGo(StateMachine::State::new_cmd);
        return;
    }

    if (status == cb::mcbp::Status::NotMyVbucket) {
        sendNotMyVBucket();
        return;
    }

    // fall back sending the error message (and include the JSON payload etc)
    sendResponse(status, {}, {}, {}, cb::mcbp::Datatype::Raw, cas);
}

void Cookie::sendResponse(cb::engine_errc code) {
    sendResponse(cb::mcbp::to_status(code));
}

void Cookie::sendResponse(cb::mcbp::Status status,
                          cb::const_char_buffer extras,
                          cb::const_char_buffer key,
                          cb::const_char_buffer value,
                          cb::mcbp::Datatype datatype,
                          uint64_t cas) {
    if (!connection.write->empty()) {
        // We can't continue as we might already have references
        // in the IOvector stack pointing into the existing buffer!
        throw std::logic_error(
                "Cookie::sendResponse: No data should have been inserted "
                "in the write buffer!");
    }

    if (status == cb::mcbp::Status::NotMyVbucket) {
        sendNotMyVBucket();
        return;
    }

    const auto& error_json = getErrorJson();

    if (cb::mcbp::isStatusSuccess(status)) {
        setCas(cas);
    } else {
        // This is an error message.. Inject the error JSON!
        extras = {};
        key = {};
        value = {error_json.data(), error_json.size()};
        datatype = value.empty() ? cb::mcbp::Datatype::Raw
                                 : cb::mcbp::Datatype::JSON;
    }

    size_t needed = sizeof(cb::mcbp::Header) + value.size() + key.size() +
                    extras.size();
    if (isTracingEnabled()) {
        needed += MCBP_TRACING_RESPONSE_SIZE;
    }
    connection.write->ensureCapacity(needed);

    mcbp_add_header(*this,
                    status,
                    uint8_t(extras.size()),
                    uint16_t(key.size()),
                    uint32_t(value.size() + key.size() + extras.size()),
                    connection.getEnabledDatatypes(
                            protocol_binary_datatype_t(datatype)));

    if (!extras.empty()) {
        auto wdata = connection.write->wdata();
        std::copy(extras.begin(), extras.end(), wdata.begin());
        connection.write->produced(extras.size());
        connection.addIov(wdata.data(), extras.size());
    }

    if (!key.empty()) {
        auto wdata = connection.write->wdata();
        std::copy(key.begin(), key.end(), wdata.begin());
        connection.write->produced(key.size());
        connection.addIov(wdata.data(), key.size());
    }

    if (!value.empty()) {
        auto wdata = connection.write->wdata();
        std::copy(value.begin(), value.end(), wdata.begin());
        connection.write->produced(value.size());
        connection.addIov(wdata.data(), value.size());
    }

    connection.setState(StateMachine::State::send_data);
    connection.setWriteAndGo(StateMachine::State::new_cmd);
}

const DocKey Cookie::getRequestKey() const {
    return connection.makeDocKey(getRequest().getKey());
}

std::string Cookie::getPrintableRequestKey() const {
    const auto key = getRequest().getKey();

    std::string buffer{reinterpret_cast<const char*>(key.data()), key.size()};
    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    return cb::tagUserData(buffer);
}

void Cookie::logCommand() const {
    if (Settings::instance().getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    const auto opcode = getRequest().getClientOpcode();
    LOG_DEBUG("{}> {} {}",
              connection.getId(),
              to_string(opcode),
              getPrintableRequestKey());
}

void Cookie::logResponse(const char* reason) const {
    const auto opcode = getRequest().getClientOpcode();
    LOG_DEBUG("{}< {} {} - {}",
              connection.getId(),
              to_string(opcode),
              getPrintableRequestKey(),
              reason);
}

void Cookie::logResponse(ENGINE_ERROR_CODE code) const {
    if (Settings::instance().getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    if (code == ENGINE_EWOULDBLOCK) {
        // This is a temporary state
        return;
    }

    logResponse(cb::to_string(cb::engine_errc(code)).c_str());
}

void Cookie::setCommandContext(CommandContext* ctx) {
    commandContext.reset(ctx);
}

void Cookie::maybeLogSlowCommand(
        std::chrono::steady_clock::duration elapsed) const {
    const auto opcode = getRequest().getClientOpcode();
    const auto limit = cb::mcbp::sla::getSlowOpThreshold(opcode);

    if (elapsed > limit) {
        const auto& header = getHeader();
        std::chrono::nanoseconds timings(elapsed);
        std::string command;
        try {
            command = to_string(opcode);
        } catch (const std::exception&) {
            char opcode_s[16];
            checked_snprintf(
                    opcode_s, sizeof(opcode_s), "0x%X", header.getOpcode());
            command.assign(opcode_s);
        }

        auto& c = getConnection();

        TRACE_COMPLETE2("memcached/slow",
                        "Slow cmd",
                        getStart(),
                        getStart() + elapsed,
                        "opcode",
                        getHeader().getOpcode(),
                        "connection_id",
                        c.getId());

        LOG_WARNING(
                R"({}: Slow operation. {{"cid":"{}/{:x}","duration":"{}","trace":"{}","command":"{}","peer":"{}","bucket":"{}","packet":{}}})",
                c.getId(),
                c.getConnectionId().data(),
                ntohl(getHeader().getOpaque()),
                cb::time2text(timings),
                tracer.to_string(),
                command,
                c.getPeername(),
                c.getBucket().name,
                getHeader().toJSON(validated));
    }
}

Cookie::Cookie(Connection& conn) : connection(conn) {
}

void Cookie::initialize(cb::const_byte_buffer header, bool tracing_enabled) {
    reset();
    setTracingEnabled(tracing_enabled ||
                      Settings::instance().alwaysCollectTraceInfo());
    setPacket(Cookie::PacketContent::Header, header);
    start = std::chrono::steady_clock::now();
    tracer.begin(cb::tracing::Code::Request, start);
}

void Cookie::reset() {
    event_id.clear();
    error_context.clear();
    json_message.clear();
    packet = {};
    validated = false;
    cas = 0;
    commandContext.reset();
    dynamicBuffer.clear();
    tracer.clear();
    ewouldblock = false;
    openTracingContext.clear();
    authorized = false;
}

void Cookie::setOpenTracingContext(cb::const_byte_buffer context) {
    try {
        openTracingContext.assign(reinterpret_cast<const char*>(context.data()),
                                  context.size());
    } catch (const std::bad_alloc&) {
        // Drop tracing if we run out of memory
    }
}

CookieTraceContext Cookie::extractTraceContext() {
    if (openTracingContext.empty()) {
        throw std::logic_error(
                "Cookie::extractTraceContext should only be called if we have "
                "a context");
    }

    auto& header = getHeader();
    return CookieTraceContext{cb::mcbp::Magic(header.getMagic()),
                              header.getOpcode(),
                              header.getOpaque(),
                              header.getKey(),
                              std::move(openTracingContext),
                              tracer.extractDurations()};
}
