/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cookie.h"

#include "buckets.h"
#include "connection.h"
#include "cookie_trace_context.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "get_authorization_task.h"
#include "mcaudit.h"
#include "mcbp_executors.h"
#include "mcbp_validators.h"
#include "memcached.h"
#include "resource_allocation_domain.h"
#include "settings.h"
#include "tracing.h"

#include <folly/io/IOBuf.h>
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/compress.h>
#include <platform/histogram.h>
#include <platform/json_log_conversions.h>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <platform/uuid.h>
#include <serverless/config.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/logtags.h>
#include <cctype>
#include <chrono>

nlohmann::json Cookie::to_json() const {
    nlohmann::json ret;

    if (packet == nullptr) {
        ret["packet"] = nlohmann::json();
    } else {
        const auto& header = getHeader();
        try {
            ret["packet"] = header.to_json(validated);
        } catch (const std::exception& e) {
            ret["packet"]["error"] = e.what();
        }
    }

    if (!error_json.empty()) {
        ret["error_json"] = error_json;
    }

    if (cas != 0) {
        ret["cas"] = std::to_string(cas);
    }

    ret["ewouldblock"] = to_string(ewouldblock);
    ret["aiostat"] = to_string(aiostat);
    ret["throttled"] = throttled.load();
    ret["refcount"] = uint32_t(refcount);
    ret["started"] = fmt::format(
            "{} ({} ago)",
            start.time_since_epoch().count(),
            cb::time2text(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - start)));

    auto es = engine_storage.withLock([](const auto& ptr) -> nlohmann::json {
        if (ptr) {
            auto json = ptr->to_json();
            if (json.empty()) {
                return cb::to_hex(uint64_t(ptr.get()));
            }
        }
        return {};
    });
    if (!es.empty()) {
        ret["engine_storage"] = es;
    }
    return ret;
}

std::string Cookie::getEventId() {
    if (!error_json.contains("error") || !error_json["error"].contains("ref")) {
        error_json["error"]["ref"] = to_string(cb::uuid::random());
    }

    return error_json["error"]["ref"];
}

void Cookie::setEventId(std::string uuid) {
    error_json["error"]["ref"] = std::move(uuid);
}

void Cookie::setErrorJsonExtras(const nlohmann::json& json) {
    // verify that we don't override the generated ref and context
    if (json.contains("error")) {
        if (json["error"].contains("ref")) {
            throw std::runtime_error(fmt::format(
                    "Cookie::setErrorJsonExtras: cannot specify a value for "
                    "\"error.ref\". Provided JSON: {}",
                    json.dump()));
        }
        if (json["error"].contains("context")) {
            throw std::runtime_error(fmt::format(
                    "Cookie::setErrorJsonExtras: cannot specify a value for "
                    "\"error.context\". Provided JSON: {}",
                    json.dump()));
        }
    }

    error_json.update(json);
}

void Cookie::setErrorContext(std::string message) {
    error_json["error"]["context"] = std::move(message);
}

std::string Cookie::getErrorContext() const {
    if (error_json.contains("error")) {
        return error_json["error"].value("context", std::string{});
    }
    return {};
}

std::string Cookie::getErrorJson() {
    if (error_json.empty()) {
        return {};
    }

    return error_json.dump();
}

bool Cookie::doExecute() {
    if (!validated) {
        throw std::runtime_error("Cookie::execute: validate() not called");
    }

    Expects(!isEwouldblock() &&
            "Cookie::doExecute(): Executing while ewouldblock is true could "
            "result in a background task notifying while executing.");

    if (euid && !euidPrivilegeContext && !connection.isInternal()) {
        // We're supposed to run as a different user, but we don't
        // have the privilege set configured yet...
        // Internal services perform the access checks by using cbauth
        // before running impersonate
        if (!fetchEuidPrivilegeSet()) {
            // we failed  to fetch the access privileges for the requested user
            audit_command_access_failed(*this);
            return true;
        }

        if (isEwouldblock()) {
            return false;
        }
    }

    const auto& header = getHeader();
    using cb::mcbp::Magic;
    // We've already verified that the packet is a legal packet
    switch (Magic(header.getMagic())) {
    case Magic::ClientRequest:
    case Magic::AltClientRequest:
        execute_client_request_packet(*this, header.getRequest());
        break;

    case Magic::ClientResponse:
    case Magic::AltClientResponse:
        execute_client_response_packet(*this, header.getResponse());
        break;

    case Magic::ServerRequest:
        throw std::runtime_error(
                "cookie::doExecute(): processing server requests is not "
                "(yet) supported");
        break;
    case Magic::ServerResponse:
        execute_server_response_packet(*this, header.getResponse());
        break;
    }

    if (isEwouldblock() || isOutputbufferFull()) {
        return false;
    }

    return true;
}

uint32_t Cookie::getConnectionId() const {
    return connection.getId();
}

std::string_view Cookie::getAgentName() const {
    return connection.getAgentName();
}

bool Cookie::execute(bool useStartTime) {
    auto ts = useStartTime ? start : std::chrono::steady_clock::now();

    auto done = doExecute();

    auto te = std::chrono::steady_clock::now();
    tracer.record(cb::tracing::Code::Execute, ts, te);

    if (done) {
        static const bool command_log_enabled =
                getenv("MEMCACHED_ENABLE_COMMAND_LOGGING");
        collectTimings(te);
        connection.commandExecuted(*this);
        if (command_log_enabled && getHeader().isRequest()) {
            LOG_INFO_CTX("Command executed",
                         {"conn_id", getConnectionId()},
                         {"time", te - ts},
                         {"command", getRequest().to_json(true)});
        }

        return true;
    }
    return false;
}

void Cookie::setPacket(const cb::mcbp::Header& header, bool copy) {
    if (copy) {
        auto frame = header.getFrame();
        frame_copy = std::make_unique<uint8_t[]>(frame.size());
        std::ranges::copy(frame, frame_copy.get());
        packet = reinterpret_cast<const cb::mcbp::Header*>(frame_copy.get());
    } else {
        packet = &header;
    }
}

cb::const_byte_buffer Cookie::getPacket() const {
    if (packet == nullptr) {
        return {};
    }

    return packet->getFrame();
}

const cb::mcbp::Header& Cookie::getHeader() const {
    if (packet == nullptr) {
        throw std::logic_error("Cookie::getHeader(): packet not available");
    }

    return *packet;
}

const cb::mcbp::Request& Cookie::getRequest() const {
    if (packet == nullptr) {
        throw std::logic_error("Cookie::getRequest(): packet not available");
    }

    return packet->getRequest();
}

cb::engine_errc Cookie::swapAiostat(cb::engine_errc value) {
    auto ret = getAiostat();
    setAiostat(value);
    return ret;
}

cb::engine_errc Cookie::getAiostat() const {
    return aiostat;
}

void Cookie::setAiostat(cb::engine_errc value) {
    aiostat = value;
}

void Cookie::setEwouldblockVariable(cb::engine_errc value) {
    Expects(value == cb::engine_errc::success ||
            value == cb::engine_errc::would_block ||
            value == cb::engine_errc::too_much_data_in_output_buffer);
    if (value == cb::engine_errc::would_block && !connection.isDCP()) {
        setAiostat(value);
    }

    ewouldblock = value;
}

void Cookie::sendNotMyVBucket() {
    auto pushed = connection.getPushedClustermapRevno();
    auto config =
            connection.getBucket().clusterConfiguration.maybeGetConfiguration(
                    pushed,
                    Settings::instance().isDedupeNmvbMaps() ||
                            connection.dedupeNmvbMaps());

    if (config) {
        if (connection.supportsSnappyEverywhere()) {
            connection.sendResponse(*this,
                                    cb::mcbp::Status::NotMyVbucket,
                                    {},
                                    {},
                                    config->compressed,
                                    connection.getEnabledDatatypes(
                                            PROTOCOL_BINARY_DATATYPE_JSON |
                                            PROTOCOL_BINARY_DATATYPE_SNAPPY),
                                    {});

        } else {
            connection.sendResponse(*this,
                                    cb::mcbp::Status::NotMyVbucket,
                                    {},
                                    {},
                                    config->uncompressed,
                                    connection.getEnabledDatatypes(
                                            PROTOCOL_BINARY_DATATYPE_JSON),
                                    {});
        }
        connection.setPushedClustermapRevno(config->version);
    } else {
        // We don't have a vbucket map, or we've already sent it to the
        // client
        connection.sendResponse(*this,
                                cb::mcbp::Status::NotMyVbucket,
                                {},
                                {},
                                {},
                                PROTOCOL_BINARY_RAW_BYTES,
                                {});
    }
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
            return;
        }

        connection.sendResponse(
                *this, status, {}, {}, {}, PROTOCOL_BINARY_RAW_BYTES, {});
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
                          std::string_view extras,
                          std::string_view key,
                          std::string_view value,
                          cb::mcbp::Datatype datatype,
                          uint64_t casValue) {
    if (status == cb::mcbp::Status::NotMyVbucket) {
        sendNotMyVBucket();
        return;
    }

    const auto error_json_string = getErrorJson();

    if (cb::mcbp::isStatusSuccess(status)) {
        setCas(casValue);
    } else {
        // This is an error message.. Inject the error JSON!
        extras = {};
        key = {};
        value = error_json_string;
        datatype = value.empty() ? cb::mcbp::Datatype::Raw
                                 : cb::mcbp::Datatype::JSON;
    }

    connection.sendResponse(*this,
                            status,
                            extras,
                            key,
                            value,
                            connection.getEnabledDatatypes(
                                    protocol_binary_datatype_t(datatype)),
                            {});
}

void Cookie::sendResponse(cb::engine_errc status,
                          std::string_view extras,
                          std::string_view key,
                          std::string_view value,
                          cb::mcbp::Datatype datatype,
                          uint64_t casval) {
    sendResponse(
            cb::mcbp::to_status(status), extras, key, value, datatype, casval);
}

const DocKeyView Cookie::getRequestKey() const {
    return connection.makeDocKey(getRequest().getKey());
}

void Cookie::setCommandContext(CommandContext* ctx) {
    commandContext.reset(ctx);
}

void Cookie::maybeLogSlowCommand(
        std::chrono::steady_clock::duration elapsed) const {
    const auto opcode = getRequest().getClientOpcode();
    const auto limit = cb::mcbp::sla::getSlowOpThreshold(opcode);

    // We don't want to spam the logs with slow ops entries just because
    // the system throttled the operation due to the user pushing too many
    // operations at the system.
    if ((elapsed - total_throttle_time) > limit) {
        std::chrono::nanoseconds timings(elapsed);
        auto& c = getConnection();

        // Trace as an Async event as we record at the thread level, and there
        // could be many other events which overlap this one.
        TRACE_ASYNC_COMPLETE2("memcached/slow",
                              "Slow cmd",
                              this,
                              start,
                              start + elapsed,
                              "opcode",
                              getHeader().getOpcode(),
                              "connection_id",
                              c.getId());

        cb::logger::Json details{{"conn_id", c.getId()},
                                 {"bucket", c.getBucket().name},
                                 {"command", opcode},
                                 {"duration", timings},
                                 {"trace", tracer.to_string()},
                                 {"cid",
                                  fmt::format("{}/{:x}",
                                              c.getConnectionId().data(),
                                              getHeader().getOpaque())},
                                 {"peer", c.getPeername()},
                                 {"packet", getHeader().to_json(validated)},
                                 {"worker_tid", folly::getCurrentThreadID()}};
        if (responseStatus != cb::mcbp::Status::COUNT) {
            details["status"] = to_string(responseStatus);
        }

        const auto [read, write] = getDocumentRWBytes();
        if (read > 0) {
            details["document_bytes_read"] = read;
        }
        if (write > 0) {
            details["document_bytes_write"] = write;
        }

        LOG_WARNING_CTX("Slow operation", std::move(details));
    }
}

Cookie::Cookie(Connection& conn)
    : connection(conn),
      resource_allocation_domain(ResourceAllocationDomain::None) {
}

void Cookie::initialize(std::chrono::steady_clock::time_point now,
                        const cb::mcbp::Header& header) {
    reset();
    setPacket(header);
    start = now;
}

cb::mcbp::Status Cookie::validate() {
    using cb::mcbp::Status;

    static McbpValidator packetValidator;

    // Note: validate is called after the connection fetched the frame
    //       off the network, and in order to know when the frame ends
    //       it needed to validate the frame layout (known magic and
    //       all the length fields inside the raw header adds up so
    //       that the bodylen() contains the entire data).
    const auto& header = getHeader();
    if (header.isResponse()) {
        // We don't have packet validators for responses (yet)
        validated = true;
        return cb::mcbp::Status::Success;
    }

    const auto& request = header.getRequest();
    if (cb::mcbp::is_server_magic((request.getMagic()))) {
        // We should not be receiving a server command.
        auto opcode = request.getServerOpcode();
        if (!cb::mcbp::is_valid_opcode(opcode)) {
            return cb::mcbp::Status::UnknownCommand;
        }

        audit_invalid_packet(connection, getPacket());
        return cb::mcbp::Status::NotSupported;
    }

    auto opcode = request.getClientOpcode();

    if (!connection.isAuthenticated()) {
        // We're not authenticated. To reduce the attack vector we'll only
        // allow certain commands to be executed
        constexpr std::array<cb::mcbp::ClientOpcode, 5> allowed{
                {cb::mcbp::ClientOpcode::Hello,
                 cb::mcbp::ClientOpcode::SaslListMechs,
                 cb::mcbp::ClientOpcode::SaslAuth,
                 cb::mcbp::ClientOpcode::SaslStep,
                 cb::mcbp::ClientOpcode::GetErrorMap}};
        if (std::ranges::find(allowed, opcode) == allowed.end()) {
#if CB_DEVELOPMENT_ASSERTS
            if (cb::mcbp::is_valid_opcode(opcode)) {
                LOG_WARNING_CTX(
                        "Trying to execute before authentication. Returning "
                        "eaccess",
                        {"conn_id", getConnectionId()},
                        {"opcode", opcode});
            }
#endif
            return cb::mcbp::Status::Eaccess;
        }
    }

    if (!cb::mcbp::is_valid_opcode(opcode)) {
        // We don't know about this command. Stop processing
        // It is legal to send unknown commands, so we don't log or audit this
        return cb::mcbp::Status::UnknownCommand;
    }

    // A cluster-only bucket don't have an associated engine parameter
    // so we don't even want to try to validate the packet unless it is
    // one of the few commands allowed to be executed in such a bucket
    // (to avoid a potential call throug a nil pointer)
    if (connection.getBucket().type == BucketType::ClusterConfigOnly) {
        constexpr std::array<cb::mcbp::ClientOpcode, 4> allowed{
                {cb::mcbp::ClientOpcode::Hello,
                 cb::mcbp::ClientOpcode::GetErrorMap,
                 cb::mcbp::ClientOpcode::GetClusterConfig,
                 cb::mcbp::ClientOpcode::SelectBucket}};
        if (std::ranges::find(allowed, opcode) == allowed.end()) {
            if (connection.isXerrorSupport()) {
                return cb::mcbp::Status::EConfigOnly;
            }
            return cb::mcbp::Status::NotSupported;
        }
    }

    auto result = packetValidator.validate(opcode, *this);
    if (!cb::mcbp::isStatusSuccess(result)) {
        if (result != Status::UnknownCollection &&
            result != Status::NotMyVbucket &&
            result != Status::BucketSizeLimitExceeded &&
            result != Status::BucketResidentRatioTooLow &&
            result != Status::BucketDataSizeTooBig &&
            result != Status::BucketDiskSpaceTooLow) {
            LOG_WARNING_CTX("Packet validation failed",
                            {"conn_id", connection.getId()},
                            {"request", request.to_json(false)},
                            {"error", error_json},
                            {"status", result});
            audit_invalid_packet(getConnection(), getPacket());
        }
        return result;
    }

    // Add a barrier to the command if we don't support reordering it!
    if (reorder && !is_reorder_supported(request.getClientOpcode())) {
        setBarrier();
    }

    if (must_preserve_buffer(request.getClientOpcode())) {
        // MB-52884:
        // The command may try to call operations on the cookie to get
        // information from the input packet from a different thread than
        // the front end thread (for instance sending a response).
        // To avoid this from racing with where we preserve the packet
        // in the ewb case just copy out the packet
        preserveRequest();
    }

    validated = true;
    return Status::Success;
}

Cookie::~Cookie() {
    if (isEwouldblock()) {
        LOG_CRITICAL_CTX(
                "Ewouldblock should NOT be set in the destructor as that "
                "indicates that someone is using the cookie",
                {"conn_id", connection.getId()},
                {"cookie", to_json()},
                {"description", connection.to_json()});
        cb::logger::flush();
        std::terminate();
    }
}

void Cookie::reset() {
    setEngineStorage({});
    document_bytes_read = 0;
    document_bytes_written = 0;
    total_throttle_time = total_throttle_time.zero();
    error_json.clear();
    packet = {};
    validated = false;
    cas = 0;
    commandContext.reset();
    tracer.clear();
    Expects(!isEwouldblock() &&
            "Cookie::reset() when ewouldblock is true could result in an "
            "outstanding notifyIoComplete occuring on wrong cookie");
    authorized = false;
    preserveTtl = false;
    reorder = connection.allowUnorderedExecution();
    inflated_input_payload.reset();
    currentCollectionInfo.reset();
    privilegeContext = connection.getPrivilegeContext();
    euid.reset();
    euidPrivilegeContext.reset();
    frame_copy.reset();
    responseStatus = cb::mcbp::Status::COUNT;
    euidExtraPrivileges.reset();
    read_thottling_factor = 1.0;
    write_thottling_factor = 1.0;
    resource_allocation_domain = ResourceAllocationDomain::None;
}

void Cookie::setThrottled(bool val) {
    throttled.store(val, std::memory_order_release);
    const auto now = std::chrono::steady_clock::now();
    if (val) {
        throttle_start = now;
    } else {
        total_throttle_time +=
                std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                      start);
        tracer.record(cb::tracing::Code::Throttled, throttle_start, now);
    }
}

void Cookie::collectTimings(
        const std::chrono::steady_clock::time_point& endTime) {
    // The state machinery cause this method to be called for all kinds
    // of packets, but the header musts be a client request for the timings
    // to make sense (and not when we handled a ServerResponse message etc ;)
    if (!packet->isRequest()) {
        return;
    }

    const auto opcode = packet->getRequest().getClientOpcode();
    const auto elapsed = endTime - start;
    tracer.record(cb::tracing::Code::Request, start, endTime);

    BucketManager::instance().aggregatedTimings.collect(opcode, elapsed);

    // Bucket index will be zero initially before you run SASL auth,
    // or if someone tries to delete the bucket you're associated with
    // and you're idle.
    // We record timings to the "current" bucket; even for the no-bucket (0),
    // so we include timings for commands like HELO, SASL_* which occur before
    // bucket selection.
    connection.getBucket().timings.collect(opcode, elapsed);

    // Log operations taking longer than the "slow" threshold for the opcode.
    maybeLogSlowCommand(elapsed);
}

std::string_view Cookie::getInflatedInputPayload() const {
    if (inflated_input_payload) {
        return folly::StringPiece{inflated_input_payload->coalesce()};
    }

    return getHeader().getValueString();
}

bool Cookie::inflateInputPayload(const cb::mcbp::Header& header) {
    inflated_input_payload.reset();
    if (!cb::mcbp::datatype::is_snappy(header.getDatatype())) {
        return true;
    }

    try {
        inflated_input_payload = inflateSnappy(header.getValueString());
        return true;
    } catch (const std::range_error&) {
        setErrorContext("Inflated data is too big");
    } catch (const std::runtime_error&) {
        setErrorContext("Failed to inflate payload");
    } catch (const std::bad_alloc&) {
        setErrorContext("Failed to allocate memory");
    }

    return false;
}

std::unique_ptr<folly::IOBuf> Cookie::inflateSnappy(std::string_view input) {
    using namespace cb::tracing;
    ScopeTimer2<HdrMicroSecStopwatch, SpanStopwatch<Code>> timer(
            std::forward_as_tuple(
                    getConnection().getBucket().snappyDecompressionTimes),
            std::forward_as_tuple(*this, Code::SnappyDecompress));

    return cb::compression::inflateSnappy(input);
}

nlohmann::json Cookie::getPrivilegeFailedErrorMessage(
        std::string opcode,
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) {
    nlohmann::json json;
    json["bucket"] = connection.getBucket().name;
    if (euid) {
        auto nm = nlohmann::json(*euid);
        json["euid"] = nm;
        json["euid"]["user"] = cb::tagUserData(nm["user"]);
        if (euidPrivilegeContext) {
            json["euid"]["context"] = euidPrivilegeContext->to_string();
        }
    }
    if (sid) {
        json["scope"] = sid->to_string();
    }
    if (cid) {
        json["collection"] = cid->to_string();
    }
    json["context"] = privilegeContext->to_string();
    json["UUID"] = getEventId();
    json["privilege"] = to_string(privilege);
    json["command"] = std::move(opcode);
    return json;
}

cb::rbac::PrivilegeAccess Cookie::checkPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) {
    using cb::rbac::PrivilegeAccess;
    auto ret = testPrivilege(privilege, sid, cid);
    if (ret.success()) {
        return ret;
    }

    auto command = to_string(getRequest().getClientOpcode());
    auto json = getPrivilegeFailedErrorMessage(command, privilege, sid, cid);

    audit_command_access_failed(*this);
    LOG_WARNING_CTX("RBAC missing privilege",
                    {"conn_id", connection.getId()},
                    {"description", connection.getDescription()},
                    {"error", json});

    // Add a textual error as well
    setErrorContext(
            fmt::format("Authorization failure: can't execute {} operation "
                        "without the {} privilege",
                        command,
                        to_string(privilege)));

    return ret;
}

cb::rbac::PrivilegeAccess Cookie::testPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    using cb::rbac::PrivilegeAccess;

    const auto ret = privilegeContext->check(privilege, sid, cid);
    if (ret.failed()) {
        return ret;
    }

    if (euidPrivilegeContext) {
        const auto idx = size_t(privilege);
        if (euidExtraPrivileges.test(idx)) {
            // the caller explicitly granted the privilege
            return cb::rbac::PrivilegeAccessOk;
        }
        return euidPrivilegeContext->check(privilege, sid, cid);
    }

    return ret;
}

cb::rbac::PrivilegeAccess Cookie::checkForPrivilegeAtLeastInOneCollection(
        cb::rbac::Privilege privilege) const {
    auto ret = privilegeContext->checkForPrivilegeAtLeastInOneCollection(
            privilege);

    if (ret.success() && euidPrivilegeContext) {
        const auto idx = size_t(privilege);
        if (euidExtraPrivileges.test(idx)) {
            // the caller explicitly granted the privilege
            return cb::rbac::PrivilegeAccessOk;
        }
        return euidPrivilegeContext->checkForPrivilegeAtLeastInOneCollection(
                privilege);
    }

    return ret;
}

cb::mcbp::Status Cookie::setEffectiveUser(const cb::rbac::UserIdent& e) {
    if (e.name.empty()) {
        setErrorContext("Username must be at least one character");
        return cb::mcbp::Status::Einval;
    }

    euid = std::make_unique<cb::rbac::UserIdent>(e);

    return cb::mcbp::Status::Success;
}

bool Cookie::fetchEuidPrivilegeSet() {
    if (privilegeContext->check(cb::rbac::Privilege::Impersonate, {}, {})
                .failed()) {
        const auto command = to_string(getRequest().getClientOpcode());
        auto json = getPrivilegeFailedErrorMessage(
                command, cb::rbac::Privilege::Impersonate, {}, {});
        audit_command_access_failed(*this);
        LOG_WARNING_CTX("RBAC missing privilege",
                        {"conn_id", connection.getId()},
                        {"description", connection.getDescription()},
                        {"error", json});
        // Add a textual error as well
        setErrorContext(
                fmt::format("Authorization failure: can't execute {} operation "
                            "without the Impersonate privilege",
                            command));
        sendResponse(cb::mcbp::Status::Eaccess);
        return false;
    }

    if (euid->domain == cb::rbac::Domain::External) {
        if (!Settings::instance().isExternalAuthServiceEnabled()) {
            setErrorContext("External authentication service not configured");
            sendResponse(cb::mcbp::Status::NotSupported);
            return false;
        }

        if (getAuthorizationTask) {
            auto* task = getAuthorizationTask.get();
            const auto ret = task->getStatus();
            getAuthorizationTask.reset();
            if (ret != cb::sasl::Error::OK) {
                sendResponse(cb::mcbp::Status::Eaccess);
                return false;
            }
        } else if (!externalAuthManager->haveRbacEntryForUser(euid->name)) {
            getAuthorizationTask =
                    std::make_shared<GetAuthorizationTask>(*this, *euid);
            externalAuthManager->enqueueRequest(*getAuthorizationTask);
            ewouldblock = cb::engine_errc::would_block;
            return true;
        }
    }

    try {
        euidPrivilegeContext = std::make_unique<cb::rbac::PrivilegeContext>(
                cb::rbac::createContext(*euid, connection.getBucket().name));
    } catch (const cb::rbac::NoSuchBucketException&) {
        // The user exists, but don't have access to the bucket.
        // Let the command continue to execute, but set create an empty
        // privilege set (this make sure that we get the correcct audit
        // event at a later time.
        euidPrivilegeContext =
                std::make_unique<cb::rbac::PrivilegeContext>(euid->domain);
    } catch (const cb::rbac::Exception&) {
        setErrorContext("User \"" + euid->name + "\" is not a Couchbase user");
        sendResponse(cb::mcbp::Status::Eaccess);
        return false;
    }

    return true;
}

void Cookie::setUnknownCollectionErrorContext() {
    setUnknownCollectionErrorContext(currentCollectionInfo.manifestUid);
}

void Cookie::setUnknownCollectionErrorContext(uint64_t manifestUid) {
    nlohmann::json json;
    // return the uid without the 0x prefix
    json["manifest_uid"] = fmt::format("{0:x}", manifestUid);
    setErrorJsonExtras(json);
    // For simplicity set one error message for either unknown scope or
    // collection. This message is only here to ensure we don't also send
    // back a privilege violation message when collection visibility checks
    // occur.
    setErrorContext("Unknown scope or collection in operation");

    // ensure that a valid collection which is 'invisible' from a priv check
    // doesn't expose the priv check failure via an event_id
    error_json["error"].erase("ref");
}

bool Cookie::isMutationExtrasSupported() const {
    return connection.isSupportsMutationExtras();
}
bool Cookie::isCollectionsSupported() const {
    return connection.isCollectionsSupported();
}
bool Cookie::isDatatypeSupported(protocol_binary_datatype_t datatype) const {
    return connection.isDatatypeEnabled(datatype);
}

bool Cookie::isDurable() const {
    if (packet->isRequest()) {
        bool found = false;
        getRequest().parseFrameExtras(
                [&found](cb::mcbp::request::FrameInfoId id,
                         cb::const_byte_buffer data) -> bool {
                    if (id ==
                        cb::mcbp::request::FrameInfoId::DurabilityRequirement) {
                        found = true;
                        // stop parsing
                        return false;
                    }
                    // Continue parsing
                    return true;
                });
        return found;
    }
    return false;
}

std::pair<size_t, size_t> Cookie::getDocumentMeteringRWUnits() const {
    if (!cb::mcbp::isStatusSuccess(responseStatus) ||
        !currentCollectionInfo.metered ||
        testPrivilege(cb::rbac::Privilege::Unmetered).success()) {
        return {size_t{0}, {0}};
    }
    auto& inst = cb::serverless::Config::instance();

    size_t wb =
            inst.to_wu(document_bytes_written.load(std::memory_order_acquire));
    if (wb) {
        if (isDurable()) {
            return {size_t{0}, wb * 2};
        }
        return {size_t{0}, wb};
    }
    return {inst.to_ru(document_bytes_read.load(std::memory_order_acquire)),
            size_t{0}};
}

ConnectionIface& Cookie::getConnectionIface() {
    return connection;
}

void Cookie::notifyIoComplete(cb::engine_errc status) {
    auto& thr = getConnection().getThread();
    thr.eventBase.runInEventBaseThreadAlwaysEnqueue([this, status]() {
        TRACE_LOCKGUARD_TIMED(getConnection().getThread().mutex,
                              "mutex",
                              "notifyIoComplete",
                              SlowMutexThreshold);
        getConnection().processNotifiedCookie(*this, status);
    });
}

bool Cookie::checkThrottle(size_t pendingRBytes, size_t pendingWBytes) {
    return connection.getBucket().shouldThrottle(
            *this, true, pendingRBytes + pendingWBytes);
}

bool Cookie::sendResponse(cb::engine_errc status,
                          std::string_view extras,
                          std::string_view value) {
    return connection.sendResponse(*this,
                                   cb::mcbp::to_status(status),
                                   extras,
                                   {},
                                   value,
                                   uint8_t(cb::mcbp::Datatype::Raw),
                                   {});
}

bool Cookie::mayAccessBucket(std::string_view bucket) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(
            *this, Code::CreateRbacContext);
    return connection.mayAccessBucket(bucket);
}

uint32_t Cookie::getPrivilegeContextRevision() {
    Expects(privilegeContext);
    return privilegeContext->getGeneration();
}

bool Cookie::isValidJson(std::string_view view) {
    return getConnection().getThread().isValidJson(*this, view);
}

void Cookie::reserve() {
    incrementRefcount();
}

void Cookie::release() {
    connection.getThread().eventBase.runInEventBaseThreadAlwaysEnqueue(
            [this]() {
                TRACE_LOCKGUARD_TIMED(connection.getThread().mutex,
                                      "mutex",
                                      "release",
                                      SlowMutexThreshold);
                decrementRefcount();
                getConnection().triggerCallback();
            });
}

void Cookie::auditDocumentAccess(cb::audit::document::Operation operation) {
    cb::audit::document::add(*this, operation, getRequestKey());
}
cb::engine_errc Cookie::preLinkDocument(item_info& info) {
    if (commandContext) {
        return commandContext->pre_link_document(info);
    }

    return cb::engine_errc::success;
}
