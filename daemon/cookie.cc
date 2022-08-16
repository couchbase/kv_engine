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

#include "cookie.h"

#include "buckets.h"
#include "connection.h"
#include "cookie_trace_context.h"
#include "external_auth_manager_thread.h"
#include "get_authorization_task.h"
#include "mcaudit.h"
#include "mcbp_executors.h"
#include "mcbp_validators.h"
#include "memcached.h"
#include "opentelemetry.h"
#include "settings.h"

#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/compress.h>
#include <platform/histogram.h>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <platform/uuid.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/logtags.h>
#include <cctype>
#include <chrono>

nlohmann::json Cookie::toJSON() const {
    nlohmann::json ret;

    if (packet == nullptr) {
        ret["packet"] = nlohmann::json();
    } else {
        const auto& header = getHeader();
        try {
            ret["packet"] = header.toJSON(validated);
        } catch (const std::exception& e) {
            ret["packet"]["error"] = e.what();
        }
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
    ret["throttled"] = throttled.load();
    ret["refcount"] = uint32_t(refcount);
    ret["engine_storage"] = cb::to_hex(uint64_t(engine_storage.load()));
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

std::string Cookie::getErrorJson() {
    if (error_context.empty() && event_id.empty() && error_extra_json.empty()) {
        return {};
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
    return root.dump();
}

bool Cookie::doExecute() {
    if (!validated) {
        throw std::runtime_error("Cookie::execute: validate() not called");
    }

    Expects(!ewouldblock &&
            "Cookie::doExecute(): Executing while ewouldblock is true could "
            "result in a background task notifying while executing.");

    if (euid && !euidPrivilegeContext) {
        // We're supposed to run as a different user, but we don't
        // have the privilege set configured yet...
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
    if (header.isResponse()) {
        execute_response_packet(*this, header.getResponse());
    } else {
        // We've already verified that the packet is a legal packet
        // so it must be a request
        execute_request_packet(*this, header.getRequest());
    }

    if (isEwouldblock()) {
        return false;
    }

    return true;
}

uint32_t Cookie::getConnectionId() const {
    return connection.getId();
}

bool Cookie::execute(bool useStartTime) {
    auto ts = useStartTime ? start : std::chrono::steady_clock::now();

    auto done = doExecute();

    auto te = std::chrono::steady_clock::now();
    tracer.record(cb::tracing::Code::Execute, ts, te);

    if (done) {
        collectTimings(te);
        connection.commandExecuted(*this);
        return true;
    }
    return false;
}

void Cookie::setPacket(const cb::mcbp::Header& header, bool copy) {
    if (copy) {
        auto frame = header.getFrame();
        frame_copy = std::make_unique<uint8_t[]>(frame.size());
        std::copy(frame.begin(), frame.end(), frame_copy.get());
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

void Cookie::setEwouldblock(bool value) {
    if (value && !connection.isDCP()) {
        setAiostat(cb::engine_errc::would_block);
    }

    ewouldblock = value;
}

void Cookie::sendNotMyVBucket() {
    auto pushed = connection.getPushedClustermapRevno();
    auto config =
            connection.getBucket().clusterConfiguration.maybeGetConfiguration(
                    pushed, Settings::instance().isDedupeNmvbMaps());

    if (config) {
        connection.sendResponse(*this,
                                cb::mcbp::Status::NotMyVbucket,
                                {},
                                {},
                                {config->config.data(), config->config.size()},
                                PROTOCOL_BINARY_DATATYPE_JSON,
                                {});
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

    const auto error_json = getErrorJson();

    if (cb::mcbp::isStatusSuccess(status)) {
        setCas(casValue);
    } else {
        // This is an error message.. Inject the error JSON!
        extras = {};
        key = {};
        value = error_json;
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
                          uint64_t cas) {
    sendResponse(
            cb::mcbp::to_status(status), extras, key, value, datatype, cas);
}

const DocKey Cookie::getRequestKey() const {
    return connection.makeDocKey(getRequest().getKey());
}

std::string Cookie::getPrintableRequestKey(bool addUserDataTags,
                                           bool stripCollection) const {
    const auto key = getRequest().getKey();

    std::string buffer;
    if (stripCollection) {
        buffer = getRequestKey().makeDocKeyWithoutCollectionID().getBuffer();
    } else {
        buffer = {reinterpret_cast<const char*>(key.data()), key.size()};
    }

    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    if (addUserDataTags) {
        return cb::tagUserData(buffer);
    }

    return buffer;
}

std::string Cookie::getPrintableRequestCollectionID() const {
    return getRequestKey().getCollectionID().to_string();
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

void Cookie::logResponse(cb::engine_errc code) const {
    if (Settings::instance().getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    if (code == cb::engine_errc::would_block) {
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
    if (total_throttle_time.count()) {
        // @todo we don't want to flood the logs, but we should only
        // mute if we've reported it back to the clients
        return;
    }

    const auto opcode = getRequest().getClientOpcode();
    const auto limit = cb::mcbp::sla::getSlowOpThreshold(opcode);
    if (elapsed > limit) {
        std::chrono::nanoseconds timings(elapsed);
        auto& c = getConnection();

        TRACE_COMPLETE2("memcached/slow",
                        "Slow cmd",
                        start,
                        start + elapsed,
                        "opcode",
                        getHeader().getOpcode(),
                        "connection_id",
                        c.getId());

        nlohmann::json details = {
                {"cid",
                 fmt::format("{}/{:x}",
                             c.getConnectionId().data(),
                             ntohl(getHeader().getOpaque()))},
                {"duration", cb::time2text(timings)},
                {"trace", tracer.to_string()},
                {"command", to_string(opcode)},
                {"peer", nlohmann::json::parse(c.getPeername())},
                {"bucket", c.getBucket().name},
                {"packet", getHeader().toJSON(validated)},
                {"worker_tid", folly::getCurrentThreadID()}};
        if (responseStatus != cb::mcbp::Status::COUNT) {
            details["response"] = to_string(responseStatus);
        }
        LOG_WARNING(R"({}: Slow operation: {})", c.getId(), details.dump());
    }
}

Cookie::Cookie(Connection& conn)
    : connection(conn), privilegeContext(conn.getUser().domain) {
}

void Cookie::initialize(const cb::mcbp::Header& header, bool tracing_enabled) {
    reset();
    setTracingEnabled(tracing_enabled ||
                      Settings::instance().alwaysCollectTraceInfo());
    setPacket(header);
    start = std::chrono::steady_clock::now();
    tracer.begin(cb::tracing::Code::Request, start);

    if (Settings::instance().getVerbose() > 1) {
        try {
            LOG_TRACE(">{} Read command {}",
                      connection.getId(),
                      header.toJSON(false).dump());
        } catch (const std::exception&) {
            // Failed to decode the header.. do a raw dump instead
            LOG_TRACE(">{} Read command {}",
                      connection.getId(),
                      cb::to_hex({reinterpret_cast<const uint8_t*>(packet),
                                  sizeof(*packet)}));
        }
    }
}

cb::mcbp::Status Cookie::validate() {
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
        const std::array<cb::mcbp::ClientOpcode, 5> allowed{
                {cb::mcbp::ClientOpcode::Hello,
                 cb::mcbp::ClientOpcode::SaslListMechs,
                 cb::mcbp::ClientOpcode::SaslAuth,
                 cb::mcbp::ClientOpcode::SaslStep,
                 cb::mcbp::ClientOpcode::GetErrorMap}};
        if (std::find(allowed.begin(), allowed.end(), opcode) ==
            allowed.end()) {
#if CB_DEVELOPMENT_ASSERTS
            if (cb::mcbp::is_valid_opcode(opcode)) {
                LOG_WARNING(
                        "{}: Trying to execute {} before authentication. "
                        "Returning eaccess",
                        getConnectionId(),
                        to_string(opcode));
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
        const std::array<cb::mcbp::ClientOpcode, 4> allowed{
                {cb::mcbp::ClientOpcode::Hello,
                 cb::mcbp::ClientOpcode::GetErrorMap,
                 cb::mcbp::ClientOpcode::GetClusterConfig,
                 cb::mcbp::ClientOpcode::SelectBucket}};
        if (std::find(allowed.begin(), allowed.end(), opcode) ==
            allowed.end()) {
            LOG_DEBUG(
                    "{} Can't run the requested command on "
                    "cluster-config-bucket: {}",
                    connection.getId(),
                    to_string(opcode));
            if (connection.isXerrorSupport()) {
                return cb::mcbp::Status::EConfigOnly;
            }
            return cb::mcbp::Status::NotSupported;
        }
    }

    auto result = packetValidator.validate(opcode, *this);
    if (result != cb::mcbp::Status::Success) {
        if (result != cb::mcbp::Status::UnknownCollection &&
            result != cb::mcbp::Status::NotMyVbucket &&
            result != cb::mcbp::Status::BucketSizeLimitExceeded) {
            LOG_WARNING(
                    "{}: Packet validation failed for \"{}\" - Status: \"{}\" "
                    "- Packet:[{}] - Returned payload:[{}]",
                    connection.getId(),
                    to_string(opcode),
                    to_string(result),
                    request.toJSON(false).dump(),
                    getErrorJson());
            audit_invalid_packet(getConnection(), getPacket());
        }
        return result;
    }

    // Add a barrier to the command if we don't support reordering it!
    if (reorder && !is_reorder_supported(request.getClientOpcode())) {
        setBarrier();
    }

    // MB-52884 - Some commands returns data in different threads
    //            than the front end thread, and will race trying
    //            to access the incomming packet data. In these
    //            cases we might need to copy out the incomming
    //            packet.
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::Get:
    case cb::mcbp::ClientOpcode::Set:
    case cb::mcbp::ClientOpcode::Add:
    case cb::mcbp::ClientOpcode::Replace:
    case cb::mcbp::ClientOpcode::Delete:
    case cb::mcbp::ClientOpcode::Increment:
    case cb::mcbp::ClientOpcode::Decrement:
    case cb::mcbp::ClientOpcode::Quit:
    case cb::mcbp::ClientOpcode::Flush:
    case cb::mcbp::ClientOpcode::Getq:
    case cb::mcbp::ClientOpcode::Noop:
    case cb::mcbp::ClientOpcode::Version:
    case cb::mcbp::ClientOpcode::Getk:
    case cb::mcbp::ClientOpcode::Getkq:
    case cb::mcbp::ClientOpcode::Append:
    case cb::mcbp::ClientOpcode::Prepend:
    case cb::mcbp::ClientOpcode::Setq:
    case cb::mcbp::ClientOpcode::Addq:
    case cb::mcbp::ClientOpcode::Replaceq:
    case cb::mcbp::ClientOpcode::Deleteq:
    case cb::mcbp::ClientOpcode::Incrementq:
    case cb::mcbp::ClientOpcode::Decrementq:
    case cb::mcbp::ClientOpcode::Quitq:
    case cb::mcbp::ClientOpcode::Flushq:
    case cb::mcbp::ClientOpcode::Appendq:
    case cb::mcbp::ClientOpcode::Prependq:
    case cb::mcbp::ClientOpcode::Verbosity:
    case cb::mcbp::ClientOpcode::Touch:
    case cb::mcbp::ClientOpcode::Gat:
    case cb::mcbp::ClientOpcode::Gatq:
    case cb::mcbp::ClientOpcode::Hello:
    case cb::mcbp::ClientOpcode::SaslListMechs:
    case cb::mcbp::ClientOpcode::SaslAuth:
    case cb::mcbp::ClientOpcode::SaslStep:
    case cb::mcbp::ClientOpcode::IoctlGet:
    case cb::mcbp::ClientOpcode::IoctlSet:
    case cb::mcbp::ClientOpcode::ConfigValidate:
    case cb::mcbp::ClientOpcode::ConfigReload:
    case cb::mcbp::ClientOpcode::AuditPut:
    case cb::mcbp::ClientOpcode::AuditConfigReload:
    case cb::mcbp::ClientOpcode::Shutdown:
    case cb::mcbp::ClientOpcode::SetBucketUnitThrottleLimits:
    case cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded:
    case cb::mcbp::ClientOpcode::Rget_Unsupported:
    case cb::mcbp::ClientOpcode::Rset_Unsupported:
    case cb::mcbp::ClientOpcode::Rsetq_Unsupported:
    case cb::mcbp::ClientOpcode::Rappend_Unsupported:
    case cb::mcbp::ClientOpcode::Rappendq_Unsupported:
    case cb::mcbp::ClientOpcode::Rprepend_Unsupported:
    case cb::mcbp::ClientOpcode::Rprependq_Unsupported:
    case cb::mcbp::ClientOpcode::Rdelete_Unsupported:
    case cb::mcbp::ClientOpcode::Rdeleteq_Unsupported:
    case cb::mcbp::ClientOpcode::Rincr_Unsupported:
    case cb::mcbp::ClientOpcode::Rincrq_Unsupported:
    case cb::mcbp::ClientOpcode::Rdecr_Unsupported:
    case cb::mcbp::ClientOpcode::Rdecrq_Unsupported:
    case cb::mcbp::ClientOpcode::SetVbucket:
    case cb::mcbp::ClientOpcode::GetVbucket:
    case cb::mcbp::ClientOpcode::DelVbucket:
    case cb::mcbp::ClientOpcode::TapConnect_Unsupported:
    case cb::mcbp::ClientOpcode::TapMutation_Unsupported:
    case cb::mcbp::ClientOpcode::TapDelete_Unsupported:
    case cb::mcbp::ClientOpcode::TapFlush_Unsupported:
    case cb::mcbp::ClientOpcode::TapOpaque_Unsupported:
    case cb::mcbp::ClientOpcode::TapVbucketSet_Unsupported:
    case cb::mcbp::ClientOpcode::TapCheckpointStart_Unsupported:
    case cb::mcbp::ClientOpcode::TapCheckpointEnd_Unsupported:
    case cb::mcbp::ClientOpcode::GetAllVbSeqnos:
    case cb::mcbp::ClientOpcode::DcpOpen:
    case cb::mcbp::ClientOpcode::DcpAddStream:
    case cb::mcbp::ClientOpcode::DcpCloseStream:
    case cb::mcbp::ClientOpcode::DcpStreamReq:
    case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
    case cb::mcbp::ClientOpcode::DcpStreamEnd:
    case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
    case cb::mcbp::ClientOpcode::DcpMutation:
    case cb::mcbp::ClientOpcode::DcpDeletion:
    case cb::mcbp::ClientOpcode::DcpExpiration:
    case cb::mcbp::ClientOpcode::DcpFlush_Unsupported:
    case cb::mcbp::ClientOpcode::DcpSetVbucketState:
    case cb::mcbp::ClientOpcode::DcpNoop:
    case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
    case cb::mcbp::ClientOpcode::DcpControl:
    case cb::mcbp::ClientOpcode::DcpSystemEvent:
    case cb::mcbp::ClientOpcode::DcpPrepare:
    case cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged:
    case cb::mcbp::ClientOpcode::DcpCommit:
    case cb::mcbp::ClientOpcode::DcpAbort:
    case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
    case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
    case cb::mcbp::ClientOpcode::StopPersistence:
    case cb::mcbp::ClientOpcode::StartPersistence:
    case cb::mcbp::ClientOpcode::SetParam:
    case cb::mcbp::ClientOpcode::GetReplica:
    case cb::mcbp::ClientOpcode::CreateBucket:
    case cb::mcbp::ClientOpcode::DeleteBucket:
    case cb::mcbp::ClientOpcode::ListBuckets:
    case cb::mcbp::ClientOpcode::SelectBucket:
    case cb::mcbp::ClientOpcode::ObserveSeqno:
    case cb::mcbp::ClientOpcode::Observe:
    case cb::mcbp::ClientOpcode::EvictKey:
    case cb::mcbp::ClientOpcode::GetLocked:
    case cb::mcbp::ClientOpcode::UnlockKey:
    case cb::mcbp::ClientOpcode::GetFailoverLog:
    case cb::mcbp::ClientOpcode::LastClosedCheckpoint_Unsupported:
    case cb::mcbp::ClientOpcode::DeregisterTapClient_Unsupported:
    case cb::mcbp::ClientOpcode::ResetReplicationChain_Unsupported:
    case cb::mcbp::ClientOpcode::GetMeta:
    case cb::mcbp::ClientOpcode::GetqMeta:
    case cb::mcbp::ClientOpcode::SetWithMeta:
    case cb::mcbp::ClientOpcode::SetqWithMeta:
    case cb::mcbp::ClientOpcode::AddWithMeta:
    case cb::mcbp::ClientOpcode::AddqWithMeta:
    case cb::mcbp::ClientOpcode::SnapshotVbStates_Unsupported:
    case cb::mcbp::ClientOpcode::VbucketBatchCount_Unsupported:
    case cb::mcbp::ClientOpcode::DelWithMeta:
    case cb::mcbp::ClientOpcode::DelqWithMeta:
    case cb::mcbp::ClientOpcode::CreateCheckpoint_Unsupported:
    case cb::mcbp::ClientOpcode::NotifyVbucketUpdate_Unsupported:
    case cb::mcbp::ClientOpcode::EnableTraffic:
    case cb::mcbp::ClientOpcode::DisableTraffic:
    case cb::mcbp::ClientOpcode::Ifconfig:
    case cb::mcbp::ClientOpcode::ChangeVbFilter_Unsupported:
    case cb::mcbp::ClientOpcode::CheckpointPersistence_Unsupported:
    case cb::mcbp::ClientOpcode::ReturnMeta:
    case cb::mcbp::ClientOpcode::CompactDb:
    case cb::mcbp::ClientOpcode::SetClusterConfig:
    case cb::mcbp::ClientOpcode::GetClusterConfig:
    case cb::mcbp::ClientOpcode::GetRandomKey:
    case cb::mcbp::ClientOpcode::SeqnoPersistence:
    case cb::mcbp::ClientOpcode::CollectionsSetManifest:
    case cb::mcbp::ClientOpcode::CollectionsGetManifest:
    case cb::mcbp::ClientOpcode::CollectionsGetID:
    case cb::mcbp::ClientOpcode::CollectionsGetScopeID:
    case cb::mcbp::ClientOpcode::SetDriftCounterState_Unsupported:
    case cb::mcbp::ClientOpcode::GetAdjustedTime_Unsupported:
    case cb::mcbp::ClientOpcode::SubdocGet:
    case cb::mcbp::ClientOpcode::SubdocExists:
    case cb::mcbp::ClientOpcode::SubdocDictAdd:
    case cb::mcbp::ClientOpcode::SubdocDictUpsert:
    case cb::mcbp::ClientOpcode::SubdocDelete:
    case cb::mcbp::ClientOpcode::SubdocReplace:
    case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
    case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
    case cb::mcbp::ClientOpcode::SubdocArrayInsert:
    case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
    case cb::mcbp::ClientOpcode::SubdocCounter:
    case cb::mcbp::ClientOpcode::SubdocMultiLookup:
    case cb::mcbp::ClientOpcode::SubdocMultiMutation:
    case cb::mcbp::ClientOpcode::SubdocGetCount:
    case cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr:
    case cb::mcbp::ClientOpcode::Scrub:
    case cb::mcbp::ClientOpcode::IsaslRefresh:
    case cb::mcbp::ClientOpcode::SslCertsRefresh:
    case cb::mcbp::ClientOpcode::GetCmdTimer:
    case cb::mcbp::ClientOpcode::SetCtrlToken:
    case cb::mcbp::ClientOpcode::GetCtrlToken:
    case cb::mcbp::ClientOpcode::UpdateExternalUserPermissions:
    case cb::mcbp::ClientOpcode::RbacRefresh:
    case cb::mcbp::ClientOpcode::AuthProvider:
    case cb::mcbp::ClientOpcode::DropPrivilege:
    case cb::mcbp::ClientOpcode::AdjustTimeofday:
    case cb::mcbp::ClientOpcode::EwouldblockCtl:
    case cb::mcbp::ClientOpcode::GetErrorMap:
    case cb::mcbp::ClientOpcode::RangeScanCreate:
    case cb::mcbp::ClientOpcode::RangeScanCancel:
    case cb::mcbp::ClientOpcode::Invalid:
        break;

    case cb::mcbp::ClientOpcode::RangeScanContinue:
    case cb::mcbp::ClientOpcode::Stat:
    case cb::mcbp::ClientOpcode::GetKeys:
        // The command may try to call operations on the cookie to get
        // information from the input packet from a different thread than
        // the front end thread (for instance sending a response).
        // To avoid this from racing with where we preserve the packet
        // in the ewb case just copy out the packet
        preserveRequest();
    }

    validated = true;
    return cb::mcbp::Status::Success;
}

void Cookie::reset() {
    document_bytes_read = 0;
    document_bytes_written = 0;
    total_throttle_time = total_throttle_time.zero();
    event_id.clear();
    error_context.clear();
    error_extra_json.clear();
    packet = {};
    validated = false;
    cas = 0;
    commandContext.reset();
    tracer.clear();
    durable = false;
    Expects(!ewouldblock &&
            "Cookie::reset() when ewouldblock is true could result in an "
            "outstanding notifyIoComplete occuring on wrong cookie");
    openTracingContext.clear();
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
}

void Cookie::setThrottled(bool val) {
    throttled.store(val, std::memory_order_release);
    if (tracingEnabled) {
        const auto now = std::chrono::steady_clock::now();
        if (val) {
            throttle_start = now;
        } else {
            total_throttle_time +=
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            now - start);
            tracer.record(cb::tracing::Code::Throttled, throttle_start, now);
        }
    }
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

    // End the tracing span (Request) which is the first span in the tracer
    tracer.end(cb::tracing::SpanId{0}, endTime);

    // aggregated timing for all buckets
    all_buckets[0].timings.collect(opcode, elapsed);

    // timing for current bucket
    const auto bucketid = connection.getBucketIndex();
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(opcode, elapsed);
    }

    // Log operations taking longer than the "slow" threshold for the opcode.
    maybeLogSlowCommand(elapsed);

    if (isOpenTracingEnabled()) {
        OpenTelemetry::pushTraceLog(extractTraceContext());
    }
}

std::string_view Cookie::getInflatedInputPayload() const {
    if (!inflated_input_payload.empty()) {
        return inflated_input_payload;
    }

    const auto value = getHeader().getValue();
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

bool Cookie::inflateInputPayload(const cb::mcbp::Header& header) {
    inflated_input_payload.reset();
    if (!mcbp::datatype::is_snappy(header.getDatatype())) {
        return true;
    }

    try {
        auto val = header.getValue();
        if (!inflateSnappy(
                    {reinterpret_cast<const char*>(val.data()), val.size()},
                    inflated_input_payload)) {
            setErrorContext("Failed to inflate payload");
            return false;
        }
    } catch (const std::bad_alloc&) {
        setErrorContext("Failed to allocate memory");
        return false;
    }

    return true;
}

bool Cookie::inflateSnappy(std::string_view input,
                           cb::compression::Buffer& output) {
    // Record how long Snappy decompression takes to both Tracer and
    // bucket-level histogram.
    using namespace cb::tracing;
    ScopeTimer2<HdrMicroSecStopwatch, SpanStopwatch> timer(
            std::forward_as_tuple(
                    getConnection().getBucket().snappyDecompressionTimes),
            std::forward_as_tuple(*this, Code::SnappyDecompress));

    return cb::compression::inflate(
            cb::compression::Algorithm::Snappy, input, output);
}

cb::rbac::PrivilegeAccess Cookie::checkPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) {
    using cb::rbac::PrivilegeAccess;
    auto ret = checkPrivilege(privilegeContext, privilege, sid, cid);

    if (ret.success() && euidPrivilegeContext) {
        const auto idx = size_t(privilege);
        if (euidExtraPrivileges.test(idx)) {
            // the caller explicitly granted the privilege
            return cb::rbac::PrivilegeAccessOk;
        }
        return checkPrivilege(*euidPrivilegeContext, privilege, sid, cid);
    }

    return ret;
}

cb::rbac::PrivilegeAccess Cookie::checkPrivilege(
        const cb::rbac::PrivilegeContext& ctx,
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) {
    const auto ret = ctx.check(privilege, sid, cid);
    if (ret.failed()) {
        const auto opcode = getRequest().getClientOpcode();
        const auto command(to_string(opcode));
        const auto privilege_string = cb::rbac::to_string(privilege);
        const auto context = ctx.to_string();

        nlohmann::json json;
        json["bucket"] = connection.getBucket().name;
        if (&ctx != &privilegeContext) {
            auto nm = euid->to_json();
            json["euid"] = nm;
            json["euid"]["user"] = cb::tagUserData(nm["user"]);
        }
        if (sid) {
            json["scope"] = sid->to_string();
        }
        if (cid) {
            json["collection"] = cid->to_string();
        }
        json["context"] = context;
        json["UUID"] = getEventId();
        json["privilege"] = privilege_string;
        json["command"] = command;

        if (Settings::instance().isPrivilegeDebug()) {
            audit_privilege_debug(*this,
                                  command,
                                  connection.getBucket().name,
                                  privilege_string,
                                  context);

            LOG_INFO("{}: RBAC {} privilege debug: {}",
                     connection.getId(),
                     connection.getDescription(),
                     json.dump());

            return cb::rbac::PrivilegeAccessOk;
        } else {
            audit_command_access_failed(*this);
            LOG_WARNING("{} RBAC {} missing privilege: {}",
                        connection.getId(),
                        connection.getDescription(),
                        json.dump());
            // Add a textual error as well
            setErrorContext("Authorization failure: can't execute " + command +
                            " operation without the " + privilege_string +
                            " privilege");
        }
    }

    return ret;
}

cb::rbac::PrivilegeAccess Cookie::testPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    using cb::rbac::PrivilegeAccess;
    auto ret = testPrivilege(privilegeContext, privilege, sid, cid);

    if (ret.success() && euidPrivilegeContext) {
        const auto idx = size_t(privilege);
        if (euidExtraPrivileges.test(idx)) {
            // the caller explicitly granted the privilege
            return cb::rbac::PrivilegeAccessOk;
        }
        return testPrivilege(*euidPrivilegeContext, privilege, sid, cid);
    }

    return ret;
}

cb::rbac::PrivilegeAccess Cookie::testPrivilege(
        const cb::rbac::PrivilegeContext& ctx,
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    return ctx.check(privilege, sid, cid);
}

cb::rbac::PrivilegeAccess Cookie::checkForPrivilegeAtLeastInOneCollection(
        cb::rbac::Privilege privilege) const {
    auto ret =
            privilegeContext.checkForPrivilegeAtLeastInOneCollection(privilege);

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
    if (checkPrivilege(
                privilegeContext, cb::rbac::Privilege::Impersonate, {}, {})
                .failed()) {
        setErrorContext(
                "You need the Impersonate privilege in order to impersonate "
                "users");
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
            auto* task = dynamic_cast<GetAuthorizationTask*>(
                    getAuthorizationTask.get());
            if (task == nullptr) {
                throw std::runtime_error(
                        "Cookie::fetchEuidPrivilegeSet: Invalid object stored "
                        "in getAuthorizationTask");
            }

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
            ewouldblock = true;
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
    event_id.clear();
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
