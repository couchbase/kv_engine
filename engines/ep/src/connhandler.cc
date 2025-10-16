/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connhandler.h"
#include "bucket_logger.h"
#include "connhandler_impl.h"
#include "dcp/stream.h"
#include "ep_engine.h"
#include "ep_time.h"

#include <fmt/format.h>
#include <memcached/connection_iface.h>
#include <memcached/cookie_iface.h>
#include <memcached/durability_spec.h>
#include <memcached/rbac/privilege_database.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_time.h>
#include <platform/timeutils.h>
#include <utilities/json_utilities.h>
#include <utilities/logtags.h>

std::string to_string(ConnHandler::PausedReason r) {
    switch (r) {
    case ConnHandler::PausedReason::BufferLogFull:
        return "BufferLogFull";
    case ConnHandler::PausedReason::Initializing:
        return "Initializing";
    case ConnHandler::PausedReason::OutOfMemory:
        return "OutOfMemory";
    case ConnHandler::PausedReason::ReadyListEmpty:
        return "ReadyListEmpty";
    case ConnHandler::PausedReason::Unknown:
        return "Unknown";
    }
    return "Invalid";
}

ConnHandler::ConnHandler(EventuallyPersistentEngine& e,
                         CookieIface* c,
                         std::string n)
    : engine_(e),
      stats(engine_.getEpStats()),
      created(ep_uptime_now()),
      name(std::move(n)),
      cookie(c),
      disconnect(false),
      paused(false),
      authenticatedUser(c->getConnectionIface().getUser().name),
      connected_port(
              c->getConnectionIface().getSockname()["port"].get<in_port_t>()),
      idleTimeout(e.getConfiguration().getDcpIdleTimeout()) {
    logger = BucketLogger::createBucketLogger(
            std::to_string(reinterpret_cast<uintptr_t>(this)));

    logger->setConnectionId(c->getConnectionId());
    engine_.reserveCookie(*cookie);
}

ConnHandler::~ConnHandler() {
    logger->unregister();
    engine_.releaseCookie(*cookie);
}

cb::engine_errc ConnHandler::addStream(uint32_t,
                                       Vbid,
                                       cb::mcbp::DcpAddStreamFlag) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp add stream API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::closeStream(uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp close stream API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::streamEnd(uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp stream end API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::mutation(uint32_t opaque,
                                      const DocKeyView& key,
                                      cb::const_byte_buffer value,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint32_t flags,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t expiration,
                                      uint32_t lock_time,
                                      uint8_t nru) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the mutation API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::deletion(uint32_t opaque,
                                      const DocKeyView& key,
                                      cb::const_byte_buffer value,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the deletion API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::deletionV2(uint32_t opaque,
                                        const DocKeyView& key,
                                        cb::const_byte_buffer value,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the deletionV2 API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::expiration(uint32_t opaque,
                                        const DocKeyView& key,
                                        cb::const_byte_buffer value,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t deleteTime) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the expiration API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::snapshotMarker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> high_prepared_seqno,
        std::optional<uint64_t> max_visible_seqno,
        std::optional<uint64_t> purge_seqno) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp snapshot marker API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::setVBucketState(uint32_t opaque,
                                             Vbid vbucket,
                                             vbucket_state_t state) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the set vbucket state API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::streamRequest(
        cb::mcbp::DcpAddStreamFlag,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t* rollback_seqno,
        dcp_add_failover_log callback,
        std::optional<std::string_view> json) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp stream request API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::noop(uint32_t opaque) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the noop API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::bufferAcknowledgement(uint32_t opaque,
                                                   uint32_t buffer_bytes) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the buffer acknowledgement API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::control(uint32_t opaque,
                                     std::string_view key,
                                     std::string_view value) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the control API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::step(bool, DcpMessageProducersIface&) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp step API");
    return cb::engine_errc::disconnect;
}

bool ConnHandler::handleResponse(const cb::mcbp::Response& resp) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp response handler API");
    return false;
}

cb::engine_errc ConnHandler::systemEvent(uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) {
    logger->warn(
            "Disconnecting - This connections doesn't "
            "support the dcp system_event API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::prepare(uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint32_t flags,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t expiration,
                                     uint32_t lock_time,
                                     uint8_t nru,
                                     DocumentState document_state,
                                     cb::durability::Level level) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp prepare "
            "API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::commit(uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKeyView& key,
                                    uint64_t prepare_seqno,
                                    uint64_t commit_seqno) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp commit "
            "API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::abort(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKeyView& key,
                                   uint64_t prepareSeqno,
                                   uint64_t abortSeqno) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp abort "
            "API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::seqno_acknowledged(uint32_t opaque,
                                                Vbid vbucket,
                                                uint64_t prepared_seqno) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp "
            "seqno_acknowledged API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::cached_value(uint32_t opaque,
                                          const DocKeyView& key,
                                          cb::const_byte_buffer value,
                                          uint8_t datatype,
                                          uint64_t cas,
                                          Vbid vbucket,
                                          uint32_t flags,
                                          uint64_t bySeqno,
                                          uint64_t revSeqno,
                                          uint32_t expiration,
                                          uint8_t nru) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp "
            "cached_value API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::cached_key_meta(uint32_t opaque,
                                             const DocKeyView& key,
                                             uint8_t datatype,
                                             uint64_t cas,
                                             Vbid vbucket,
                                             uint32_t flags,
                                             uint64_t bySeqno,
                                             uint64_t revSeqno,
                                             uint32_t expiration) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp "
            "cached_key_meta API");
    return cb::engine_errc::disconnect;
}

BucketLogger& ConnHandler::getLogger() {
    return *logger;
}

void ConnHandler::addStats(const AddStatFn& add_stat, CookieIface& c) {
    using namespace std::chrono;

    // We can log internal users without redaction, and given that most/all DCP
    // consumers will be internal, there is value in only redacting those we
    // must.
    auto nameForStats = getAuthenticatedUser();
    if (!cb::rbac::UserIdent::is_internal(nameForStats)) {
        nameForStats = cb::tagUserData(nameForStats);
    }
    addStat("user", nameForStats, add_stat, c);
    addStat("type", getType(), add_stat, c);
    addStat("created",
            duration_cast<seconds>(created.time_since_epoch()).count(),
            add_stat,
            c);
    addStat("pending_disconnect", disconnect.load(), add_stat, c);
    addStat("supports_ack", supportAck.load(), add_stat, c);
    addStat("paused", isPaused(), add_stat, c);

    const auto details = pausedDetails.copy();
    addStat("paused_count", details.pausedCounter, add_stat, c);
    addStat("unpaused_count", details.unpausedCounter, add_stat, c);
    if (isPaused()) {
        addStat("paused_current_reason",
                to_string(details.reason),
                add_stat,
                c);
        addStat("paused_current_duration",
                cb::time2text(cb::time::steady_clock::now() -
                              details.lastPaused),
                add_stat,
                c);
    }
    for (size_t reason = 0; reason < PausedReasonCount; reason++) {
        const auto count = details.reasonCounts[reason];
        if (count) {
            std::string key{"paused_previous_" +
                            to_string(PausedReason(reason)) + "_count"};
            addStat(key, count, add_stat, c);

            const auto duration = details.reasonDurations[reason];
            key = {"paused_previous_" + to_string(PausedReason(reason)) +
                   "_duration"};
            addStat(key, cb::time2text(duration), add_stat, c);
        }
    }
    const auto priority = cookie.load()->getConnectionIface().getPriority();
    addStat("priority", format_as(priority), add_stat, c);
    addStat(DcpControlKeys::FlatBuffersSystemEvents,
            areFlatBuffersSystemEventsEnabled(),
            add_stat,
            c);
    addStat("change_streams_enabled", areChangeStreamsEnabled(), add_stat, c);
}

void ConnHandler::pause(ConnHandler::PausedReason r) {
    paused.store(true);
    auto now = cb::time::steady_clock::now();
    pausedDetails.withLock([r, now](auto& details) {
        details.reason = r;
        details.lastPaused = now;
        if (r == PausedReason::BufferLogFull) {
            ++details.pausedCounter;
        }
    });
}

void ConnHandler::setLogContext(const std::string& header,
                                const nlohmann::json& ctx) {
    logger->setPrefix(ctx, header);
}

const char* ConnHandler::logHeader() const {
    return logger->getFmtPrefix().c_str();
}

void ConnHandler::unPause() {
    auto wasPaused = paused.exchange(false);
    if (!wasPaused) {
        return;
    }
    auto now = cb::time::steady_clock::now();
    pausedDetails.withLock([now](auto& details) {
        auto index = static_cast<std::underlying_type_t<PausedReason>>(
                details.reason);
        details.reasonCounts[index]++;
        details.reasonDurations[index] += (now - details.lastPaused);
        if (details.reason == PausedReason::BufferLogFull) {
            ++details.unpausedCounter;
        }
    });
}

nlohmann::json ConnHandler::getPausedDetailsDescription() const {
    const auto details = pausedDetails.copy();
    auto json = nlohmann::json::object();
    size_t totalCount = 0;
    std::chrono::nanoseconds totalDuration{};
    for (uint8_t reason = 0; size_t{reason} < PausedReasonCount; reason++) {
        const auto count = details.reasonCounts[reason];
        if (count) {
            const auto duration = details.reasonDurations[reason];
            json[to_string(ConnHandler::PausedReason{reason})] = {
                    {"count", count}, {"duration", cb::time2text(duration)}};
            totalCount += count;
            totalDuration += duration;
        }
    }

    return {{"reasons", std::move(json)},
            {"paused_count", totalCount},
            {"paused_duration", cb::time2text(totalDuration)}};
}

std::pair<size_t, size_t> ConnHandler::getPausedCounters() const {
    return pausedDetails.withLock([](const auto& details) {
        return std::make_pair(details.pausedCounter, details.unpausedCounter);
    });
}

void ConnHandler::scheduleNotify() {
    if (engine_.getEpStats().isShutdown) {
        return;
    }
    if (isPaused()) {
        engine_.scheduleDcpStep(*getCookie());
    }
}

void ConnHandler::doStreamStatsLegacy(
        const std::vector<std::shared_ptr<Stream>>& streams,
        const AddStatFn& add_stat,
        CookieIface& c) {
    std::array<char, 1024> prefixed_key;
    for (const auto& stream : streams) {
        char* key_ptr = fmt::format_to(prefixed_key.data(),
                                       "{}:stream_{}_",
                                       stream->getName(),
                                       stream->getVBucket().get());
        auto prefix_size = std::distance(prefixed_key.data(), key_ptr);

        auto add_stream_stat =
                [&add_stat, &key_ptr, &prefixed_key, &prefix_size](
                        auto k, auto v, auto& c) {
                    Expects(prefix_size + k.size() < prefixed_key.size());
                    auto key_end = std::copy(k.begin(), k.end(), key_ptr);
                    std::string_view formatted_key{
                            prefixed_key.data(),
                            static_cast<size_t>(std::distance(
                                    prefixed_key.data(), key_end))};
                    add_stat(formatted_key, v, c);
                };
        stream->addStats(add_stream_stat, c);
    }
}

void ConnHandler::doStreamStatsJson(
        const std::vector<std::shared_ptr<Stream>>& streams,
        const AddStatFn& add_stat,
        CookieIface& c) {
    nlohmann::json json;
    for (const auto& stream : streams) {
        auto add_stream_stat = [&json](auto k, auto v, auto& c) {
            cb::jsonSetStringView(json, k, v);
        };
        // Make sure we don't have stale values from the previous stream. Reset
        // the (string) values instead of the object iteself, to save on
        // allocating/freeing strings.
        cb::jsonResetValues(json);
        stream->addStats(add_stream_stat, c);
        // Drop anything that wasn't set by addStats().
        cb::jsonRemoveEmptyStrings(json);

        auto key = fmt::format(
                "{}:stream_{}", stream->getName(), stream->getVBucket().get());
        add_stat(key, json.dump(), c);
    }
}

// Explicit instantiation of addStat() used outside ConnHandler and
// derived classes - for example from BackfillManager::addStats().
template void ConnHandler::addStat<std::string>(std::string_view,
                                                const std::string& val,
                                                const AddStatFn& add_stat,
                                                CookieIface& c) const;
