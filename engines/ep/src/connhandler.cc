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
#include "ep_engine.h"
#include "ep_time.h"

#include <memcached/cookie_iface.h>
#include <memcached/durability_spec.h>
#include <memcached/server_cookie_iface.h>
#include <phosphor/phosphor.h>
#include <platform/timeutils.h>

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
                         const CookieIface* c,
                         std::string n)
    : engine_(e),
      stats(engine_.getEpStats()),
      name(std::move(n)),
      cookie(const_cast<CookieIface*>(c)),
      reserved(false),
      created(ep_current_time()),
      disconnect(false),
      paused(false),
      authenticatedUser(e.getServerApi()->cookie->get_authenticated_user(*c)),
      connected_port(e.getServerApi()->cookie->get_connected_port(*c)),
      idleTimeout(e.getConfiguration().getDcpIdleTimeout()) {
    logger = BucketLogger::createBucketLogger(
            std::to_string(reinterpret_cast<uintptr_t>(this)));

    auto* cookie_api = e.getServerApi()->cookie;
    cookie_api->setDcpConnHandler(*c, this);
    logger->setConnectionId(c->getConnectionId());
}

ConnHandler::~ConnHandler() {
    // Log runtime / pause information when we destruct.
    using namespace std::chrono;
    const auto details = pausedDetails.copy();
    fmt::memory_buffer buf;
    bool addComma = false;
    size_t totalCount = 0;
    nanoseconds totalDuration{};
    for (uint8_t reason = 0; size_t{reason} < PausedReasonCount; reason++) {
        const auto count = details.reasonCounts[reason];
        if (count) {
            if (addComma) {
                format_to(buf, ",");
            }
            addComma = true;
            const auto duration = details.reasonDurations[reason];
            format_to(buf,
                      R"("{}": {{"count":{}, "duration":"{}"}})",
                      to_string(PausedReason{reason}),
                      count,
                      cb::time2text(duration));
            totalCount += count;
            totalDuration += duration;
        }
    }
    logger->info(
            "Destroying connection. Created {} s ago. Paused {} times, for {} "
            "total. "
            "Details: {{{}}}",
            (ep_current_time() - created),
            totalCount,
            cb::time2text(totalDuration),
            std::string_view{buf.data(), buf.size()});
    logger->unregister();
}

cb::engine_errc ConnHandler::addStream(uint32_t opaque, Vbid, uint32_t flags) {
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
                                      const DocKey& key,
                                      cb::const_byte_buffer value,
                                      size_t priv_bytes,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint32_t flags,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t expiration,
                                      uint32_t lock_time,
                                      cb::const_byte_buffer meta,
                                      uint8_t nru) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the mutation API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::deletion(uint32_t opaque,
                                      const DocKey& key,
                                      cb::const_byte_buffer value,
                                      size_t priv_bytes,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      cb::const_byte_buffer meta) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the deletion API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::deletionV2(uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
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
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
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
        uint32_t flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> max_visible_seqno) {
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
        uint32_t flags,
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
                                                   Vbid vbucket,
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

cb::engine_errc ConnHandler::step(DcpMessageProducersIface&) {
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
                                     const DocKey& key,
                                     cb::const_byte_buffer value,
                                     size_t priv_bytes,
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
                                    const DocKey& key,
                                    uint64_t prepare_seqno,
                                    uint64_t commit_seqno) {
    logger->warn(
            "Disconnecting - This connection doesn't support the dcp commit "
            "API");
    return cb::engine_errc::disconnect;
}

cb::engine_errc ConnHandler::abort(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
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

BucketLogger& ConnHandler::getLogger() {
    return *logger;
}

void ConnHandler::releaseReference()
{
    bool inverse = true;
    if (reserved.compare_exchange_strong(inverse, false)) {
        engine_.releaseCookie(cookie);
    }
}

void ConnHandler::addStats(const AddStatFn& add_stat, const CookieIface* c) {
    using namespace std::chrono;

    addStat("type", getType(), add_stat, c);
    addStat("created", created, add_stat, c);
    addStat("pending_disconnect", disconnect.load(), add_stat, c);
    addStat("supports_ack", supportAck.load(), add_stat, c);
    addStat("reserved", reserved.load(), add_stat, c);
    addStat("paused", isPaused(), add_stat, c);
    const auto details = pausedDetails.copy();
    if (isPaused()) {
        addStat("paused_current_reason",
                to_string(details.reason),
                add_stat,
                c);
        addStat("paused_current_duration",
                cb::time2text(steady_clock::now() - details.lastPaused),
                add_stat,
                c);
    }
    for (size_t reason = 0; reason < PausedReasonCount; reason++) {
        const auto count = details.reasonCounts[reason];
        if (count) {
            std::string key{"paused_previous_" +
                            to_string(PausedReason(reason)) + "_count"};
            addStat(key.c_str(), count, add_stat, c);

            const auto duration = details.reasonDurations[reason];
            key = {"paused_previous_" + to_string(PausedReason(reason)) +
                   "_duration"};
            addStat(key.c_str(), cb::time2text(duration), add_stat, c);
        }
    }
    const auto priority = engine_.getDCPPriority(cookie);
    const char* priString = "<INVALID>";
    switch (priority) {
    case ConnectionPriority::High:
        priString = "high";
        break;
    case ConnectionPriority::Medium:
        priString = "medium";
        break;
    case ConnectionPriority::Low:
        priString = "low";
        break;
    }
    addStat("priority", priString, add_stat, c);
}

void ConnHandler::pause(ConnHandler::PausedReason r) {
    paused.store(true);
    auto now = std::chrono::steady_clock::now();
    pausedDetails.withLock([r, now](auto& details) {
        details.reason = r;
        details.lastPaused = now;
    });
}

void ConnHandler::setLogHeader(const std::string& header) {
    logger->prefix = header;
}

const char* ConnHandler::logHeader() {
    return logger->prefix.c_str();
}

void ConnHandler::unPause() {
    auto wasPaused = paused.exchange(false);
    if (!wasPaused) {
        return;
    }
    using namespace std::chrono;
    auto now = steady_clock::now();
    pausedDetails.withLock([now](auto& details) {
        auto index = static_cast<std::underlying_type_t<PausedReason>>(
                details.reason);
        details.reasonCounts[index]++;
        details.reasonDurations[index] += (now - details.lastPaused);
    });
}

// Explicit instantition of addStat() used outside of ConnHandler and
// derived classes - for example from BackfillManager::addStats().
template void ConnHandler::addStat<std::string>(const char *nm,
                                                const std::string &val,
                                                const AddStatFn &add_stat,
                                                const void *c) const;
