/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "connhandler.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_time.h"

#include <memcached/server_cookie_iface.h>
#include <phosphor/phosphor.h>

std::string to_string(ConnHandler::PausedReason r) {
    switch (r) {
    case ConnHandler::PausedReason::BufferLogFull:
        return "PausedReason::BufferLogFull";
    case ConnHandler::PausedReason::Initializing:
        return "PausedReason::Initializing";
    case ConnHandler::PausedReason::OutOfMemory:
        return "PausedReason::OutOfMemory";
    case ConnHandler::PausedReason::ReadyListEmpty:
        return "PausedReason::ReadyListEmpty";
    case ConnHandler::PausedReason::Unknown:
        return "PausedReason::Unknown";
    }
    return "PausedReason::Invalid";
}

ConnHandler::ConnHandler(EventuallyPersistentEngine& e,
                         const void* c,
                         const std::string& n)
    : engine_(e),
      stats(engine_.getEpStats()),
      name(n),
      cookie(const_cast<void*>(c)),
      reserved(false),
      created(ep_current_time()),
      disconnect(false),
      paused(false) {
    logger = BucketLogger::createBucketLogger(
            std::to_string(reinterpret_cast<uintptr_t>(this)));

    auto connId = e.getServerApi()->cookie->get_log_info(c).first;
    logger->setConnectionId(connId);
}

ENGINE_ERROR_CODE ConnHandler::addStream(uint32_t opaque,
                                         Vbid,
                                         uint32_t flags) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp add stream API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::closeStream(uint32_t opaque,
                                           Vbid vbucket,
                                           DcpStreamId sid) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp close stream API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamEnd(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp stream end API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::mutation(uint32_t opaque,
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
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::deletion(uint32_t opaque,
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
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::deletionV2(uint32_t opaque,
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
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::expiration(uint32_t opaque,
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
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::snapshotMarker(uint32_t opaque,
                                              Vbid vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint32_t flags) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp snapshot marker API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::setVBucketState(uint32_t opaque,
                                               Vbid vbucket,
                                               vbucket_state_t state) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the set vbucket state API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamRequest(
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
        boost::optional<cb::const_char_buffer> json) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp stream request API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::noop(uint32_t opaque) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the noop API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::bufferAcknowledgement(uint32_t opaque,
                                                     Vbid vbucket,
                                                     uint32_t buffer_bytes) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the buffer acknowledgement API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::control(uint32_t opaque,
                                       cb::const_char_buffer key,
                                       cb::const_char_buffer value) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the control API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::step(struct dcp_message_producers* producers) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp step API");
    return ENGINE_DISCONNECT;
}

bool ConnHandler::handleResponse(const protocol_binary_response_header* resp) {
    logger->warn(
            "Disconnecting - This connection doesn't "
            "support the dcp response handler API");
    return false;
}

ENGINE_ERROR_CODE ConnHandler::systemEvent(uint32_t opaque,
                                           Vbid vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           mcbp::systemevent::version version,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData) {
    logger->warn(
            "Disconnecting - This connections doesn't "
            "support the dcp system_event API");
    return ENGINE_DISCONNECT;
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

void ConnHandler::addStats(ADD_STAT add_stat, const void* c) {
    addStat("type", getType(), add_stat, c);
    addStat("created", created.load(), add_stat, c);
    addStat("pending_disconnect", disconnect.load(), add_stat, c);
    addStat("supports_ack", supportAck.load(), add_stat, c);
    addStat("reserved", reserved.load(), add_stat, c);
    addStat("paused", isPaused(), add_stat, c);
    if (isPaused()) {
        addStat("paused_reason", to_string(reason), add_stat, c);
    }
    const auto priority = engine_.getDCPPriority(cookie);
    const char* priString = "<INVALID>";
    switch (priority) {
    case CONN_PRIORITY_HIGH:
        priString = "high";
        break;
    case CONN_PRIORITY_MED:
        priString = "medium";
        break;
    case CONN_PRIORITY_LOW:
        priString = "low";
        break;
    }
    addStat("priority", priString, add_stat, c);
}

void ConnHandler::setLogHeader(const std::string& header) {
    logger->prefix = header;
}

const char* ConnHandler::logHeader() {
    return logger->prefix.c_str();
}
