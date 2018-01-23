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

#include <phosphor/phosphor.h>

#include "connhandler.h"
#include "ep_engine.h"
#include "ep_time.h"

ConnHandler::ConnHandler(EventuallyPersistentEngine& e, const void* c,
                         const std::string& n) :
    engine_(e),
    stats(engine_.getEpStats()),
    name(n),
    cookie(const_cast<void*>(c)),
    reserved(false),
    created(ep_current_time()),
    disconnect(false),
    paused(false) {}

ENGINE_ERROR_CODE ConnHandler::addStream(uint32_t opaque, uint16_t,
                                         uint32_t flags) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp add stream API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::closeStream(uint32_t opaque, uint16_t vbucket) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp close stream API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp stream end API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::mutation(uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint32_t flags,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t expiration,
                                        uint32_t lock_time,
                                        cb::const_byte_buffer meta,
                                        uint8_t nru) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the mutation API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::deletion(uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        cb::const_byte_buffer meta) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the deletion API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::deletionV2(uint32_t opaque,
                                          const DocKey& key,
                                          cb::const_byte_buffer value,
                                          size_t priv_bytes,
                                          uint8_t datatype,
                                          uint64_t cas,
                                          uint16_t vbucket,
                                          uint64_t by_seqno,
                                          uint64_t rev_seqno,
                                          uint32_t delete_time) {
    logger.log(EXTENSION_LOG_WARNING,
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
                                          uint16_t vbucket,
                                          uint64_t by_seqno,
                                          uint64_t rev_seqno,
                                          cb::const_byte_buffer meta) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the expiration API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint32_t flags)
{
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp snapshot marker API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::flushall(uint32_t opaque, uint16_t vbucket) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the flush API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the set vbucket state API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t snapStartSeqno,
                                             uint64_t snapEndSeqno,
                                             uint64_t *rollback_seqno,
                                             dcp_add_failover_log callback) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp stream request API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              dcp_add_failover_log callback) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp get failover log API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::noop(uint32_t opaque) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the noop API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::bufferAcknowledgement(uint32_t opaque,
                                                     uint16_t vbucket,
                                                     uint32_t buffer_bytes) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the buffer acknowledgement API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::control(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the control API");
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE ConnHandler::step(struct dcp_message_producers* producers) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp step API");
    return ENGINE_DISCONNECT;
}

bool ConnHandler::handleResponse(const protocol_binary_response_header* resp) {
    logger.log(EXTENSION_LOG_WARNING, "Disconnecting - This connection doesn't "
        "support the dcp response handler API");
    return false;
}

ENGINE_ERROR_CODE ConnHandler::systemEvent(uint32_t opaque,
                                           uint16_t vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData) {
    logger.log(EXTENSION_LOG_WARNING,
               "Disconnecting - This connection doesn't "
               "support the dcp system_event API");
    return ENGINE_DISCONNECT;
}

const Logger& ConnHandler::getLogger() const {
    return logger;
}

void ConnHandler::releaseReference()
{
    bool inverse = true;
    if (reserved.compare_exchange_strong(inverse, false)) {
        engine_.releaseCookie(cookie);
    }
}
