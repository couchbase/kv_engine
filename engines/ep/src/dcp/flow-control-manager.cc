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

#include "flow-control-manager.h"

#include "bucket_logger.h"
#include "dcp/consumer.h"
#include "ep_engine.h"

DcpFlowControlManager::DcpFlowControlManager(EventuallyPersistentEngine& engine)
    : engine_(engine) {
    dcpConnBufferRatio = engine.getConfiguration().getDcpConnBufferRatio();
}

void DcpFlowControlManager::setBufSizeWithinBounds(DcpConsumer* consumerConn,
                                                   size_t& bufSize) {
    Configuration& config = engine_.getConfiguration();
    /* Make sure that the flow control buffer size is within a max and min
     range */
    if (bufSize < config.getDcpConnBufferSize()) {
        bufSize = config.getDcpConnBufferSize();
        EP_LOG_DEBUG(
                "{} Conn flow control buffer is set to "
                "minimum, bucket size: {}",
                consumerConn->logHeader(),
                engine_.getEpStats().getMaxDataSize());
    } else if (bufSize > config.getDcpConnBufferSizeMax()) {
        bufSize = config.getDcpConnBufferSizeMax();
        EP_LOG_DEBUG(
                "{} Conn flow control buffer is set to "
                "maximum, bucket size: {}",
                consumerConn->logHeader(),
                engine_.getEpStats().getMaxDataSize());
    }
}

size_t DcpFlowControlManager::newConsumerConn(DcpConsumer* consumerConn) {
    if (consumerConn == nullptr) {
        throw std::invalid_argument(
                "DcpFlowControlManager::newConsumerConn: resp is NULL");
    }

    auto lockedConsumers = consumers.wlock();

    // Calculate new per conn buf size
    const auto bucketQuota = engine_.getEpStats().getMaxDataSize();
    size_t bufferSize =
            (dcpConnBufferRatio * bucketQuota) / (lockedConsumers->size() + 1);

    // Make sure that the flow control buffer size is within a max and min range
    setBufSizeWithinBounds(consumerConn, bufferSize);
    EP_LOG_DEBUG("{} Conn flow control buffer is {}",
                 consumerConn->logHeader(),
                 bufferSize);

    // Resize buffer of all existing consumers
    for (auto* c : *lockedConsumers) {
        c->setFlowControlBufSize(bufferSize);
    }

    // Add the new consumer to the tracked set
    lockedConsumers->emplace(consumerConn);

    return bufferSize;
}

void DcpFlowControlManager::handleDisconnect(DcpConsumer* conn) {
    auto lockedConsumers = consumers.wlock();

    // Remove consumer
    const auto numRemoved = lockedConsumers->erase(conn);
    Expects(numRemoved == 1);

    if (lockedConsumers->empty()) {
        return;
    }

    const auto bucketQuota = engine_.getEpStats().getMaxDataSize();
    size_t bufferSize =
            (dcpConnBufferRatio * bucketQuota) / lockedConsumers->size();

    /* Make sure that the flow control buffer size is within a max and
     min range */
    setBufSizeWithinBounds(conn, bufferSize);
    EP_LOG_DEBUG(
            "{} Conn flow control buffer is {}", conn->logHeader(), bufferSize);

    // Resize buffer of all existing consumers
    for (auto* c : *lockedConsumers) {
        c->setFlowControlBufSize(bufferSize);
    }
}