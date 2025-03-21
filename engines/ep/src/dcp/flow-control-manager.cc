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

DcpFlowControlManager::DcpFlowControlManager(
        const EventuallyPersistentEngine& engine)
    : engine(engine) {
    dcpConsumerBufferRatio =
            engine.getConfiguration().getDcpConsumerBufferRatio();
}

void DcpFlowControlManager::updateConsumersBufferSize(
        ConsumerContainer& consumers) {
    const auto numConsumers = consumers.size();
    if (numConsumers == 0) {
        return;
    }

    // Compute new per-consumer buffer size and resize buffer of all existing
    // consumers
    const auto bucketQuota = engine.getEpStats().getMaxDataSize();
    const size_t bufferSize =
            (dcpConsumerBufferRatio * bucketQuota) / numConsumers;
    for (auto* c : consumers) {
        c->setFlowControlBufSize(bufferSize);
    }

    EP_LOG_DEBUG_CTX(
            "DcpFlowControlManager::computeBufferSize: new FlowControl "
            "buffer size for DCP Consumers",
            {"buffer_size", bufferSize});
}

void DcpFlowControlManager::newConsumer(DcpConsumer& consumer) {
    // Add the new consumer to the tracked set
    auto lockedConsumers = consumers.wlock();
    lockedConsumers->emplace(&consumer);

    updateConsumersBufferSize(*lockedConsumers);
}

void DcpFlowControlManager::handleDisconnect(DcpConsumer& consumer) {
    // Remove consumer
    auto lockedConsumers = consumers.wlock();
    const auto numRemoved = lockedConsumers->erase(&consumer);
    Expects(numRemoved == 1);

    updateConsumersBufferSize(*lockedConsumers);
}

void DcpFlowControlManager::setDcpConsumerBufferRatio(float ratio) {
    dcpConsumerBufferRatio = ratio;
    updateConsumersBufferSize(*consumers.wlock());
}

float DcpFlowControlManager::getDcpConsumerBufferRatio() const {
    return dcpConsumerBufferRatio;
}

size_t DcpFlowControlManager::getNumConsumers() const {
    return consumers.wlock()->size();
}