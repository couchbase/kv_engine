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
    dcpConnBufferRatio = engine.getConfiguration().getDcpConnBufferRatio();
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
    const size_t bufferSize = (dcpConnBufferRatio * bucketQuota) / numConsumers;
    for (auto* c : consumers) {
        c->setFlowControlBufSize(bufferSize);
    }

    EP_LOG_DEBUG(
            "DcpFlowControlManager::computeBufferSize: The new FlowControl "
            "buffer size for DCP Consumers is {}",
            bufferSize);
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

void DcpFlowControlManager::setDcpConnBufferRatio(float ratio) {
    if (dcpConnBufferRatio == ratio) {
        return;
    }
    dcpConnBufferRatio = ratio;

    updateConsumersBufferSize(*consumers.wlock());
}

size_t DcpFlowControlManager::getNumConsumers() const {
    return consumers.wlock()->size();
}