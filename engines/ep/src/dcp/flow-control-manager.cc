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

size_t DcpFlowControlManager::computeBufferSize(size_t numConsumers) const {
    Expects(numConsumers > 0);

    const auto bucketQuota = engine_.getEpStats().getMaxDataSize();
    size_t bufferSize = (dcpConnBufferRatio * bucketQuota) / numConsumers;

    // Make sure that the flow control buffer size is within a max and min range
    const auto& config = engine_.getConfiguration();
    if (bufferSize < config.getDcpConnBufferSize()) {
        bufferSize = config.getDcpConnBufferSize();
    } else if (bufferSize > config.getDcpConnBufferSizeMax()) {
        bufferSize = config.getDcpConnBufferSizeMax();
    }

    EP_LOG_DEBUG(
            "DcpFlowControlManager::computeBufferSize: The new FlowControl "
            "buffer size for DCP Consumers is {}",
            bufferSize);

    return bufferSize;
}

void DcpFlowControlManager::newConsumer(DcpConsumer* consumer) {
    if (consumer == nullptr) {
        throw std::invalid_argument(
                "DcpFlowControlManager::newConsumer: resp is NULL");
    }

    // Add the new consumer to the tracked set
    auto lockedConsumers = consumers.wlock();
    lockedConsumers->emplace(consumer);

    // Compute new per conn buf size and resize buffer of all existing consumers
    const auto bufferSize = computeBufferSize(lockedConsumers->size());
    for (auto* c : *lockedConsumers) {
        c->setFlowControlBufSize(bufferSize);
    }
}

void DcpFlowControlManager::handleDisconnect(DcpConsumer* consumer) {
    // Remove consumer
    auto lockedConsumers = consumers.wlock();
    const auto numRemoved = lockedConsumers->erase(consumer);
    Expects(numRemoved == 1);

    if (lockedConsumers->empty()) {
        return;
    }

    // Compute new per conn buf size and resize buffer of all existing consumers
    const auto bufferSize = computeBufferSize(lockedConsumers->size());
    for (auto* c : *lockedConsumers) {
        c->setFlowControlBufSize(bufferSize);
    }
}