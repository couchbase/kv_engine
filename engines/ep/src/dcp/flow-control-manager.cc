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

DcpFlowControlManager::DcpFlowControlManager(EventuallyPersistentEngine &engine)
    : engine_(engine)
{
}

DcpFlowControlManager::~DcpFlowControlManager() {}

size_t DcpFlowControlManager::newConsumerConn(DcpConsumer *) {
    return 0;
}

void DcpFlowControlManager::handleDisconnect(DcpConsumer *) {}

bool DcpFlowControlManager::isEnabled() const
{
    return false;
}

void DcpFlowControlManager::setBufSizeWithinBounds(DcpConsumer *consumerConn,
                                                   size_t &bufSize)
{
    Configuration &config = engine_.getConfiguration();
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

DcpFlowControlManagerAggressive::DcpFlowControlManagerAggressive(
                                        EventuallyPersistentEngine &engine) :
    DcpFlowControlManager(engine)
{
    dcpConnBufferSizeAggrFrac = static_cast<double>
    (engine.getConfiguration().getDcpConnBufferSizeAggressivePerc())/100;
}

DcpFlowControlManagerAggressive::~DcpFlowControlManagerAggressive() {}

size_t DcpFlowControlManagerAggressive::newConsumerConn(
                                                    DcpConsumer *consumerConn)
{
    std::lock_guard<std::mutex> lh(dcpConsumersMapMutex);

    if (consumerConn == nullptr) {
        throw std::invalid_argument(
                "DcpFlowControlManagerAggressive::newConsumerConn: resp is NULL");
    }
    /* Calculate new per conn buf size */
    uint32_t totalConns = dcpConsumersMap.size();

    size_t bufferSize = (dcpConnBufferSizeAggrFrac *
                        engine_.getEpStats().getMaxDataSize()) /
                        (totalConns + 1);

    /* Make sure that the flow control buffer size is within a max and min
     range */
    setBufSizeWithinBounds(consumerConn, bufferSize);
    EP_LOG_DEBUG("{} Conn flow control buffer is {}",
                 consumerConn->logHeader(),
                 bufferSize);

    /* resize all flow control buffers */
    resizeBuffers_UNLOCKED(bufferSize);

    /* Add this connection to the list of connections */
    dcpConsumersMap[consumerConn->getCookie()] = consumerConn;

    return bufferSize;
}

void DcpFlowControlManagerAggressive::handleDisconnect(
                                                    DcpConsumer *consumerConn)
{
    std::lock_guard<std::mutex> lh(dcpConsumersMapMutex);

    size_t bufferSize = 0;
    /* Remove this connection to the list of connections */
    auto iter = dcpConsumersMap.find(consumerConn->getCookie());
    /* Calculate new per conn buf size */
    if (iter != dcpConsumersMap.end()) {
        dcpConsumersMap.erase(iter);
        if (!dcpConsumersMap.empty()) {
            bufferSize = (dcpConnBufferSizeAggrFrac *
                          engine_.getEpStats().getMaxDataSize()) /
                         (dcpConsumersMap.size());
            /* Make sure that the flow control buffer size is within a max and
             min range */
            setBufSizeWithinBounds(consumerConn, bufferSize);
            EP_LOG_DEBUG("{} Conn flow control buffer is {}",
                         consumerConn->logHeader(),
                         bufferSize);
        }
    }

    /* Set buffer size of all existing connections to the new buf size */
    if (bufferSize != 0) {
        resizeBuffers_UNLOCKED(bufferSize);
    }
}

bool DcpFlowControlManagerAggressive::isEnabled() const
{
    return true;
}

void DcpFlowControlManagerAggressive::resizeBuffers_UNLOCKED(size_t bufferSize)
{
    /* Set buffer size of all existing connections to the new buf size */
    for (auto& iter : dcpConsumersMap) {
        iter.second->setFlowControlBufSize(bufferSize);
    }
}
