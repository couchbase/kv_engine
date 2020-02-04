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

DcpFlowControlManagerStatic::DcpFlowControlManagerStatic(
                                        EventuallyPersistentEngine &engine) :
    DcpFlowControlManager(engine)
{
}

DcpFlowControlManagerStatic::~DcpFlowControlManagerStatic() {}

size_t DcpFlowControlManagerStatic::newConsumerConn(DcpConsumer *consumerConn)
{
    return engine_.getConfiguration().getDcpConnBufferSize();
}

bool DcpFlowControlManagerStatic::isEnabled() const
{
    return true;
}

DcpFlowControlManagerDynamic::DcpFlowControlManagerDynamic(
                                        EventuallyPersistentEngine &engine) :
    DcpFlowControlManager(engine), aggrDcpConsumerBufferSize(0)
{
}

DcpFlowControlManagerDynamic::~DcpFlowControlManagerDynamic() {}

size_t DcpFlowControlManagerDynamic::newConsumerConn(DcpConsumer *consumerConn)
{
    if (consumerConn == nullptr) {
        throw std::invalid_argument(
                "DcpFlowControlManagerDynamic::newConsumerConn: resp is NULL");
    }
    Configuration &config = engine_.getConfiguration();
    double dcpConnBufferSizePerc = static_cast<double>
                                        (config.getDcpConnBufferSizePerc())/100;
    size_t bufferSize = dcpConnBufferSizePerc *
                                          engine_.getEpStats().getMaxDataSize();

    /* Make sure that the flow control buffer size is within a max and min
     range */
    setBufSizeWithinBounds(consumerConn, bufferSize);

    /* If aggr memory used for flow control buffers across all consumers
     exceeds the threshold, then we limit it to min size */
    double dcpConnBufferSizeThreshold = static_cast<double>
                            (config.getDcpConnBufferSizeAggrMemThreshold())/100;
    if ((aggrDcpConsumerBufferSize + bufferSize)
        > dcpConnBufferSizeThreshold * engine_.getEpStats().getMaxDataSize())
    {
        /* Setting to default minimum size */
        bufferSize = config.getDcpConnBufferSize();
        EP_LOG_DEBUG(
                "{} Conn flow control buffer is set to"
                "minimum, as aggr memory used for flow control buffers across"
                "all consumers is {} and is above the threshold ({}) * ({})",
                consumerConn->logHeader(),
                aggrDcpConsumerBufferSize.load(std::memory_order_relaxed),
                dcpConnBufferSizeThreshold,
                engine_.getEpStats().getMaxDataSize());
    }
    aggrDcpConsumerBufferSize += bufferSize;
    EP_LOG_DEBUG("{} Conn flow control buffer is {}",
                 consumerConn->logHeader(),
                 bufferSize);
    return bufferSize;
}

void DcpFlowControlManagerDynamic::handleDisconnect(DcpConsumer *consumerConn)
{
    aggrDcpConsumerBufferSize -= consumerConn->getFlowControlBufSize();
}

bool DcpFlowControlManagerDynamic::isEnabled() const
{
    return true;
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
