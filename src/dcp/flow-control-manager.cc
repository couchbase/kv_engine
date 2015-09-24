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

#include "ep_engine.h"
#include "config.h"

#include "flow-control-manager.h"
#include "dcp/consumer.h"

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
        LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is set to "
            "minimum, bucket size: %zu\n", consumerConn->logHeader(),
            engine_.getEpStats().getMaxDataSize());
    } else if (bufSize > config.getDcpConnBufferSizeMax()) {
        bufSize = config.getDcpConnBufferSizeMax();
        LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is set to "
            "maximum, bucket size: %zu\n", consumerConn->logHeader(),
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
        LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is set to"
            "minimum, as aggr memory used for flow control buffers across"
            "all consumers is %zu and is above the threshold (%f) * (%zu)",
            consumerConn->logHeader(),
            aggrDcpConsumerBufferSize,
            dcpConnBufferSizeThreshold,
            engine_.getEpStats().getMaxDataSize());
    }
    aggrDcpConsumerBufferSize += bufferSize;
    LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is %zu",
        consumerConn->logHeader(), bufferSize);
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
    LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is %zu",
        consumerConn->logHeader(), bufferSize);

    /* resize all flow control buffers */
    resizeBuffers(bufferSize);

    /* Add this connection to the list of connections */
    dcpConsumersMap[consumerConn->getCookie()] = consumerConn;

    return bufferSize;
}

void DcpFlowControlManagerAggressive::handleDisconnect(
                                                    DcpConsumer *consumerConn)
{
    size_t bufferSize = 0;
    /* Remove this connection to the list of connections */
    auto iter = dcpConsumersMap.find(consumerConn->getCookie());
    /* Calculate new per conn buf size */
    if (iter != dcpConsumersMap.end()) {
        dcpConsumersMap.erase(iter);
        if (dcpConsumersMap.size()) {
            bufferSize = (dcpConnBufferSizeAggrFrac *
                          engine_.getEpStats().getMaxDataSize()) /
                         (dcpConsumersMap.size());
            /* Make sure that the flow control buffer size is within a max and
             min range */
            setBufSizeWithinBounds(consumerConn, bufferSize);
            LOG(EXTENSION_LOG_INFO, "%s Conn flow control buffer is %zu",
                consumerConn->logHeader(), bufferSize);
        }
    }

    /* Set buffer size of all existing connections to the new buf size */
    if (bufferSize != 0) {
        resizeBuffers(bufferSize);
    }
}

bool DcpFlowControlManagerAggressive::isEnabled() const
{
    return true;
}

void DcpFlowControlManagerAggressive::resizeBuffers(size_t bufferSize)
{
    /* Set buffer size of all existing connections to the new buf size */
    for (auto& iter : dcpConsumersMap) {
        iter.second->setFlowControlBufSize(bufferSize);
    }
}
