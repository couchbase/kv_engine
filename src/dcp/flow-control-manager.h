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

#ifndef SRC_DCP_FLOW_CONTROL_MANAGER_H_
#define SRC_DCP_FLOW_CONTROL_MANAGER_H_ 1

#include "memcached/types.h"
#include "dcp/consumer.h"

/**
 * DcpFlowControlManager is a base class for handling/enforcing flow control
 * policies on the flow control buffer in DCP Consumer. The base class provides
 * apis for handling flow control buffer sizes during connection and
 * disconnection of a consumer connection. Using this base class only
 * implies that no flow control policy is adopted. To add a new flow control
 * policy just derive a new class from this base class.
 *
 * This class and the derived classes are not thread safe. The clients should
 * use external locks if needed. Currently the functions are only invoked while
 * holding "connsLock" during DCP consumer connection creation and deletion
 */
class DcpFlowControlManager {
public:
    DcpFlowControlManager(EventuallyPersistentEngine &engine);

    virtual ~DcpFlowControlManager();

    /* To be called when a new consumer connection is created.
       Returns the size of flow control buffer for the connection */
    virtual size_t newConsumerConn(DcpConsumer *);

    /* To be called when a consumer connection is deleted */
    virtual void handleDisconnect(DcpConsumer *);

    /* Will indicate if flow control is enabled */
    virtual bool isEnabled(void) const;

protected:
    void setBufSizeWithinBounds(DcpConsumer *consumerConn, size_t &bufSize);

    /* Reference to ep engine instance */
    EventuallyPersistentEngine &engine_;

};

/**
 * In this policy all flow control buffer sizes are fixed to a particular value
 * (10 MB)
 */
class DcpFlowControlManagerStatic : public DcpFlowControlManager {
public:
    DcpFlowControlManagerStatic(EventuallyPersistentEngine &engine);

    ~DcpFlowControlManagerStatic();

    size_t newConsumerConn(DcpConsumer *consumerConn);

    bool isEnabled(void) const;
};

/**
 * In this policy flow control buffer sizes are set only once during the
 * connection set up. It is set as a percentage of bucket mem quota and also
 * within max (50MB) and a min value (10 MB). Once aggr flow control buffer
 * memory usage goes beyond a threshold (10% of bucket memory), all subsequent
 * connections get a flow control buffer size of min value (10MB)
 */
class DcpFlowControlManagerDynamic : public DcpFlowControlManager {
public:
    DcpFlowControlManagerDynamic(EventuallyPersistentEngine &engine);

    ~DcpFlowControlManagerDynamic();

    size_t newConsumerConn(DcpConsumer *consumerConn);

    void handleDisconnect(DcpConsumer *consumerConn);

    bool isEnabled(void) const;

private:
    /* Total memory used by all DCP consumer buffers */
    size_t aggrDcpConsumerBufferSize;
};

/**
 * In this policy flow control buffer sizes are always set as percentage (5%) of
 * bucket memory quota across all flow control buffers, but within max (50MB)
 * and a min value (10 MB). Every time a new connection is made or a disconnect
 * happens, flow control buffer size of all other connections is changed to
 * share an aggregate percentage(5%) of bucket memory
 */
class DcpFlowControlManagerAggressive : public DcpFlowControlManager {
public:
    DcpFlowControlManagerAggressive(EventuallyPersistentEngine &engine);

    ~DcpFlowControlManagerAggressive();

    size_t newConsumerConn(DcpConsumer *consumerConn);

    void handleDisconnect(DcpConsumer *consumerConn);

    bool isEnabled(void) const;

private:
    /* Resize all flow control buffers in dcpConsumersMap */
    void resizeBuffers(size_t bufferSize);
    /* All DCP Consumers with flow control buffer */
    std::map<const void*, DcpConsumer*> dcpConsumersMap;
    /* Fraction of memQuota for all dcp consumer connection buffers */
    double dcpConnBufferSizeAggrFrac;
};
#endif  /* SRC_DCP_FLOW_CONTROL_MANAGER_H_ */
