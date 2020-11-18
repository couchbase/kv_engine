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
#pragma once

#include "atomic.h"
#include "memcached/engine.h"

#include <relaxed_atomic.h>

class DcpConsumer;
class EventuallyPersistentEngine;
struct DcpMessageProducersIface;

/**
 * This class handles the consumer side flow control in a DCP connection.
 * It is always associated with a DCP consumer.
 * Flow control buffer size is set when the class obj is initialized.
 * The class obj subsequently handles sending control messages and
 * sending bytes processed acks to the DCP producer
 */
class FlowControl {
public:
    FlowControl(EventuallyPersistentEngine &engine, DcpConsumer* consumer);

    ~FlowControl();

    ENGINE_ERROR_CODE handleFlowCtl(DcpMessageProducersIface& producers);

    void incrFreedBytes(uint32_t bytes);

    uint32_t getFlowControlBufSize();

    void setFlowControlBufSize(uint32_t newSize);

    bool isBufferSufficientlyDrained();

    void addStats(const AddStatFn& add_stat, const void* c) const;

    uint64_t getFreedBytes() const {
        return freedBytes.load();
    }

    bool isEnabled() const {
        return enabled;
    }

private:
    void setBufSizeWithinBounds(size_t &bufSize);

    bool isBufferSufficientlyDrained_UNLOCKED(uint32_t ackable_bytes);

    /* Associated consumer connection handler */
    DcpConsumer* consumerConn;

    /* Reference to ep engine instance */
    EventuallyPersistentEngine &engine_;

    /* Indicates if flow control is enabled for this connection */
    const bool enabled;

    /* Indicates whether control msg regarding flow control has been sent to
       the producer */
    bool pendingControl;

    /* Flow control buffer size */
    cb::RelaxedAtomic<uint32_t> bufferSize;

    /* Lock while updating buffersize and pendingControl */
    std::mutex bufferSizeLock;

    /* To keep track of when last buffer ack was sent */
    rel_time_t lastBufferAck;

    /* Total bytes acked by this connection. This is used to for stats */
    std::atomic<uint64_t> ackedBytes;

    /* Bytes processed from the flow control buffer */
    std::atomic<uint64_t> freedBytes;
};
