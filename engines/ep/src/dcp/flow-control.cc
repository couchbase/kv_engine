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

#include "dcp/flow-control.h"
#include "connhandler_impl.h"
#include "dcp/consumer.h"
#include "dcp/flow-control-manager.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "objectregistry.h"

FlowControl::FlowControl(EventuallyPersistentEngine& engine,
                         DcpConsumer& consumer)
    : consumerConn(consumer),
      engine_(engine),
      enabled(engine.getDcpFlowControlManager().isEnabled()),
      pendingControl(true),
      lastBufferAck(ep_current_time()),
      ackedBytes(0),
      freedBytes(0) {
    if (enabled) {
        bufferSize =
                engine.getDcpFlowControlManager().newConsumerConn(&consumer);
    }
}

FlowControl::~FlowControl() {
    engine_.getDcpFlowControlManager().handleDisconnect(&consumerConn);
}

cb::engine_errc FlowControl::handleFlowCtl(
        DcpMessageProducersIface& producers) {
    if (enabled) {
        cb::engine_errc ret;
        uint32_t ackable_bytes = freedBytes.load();
        std::unique_lock<std::mutex> lh(bufferSizeLock);
        if (pendingControl) {
            pendingControl = false;
            std::string buf_size(std::to_string(bufferSize));
            lh.unlock();
            uint64_t opaque = consumerConn.incrOpaqueCounter();
            const std::string& controlMsgKey = consumerConn.getControlMsgKey();
            NonBucketAllocationGuard guard;
            ret = producers.control(opaque, controlMsgKey, buf_size);
            return ret;
        } else if (isBufferSufficientlyDrained_UNLOCKED(ackable_bytes)) {
            lh.unlock();
            /* Send a buffer ack when at least 20% of the buffer is drained */
            uint64_t opaque = consumerConn.incrOpaqueCounter();
            ret = producers.buffer_acknowledgement(
                    opaque, Vbid(0), ackable_bytes);
            lastBufferAck = ep_current_time();
            ackedBytes.fetch_add(ackable_bytes);
            freedBytes.fetch_sub(ackable_bytes);
            return ret;
        } else if (ackable_bytes > 0 &&
                   (ep_current_time() - lastBufferAck) > 5) {
            lh.unlock();
            /* Ack at least every 5 seconds */
            uint64_t opaque = consumerConn.incrOpaqueCounter();
            ret = producers.buffer_acknowledgement(
                    opaque, Vbid(0), ackable_bytes);
            lastBufferAck = ep_current_time();
            ackedBytes.fetch_add(ackable_bytes);
            freedBytes.fetch_sub(ackable_bytes);
            return ret;
        } else {
            lh.unlock();
        }
    }
    return cb::engine_errc::failed;
}

void FlowControl::incrFreedBytes(uint32_t bytes)
{
    freedBytes.fetch_add(bytes);
}

uint32_t FlowControl::getFlowControlBufSize()
{
    return bufferSize;
}

void FlowControl::setFlowControlBufSize(uint32_t newSize)
{
    std::lock_guard<std::mutex> lh(bufferSizeLock);
    if (newSize != bufferSize) {
        bufferSize = newSize;
        pendingControl = true;
    }
}

bool FlowControl::isBufferSufficientlyDrained() {
    std::lock_guard<std::mutex> lh(bufferSizeLock);
    return isBufferSufficientlyDrained_UNLOCKED(freedBytes.load());
}

bool FlowControl::isBufferSufficientlyDrained_UNLOCKED(uint32_t ackable_bytes) {
    return ackable_bytes > (bufferSize * .2);
}

void FlowControl::addStats(const AddStatFn& add_stat, const void* c) const {
    consumerConn.addStat("total_acked_bytes", ackedBytes, add_stat, c);
    consumerConn.addStat("max_buffer_bytes", bufferSize, add_stat, c);
    consumerConn.addStat("unacked_bytes", freedBytes, add_stat, c);
}