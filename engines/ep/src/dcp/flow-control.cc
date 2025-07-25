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
      engine(engine),
      enabled(engine.getConfiguration().isDcpConsumerFlowControlEnabled()),
      lastBufferAck(ep_current_time()),
      ackedBytes(0),
      freedBytes(0),
      ackRatio(engine.getConfiguration().getDcpConsumerFlowControlAckRatio()),
      ackSeconds(
              engine.getConfiguration().getDcpConsumerFlowControlAckSeconds()) {
    if (enabled) {
        // This call is responsible for recomputing the per-consumer buffer size
        // (based on the new number of consumers on this node) for all
        // consumers - this new consumer included.
        engine.getDcpFlowControlManager().newConsumer(consumer);
    }
}

FlowControl::~FlowControl() {
    if (enabled) {
        engine.getDcpFlowControlManager().handleDisconnect(consumerConn);
    }
}

cb::engine_errc FlowControl::handleFlowCtl(
        DcpMessageProducersIface& producers) {
    if (!enabled) {
        return cb::engine_errc::failed;
    }

    {
        auto lockedBuffer = buffer.wlock();
        if (lockedBuffer->isPendingControl()) {
            lockedBuffer->clearPendingControl();
            const auto bufferSize = lockedBuffer->getSize();
            lockedBuffer.unlock();
            consumerConn.addBufferSizeControl(bufferSize);
            // return failed so that the DcpConsumer::step continues and may
            // send this control in this step
            return cb::engine_errc::failed;
        }
    }

    // Send a buffer ack when the buffer is sufficiently drained, or every 5
    // secs if there's any unacked byte.
    const auto ackableBytes = freedBytes.load();
    if (ackableBytes > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
                fmt::format("FlowControl::handleFlowCtl: ackableBytes "
                            "value {} exceeds 4-byte storage",
                            ackableBytes));
    }
    const bool byteThresholdCondition = ackableBytes > getBufferAckThreshold();
    const bool timeThresholdCondition =
            ackableBytes > 0 &&
            (ep_current_time() - lastBufferAck) > ackSeconds.count();
    if (byteThresholdCondition || timeThresholdCondition) {
        lastBufferAck = ep_current_time();
        ackedBytes.fetch_add(ackableBytes);
        freedBytes.fetch_sub(ackableBytes);
        return producers.buffer_acknowledgement(
                consumerConn.incrOpaqueCounter(),
                gsl::narrow_cast<uint32_t>(ackableBytes));
    }

    return cb::engine_errc::failed;
}

void FlowControl::incrFreedBytes(size_t bytes) {
    freedBytes.fetch_add(bytes);
}

size_t FlowControl::getBufferSize() const {
    return buffer.rlock()->getSize();
}

void FlowControl::setBufferSize(size_t newSize) {
    auto lockedBuffer = buffer.wlock();
    if (newSize != lockedBuffer->getSize()) {
        lockedBuffer->setSize(newSize);
    }
}

size_t FlowControl::getBufferAckThreshold() const {
    return static_cast<size_t>(buffer.rlock()->getSize() * ackRatio);
}

bool FlowControl::isBufferSufficientlyDrained() {
    return freedBytes > getBufferAckThreshold();
}

void FlowControl::addStats(const AddStatFn& add_stat, CookieIface& c) const {
    consumerConn.addStat("total_acked_bytes", ackedBytes, add_stat, c);
    consumerConn.addStat(
            "max_buffer_bytes", buffer.rlock()->getSize(), add_stat, c);
    consumerConn.addStat("unacked_bytes", freedBytes, add_stat, c);
}