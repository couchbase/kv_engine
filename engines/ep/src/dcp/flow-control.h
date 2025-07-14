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
#pragma once

#include "memcached/engine.h"

#include <relaxed_atomic.h>
#include <folly/Synchronized.h>
#include <mutex>

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
    FlowControl(EventuallyPersistentEngine& engine, DcpConsumer& consumer);

    ~FlowControl();

    cb::engine_errc handleFlowCtl(DcpMessageProducersIface& producers);

    void incrFreedBytes(size_t bytes);

    size_t getBufferSize() const;

    void setBufferSize(size_t size);

    /**
     * Checks whether we shouldsend a BufferAck given the current state of the
     * FlowControl buffer.
     */
    bool isBufferSufficientlyDrained();

    void addStats(const AddStatFn& add_stat, CookieIface& c) const;

    auto getFreedBytes() const {
        return freedBytes.load();
    }

    bool isEnabled() const {
        return enabled;
    }

private:
    /**
     * Returns the number of bytes above which we send a buffer acknowledgement.
     */
    size_t getBufferAckThreshold() const;

    /// Associated consumer connection handler
    DcpConsumer& consumerConn;

    /// Reference to ep engine instance
    EventuallyPersistentEngine& engine;

    /* Indicates if flow control is enabled for this connection */
    const bool enabled;

    // The buffer size is a dynamic quantity that requires sending a new control
    // message to the Producer for informing it of the new FlowControl setting
    // whenevet the buffer size changes here at Consumer.
    class Buffer {
    public:
        void setSize(size_t s) {
            size = s;
            pendingControl = true;
        }

        size_t getSize() const {
            return size;
        }

        bool isPendingControl() const {
            return pendingControl;
        }

        void clearPendingControl() {
            pendingControl = false;
        }

    private:
        size_t size = 0;
        bool pendingControl = false;
    };
    // Requires synchronization as this caches a dynamic configuration parameter
    folly::Synchronized<Buffer> buffer;

    /* To keep track of when last buffer ack was sent */
    rel_time_t lastBufferAck;

    /* Total bytes acked by this connection. This is used for stats */
    std::atomic<size_t> ackedBytes;

    /* Bytes processed from the flow control buffer */
    std::atomic<size_t> freedBytes;

    // Ratio of freed bytes in the DCP Consumer buffer that triggers a BufferAck
    // message to the Producer
    const double ackRatio;

    // Max seconds after which a Consumer acks all the remaining freed bytes,
    // regardless of whether dcp_consumer_flow_control_ack_ratio has kicked-in
    // or not
    const std::chrono::seconds ackSeconds;
};
