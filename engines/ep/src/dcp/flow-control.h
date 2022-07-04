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

    void incrFreedBytes(uint32_t bytes);

    uint32_t getFlowControlBufSize();

    void setFlowControlBufSize(uint32_t newSize);

    bool isBufferSufficientlyDrained();

    void addStats(const AddStatFn& add_stat, const CookieIface* c) const;

    uint64_t getFreedBytes() const {
        return freedBytes.load();
    }

    bool isEnabled() const {
        return enabled;
    }

private:
    /// Associated consumer connection handler
    DcpConsumer& consumerConn;

    /// Reference to ep engine instance
    EventuallyPersistentEngine &engine_;

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

    /* Total bytes acked by this connection. This is used to for stats */
    std::atomic<uint64_t> ackedBytes;

    /* Bytes processed from the flow control buffer */
    std::atomic<uint64_t> freedBytes;
};
