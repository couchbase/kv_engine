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

#include "memcached/types.h"

#include <atomic>
#include <map>
#include <mutex>

class CookieIface;
class DcpConsumer;
class EventuallyPersistentEngine;

/**
 * DcpFlowControlManager handles the flow control behaviour for DCP Consumers in
 * the engine. The class provides apis for handling flow control buffer sizes
 * during connection and disconnection of a consumer connection.
 * Flow control buffer sizes are always set as percentage (5%) of bucket memory
 * quota across all flow control buffers, but within max (50MB) and a min value
 * (10 MB). Every time a new connection is made or a disconnect happens, flow
 * control buffer size of all other connections is changed to share an aggregate
 * percentage(5%) of bucket memory
 */
class DcpFlowControlManager {
public:
    explicit DcpFlowControlManager(EventuallyPersistentEngine& engine);

    size_t newConsumerConn(DcpConsumer* consumerConn);

    void handleDisconnect(DcpConsumer* consumerConn);

    bool isEnabled() const;

protected:
    void setBufSizeWithinBounds(DcpConsumer* consumerConn, size_t& bufSize);

    EventuallyPersistentEngine& engine_;

private:
    /**
     * Resize all flow control buffers in dcpConsumersMap.
     * It assumes the dcpConsumersMapMutex is already taken.
     */
    void resizeBuffers_UNLOCKED(size_t bufferSize);

    /* Mutex to ensure dcpConsumersMap is thread safe */
    std::mutex dcpConsumersMapMutex;

    /* All DCP Consumers with flow control buffer */
    std::map<const CookieIface*, DcpConsumer*> dcpConsumersMap;

    /* Fraction of memQuota for all dcp consumer connection buffers */
    std::atomic<double> dcpConnBufferSizeAggrFrac;
};
