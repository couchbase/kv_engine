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
 * DcpFlowControlManager is a base class for handling/enforcing flow control
 * policies on the flow control buffer in DCP Consumer. The base class provides
 * apis for handling flow control buffer sizes during connection and
 * disconnection of a consumer connection. Using this base class only
 * implies that no flow control policy is adopted. To add a new flow control
 * policy just derive a new class from this base class.
 *
 * This class and the derived classes are thread safe.
 */
class DcpFlowControlManager {
public:
    explicit DcpFlowControlManager(EventuallyPersistentEngine& engine);

    virtual ~DcpFlowControlManager();

    /* To be called when a new consumer connection is created.
       Returns the size of flow control buffer for the connection */
    virtual size_t newConsumerConn(DcpConsumer *);

    /* To be called when a consumer connection is deleted */
    virtual void handleDisconnect(DcpConsumer *);

    /* Will indicate if flow control is enabled */
    virtual bool isEnabled() const;

protected:
    void setBufSizeWithinBounds(DcpConsumer *consumerConn, size_t &bufSize);

    /* Reference to ep engine instance */
    EventuallyPersistentEngine &engine_;

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
    explicit DcpFlowControlManagerAggressive(
            EventuallyPersistentEngine& engine);

    ~DcpFlowControlManagerAggressive() override;

    size_t newConsumerConn(DcpConsumer* consumerConn) override;

    void handleDisconnect(DcpConsumer* consumerConn) override;

    bool isEnabled() const override;

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
