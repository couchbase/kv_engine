/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/dcp-types.h"

#include <memory>
#include <string>
#include <string_view>

class CookieIface;
class EventuallyPersistentEngine;
class MockDcpProducer;

/**
 * Class aims to contain all possible DcpProducer configuration state and
 * provide a factory MockDcpProducer method to obtain a producer using the
 * configuration.
 */
class DcpProducerConfig {
public:
    // Default constructor yields the most "modern" replication configuration
    DcpProducerConfig() = default;

    DcpProducerConfig(std::string_view name,
                      OutOfOrderSnapshots outOfOrderSnapshots,
                      SyncReplication syncReplication,
                      ChangeStreams changeStreams,
                      IncludeXattrs includeXattrs,
                      IncludeDeleteTime includeDeleteTime,
                      FlatBuffersEvents flatBuffersEvents);

    /**
     * @return MockDcpProducer which has been configured using the given
     *         constructor parameters
     */
    std::shared_ptr<MockDcpProducer> createDcpProducer(
            EventuallyPersistentEngine& engine, CookieIface* cookieP) const;

    bool useOSOSnapshots() const;
    bool useFlatBufferEvents() const;
    bool useSyncReplication() const;

private:
    const std::string& getName() const;
    bool useChangeStreams() const;
    bool useXattrs() const;
    bool useDeleteTimes() const;

    // runs DcpControl setup
    void configure(MockDcpProducer& producer) const;

    // Default initialisation yields the most "modern" replication configuration
    std::string name{"test_producer"};
    OutOfOrderSnapshots outOfOrderSnapshots{OutOfOrderSnapshots::No};
    SyncReplication syncReplication{SyncReplication::SyncReplication};
    ChangeStreams changeStreams{ChangeStreams::Yes};
    IncludeXattrs includeXattrs{IncludeXattrs::Yes};
    IncludeDeleteTime includeDeleteTime{IncludeDeleteTime::Yes};
    FlatBuffersEvents flatBuffersEvents{FlatBuffersEvents::Yes};
};