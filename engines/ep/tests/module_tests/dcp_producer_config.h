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
    DcpProducerConfig(std::string_view name,
                      OutOfOrderSnapshots outOfOrderSnapshots,
                      bool enableSyncRep,
                      ChangeStreams changeStreams,
                      IncludeXattrs includeXattrs,
                      IncludeDeleteTime includeDeleteTime);

    /**
     * @return MockDcpProducer which has been configured using the given
     *         constructor parameters
     */
    std::shared_ptr<MockDcpProducer> createDcpProducer(
            EventuallyPersistentEngine& engine, CookieIface* cookieP) const;

    bool useOSOSnapshots() const;

private:
    const std::string& getName() const;
    bool useSyncReplication() const;
    bool useFlatBufferEvents() const;
    bool useChangeStreams() const;
    bool useXattrs() const;
    bool useDeleteTimes() const;

    // runs DcpControl setup
    void configure(MockDcpProducer& producer) const;

    std::string name;
    OutOfOrderSnapshots outOfOrderSnapshots;
    bool enableSyncRep{false};
    ChangeStreams changeStreams;
    IncludeXattrs includeXattrs;
    IncludeDeleteTime includeDeleteTime;
};