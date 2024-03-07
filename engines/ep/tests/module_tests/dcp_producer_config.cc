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

#include "tests/module_tests/dcp_producer_config.h"

#include "tests/mock/mock_dcp_producer.h"

#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>

DcpProducerConfig::DcpProducerConfig(std::string_view name,
                                     OutOfOrderSnapshots outOfOrderSnapshots,
                                     SyncReplication syncReplication,
                                     ChangeStreams changeStreams,
                                     IncludeXattrs includeXattrs,
                                     IncludeDeleteTime includeDeleteTime,
                                     FlatBuffersEvents flatBuffersEvents)

    : name(name),
      outOfOrderSnapshots(outOfOrderSnapshots),
      syncReplication(syncReplication),
      changeStreams(changeStreams),
      includeXattrs(includeXattrs),
      includeDeleteTime(includeDeleteTime),
      flatBuffersEvents(flatBuffersEvents) {
}

const std::string& DcpProducerConfig::getName() const {
    return name;
}

bool DcpProducerConfig::useOSOSnapshots() const {
    return outOfOrderSnapshots != OutOfOrderSnapshots::No;
}

bool DcpProducerConfig::useSyncReplication() const {
    return syncReplication != SyncReplication::No;
}

bool DcpProducerConfig::useFlatBufferEvents() const {
    return flatBuffersEvents == FlatBuffersEvents::Yes;
}

bool DcpProducerConfig::useChangeStreams() const {
    return changeStreams == ChangeStreams::Yes;
}

bool DcpProducerConfig::useXattrs() const {
    return includeXattrs == IncludeXattrs::Yes;
}

bool DcpProducerConfig::useDeleteTimes() const {
    return includeDeleteTime == IncludeDeleteTime::Yes;
}

std::shared_ptr<MockDcpProducer> DcpProducerConfig::createDcpProducer(
        EventuallyPersistentEngine& engine, CookieIface* cookie) const {
    using cb::mcbp::DcpOpenFlag;
    auto flags{DcpOpenFlag::None};

    if (useXattrs()) {
        flags |= DcpOpenFlag::IncludeXattrs;
    }

    if (useDeleteTimes()) {
        flags |= DcpOpenFlag::IncludeDeleteTimes;
    }

    auto newProducer = std::make_shared<MockDcpProducer>(
            engine, cookie, getName(), flags, false /*startTask*/);

    // Create the task object, but don't schedule
    newProducer->createCheckpointProcessorTask();

    // Need to enable NOOP for XATTRS (and collections), but don't actually
    // care about receiving DcpNoop messages.
    if (useXattrs()) {
        newProducer->setNoopEnabled(
                MockDcpProducer::NoopMode::EnabledButNeverSent);
    }

    configure(*newProducer);

    return newProducer;
}

void DcpProducerConfig::configure(MockDcpProducer& producer) const {
    if (useOSOSnapshots()) {
        producer.setOutOfOrderSnapshots(outOfOrderSnapshots);
    }

    if (useSyncReplication()) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer.control(1, "enable_sync_writes", "true"));
        EXPECT_EQ(cb::engine_errc::success,
                  producer.control(1, "consumer_name", "mock_replication"));
    }

    if (useFlatBufferEvents()) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer.control(
                          1, DcpControlKeys::FlatBuffersSystemEvents, "true"));
    }
    if (useChangeStreams()) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer.control(1, DcpControlKeys::ChangeStreams, "true"));
    }
}
