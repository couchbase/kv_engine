/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "dcp/dcp-types.h"
#include "evp_engine_test.h"
#include "evp_store_single_threaded_test.h"
#include "vbucket_fwd.h"
#include <gsl/gsl-lite.hpp>
#include <memcached/engine_error.h>

class Item;
class MockDcpProducer;
class MockActiveStream;
struct DcpMessageProducersIface;

/**
 * Test fixture for unit tests related to DCP.
 */
class DCPTest : virtual public EventuallyPersistentEngineTest {
protected:
    void SetUp() override;

    void TearDown() override;

    // Create a DCP producer; initially with no streams associated.
    void create_dcp_producer(
            int flags = 0,
            IncludeValue includeVal = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes,
            std::vector<std::pair<std::string, std::string>> controls = {});

    // Setup a DCP producer and attach a stream and cursor to it.
    void setup_dcp_stream(
            int flags = 0,
            IncludeValue includeVal = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes,
            std::vector<std::pair<std::string, std::string>> controls = {});

    cb::engine_errc destroy_dcp_stream();

    struct StreamRequestResult {
        cb::engine_errc status;
        uint64_t rollbackSeqno;
    };

    /**
     * Helper function to simplify calling producer->streamRequest() - provides
     * sensible default values for common invocations.
     */
    static StreamRequestResult doStreamRequest(DcpProducer& producer,
                                               uint64_t startSeqno = 0,
                                               uint64_t endSeqno = ~0,
                                               uint64_t snapStart = 0,
                                               uint64_t snapEnd = ~0,
                                               uint64_t vbUUID = 0);

    /**
     * Helper function to simplify the process of preparing DCP items to be
     * fetched from the stream via step().
     * Should be called once all expected items are present in the vbuckets'
     * checkpoint, it will then run the correct background tasks to
     * copy CheckpointManager items to the producers' readyQ.
     */
    static void prepareCheckpointItemsForStep(
            DcpMessageProducersIface& msgProducers,
            MockDcpProducer& producer,
            VBucket& vb);

    /*
     * Creates an item with the key \"key\", containing json data and xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithXattrs();

    /*
     * Creates an item with the key \"key\", containing json data and no xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithoutXattrs();

    /* Add items onto the vbucket and wait for the checkpoint to be removed */
    void addItemsAndRemoveCheckpoint(int numItems);

    void removeCheckpoint(int numItems);

    void runCheckpointProcessor(DcpMessageProducersIface& producers);

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
    VBucketPtr vb0;

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static cb::engine_errc fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        callbackCount++;
        return cb::engine_errc::success;
    }

    // callbackCount needs to be static as its used inside of the static
    // function fakeDcpAddFailoverLog.
    static int callbackCount;
};

class FlowControlTest : public KVBucketTest,
                        public ::testing::WithParamInterface<bool> {
protected:
    void SetUp() override;

    bool flowControlEnabled;
};