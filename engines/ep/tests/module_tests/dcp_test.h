/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "dcp/dcp-types.h"
#include "evp_engine_test.h"
#include "evp_store_single_threaded_test.h"
#include "vbucket_fwd.h"
#include <memcached/engine_error.h>
#include <gsl/gsl>

class Item;
class MockDcpProducer;
class MockActiveStream;
struct dcp_message_producers;

/**
 * Test fixture for unit tests related to DCP.
 */
class DCPTest : public EventuallyPersistentEngineTest {
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

    void destroy_dcp_stream();

    struct StreamRequestResult {
        ENGINE_ERROR_CODE status;
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
            dcp_message_producers& msgProducers,
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

    void runCheckpointProcessor(dcp_message_producers& producers);

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
    VBucketPtr vb0;

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        callbackCount++;
        return ENGINE_SUCCESS;
    }

    // callbackCount needs to be static as its used inside of the static
    // function fakeDcpAddFailoverLog.
    static int callbackCount;
};
