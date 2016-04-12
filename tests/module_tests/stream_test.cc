/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "dcp-producer.h"
#include "dcp-stream.h"
#include "../programs/engine_testapp/mock_server.h"

#include <platform/dirutils.h>

// Simple backport of the GTest-style EXPECT_EQ macro.
#define EXPECT_EQ(a, b, msg) \
    do { \
        if ((a) != (b)) { \
            throw std::runtime_error(msg); \
        } \
    } while(0)

// Mock of the ActiveStream class. Wraps the real ActiveStream, but exposes
// normally protected methods publically for test purposes.
class MockActiveStream : public ActiveStream {
public:
    MockActiveStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                     const std::string &name, uint32_t flags, uint32_t opaque,
                     uint16_t vb, uint64_t st_seqno, uint64_t en_seqno,
                     uint64_t vb_uuid, uint64_t snap_start_seqno,
                     uint64_t snap_end_seqno)
    : ActiveStream(e, p, name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
                   snap_start_seqno, snap_end_seqno) {}

    // Expose underlying protected ActiveStream methods as public
    void public_getOutstandingItems(RCPtr<VBucket> &vb,
                                    std::deque<queued_item> &items) {
        getOutstandingItems(vb, items);
    }

    void public_processItems(std::deque<queued_item>& items) {
        processItems(items);
    }

    bool public_nextCheckpointItem() {
        return nextCheckpointItem();
    }
};

/* Regression test for MB-17766 - ensure that when an ActiveStream is preparing
 * queued items to be sent out via a DCP consumer, that nextCheckpointItem()
 * doesn't incorrectly return false (meaning that there are no more checkpoint
 * items to send).
 */
static void test_mb17766(const std::string& test_dbname) {
    // Setup an engine with a single active vBucket.
    ENGINE_HANDLE* handle;
    EXPECT_EQ(ENGINE_SUCCESS,
              create_instance(1, get_mock_server_api, &handle),
              "Failed to created ep engine instance");

    EventuallyPersistentEngine* engine =
            reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);
    std::string config = "dbname=" + test_dbname;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->initialize(config.c_str()),
              "Failed to initialize engine.");

    const uint16_t vbid = 0;
    engine->setVBucketState(vbid, vbucket_state_active, false);

    // Wait for warmup to complete.
    while (engine->getEpStore()->isWarmingUp()) {
        usleep(10);
    }

    // Set AuxIO threads to zero, so that the producer's
    // ActiveStreamCheckpointProcesserTask doesn't run.
    ExecutorPool::get()->setMaxAuxIO(0);

    // Add an item.
    std::string value("value");
    Item item("key", /*flags*/0, /*exp*/0, value.c_str(), value.size());

    uint64_t cas;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->store(NULL, &item, &cas, OPERATION_SET, vbid),
              "Store failed");

    // Create a DCP producer and register with checkpoint Manager.
    dcp_producer_t producer = new DcpProducer(*engine, /*cookie*/NULL,
                                              "test_mb_17766_producer",
                                              /*notifyOnly*/false);
    stream_t stream = new MockActiveStream(engine, producer,
                                           producer->getName(),
                                           /*flags*/0,
                                            /*opaque*/0, vbid,
                                            /*st_seqno*/0,
                                            /*en_seqno*/~0,
                                            /*vb_uuid*/0xabcd,
                                            /*snap_start_seqno*/0,
                                            /*snap_end_seqno*/~0);

    RCPtr<VBucket> vb0 = engine->getVBucket(0);
    EXPECT_EQ(true, vb0, "Failed to get valid VBucket object for id 0");
    vb0->checkpointManager.registerTAPCursor(producer->getName());

    // Should start with nextCheckpointItem() returning true.
    MockActiveStream* mock_stream = static_cast<MockActiveStream*>(stream.get());
    EXPECT_EQ(true,
              mock_stream->public_nextCheckpointItem(),
              "nextCheckpointItem() should initially be true.");

    std::deque<queued_item> items;

    // Get the set of outstanding items
    mock_stream->public_getOutstandingItems(vb0, items);

    // REGRESSION CHECK: nextCheckpointItem() should still return true
    EXPECT_EQ(true,
              mock_stream->public_nextCheckpointItem(),
              "nextCheckpointItem() after getting outstanding items should be true.");

    // Process the set of items
    mock_stream->public_processItems(items);

    // Should finish with nextCheckpointItem() returning false.
    EXPECT_EQ(false,
              mock_stream->public_nextCheckpointItem(),
              "nextCheckpointItem() after processing items should be false.");

    producer->cancelCheckpointProcessorTask();
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    bool success = true;
    try {
        test_mb17766("stream_test_db");
    } catch (std::exception& e) {
        std::cerr << "FAILED: " << e.what() << std::endl;
        success = false;
    }

    // Cleanup any files we created.
    CouchbaseDirectoryUtilities::rmrf("stream_test_db");

    return success ? 0 : 1;
}
