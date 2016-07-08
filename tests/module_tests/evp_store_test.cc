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

/*
 * Unit tests for the EventuallyPersistentStore class.
 *
 * Note that these test do *not* have the normal Tasks running (BGFetcher,
 * flusher etc) as we do not initialise EPEngine. This means that such tasks
 * need to be manually run. This can be very helpful as it essentially gives us
 * synchronous control of EPStore.
 */

#include "evp_store_test.h"

#include "bgfetcher.h"
#include "checkpoint.h"
#include "checkpoint_remover.h"
#include "connmap.h"
#include "ep_engine.h"
#include "flusher.h"
#include "tapthrottle.h"
#include "../mock/mock_dcp_producer.h"

#include "programs/engine_testapp/mock_server.h"
#include <platform/dirutils.h>

SynchronousEPEngine::SynchronousEPEngine(const std::string& extra_config)
    : EventuallyPersistentEngine(get_mock_server_api) {
    maxFailoverEntries = 1;

    EventuallyPersistentEngine::loggerApi = get_mock_server_api()->log;

    // Merge any extra config into the main configuration.
    if (extra_config.size() > 0) {
        if (!configuration.parseConfiguration(extra_config.c_str(),
                                              serverApi)) {
            throw std::invalid_argument("Unable to parse config string: " +
                                        extra_config);
        }
    }

    // workload is needed by EPStore's constructor (to construct the
    // VBucketMap).
    workload = new WorkLoadPolicy(/*workers*/1, /*shards*/1);

    // dcpConnMap_ is needed by EPStore's constructor.
    dcpConnMap_ = new DcpConnMap(*this);

    // tapConnMap is needed by queueDirty.
    tapConnMap = new TapConnMap(*this);

    // checkpointConfig is needed by CheckpointManager (via EPStore).
    checkpointConfig = new CheckpointConfig(*this);

    // tapConfig is needed by doTapStats().
    tapConfig = new TapConfig(*this);

    // tapThrottle is needed by doEngineStats().
    tapThrottle = new TapThrottle(configuration, stats);
}

void SynchronousEPEngine::setEPStore(EventuallyPersistentStore* store) {
    cb_assert(epstore == NULL);
    epstore = store;
}

MockEPStore::MockEPStore(EventuallyPersistentEngine &theEngine)
    : EventuallyPersistentStore(theEngine) {
    // Perform a limited set of setup (normally done by EPStore::initialize) -
    // enough such that objects which are assumed to exist are present.

    // Create the closed checkpoint removed task. Note we do _not_ schedule
    // it, unlike EPStore::initialize
    chkTask = new ClosedUnrefCheckpointRemoverTask
            (&engine, stats, theEngine.getConfiguration().getChkRemoverStime());
}

VBucketMap& MockEPStore::getVbMap() {
    return vbMap;
}

/* Mock Task class. Doesn't actually run() or snooze() - they both do nothing.
 */
class MockGlobalTask : public GlobalTask {
public:
    MockGlobalTask(EventuallyPersistentEngine* e, TaskId t)
        : GlobalTask(e, t) {}

    bool run() { return false; }
    std::string getDescription() { return "MockGlobalTask"; }

    void snooze(const double secs) {}
};

void EventuallyPersistentStoreTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    CouchbaseDirectoryUtilities::rmrf(test_dbname);

    // Add dbname to config string.
    std::string config = config_string;
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);

    vbid = 0;

    engine = new SynchronousEPEngine(config);
    ObjectRegistry::onSwitchThread(engine);

    store = new MockEPStore(*engine);
    engine->setEPStore(store);

    // Ensure that EPEngine is hold about necessary server callbacks
    // (client disconnect, bucket delete).
    engine->public_initializeEngineCallbacks();

    // Need to initialize ep_real_time and friends.
    initialize_time_functions(get_mock_server_api()->core);

    cookie = create_mock_cookie();
}

void EventuallyPersistentStoreTest::TearDown() {
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    if (engine) {
        engine->getDcpConnMap().manageConnections();
    }

    // Need to have the current engine valid before deleting (this is what
    // EvpDestroy does normally; however we have a smart ptr to the engine
    // so must delete via that).
    ObjectRegistry::onSwitchThread(engine);
    delete engine;

    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EventuallyPersistentStore object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
}

void EventuallyPersistentStoreTest::store_item(uint16_t vbid,
                                               const std::string& key,
                                               const std::string& value) {
    Item item(key.c_str(), key.size(), /*flags*/0, /*exp*/0, value.c_str(),
              value.size());
    item.setVBucketId(vbid);
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, NULL));
}


//
// EPStoreEvictionTest disabled in 3.0.x backport - there's an unknown
// bug where onSwitchThread() ends up NULL, meaning that we eventually hit
// an assert and crash.
//


const char EventuallyPersistentStoreTest::test_dbname[] = "ep_engine_ep_unit_tests_db";
