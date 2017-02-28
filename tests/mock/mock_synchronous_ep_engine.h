/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <ep_bucket.h>
#include <ep_engine.h>

class MockEPStore;

/* A class which subclasses the real EPEngine. Its main purpose is to allow
 * us to construct and setup an EPStore without starting all the various
 * background tasks which are normally started by EPEngine as part of creating
 * EPStore (in the initialize() method).
 *
 * The net result is a (mostly) synchronous environment - while the
 * ExecutorPool's threads exist, none of the normally-created background Tasks
 * should be running. Note however that /if/ any new tasks are created, they
 * will be scheduled on the ExecutorPools' threads asynchronously.
 */
class SynchronousEPEngine : public EventuallyPersistentEngine {
public:
    SynchronousEPEngine(const std::string& extra_config);

    void setEPStore(KVBucket* store);

    /* Allow us to call normally protected methods */

    ENGINE_ERROR_CODE public_doTapVbTakeoverStats(const void* cookie,
                                                  ADD_STAT add_stat,
                                                  std::string& key,
                                                  uint16_t vbid) {
        return doTapVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    ENGINE_ERROR_CODE public_doDcpVbTakeoverStats(const void* cookie,
                                                  ADD_STAT add_stat,
                                                  std::string& key,
                                                  uint16_t vbid) {
        return doDcpVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    void public_initializeEngineCallbacks() {
        return initializeEngineCallbacks();
    }

    /*
     * Initialize the connmap objects, which creates tasks
     * so must be done after executorpool is created
     */
    void initializeConnmaps();
};

/* Subclass of EPStore to expose normally non-public members for test
 * purposes.
 */
class MockEPStore : public EPBucket {
public:
    MockEPStore(EventuallyPersistentEngine& theEngine);

    virtual ~MockEPStore() {
    }

    VBucketMap& getVbMap();

    void public_stopWarmup() {
        stopWarmup();
    }

    GetValue public_getInternal(const StoredDocKey& key,
                                uint16_t vbucket,
                                const void* cookie,
                                vbucket_state_t allowedState,
                                get_options_t options) {
        return getInternal(key, vbucket, cookie, allowedState, options);
    }
};
