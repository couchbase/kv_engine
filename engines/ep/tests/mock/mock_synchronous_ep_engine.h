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

#include <dcp/dcpconnmap.h>
#include <ep_engine.h>

class SynchronousEPEngine;

struct SynchronousEPEngineDeleter {
    void operator()(SynchronousEPEngine*);
};

using SynchronousEPEngineUniquePtr =
        std::unique_ptr<SynchronousEPEngine, SynchronousEPEngineDeleter>;

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
    explicit SynchronousEPEngine(const cb::ArenaMallocClient& client,
                                 std::string extra_config = {});

    void setKVBucket(std::unique_ptr<KVBucket> store);
    void setDcpConnMap(std::unique_ptr<DcpConnMap> dcpConnMap);

    /// Constructs a SynchronousEPEngine instance, along with the necessary
    /// sub-components.
    static SynchronousEPEngineUniquePtr build(const std::string& config);

    /* Allow us to call normally protected methods */

    ENGINE_ERROR_CODE public_doDcpVbTakeoverStats(const void* cookie,
                                                  const AddStatFn& add_stat,
                                                  std::string& key,
                                                  Vbid vbid) {
        return doDcpVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    /*
     * Initialize the connmap object, which creates tasks
     * so must be done after executorpool is created
     */
    void initializeConnmap();

    std::unique_ptr<KVBucket> public_makeBucket(Configuration& config);

    std::unique_ptr<KVBucket> public_makeMockBucket(Configuration& config);

    ENGINE_ERROR_CODE public_setWithMeta(Vbid vbucket,
                                         DocKey key,
                                         cb::const_byte_buffer value,
                                         ItemMetaData itemMeta,
                                         bool isDeleted,
                                         protocol_binary_datatype_t datatype,
                                         uint64_t& cas,
                                         uint64_t* seqno,
                                         const void* cookie,
                                         PermittedVBStates permittedVBStates,
                                         CheckConflicts checkConflicts,
                                         bool allowExisting,
                                         GenerateBySeqno genBySeqno,
                                         GenerateCas genCas,
                                         cb::const_byte_buffer emd);

    DocKey public_makeDocKey(const void* cookie, const std::string& key);

    bool public_enableTraffic(bool enable) {
        return enableTraffic(enable);
    }

    using EventuallyPersistentEngine::doCollectionStats;
    using EventuallyPersistentEngine::doConnAggStats;
};
