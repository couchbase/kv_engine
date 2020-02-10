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

#include "mock_synchronous_ep_engine.h"

#include "checkpoint_config.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "mock_dcp_conn_map.h"
#include "mock_ep_bucket.h"
#include "mock_ephemeral_bucket.h"
#include "replicationthrottle.h"
#include <platform/cbassert.h>
#include <programs/engine_testapp/mock_server.h>
#include <string>

SynchronousEPEngine::SynchronousEPEngine(const cb::ArenaMallocClient& client,
                                         std::string extra_config)
    : EventuallyPersistentEngine(get_mock_server_api, client) {
    // Tests may need to create multiple failover table entries, so allow that
    maxFailoverEntries = 5;

    // Default to a reduced number of vBuckets & shards to speed up test
    // setup / teardown (fewer VBucket & other related objects).
    // Tests which require additional vbuckets can specify that in
    // extra_config string.
    if (!configuration.parseConfiguration("max_vbuckets=4;max_num_shards=2",
                                          serverApi)) {
        throw std::invalid_argument(
                "SynchronousEPEngine: Unable to set reduced max_vbuckets & "
                "max_num_shards");
    }

    // Merge any extra config into the main configuration.
    if (!extra_config.empty()) {
        if (!configuration.parseConfiguration(extra_config.c_str(),
                                              serverApi)) {
            throw std::invalid_argument("Unable to parse config string: " +
                                        extra_config);
        }
    }

    name = "SynchronousEPEngine";

    // workload is needed by EPStore's constructor (to construct the
    // VBucketMap).
    workload = new WorkLoadPolicy(/*workers*/ 1, /*shards*/ 1);

    // dcpConnMap_ is needed by EPStore's constructor.
    dcpConnMap_ = std::make_unique<MockDcpConnMap>(*this);

    // checkpointConfig is needed by CheckpointManager (via EPStore).
    checkpointConfig = new CheckpointConfig(*this);

    dcpFlowControlManager_ = std::make_unique<DcpFlowControlManager>(*this);

    enableTraffic(true);

    maxItemSize = configuration.getMaxItemSize();

    setCompressionMode(configuration.getCompressionMode());

    allowDelWithMetaPruneUserData =
            configuration.isAllowDelWithMetaPruneUserData();

    if (configuration.getMemLowWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_low_wat_percent.store(0.75);
    } else {
        stats.mem_low_wat_percent.store(double(configuration.getMemLowWat()) /
                                        configuration.getMaxSize());
    }

    if (configuration.getMemHighWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_high_wat_percent.store(0.85);
    } else {
        stats.mem_high_wat_percent.store(double(configuration.getMemHighWat()) /
                                         configuration.getMaxSize());
    }
}

void SynchronousEPEngine::setKVBucket(std::unique_ptr<KVBucket> store) {
    cb_assert(kvBucket == nullptr);
    kvBucket = std::move(store);
}

void SynchronousEPEngine::setDcpConnMap(
        std::unique_ptr<DcpConnMap> dcpConnMap) {
    dcpConnMap_ = std::move(dcpConnMap);
}

SynchronousEPEngineUniquePtr SynchronousEPEngine::build(
        const std::string& config) {
    auto client = cb::ArenaMalloc::registerClient();
    cb::ArenaMalloc::switchToClient(client);
    SynchronousEPEngineUniquePtr engine(
            new SynchronousEPEngine(client, config));

    // switch current thread to this new engine, so all sub-created objects
    // are accounted in it's mem_used.
    ObjectRegistry::onSwitchThread(engine.get());

    engine->setKVBucket(
            engine->public_makeMockBucket(engine->getConfiguration()));

    engine->setMaxDataSize(engine->getConfiguration().getMaxSize());

    return engine;
}

void SynchronousEPEngineDeleter::operator()(SynchronousEPEngine* engine) {
    ObjectRegistry::onSwitchThread(engine);
    delete engine;
    ObjectRegistry::onSwitchThread(nullptr);
}

void SynchronousEPEngine::initializeConnmap() {
    dcpConnMap_->initialize();
}

std::unique_ptr<KVBucket> SynchronousEPEngine::public_makeMockBucket(
        Configuration& config) {
    const auto bucketType = config.getBucketType();
    if (bucketType == "persistent") {
        return std::make_unique<MockEPBucket>(*this);
    } else if (bucketType == "ephemeral") {
        EphemeralBucket::reconfigureForEphemeral(configuration);
        return std::make_unique<MockEphemeralBucket>(*this);
    }
    throw std::invalid_argument(bucketType +
                                " is not a recognized bucket "
                                "type");
}

std::unique_ptr<KVBucket> SynchronousEPEngine::public_makeBucket(
        Configuration& config) {
    return makeBucket(config);
}
