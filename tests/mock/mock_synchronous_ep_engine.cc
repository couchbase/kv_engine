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

#include <checkpoint_remover.h>
#include <programs/engine_testapp/mock_server.h>
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "replicationthrottle.h"
#include "tapconnmap.h"

#include <string>

SynchronousEPEngine::SynchronousEPEngine(const std::string& extra_config)
    : EventuallyPersistentEngine(get_mock_server_api) {
    maxFailoverEntries = 1;

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
    workload = new WorkLoadPolicy(/*workers*/ 1, /*shards*/ 1);

    // dcpConnMap_ is needed by EPStore's constructor.
    dcpConnMap_ = new DcpConnMap(*this);

    // tapConnMap is needed by queueDirty.
    tapConnMap = new TapConnMap(*this);

    // checkpointConfig is needed by CheckpointManager (via EPStore).
    checkpointConfig = new CheckpointConfig(*this);

    dcpFlowControlManager_ = new DcpFlowControlManager(*this);

    replicationThrottle = new ReplicationThrottle(configuration, stats);

    tapConfig = new TapConfig(*this);
}

void SynchronousEPEngine::setEPStore(KVBucket* store) {
    cb_assert(kvBucket == nullptr);
    kvBucket = store;
}

void SynchronousEPEngine::initializeConnmaps() {
    dcpConnMap_->initialize(DCP_CONN_NOTIFIER);
    tapConnMap->initialize(TAP_CONN_NOTIFIER);
}

MockEPStore::MockEPStore(EventuallyPersistentEngine& theEngine)
    : EPBucket(theEngine) {
    // Perform a limited set of setup (normally done by EPStore::initialize) -
    // enough such that objects which are assumed to exist are present.

    // Create the closed checkpoint removed task. Note we do _not_ schedule
    // it, unlike EPStore::initialize
    chkTask = new ClosedUnrefCheckpointRemoverTask(
            &engine, stats, theEngine.getConfiguration().getChkRemoverStime());
}

VBucketMap& MockEPStore::getVbMap() {
    return vbMap;
}