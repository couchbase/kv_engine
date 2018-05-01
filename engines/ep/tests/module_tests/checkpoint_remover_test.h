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

/*
 * Unit tests for the checkpoint remover and associated functions.
 */

#pragma once

#include "checkpoint.h"
#include "evp_store_single_threaded_test.h"
#include "evp_store_test.h"

class CheckpointManager;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class CheckpointRemoverTest : public SingleThreadedKVBucketTest {
public:
    /**
     * Get the maximum number of items allowed in a checkpoint for the given
     * vBucket
     */
    size_t getMaxCheckpointItems(VBucket& vb);
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class CheckpointRemoverEPTest : public CheckpointRemoverTest {
protected:
    EPBucket& getEPBucket() {
        return dynamic_cast<EPBucket&>(*store);
    }
};

/**
 * Stateless class used to gain privileged access into CheckpointManager for
 * testing purposes. This is a friend class of CheckpointManager.
 */
class CheckpointManagerTestIntrospector {
public:
    static const CheckpointList& public_getCheckpointList(
            CheckpointManager& checkpointManager);
};
