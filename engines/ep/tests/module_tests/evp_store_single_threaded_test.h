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
 * Unit tests for the EPBucket class.
 */

#pragma once

#include "evp_store_test.h"
#include "fakes/fake_executorpool.h"

class MockActiveStreamWithOverloadedRegisterCursor;

class MockDcpProducer;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedKVBucketTest : public KVBucketTest {
public:
    /*
     * Run the next task from the taskQ
     * The task must match the expectedTaskName parameter
     */
    ProcessClock::time_point runNextTask(TaskQueue& taskQ,
                                         const std::string& expectedTaskName);

    /*
     * Run the next task from the taskQ
     */
    ProcessClock::time_point runNextTask(TaskQueue& taskQ);

protected:
    void SetUp() override;

    void TearDown() override;

    /*
     * Change the vbucket state, and run the VBStatePeristTask (if necessary
     * for this bucket type).
     * On return the state will be changed and the task completed.
     */
    void setVBucketStateAndRunPersistTask(uint16_t vbid,
                                          vbucket_state_t newState);

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel
     */
    void shutdownAndPurgeTasks();

    void cancelAndPurgeTasks();

    /**
     * This method will keep running reader tasks until the engine shows warmup
     * is complete.
     */
    void runReadersUntilWarmedUp();

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    void notifyAndStepToCheckpoint(MockDcpProducer& producer,
                                   dcp_message_producers* producers);

    SingleThreadedExecutorPool* task_executor;
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class SingleThreadedEPBucketTest : public SingleThreadedKVBucketTest {
protected:
    EPBucket& getEPBucket() {
        return dynamic_cast<EPBucket&>(*store);
    }
};
