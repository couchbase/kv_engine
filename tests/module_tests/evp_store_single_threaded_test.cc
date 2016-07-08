/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "evp_store_test.h"

#include "fakes/fake_executorpool.h"
#include "programs/engine_testapp/mock_server.h"
#include "taskqueue.h"

/*
 * A subclass of EventuallyPersistentStoreTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedEPStoreTest : public EventuallyPersistentStoreTest {
    void SetUp() {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        EventuallyPersistentStoreTest::SetUp();
    }
};

static ENGINE_ERROR_CODE dummy_dcp_add_failover_cb(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie)
{
    return ENGINE_SUCCESS;
}

/* Arguments for the background thread used by
 * MB20054_onDeleteItem_during_bucket_deletion
 */
typedef struct {
    EventuallyPersistentEngine* engine;
    FakeExecutorThread& fake_executor_thread;
    SyncObject& backfill_cv;
    SyncObject& destroy_cv;
    TaskQueue* taskQ;
} mb20054_backfill_thread_params;

static void MB20054_run_backfill_task(void* arg) {
    mb20054_backfill_thread_params* params = static_cast<mb20054_backfill_thread_params*>(arg);
    EventuallyPersistentEngine* engine = params->engine;
    FakeExecutorThread& auxio_thread = params->fake_executor_thread;
    SyncObject& backfill_cv = params->backfill_cv;
    SyncObject& destroy_cv = params->destroy_cv;

    TaskQueue* lpAuxioQ = params->taskQ;

    ObjectRegistry::onSwitchThread(engine);

    // Run the BackfillManagerTask task to push items to readyQ. In sherlock
    // upwards this runs multiple times - so should return true.
    EXPECT_TRUE(auxio_thread.getCurrentTask()->run());

    // Notify the main thread that it can progress with destroying the
    // engine [A].
    {
        LockHolder lh(backfill_cv);
        backfill_cv.notifyOne();
    }

    // Now wait ourselves for destroy to be completed [B].
    LockHolder lh(destroy_cv);
    destroy_cv.wait();

    // This is the only "hacky" part of the test - we need to somehow
    // keep the DCPBackfill task 'running' - i.e. not call
    // completeCurrentTask - until the main thread is in
    // ExecutorPool::_stopTaskGroup. However we have no way from the test
    // to properly signal that we are *inside* _stopTaskGroup -
    // called from EVPStore's destructor.
    // Best we can do is spin on waiting for the DCPBackfill task to be
    // set to 'dead' - and only then completeCurrentTask; which will
    // cancel the task.
    while (!auxio_thread.getCurrentTask()->isdead()) {
        // spin.
    }
    auxio_thread.completeCurrentTask();

    // Cleanup - fetch the next (final) task -
    // ActiveStreamCheckpointProcessorTask - so it can be cancelled
    // and executorpool shut down.
    auxio_thread.updateCurrentTime();
    EXPECT_TRUE(lpAuxioQ->fetchNextTask(auxio_thread, false));
    EXPECT_EQ("Process checkpoint(s) for DCP producer",
              auxio_thread.getTaskName());
    auxio_thread.runCurrentTask();
}

// Check that if onDeleteItem() is called during bucket deletion, we do not
// abort due to not having a valid thread-local 'engine' pointer. This
// has been observed when we have a DCPBackfill task which is deleted during
// bucket shutdown, which has a non-zero number of Items which are destructed
// (and call onDeleteItem).
TEST_F(SingleThreadedEPStoreTest, MB20054_onDeleteItem_during_bucket_deletion) {
    SingleThreadedExecutorPool* task_executor =
            reinterpret_cast<SingleThreadedExecutorPool*>(ExecutorPool::get());

    // Should start with no tasks registered on any queues.
    TaskQ& lp_task_q = task_executor->getLpTaskQ();
    for (auto& queue : lp_task_q) {
        ASSERT_EQ(0, queue->getFutureQueueSize());
        ASSERT_EQ(0, queue->getReadyQueueSize());
    }

    // [[1] Set our state to active. This should add a VBStatePersistTask to
    // the WRITER queue.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_active, false));

    TaskQueue* lpWriterQ = task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    TaskQueue* lpAuxioQ = task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

    EXPECT_EQ(1, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());

    // Use a FakeExecutorThread to fetch and run the persistTask.
    FakeExecutorThread writer_thread(task_executor, WRITER_TASK_IDX);
    writer_thread.updateCurrentTime();
    EXPECT_TRUE(lpWriterQ->fetchNextTask(writer_thread, false));
    EXPECT_EQ("Persisting a vbucket state for vbucket: 0",
              writer_thread.getTaskName());
    EXPECT_EQ(0, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());
    writer_thread.runCurrentTask();

    // Perform one SET, then close it's checkpoint. This means that we no
    // longer have all sequence numbers in memory checkpoints, forcing the
    // DCP stream request to go to disk (backfill).
    store_item(vbid, "key", "value");

    // Force a new checkpoint.
    RCPtr<VBucket> vb = store->getVbMap().getBucket(vbid);
    CheckpointManager& ckpt_mgr = vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    EXPECT_EQ(0, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());

    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Directly flush the vbucket, ensuring data is on disk.
    //  (This would normally also wake up the checkpoint remover task, but
    //   as that task was never registered with the ExecutorPool in this test
    //   environment, we need to manually remove the prev checkpoint).
    EXPECT_EQ(1, store->flushVBucket(vbid));

    bool new_ckpt_created;
    EXPECT_EQ(1,
              ckpt_mgr.removeClosedUnrefCheckpoints(vb, new_ckpt_created));
    vb.reset();

    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Create a DCP producer, and start a stream request.
    std::string name("test_producer");
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcpOpen(cookie, /*opaque:unused*/{}, /*seqno:unused*/{},
                              DCP_OPEN_PRODUCER, name.data(), name.size()));

    // Expect to have an ActiveStreamCheckpointProcessorTask, which is
    // initially snoozed (so we can't run it).
    EXPECT_EQ(1, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    uint64_t rollbackSeqno;
    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcp.stream_req(&engine->interface, cookie, /*flags*/0,
                                     /*opaque*/0, /*vbucket*/vbid,
                                     /*start_seqno*/0, /*end_seqno*/-1,
                                     /*vb_uuid*/0xabcd, /*snap_start*/0,
                                     /*snap_end*/0, &rollbackSeqno,
                                     dummy_dcp_add_failover_cb));

    // FutureQ should now have an additional DCPBackfill task.
    EXPECT_EQ(2, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Create an executor 'thread' to obtain shared ownership of the next
    // AuxIO task (which should be BackfillManagerTask). As long as this
    // object has it's currentTask set to BackfillManagerTask, the task
    // will not be deleted.
    // Essentially we are simulating a concurrent thread running this task.
    FakeExecutorThread auxio_thread(task_executor, AUXIO_TASK_IDX);
    auxio_thread.updateCurrentTime();
    EXPECT_TRUE(lpAuxioQ->fetchNextTask(auxio_thread, false));
    EXPECT_EQ("Backfilling items for a DCP Connection",
              auxio_thread.getTaskName());

    // This is the one action we really need to perform 'concurrently' - delete
    // the engine while a DCPBackfill task is still running. We spin up a
    // separate thread which will run the DCPBackfill task
    // concurrently with destroy - specifically DCPBackfill must start running
    // (and add items to the readyQ) before destroy(), it must then continue
    // running (stop after) _stopTaskGroup is invoked.
    // To achieve this we use a couple of condition variables to synchronise
    // between the two threads - the timeline needs to look like:
    //
    //  auxIO thread:  [------- DCPBackfill ----------]
    //   main thread:          [destroy()]       [ExecutorPool::_stopTaskGroup]
    //
    //  --------------------------------------------------------> time
    //
    SyncObject backfill_cv;
    SyncObject destroy_cv;

    cb_thread_t concurrent_task_thread;
    mb20054_backfill_thread_params params = {engine, auxio_thread, backfill_cv,
                                     destroy_cv, lpAuxioQ};

    cb_create_thread(&concurrent_task_thread, MB20054_run_backfill_task, &params, 0);

    // [A] Wait for DCPBackfill to complete.
    LockHolder lh(backfill_cv);
    backfill_cv.wait();

    // 'Destroy' the engine - this doesn't delete the object, just shuts down
    // connections, marks streams as dead etc.
    engine->destroy(/*force*/false);
    destroy_mock_event_callbacks();

    {
        LockHolder lh(destroy_cv);
        destroy_cv.notifyOne();
    }

    // Need to have the current engine valid before deleting (this is what
    // EvpDestroy does normally; however we have a smart ptr to the engine
    // so must delete via that).
    ObjectRegistry::onSwitchThread(engine);
    delete engine;
    engine = NULL;

    cb_join_thread(concurrent_task_thread);
}
