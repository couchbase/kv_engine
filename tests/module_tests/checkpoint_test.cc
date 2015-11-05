/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "config.h"

#include <signal.h>

#include <algorithm>
#include <set>
#include <vector>

#include "checkpoint.h"
#include "stats.h"
#include "vbucket.h"

#include <gtest/gtest.h>

#ifdef _MSC_VER
#define alarm(a)
#endif

#define NUM_TAP_THREADS 3
#define NUM_SET_THREADS 4
#ifdef _MSC_VER
// The test takes way too long time using 50k items on my windows
// builder (22 minutes). Reduce this to 5k for now until we've
// figured out why it runs so much slower on windows than the
// other platforms.
#define NUM_ITEMS 10000
#else
#define NUM_ITEMS 50000
#endif

struct thread_args {
    SyncObject *mutex;
    SyncObject *gate;
    RCPtr<VBucket> vbucket;
    CheckpointManager *checkpoint_manager;
    int *counter;
    std::string name;
};

extern "C" {
static rel_time_t basic_current_time(void) {
    return 0;
}

rel_time_t (*ep_current_time)() = basic_current_time;

time_t ep_real_time() {
    return time(NULL);
}

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) {
        (void) dummy;
    }
};

// Test fixture for Checkpoint tests. Once constructed provides a checkpoint
// manager and single vBucket (VBID 0).
class CheckpointTest : public ::testing::Test {
protected:
    CheckpointTest()
        : callback(new DummyCB()),
          vbucket(new VBucket(0, vbucket_state_active, global_stats,
                              checkpoint_config, /*kvshard*/NULL,
                              /*lastSeqno*/1000, /*lastSnapStart*/0,
                              /*lastSnapEnd*/0, /*table*/NULL,
                              callback)) {
        createManager();
    }

    void createManager() {
        manager.reset(new CheckpointManager(global_stats, vbucket->getId(),
                                            checkpoint_config,
                                            /*lastSeqno*/1000,
                                            /*lastSnapStart*/0,/*lastSnapEnd*/0,
                                            callback));
    }

    EPStats global_stats;
    CheckpointConfig checkpoint_config;

    std::shared_ptr<Callback<uint16_t> > callback;
    RCPtr<VBucket> vbucket;
    std::unique_ptr<CheckpointManager> manager;
};

static void launch_persistence_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    bool flush = false;
    while(true) {
        size_t itemPos;
        std::vector<queued_item> items;
        const std::string cursor(CheckpointManager::pCursorName);
        args->checkpoint_manager->getAllItemsForCursor(cursor, items);
        for(itemPos = 0; itemPos < items.size(); ++itemPos) {
            queued_item qi = items.at(itemPos);
            if (qi->getOperation() == queue_op_flush) {
                flush = true;
                break;
            }
        }
        if (flush) {
            // Checkpoint start and end operations may have been introduced in
            // the items queue after the "flush" operation was added. Ignore
            // these. Anything else will be considered an error.
            for(size_t i = itemPos + 1; i < items.size(); ++i) {
                queued_item qi = items.at(i);
                EXPECT_TRUE(queue_op_checkpoint_start == qi->getOperation() ||
                            queue_op_checkpoint_end == qi->getOperation())
                    << "Unexpected operation:" << qi->getOperation();
            }
            break;
        }
    }
    EXPECT_TRUE(flush);
}

static void launch_tap_client_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    bool flush = false;
    bool isLastItem = false;
    while(true) {
        queued_item qi = args->checkpoint_manager->nextItem(args->name,
                                                            isLastItem);
        if (qi->getOperation() == queue_op_flush) {
            flush = true;
            break;
        }
    }
    EXPECT_TRUE(flush);
}

static void launch_checkpoint_cleanup_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    while (args->checkpoint_manager->getNumOfCursors() > 1) {
        bool newCheckpointCreated;
        args->checkpoint_manager->removeClosedUnrefCheckpoints(args->vbucket,
                                                               newCheckpointCreated);
    }
}

static void launch_set_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    int i(0);
    for (i = 0; i < NUM_ITEMS; ++i) {
        std::stringstream key;
        key << "key-" << i;
        queued_item qi(new Item(key.str(), args->vbucket->getId(),
                                queue_op_set, 0, 0));
        args->checkpoint_manager->queueDirty(args->vbucket, qi, true);
    }
}
}

TEST_F(CheckpointTest, basic_chk_test) {
    std::shared_ptr<Callback<uint16_t> > cb(new DummyCB());
    RCPtr<VBucket> vbucket(new VBucket(0, vbucket_state_active, global_stats,
                                       checkpoint_config, NULL, 0, 0, 0, NULL,
                                       cb));

    CheckpointManager *checkpoint_manager = new CheckpointManager(global_stats, 0,
                                                                  checkpoint_config,
                                                                  1, 0, 0, cb);
    SyncObject *mutex = new SyncObject();
    SyncObject *gate = new SyncObject();
    int *counter = new int;
    *counter = 0;

    cb_thread_t tap_threads[NUM_TAP_THREADS];
    cb_thread_t set_threads[NUM_SET_THREADS];
    cb_thread_t persistence_thread;
    cb_thread_t checkpoint_cleanup_thread;
    int i(0), rc(0);

    struct thread_args t_args;
    t_args.checkpoint_manager = checkpoint_manager;
    t_args.vbucket = vbucket;
    t_args.mutex = mutex;
    t_args.gate = gate;
    t_args.counter = counter;

    struct thread_args tap_t_args[NUM_TAP_THREADS];
    for (i = 0; i < NUM_TAP_THREADS; ++i) {
        std::stringstream name;
        name << "tap-client-" << i;
        tap_t_args[i].checkpoint_manager = checkpoint_manager;
        tap_t_args[i].vbucket = vbucket;
        tap_t_args[i].mutex = mutex;
        tap_t_args[i].gate = gate;
        tap_t_args[i].counter = counter;
        tap_t_args[i].name = name.str();
        checkpoint_manager->registerCursor(name.str());
    }

    // Start a timer so that the test can be killed if it doesn't finish in a
    // reasonable amount of time
    alarm(60);

    rc = cb_create_thread(&persistence_thread, launch_persistence_thread, &t_args, 0);
    EXPECT_EQ(0, rc);

    rc = cb_create_thread(&checkpoint_cleanup_thread,
                        launch_checkpoint_cleanup_thread, &t_args, 0);
    EXPECT_EQ(0, rc);

    for (i = 0; i < NUM_TAP_THREADS; ++i) {
        rc = cb_create_thread(&tap_threads[i], launch_tap_client_thread, &tap_t_args[i], 0);
        EXPECT_EQ(0, rc);
    }

    for (i = 0; i < NUM_SET_THREADS; ++i) {
        rc = cb_create_thread(&set_threads[i], launch_set_thread, &t_args, 0);
        EXPECT_EQ(0, rc);
    }

    // Wait for all threads to reach the starting gate
    while (true) {
        LockHolder lh(*gate);
        if (*counter == (NUM_TAP_THREADS + NUM_SET_THREADS + 2)) {
            break;
        }
        gate->wait();
    }
    sleep(1);
    mutex->notify();

    for (i = 0; i < NUM_SET_THREADS; ++i) {
        rc = cb_join_thread(set_threads[i]);
        EXPECT_EQ(0, rc);
    }

    // Push the flush command into the queue so that all other threads can be terminated.
    std::string key("flush");
    queued_item qi(new Item(key, vbucket->getId(), queue_op_flush, 0xffff, 0));
    checkpoint_manager->queueDirty(vbucket, qi, true);

    rc = cb_join_thread(persistence_thread);
    EXPECT_EQ(0, rc);

    for (i = 0; i < NUM_TAP_THREADS; ++i) {
        rc = cb_join_thread(tap_threads[i]);
        EXPECT_EQ(0, rc);
        std::stringstream name;
        name << "tap-client-" << i;
        checkpoint_manager->removeCursor(name.str());
    }

    rc = cb_join_thread(checkpoint_cleanup_thread);
    EXPECT_EQ(0, rc);

    delete checkpoint_manager;
    delete gate;
    delete mutex;
    delete counter;
}

TEST_F(CheckpointTest, reset_checkpoint_id) {
    std::shared_ptr<Callback<uint16_t> > cb(new DummyCB());
    RCPtr<VBucket> vbucket(new VBucket(0, vbucket_state_active, global_stats,
                                       checkpoint_config, NULL, 0, 0, 0, NULL,
                                       cb));
    CheckpointManager *manager =
        new CheckpointManager(global_stats, 0, checkpoint_config, 1, 0, 0, cb);

    int i;
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        queued_item qi(new Item(key.str(), vbucket->getId(), queue_op_set,
                                0, 0));
        manager->queueDirty(vbucket, qi, true);
    }
    manager->createNewCheckpoint();

    size_t itemPos;
    uint64_t chk = 1;
    size_t lastMutationId = 0;
    std::vector<queued_item> items;
    const std::string cursor(CheckpointManager::pCursorName);
    manager->getAllItemsForCursor(cursor, items);
    for(itemPos = 0; itemPos < items.size(); ++itemPos) {
        queued_item qi = items.at(itemPos);
        if (qi->getOperation() != queue_op_checkpoint_start &&
            qi->getOperation() != queue_op_checkpoint_end) {
            size_t mid = qi->getBySeqno();
            EXPECT_GT(mid, lastMutationId);
            lastMutationId = qi->getBySeqno();
        }
        if (itemPos == 0 || itemPos == (items.size() - 1)) {
            EXPECT_EQ(queue_op_checkpoint_start, qi->getOperation()) << "For itemPos:" << itemPos;
        } else if (itemPos == (items.size() - 2)) {
            EXPECT_EQ(queue_op_checkpoint_end, qi->getOperation()) << "For itemPos:" << itemPos;
            chk++;
        } else {
            EXPECT_EQ(queue_op_set, qi->getOperation()) << "For itemPos:" << itemPos;
        }
    }
    EXPECT_EQ(13, items.size());
    items.clear();

    chk = 1;
    lastMutationId = 0;
    manager->checkAndAddNewCheckpoint(1, vbucket);
    manager->getAllItemsForCursor(cursor, items);
    EXPECT_EQ(0, items.size());

    delete manager;
}

// Sanity check test fixture
TEST_F(CheckpointTest, CheckFixture) {

    // Should intially have a single cursor (persistence).
    EXPECT_EQ(1, manager->getNumOfCursors());
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    for (auto& cursor : manager->getCursorNames()) {
        EXPECT_EQ(CheckpointManager::pCursorName, cursor);
    }
    // Should initially be zero items to persist.
    EXPECT_EQ(0, manager->getNumItemsForCursor(CheckpointManager::pCursorName));
}

// Basic test of a single, open checkpoint.
TEST_F(CheckpointTest, OneOpenCkpt) {

    // Queue a set operation.
    queued_item qi(new Item("key1", vbucket->getId(), queue_op_set,
                            /*revSeq*/20, /*bySeq*/0));

    // No set_ops in queue, expect queueDirty to return true (increase
    // persistence queue size).
    EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    EXPECT_EQ(1, manager->getNumCheckpoints());  // Single open checkpoint.
    EXPECT_EQ(2, manager->getNumOpenChkItems()); // 1x op_checkpoint_start, 1x op_set
    EXPECT_EQ(1001, qi->getBySeqno());
    EXPECT_EQ(20, qi->getRevSeqno());
    EXPECT_EQ(1, manager->getNumItemsForCursor(CheckpointManager::pCursorName));

    // Adding the same key again shouldn't increase the size.
    queued_item qi2(new Item("key1", vbucket->getId(), queue_op_set,
                            /*revSeq*/21, /*bySeq*/0));
    EXPECT_FALSE(manager->queueDirty(vbucket, qi2, true));
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems());
    EXPECT_EQ(1002, qi2->getBySeqno());
    EXPECT_EQ(21, qi2->getRevSeqno());
    EXPECT_EQ(1, manager->getNumItemsForCursor(CheckpointManager::pCursorName));

    // Adding a different key should increase size.
    queued_item qi3(new Item("key2", vbucket->getId(), queue_op_set,
                            /*revSeq*/0, /*bySeq*/0));
    EXPECT_TRUE(manager->queueDirty(vbucket, qi3, true));
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems());
    EXPECT_EQ(1003, qi3->getBySeqno());
    EXPECT_EQ(0, qi3->getRevSeqno());
    EXPECT_EQ(2, manager->getNumItemsForCursor(CheckpointManager::pCursorName));
}

// Test with one open and one closed checkpoint.
TEST_F(CheckpointTest, OneOpenOneClosed) {

    // Add some items to the initial (open) checkpoint.
    for (auto i : {1,2}) {
        queued_item qi(new Item("key" + std::to_string(i), vbucket->getId(),
                                queue_op_set, /*revSeq*/0, /*bySeq*/0));
        EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    }
    EXPECT_EQ(1, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems()); // 1x op_checkpoint_start, 2x op_set
    EXPECT_EQ(3, manager->getNumItems());
    const uint64_t ckpt_id1 = manager->getOpenCheckpointId();

    // Create a new checkpoint (closing the current open one).
    const uint64_t ckpt_id2 = manager->createNewCheckpoint();
    EXPECT_NE(ckpt_id1, ckpt_id2) << "New checkpoint ID should differ from old";
    EXPECT_EQ(ckpt_id1, manager->getLastClosedCheckpointId());
    EXPECT_EQ(1, manager->getNumOpenChkItems()); // 1x op_checkpoint_start

    // Add some items to the newly-opened checkpoint (note same keys as 1st
    // ckpt).
    for (auto ii : {1,2}) {
        queued_item qi(new Item("key" + std::to_string(ii), vbucket->getId(),
                                queue_op_set, /*revSeq*/1, /*bySeq*/0));
        EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    }
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(3, manager->getNumOpenChkItems()); // 1x op_checkpoint_start, 2x op_set
    EXPECT_EQ(3 + 4,
              manager->getNumItems()); // open items + 1x op_ckpt_start, 2x op_set, 1x op_ckpt_end

    // Examine the items - should be 2 lots of two keys.
    EXPECT_EQ(4, manager->getNumItemsForCursor(CheckpointManager::pCursorName));
}

// Test the automatic creation of checkpoints based on the number of items.
TEST_F(CheckpointTest, ItemBasedCheckpointCreation) {

    // Size down the default number of items to create a new checkpoint and
    // recreate the manager
    checkpoint_config = CheckpointConfig(DEFAULT_CHECKPOINT_PERIOD,
                                         MIN_CHECKPOINT_ITEMS,
                                         /*numCheckpoints*/2,
                                         /*itemBased*/true,
                                         /*keepClosed*/false,
                                         /*enableMerge*/false);
    createManager();

    // Sanity check initial state.
    EXPECT_EQ(1, manager->getNumOfCursors());
    EXPECT_EQ(1, manager->getNumOpenChkItems());
    EXPECT_EQ(1, manager->getNumCheckpoints());

    // Create one less than the number required to create a new checkpoint.
    queued_item qi;
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS; ii++) {
        EXPECT_EQ(ii + 1, manager->getNumOpenChkItems()); /* +1 for op_ckpt_start */

        qi.reset(new Item("key" + std::to_string(ii), vbucket->getId(),
                          queue_op_set, /*revSeq*/0, /*bySeq*/0));
        EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
        EXPECT_EQ(1, manager->getNumCheckpoints());

    }

    // Add one more - should create a new checkpoint.
    qi.reset(new Item("key_epoch", vbucket->getId(), queue_op_set, /*revSeq*/0,
                      /*bySeq*/0));
    EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems()); // 1x op_ckpt_start, 1x op_set

    // Fill up this checkpoint also - note loop for MIN_CHECKPOINT_ITEMS - 1
    for (unsigned int ii = 0; ii < MIN_CHECKPOINT_ITEMS - 1; ii++) {
        EXPECT_EQ(ii + 2, manager->getNumOpenChkItems()); /* +2 op_ckpt_start, key_epoch */

        qi.reset(new Item("key" + std::to_string(ii), vbucket->getId(),
                                queue_op_set, /*revSeq*/1, /*bySeq*/0));
        EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
        EXPECT_EQ(2, manager->getNumCheckpoints());
    }

    // Add one more - as we have hit maximum checkpoints should *not* create a
    // new one.
    qi.reset(new Item("key_epoch2", vbucket->getId(), queue_op_set,
                      /*revSeq*/1, /*bySeq*/0));
    EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(12, // 1x op_ckpt_start, 1x key_epoch, 9x key_X, 1x key_epoch2
              manager->getNumOpenChkItems());

    // Fetch the items associated with the persistence cursor. This
    // moves the single cursor registered outside of the initial checkpoint,
    // allowing a new open checkpoint to be created.
    EXPECT_EQ(1, manager->getNumOfCursors());
    snapshot_range_t range;
    std::vector<queued_item> items;
    range = manager->getAllItemsForCursor(CheckpointManager::pCursorName,
                                         items);
    // Should still have the same number of checkpoints and open items.
    EXPECT_EQ(2, manager->getNumCheckpoints());
    EXPECT_EQ(12, manager->getNumOpenChkItems());

    // But adding a new item will create a new one.
    qi.reset(new Item("key_epoch3", vbucket->getId(), queue_op_set,
                      /*revSeq*/1, /*bySeq*/0));
    EXPECT_TRUE(manager->queueDirty(vbucket, qi, true));
    EXPECT_EQ(3, manager->getNumCheckpoints());
    EXPECT_EQ(2, manager->getNumOpenChkItems()); // 1x op_ckpt_start, 1x op_set
}

/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    putenv(allow_no_stats_env);

    HashTable::setDefaultNumBuckets(5);
    HashTable::setDefaultNumLocks(1);

    return RUN_ALL_TESTS();
}
