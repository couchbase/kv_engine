/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <vector>
#include <set>
#include <algorithm>

#include "assert.h"
#include "queueditem.hh"
#include "checkpoint.hh"
#include "vbucket.hh"
#include "stats.hh"

#define NUM_TAP_THREADS 3
#define NUM_SET_THREADS 4
#define NUM_ITEMS 50000

EPStats global_stats;

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

static void *launch_persistence_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    std::vector<queued_item> items;
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    bool flush = false;
    while(true) {
        args->checkpoint_manager->getAllItemsForPersistence(items);
        while (!items.empty()) {
            queued_item qi = items.back();
            items.pop_back();
            if (qi->getOperation() == queue_op_flush) {
                flush = true;
                break;
            }
        }
        if (flush) {
            break;
        }
    }
    assert(flush == true);
    assert(items.empty());
    return static_cast<void *>(0);
}

static void *launch_tap_client_thread(void *arg) {
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
        queued_item qi = args->checkpoint_manager->nextItem(args->name);
        if (qi->getOperation() == queue_op_flush) {
            flush = true;
            break;
        }
    }
    assert(flush == true);
    return static_cast<void *>(0);
}

static void *launch_checkpoint_cleanup_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    std::set<queued_item, CompareQueuedItemsByKey> items;
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    uint64_t checkpoint_id = 0;
    while (args->checkpoint_manager->getNumOfTAPCursors() > 0) {
        bool newCheckpointCreated = false;
        uint64_t id = args->checkpoint_manager->removeClosedUnrefCheckpoints(args->vbucket, items,
                                                                             newCheckpointCreated);
        if (id > 0) {
            checkpoint_id = id;
        }
        items.clear();
    }
    // Checkpoint cleanup thread should remove all the unreferenced closed checkpoints
    // except for the current open checkpoint in this test.
    assert(checkpoint_id == (args->checkpoint_manager->getOpenCheckpointId() - 1));
    assert(items.empty());
    return static_cast<void *>(0);
}

static void *launch_set_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int i(0);
    LockHolder lh(*(args->mutex));
    LockHolder lhg(*(args->gate));
    ++(*(args->counter));
    lhg.unlock();
    args->gate->notify();
    args->mutex->wait();
    lh.unlock();

    for (i = 0; i < NUM_ITEMS; ++i) {
        std::stringstream key;
        key << "key-" << i;
        queued_item qi(new QueuedItem (key.str(), 0, queue_op_set));
        args->checkpoint_manager->queueDirty(qi, args->vbucket);
    }

    return static_cast<void *>(0);
}
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    HashTable::setDefaultNumBuckets(5);
    HashTable::setDefaultNumLocks(1);
    RCPtr<VBucket> vbucket(new VBucket(0, vbucket_state_active, global_stats));

    CheckpointManager *checkpoint_manager = new CheckpointManager(global_stats, 0, 1);
    SyncObject *mutex = new SyncObject();
    SyncObject *gate = new SyncObject();
    int *counter = new int;
    *counter = 0;

    pthread_t tap_threads[NUM_TAP_THREADS];
    pthread_t set_threads[NUM_SET_THREADS];
    pthread_t persistence_thread;
    pthread_t checkpoint_cleanup_thread;
    int i(0), rc(0), result(-1);

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
        checkpoint_manager->registerTAPCursor(name.str());
    }

    alarm(60);

    rc = pthread_create(&persistence_thread, NULL, launch_persistence_thread, &t_args);
    assert(rc == 0);

    rc = pthread_create(&checkpoint_cleanup_thread, NULL,
                        launch_checkpoint_cleanup_thread, &t_args);
    assert(rc == 0);

    for (i = 0; i < NUM_TAP_THREADS; ++i) {
        rc = pthread_create(&tap_threads[i], NULL, launch_tap_client_thread, &tap_t_args[i]);
        assert(rc == 0);
    }

    for (i = 0; i < NUM_SET_THREADS; ++i) {
        rc = pthread_create(&set_threads[i], NULL, launch_set_thread, &t_args);
        assert(rc == 0);
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
        rc = pthread_join(set_threads[i], reinterpret_cast<void **>(&result));
        assert(rc == 0);
        assert(result == 0);
    }
    sleep(1);
    queued_item qi(new QueuedItem ("flush", 0xffff, queue_op_flush));
    // Push the flush command into the queue so that all other threads can be terminated.
    checkpoint_manager->queueDirty(qi, vbucket);

    rc = pthread_join(persistence_thread, reinterpret_cast<void **>(&result));
    assert(rc == 0);
    assert(result == 0);

    for (i = 0; i < NUM_TAP_THREADS; ++i) {
        rc = pthread_join(tap_threads[i], reinterpret_cast<void **>(&result));
        assert(rc == 0);
        assert(result == 0);
        std::stringstream name;
        name << "tap-client-" << i;
        checkpoint_manager->removeTAPCursor(name.str());
    }

    rc = pthread_join(checkpoint_cleanup_thread, reinterpret_cast<void **>(&result));
    assert(rc == 0);
    assert(result == 0);

    delete checkpoint_manager;
    delete gate;
    delete mutex;
    delete counter;

    return 0;
}
