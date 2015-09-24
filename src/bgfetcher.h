/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_BGFETCHER_H_
#define SRC_BGFETCHER_H_ 1

#include "config.h"

#include <list>
#include <set>
#include <string>

#include "common.h"
#include "item.h"
#include "kvstore.h"
#include "stats.h"
#include "vbucket.h"

// Forward declarations.
class EventuallyPersistentStore;
class KVShard;
class GlobalTask;

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */
class BgFetcher {
public:
    static const double sleepInterval;
    /**
     * Construct a BgFetcher
     *
     * @param s  The store
     * @param k  The shard to which this background fetcher belongs
     * @param st reference to statistics
     */
    BgFetcher(EventuallyPersistentStore *s, KVShard *k, EPStats &st) :
        store(s), shard(k), taskId(0), stats(st), pendingFetch(false) {}
    ~BgFetcher() {
        LockHolder lh(queueMutex);
        if (!pendingVbs.empty()) {
            LOG(EXTENSION_LOG_DEBUG,
                    "Terminating database reader without completing "
                    "background fetches for %ld vbuckets.\n", pendingVbs.size());
            pendingVbs.clear();
        }
    }

    void start(void);
    void stop(void);
    bool run(GlobalTask *task);
    bool pendingJob(void);
    void notifyBGEvent(void);
    void setTaskId(size_t newId) { taskId = newId; }
    void addPendingVB(VBucket::id_type vbId) {
        LockHolder lh(queueMutex);
        pendingVbs.insert(vbId);
    }

private:
    size_t doFetch(VBucket::id_type vbId);
    void clearItems(VBucket::id_type vbId);

    EventuallyPersistentStore *store;
    KVShard *shard;
    vb_bgfetch_queue_t items2fetch;
    size_t taskId;
    Mutex queueMutex;
    EPStats &stats;

    AtomicValue<bool> pendingFetch;
    std::set<VBucket::id_type> pendingVbs;
};

#endif  // SRC_BGFETCHER_H_
