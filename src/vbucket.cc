/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include <functional>
#include <list>
#include <set>
#include <string>

#include "ep_engine.h"
#include "failover-table.h"
#define STATWRITER_NAMESPACE vbucket
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "vbucket.h"

VBucketFilter VBucketFilter::filter_diff(const VBucketFilter &other) const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;
    end = std::set_symmetric_difference(acceptable.begin(),
                                        acceptable.end(),
                                        other.acceptable.begin(),
                                        other.acceptable.end(),
                                        tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

VBucketFilter VBucketFilter::filter_intersection(const VBucketFilter &other)
                                                                        const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;

    end = std::set_intersection(acceptable.begin(), acceptable.end(),
                                other.acceptable.begin(),
                                other.acceptable.end(),
                                tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

static bool isRange(std::set<uint16_t>::const_iterator it,
                    const std::set<uint16_t>::const_iterator &end,
                    size_t &length)
{
    length = 0;
    for (uint16_t val = *it;
         it != end && (val + length) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator <<(std::ostream &out, const VBucketFilter &filter)
{
    std::set<uint16_t>::const_iterator it;

    if (filter.acceptable.empty()) {
        out << "{ empty }";
    } else {
        bool needcomma = false;
        out << "{ ";
        for (it = filter.acceptable.begin();
             it != filter.acceptable.end();
             ++it) {
            if (needcomma) {
                out << ", ";
            }

            size_t length;
            if (isRange(it, filter.acceptable.end(), length)) {
                std::set<uint16_t>::iterator last = it;
                for (size_t i = 0; i < length; ++i) {
                    ++last;
                }
                out << "[" << *it << "," << *last << "]";
                it = last;
            } else {
                out << *it;
            }
            needcomma = true;
        }
        out << " }";
    }

    return out;
}

size_t VBucket::chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;

const vbucket_state_t VBucket::ACTIVE =
                     static_cast<vbucket_state_t>(htonl(vbucket_state_active));
const vbucket_state_t VBucket::REPLICA =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_replica));
const vbucket_state_t VBucket::PENDING =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_pending));
const vbucket_state_t VBucket::DEAD =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_dead));

VBucket::~VBucket() {
    if (!pendingOps.empty() || !pendingBGFetches.empty()) {
        LOG(EXTENSION_LOG_WARNING,
            "Have %ld pending ops and %ld pending reads "
            "while destroying vbucket\n",
            pendingOps.size(), pendingBGFetches.size());
    }

    stats.decrDiskQueueSize(dirtyQueueSize.load());

    size_t num_pending_fetches = 0;
    vb_bgfetch_queue_t::iterator itr = pendingBGFetches.begin();
    for (; itr != pendingBGFetches.end(); ++itr) {
        std::list<VBucketBGFetchItem *> &bgitems = itr->second;
        std::list<VBucketBGFetchItem *>::iterator vit = bgitems.begin();
        for (; vit != bgitems.end(); ++vit) {
            delete (*vit);
            ++num_pending_fetches;
        }
    }
    stats.numRemainingBgJobs.fetch_sub(num_pending_fetches);
    pendingBGFetches.clear();
    delete failovers;

    stats.memOverhead.fetch_sub(sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));
    assert(stats.memOverhead.load() < GIGANTOR);

    LOG(EXTENSION_LOG_INFO, "Destroying vbucket %d\n", id);
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine,
                         ENGINE_ERROR_CODE code) {
    if (pendingOpsStart > 0) {
        hrtime_t now = gethrtime();
        if (now > pendingOpsStart) {
            hrtime_t d = (now - pendingOpsStart) / 1000;
            stats.pendingOpsHisto.add(d);
            atomic_setIfBigger(stats.pendingOpsMaxDuration, d);
        }
    } else {
        return;
    }

    pendingOpsStart = 0;
    stats.pendingOps.fetch_sub(pendingOps.size());
    atomic_setIfBigger(stats.pendingOpsMax, pendingOps.size());

    engine.notifyIOComplete(pendingOps, code);
    pendingOps.clear();

    LOG(EXTENSION_LOG_INFO,
        "Fired pendings ops for vbucket %d in state %s\n",
        id, VBucket::toString(state));
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine) {
    LockHolder lh(pendingOpLock);

    if (state == vbucket_state_active) {
        fireAllOps(engine, ENGINE_SUCCESS);
    } else if (state == vbucket_state_pending) {
        // Nothing
    } else {
        fireAllOps(engine, ENGINE_NOT_MY_VBUCKET);
    }
}

void VBucket::setState(vbucket_state_t to, SERVER_HANDLE_V1 *sapi) {
    assert(sapi);
    vbucket_state_t oldstate(state);

    if (to == vbucket_state_active &&
        checkpointManager.getOpenCheckpointId() < 2) {
        checkpointManager.setOpenCheckpointId(2);
    }

    LOG(EXTENSION_LOG_DEBUG, "transitioning vbucket %d from %s to %s",
        id, VBucket::toString(oldstate), VBucket::toString(to));

    state = to;
}

void VBucket::doStatsForQueueing(Item& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.fetch_add(sizeof(Item));
    ++dirtyQueueFill;
    dirtyQueueAge.fetch_add(qi.getQueuedTime());
    dirtyQueuePendingWrites.fetch_add(itemBytes);
}


void VBucket::doStatsForFlushing(Item& qi, size_t itemBytes)
{
    if (dirtyQueueSize > 0) {
        --dirtyQueueSize;
    }
    if (dirtyQueueMem > sizeof(Item)) {
        dirtyQueueMem.fetch_sub(sizeof(Item));
    } else {
        dirtyQueueMem.store(0);
    }
    ++dirtyQueueDrain;

    if (dirtyQueueAge > qi.getQueuedTime()) {
        dirtyQueueAge.fetch_sub(qi.getQueuedTime());
    } else {
        dirtyQueueAge.store(0);
    }

    if (dirtyQueuePendingWrites > itemBytes) {
        dirtyQueuePendingWrites.fetch_sub(itemBytes);
    } else {
        dirtyQueuePendingWrites.store(0);
    }
}

void VBucket::resetStats() {
    opsCreate.store(0);
    opsUpdate.store(0);
    opsDelete.store(0);
    opsReject.store(0);

    stats.decrDiskQueueSize(dirtyQueueSize.load());
    dirtyQueueSize.store(0);
    dirtyQueueMem.store(0);
    dirtyQueueFill.store(0);
    dirtyQueueAge.store(0);
    dirtyQueuePendingWrites.store(0);
    dirtyQueueDrain.store(0);
    fileSpaceUsed = 0;
    fileSize = 0;
}

template <typename T>
void VBucket::addStat(const char *nm, const T &val, ADD_STAT add_stat,
                      const void *c) {
    std::stringstream name;
    name << "vb_" << id;
    if (nm != NULL) {
        name << ":" << nm;
    }
    std::stringstream value;
    value << val;
    std::string n = name.str();
    add_casted_stat(n.data(), value.str().data(), add_stat, c);
}

void VBucket::queueBGFetchItem(const std::string &key,
                               VBucketBGFetchItem *fetch,
                               BgFetcher *bgFetcher) {
    LockHolder lh(pendingBGFetchesLock);
    pendingBGFetches[key].push_back(fetch);
    bgFetcher->addPendingVB(id);
    lh.unlock();
}

bool VBucket::getBGFetchItems(vb_bgfetch_queue_t &fetches) {
    LockHolder lh(pendingBGFetchesLock);
    fetches.insert(pendingBGFetches.begin(), pendingBGFetches.end());
    pendingBGFetches.clear();
    lh.unlock();
    return fetches.size() > 0;
}

void VBucket::addHighPriorityVBEntry(uint64_t id, const void *cookie,
                                     bool isBySeqno) {
    LockHolder lh(hpChksMutex);
    if (shard) {
        ++shard->highPriorityCount;
    }
    hpChks.push_back(HighPriorityVBEntry(cookie, id, isBySeqno));
    numHpChks = hpChks.size();
}

void VBucket::notifyCheckpointPersisted(EventuallyPersistentEngine &e,
                                        uint64_t idNum,
                                        bool isBySeqno) {
    LockHolder lh(hpChksMutex);
    std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();

    while (entry != hpChks.end()) {
        if (isBySeqno != entry->isBySeqno_) {
            ++entry;
            continue;
        }

        hrtime_t wall_time(gethrtime() - entry->start);
        size_t spent = wall_time / 1000000000;
        if (entry->id <= idNum) {
            e.notifyIOComplete(entry->cookie, ENGINE_SUCCESS);
            stats.chkPersistenceHisto.add(wall_time / 1000);
            adjustCheckpointFlushTimeout(wall_time / 1000000000);
            LOG(EXTENSION_LOG_WARNING, "Notified the completion of checkpoint "
                "persistence for vbucket %d, cookie %p", id, entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else if (spent > getCheckpointFlushTimeout()) {
            adjustCheckpointFlushTimeout(spent);
            e.storeEngineSpecific(entry->cookie, NULL);
            e.notifyIOComplete(entry->cookie, ENGINE_TMPFAIL);
            LOG(EXTENSION_LOG_WARNING, "Notified the timeout on checkpoint "
                "persistence for vbucket %d, cookie %p", id, entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else {
            ++entry;
        }
    }
    numHpChks = hpChks.size();
}

void VBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine &e) {
    LockHolder lh(hpChksMutex);
    std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();
    while (entry != hpChks.end()) {
        e.notifyIOComplete(entry->cookie, ENGINE_TMPFAIL);
        entry = hpChks.erase(entry);
        if (shard) {
            --shard->highPriorityCount;
        }
    }
    lh.unlock();
    fireAllOps(e);
}

void VBucket::adjustCheckpointFlushTimeout(size_t wall_time) {
    size_t middle = (MIN_CHK_FLUSH_TIMEOUT + MAX_CHK_FLUSH_TIMEOUT) / 2;

    if (wall_time <= MIN_CHK_FLUSH_TIMEOUT) {
        chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;
    } else if (wall_time <= middle) {
        chkFlushTimeout = middle;
    } else {
        chkFlushTimeout = MAX_CHK_FLUSH_TIMEOUT;
    }
}

size_t VBucket::getHighPriorityChkSize() {
    return numHpChks;
}

size_t VBucket::getCheckpointFlushTimeout() {
    return chkFlushTimeout;
}

size_t VBucket::getNumItems(item_eviction_policy_t policy) {
    if (policy == VALUE_ONLY) {
        return ht.getNumInMemoryItems();
    } else {
        return ht.getNumItems();
    }
}

size_t VBucket::getNumNonResidentItems(item_eviction_policy_t policy) {
    if (policy == VALUE_ONLY) {
        return ht.getNumInMemoryNonResItems();
    } else {
        size_t num_items = ht.getNumItems();
        size_t num_res_items = ht.getNumInMemoryItems() -
                               ht.getNumInMemoryNonResItems();
        return num_items > num_res_items ? (num_items - num_res_items) : 0;
    }
}

void VBucket::addStats(bool details, ADD_STAT add_stat, const void *c,
                       item_eviction_policy_t policy) {
    addStat(NULL, toString(state), add_stat, c);
    if (details) {
        size_t numItems = getNumItems(policy);
        size_t tempItems = getNumTempItems();
        addStat("num_items", numItems, add_stat, c);
        addStat("num_temp_items", tempItems, add_stat, c);
        addStat("num_non_resident", getNumNonResidentItems(policy),
                add_stat, c);
        addStat("ht_memory", ht.memorySize(), add_stat, c);
        addStat("ht_item_memory", ht.getItemMemory(), add_stat, c);
        addStat("ht_cache_size", ht.cacheSize, add_stat, c);
        addStat("num_ejects", ht.getNumEjects(), add_stat, c);
        addStat("ops_create", opsCreate, add_stat, c);
        addStat("ops_update", opsUpdate, add_stat, c);
        addStat("ops_delete", opsDelete, add_stat, c);
        addStat("ops_reject", opsReject, add_stat, c);
        addStat("queue_size", dirtyQueueSize, add_stat, c);
        addStat("queue_memory", dirtyQueueMem, add_stat, c);
        addStat("queue_fill", dirtyQueueFill, add_stat, c);
        addStat("queue_drain", dirtyQueueDrain, add_stat, c);
        addStat("queue_age", getQueueAge(), add_stat, c);
        addStat("pending_writes", dirtyQueuePendingWrites, add_stat, c);
        addStat("db_data_size", fileSpaceUsed, add_stat, c);
        addStat("db_file_size", fileSize, add_stat, c);
    }
}
