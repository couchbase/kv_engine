/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc.
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

#include "warmup.h"

#include <limits>
#include <string>
#include <utility>
#include <array>
#include <random>

#include "common.h"
#include "connmap.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "mutation_log.h"
#define STATWRITER_NAMESPACE warmup
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "tapconnmap.h"

struct WarmupCookie {
    WarmupCookie(EPBucket* s, Callback<GetValue>& c) :
        cb(c), epstore(s),
        loaded(0), skipped(0), error(0)
    { /* EMPTY */ }
    Callback<GetValue>& cb;
    EPBucket* epstore;
    size_t loaded;
    size_t skipped;
    size_t error;
};

static bool batchWarmupCallback(uint16_t vbId,
                                const std::set<StoredDocKey>& fetches,
                                void *arg)
{
    WarmupCookie *c = static_cast<WarmupCookie *>(arg);

    if (!c->epstore->maybeEnableTraffic()) {
        vb_bgfetch_queue_t items2fetch;
        for (auto& key : fetches) {
            // Deleted below via a unique_ptr in the next loop
            VBucketBGFetchItem *fit = new VBucketBGFetchItem(NULL, false);
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items2fetch[key];
            bg_itm_ctx.isMetaOnly = false;
            bg_itm_ctx.bgfetched_list.push_back(fit);
        }

        c->epstore->getROUnderlying(vbId)->getMulti(vbId, items2fetch);

        // applyItem controls the  mode this loop operates in.
        // true we will attempt the callback (attempt a HashTable insert)
        // false we don't attempt the callback
        // in both cases the loop must delete the VBucketBGFetchItem we
        // allocated above.
        bool applyItem = true;
        for (auto items : items2fetch) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items.second;
            std::unique_ptr<VBucketBGFetchItem> fetchedItem(bg_itm_ctx.bgfetched_list.back());
            if (applyItem) {
                GetValue &val = fetchedItem->value;
                if (val.getStatus() == ENGINE_SUCCESS) {
                    // NB: callback will delete the GetValue's Item
                    c->cb.callback(val);
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                    "Warmup failed to load data for vBucket = %d"
                    " key{%s} error = %X\n",
                    vbId, items.first.c_str(), val.getStatus());
                    c->error++;
                }

                if (c->cb.getStatus() == ENGINE_SUCCESS) {
                    c->loaded++;
                } else {
                    // Failed to apply an Item, so fail the rest
                    applyItem = false;
                }
            } else {
                // Providing that the status is SUCCESS, delete the Item
                if (fetchedItem->value.getStatus() == ENGINE_SUCCESS) {
                    delete fetchedItem->value.getValue();
                }
                c->skipped++;
            }
        }

        return true;
    } else {
        c->skipped++;
        return false;
    }
}

static bool warmupCallback(void *arg, uint16_t vb, const DocKey& key)
{
    WarmupCookie *cookie = static_cast<WarmupCookie*>(arg);

    if (!cookie->epstore->maybeEnableTraffic()) {
        RememberingCallback<GetValue> cb;
        cookie->epstore->getROUnderlying(vb)->get(key, vb, cb);
        cb.waitForValue();

        if (cb.val.getStatus() == ENGINE_SUCCESS) {
            cookie->cb.callback(cb.val);
            cookie->loaded++;
        } else {
            LOG(EXTENSION_LOG_WARNING, "Warmup failed to load data "
                "for vb:%" PRIu16 ", key{%.*s}, error:%X\n", vb,
                int(key.size()), key.data(), cb.val.getStatus());
            cookie->error++;
        }

        return true;
    } else {
        cookie->skipped++;
        return false;
    }
}

const int WarmupState::Initialize = 0;
const int WarmupState::CreateVBuckets = 1;
const int WarmupState::EstimateDatabaseItemCount = 2;
const int WarmupState::KeyDump = 3;
const int WarmupState::CheckForAccessLog = 4;
const int WarmupState::LoadingAccessLog = 5;
const int WarmupState::LoadingKVPairs = 6;
const int WarmupState::LoadingData = 7;
const int WarmupState::Done = 8;

const char *WarmupState::toString(void) const {
    return getStateDescription(state.load());
}

const char *WarmupState::getStateDescription(int st) const {
    switch (st) {
    case Initialize:
        return "initialize";
    case CreateVBuckets:
        return "creating vbuckets";
    case EstimateDatabaseItemCount:
        return "estimating database item count";
    case KeyDump:
        return "loading keys";
    case CheckForAccessLog:
        return "determine access log availability";
    case LoadingAccessLog:
        return "loading access log";
    case LoadingKVPairs:
        return "loading k/v pairs";
    case LoadingData:
        return "loading data";
    case Done:
        return "done";
    default:
        return "Illegal state";
    }
}

void WarmupState::transition(int to, bool allowAnystate) {
    if (allowAnystate || legalTransition(to)) {
        std::stringstream ss;
        ss << "Warmup transition from state \""
           << getStateDescription(state.load()) << "\" to \""
           << getStateDescription(to) << "\"";
        LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
        state.store(to);
    } else {
        // Throw an exception to make it possible to test the logic ;)
        std::stringstream ss;
        ss << "Illegal state transition from \"" << *this << "\" to " << to;
        throw std::runtime_error(ss.str());
    }
}

bool WarmupState::legalTransition(int to) const {
    switch (state.load()) {
    case Initialize:
        return (to == CreateVBuckets);
    case CreateVBuckets:
        return (to == EstimateDatabaseItemCount);
    case EstimateDatabaseItemCount:
        return (to == KeyDump || to == CheckForAccessLog);
    case KeyDump:
        return (to == LoadingKVPairs || to == CheckForAccessLog);
    case CheckForAccessLog:
        return (to == LoadingAccessLog || to == LoadingData ||
                to == LoadingKVPairs || to == Done);
    case LoadingAccessLog:
        return (to == Done || to == LoadingData);
    case LoadingKVPairs:
        return (to == Done);
    case LoadingData:
        return (to == Done);

    default:
        return false;
    }
}

std::ostream& operator <<(std::ostream &out, const WarmupState &state)
{
    out << state.toString();
    return out;
}
void LoadStorageKVPairCallback::callback(GetValue &val) {
    // This callback method is responsible for deleting the Item
    std::unique_ptr<Item> i(val.getValue());
    bool stopLoading = false;
    if (i != NULL && !epstore.getWarmup()->isComplete()) {
        RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            setStatus(ENGINE_NOT_MY_VBUCKET);
            return;
        }
        bool succeeded(false);
        int retry = 2;
        item_eviction_policy_t policy = epstore.getItemEvictionPolicy();
        do {
            if (i->getCas() == static_cast<uint64_t>(-1)) {
                if (val.isPartial()) {
                    i->setCas(0);
                } else {
                    i->setCas(vb->nextHLCCas());
                }
            }

            const auto res = vb->ht.insert(*i, policy, shouldEject(),
                                           val.isPartial());
            switch (res) {
            case NOMEM:
                if (retry == 2) {
                    if (hasPurged) {
                        if (++stats.warmOOM == 1) {
                            LOG(EXTENSION_LOG_WARNING,
                                "Warmup dataload failure: max_size too low.");
                        }
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "Emergency startup purge to free space for load.");
                        purge();
                    }
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Cannot store an item after emergency purge.");
                    ++stats.warmOOM;
                }
                break;
            case INVALID_CAS:
                LOG(EXTENSION_LOG_DEBUG,
                    "Value changed in memory before restore from disk. "
                    "Ignored disk value for: key{%s}.", i->getKey().c_str());
                ++stats.warmDups;
                succeeded = true;
                break;
            case NOT_FOUND:
                succeeded = true;
                break;
            default:
                throw std::logic_error("LoadStorageKVPairCallback::callback: "
                        "Unexpected result from HashTable::insert: " +
                        std::to_string(res));
            }
        } while (!succeeded && retry-- > 0);

        val.setValue(NULL);

        if (maybeEnableTraffic) {
            stopLoading = epstore.maybeEnableTraffic();
        }

        switch (warmupState) {
            case WarmupState::KeyDump:
                if (stats.warmOOM) {
                    epstore.getWarmup()->setOOMFailure();
                    stopLoading = true;
                } else {
                    ++stats.warmedUpKeys;
                }
                break;
            case WarmupState::LoadingData:
            case WarmupState::LoadingAccessLog:
                if (epstore.getItemEvictionPolicy() == FULL_EVICTION) {
                    ++stats.warmedUpKeys;
                }
                ++stats.warmedUpValues;
                break;
            default:
                ++stats.warmedUpKeys;
                ++stats.warmedUpValues;
        }
    } else {
        stopLoading = true;
    }

    if (stopLoading) {
        // warmup has completed, return ENGINE_ENOMEM to
        // cancel remaining data dumps from couchstore
        if (epstore.getWarmup()->setComplete()) {
            epstore.getWarmup()->setWarmupTime();
            epstore.warmupCompleted();
            LOG(EXTENSION_LOG_NOTICE, "Warmup completed in %s",
                    hrtime2text(epstore.getWarmup()->getTime()).c_str());

        }
        LOG(EXTENSION_LOG_NOTICE,
            "Engine warmup is complete, request to stop "
            "loading remaining database");
        setStatus(ENGINE_ENOMEM);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

void LoadStorageKVPairCallback::purge() {
    class EmergencyPurgeVisitor : public VBucketVisitor,
                                  public HashTableVisitor {
    public:
        EmergencyPurgeVisitor(EPBucket& store) :
            epstore(store) {}

        void visitBucket(RCPtr<VBucket> &vb) override {
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                vb->ht.visit(*this);
            }
        }

        void visit(StoredValue *v) override {
            currentBucket->ht.unlocked_ejectItem(v,
                                             epstore.getItemEvictionPolicy());
        }

    private:
        EPBucket& epstore;
        RCPtr<VBucket> currentBucket;
    };

    auto vbucketIds(vbuckets.getBuckets());
    EmergencyPurgeVisitor epv(epstore);
    for (auto vbid : vbucketIds) {
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb) {
            epv.visitBucket(vb);
        }
    }
    hasPurged = true;
}

void LoadValueCallback::callback(CacheLookup &lookup)
{
    if (warmupState == WarmupState::LoadingData) {
        RCPtr<VBucket> vb = vbuckets.getBucket(lookup.getVBucketId());
        if (!vb) {
            return;
        }

        int bucket_num(0);
        auto lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);

        StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num);
        if (v && v->isResident()) {
            setStatus(ENGINE_KEY_EEXISTS);
            return;
        }
    }
    setStatus(ENGINE_SUCCESS);
}

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Implementation of the warmup class                                    //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////


Warmup::Warmup(EPBucket& st, Configuration& config_)
    : state(),
      store(st),
      config(config_),
      startTime(0),
      metadata(0),
      warmup(0),
      threadtask_count(0),
      estimateTime(0),
      estimatedItemCount(std::numeric_limits<size_t>::max()),
      cleanShutdown(true),
      corruptAccessLog(false),
      warmupComplete(false),
      warmupOOMFailure(false),
      estimatedWarmupCount(std::numeric_limits<size_t>::max())
{
    const size_t num_shards = store.vbMap.getNumShards();

    shardVbStates = new std::map<uint16_t, vbucket_state>[num_shards];
    shardVbIds = new std::vector<uint16_t>[num_shards];
    shardKeyDumpStatus = new bool[num_shards];
    for (size_t i = 0; i < num_shards; i++) {
        shardKeyDumpStatus[i] = false;
    }
}

void Warmup::addToTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.insert(taskId);
}

void Warmup::removeFromTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.erase(taskId);
}

Warmup::~Warmup() {
    delete [] shardVbStates;
    delete [] shardVbIds;
    delete [] shardKeyDumpStatus;
}

void Warmup::setEstimatedWarmupCount(size_t to)
{
    estimatedWarmupCount.store(to);
}

size_t Warmup::getEstimatedItemCount()
{
    return estimatedItemCount.load();
}

void Warmup::start(void)
{
    step();
}

void Warmup::stop(void)
{
    {
        LockHolder lh(taskSetMutex);
        if(taskSet.empty()) {
            return;
        }
        for (auto id : taskSet) {
            ExecutorPool::get()->cancel(id);
        }
        taskSet.clear();
    }
    transition(WarmupState::Done, true);
    done();
}

void Warmup::scheduleInitialize()
{
    ExTask task = new WarmupInitialize(store, this);
    ExecutorPool::get()->schedule(task, READER_TASK_IDX);
}

void Warmup::initialize()
{
    startTime.store(gethrtime());

    std::map<std::string, std::string> session_stats;
    store.getOneROUnderlying()->getPersistedStats(session_stats);
    store.getEPEngine().getTapConnMap().loadPrevSessionStats(session_stats);


    std::map<std::string, std::string>::const_iterator it =
        session_stats.find("ep_force_shutdown");

    if (it == session_stats.end() || it->second.compare("false") != 0) {
        cleanShutdown = false;
    }

    populateShardVbStates();
    transition(WarmupState::CreateVBuckets);
}

void Warmup::scheduleCreateVBuckets()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupCreateVBuckets(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }
}

void Warmup::createVBuckets(uint16_t shardId) {
    size_t maxEntries = store.getEPEngine().getMaxFailoverEntries();
    std::map<uint16_t, vbucket_state>& vbStates = shardVbStates[shardId];

    std::map<uint16_t, vbucket_state>::iterator itr;
    for (itr = vbStates.begin(); itr != vbStates.end(); ++itr) {
        uint16_t vbid = itr->first;
        vbucket_state vbs = itr->second;

        RCPtr<VBucket> vb = store.getVBucket(vbid);
        if (!vb) {
            FailoverTable* table;
            if (vbs.failovers.empty()) {
                table = new FailoverTable(maxEntries);
            } else {
                table = new FailoverTable(vbs.failovers, maxEntries);
            }
            KVShard* shard = store.getVBuckets().getShardByVbId(vbid);
            std::shared_ptr<Callback<uint16_t> > cb(new NotifyFlusherCB(shard));
            vb.reset(new VBucket(vbid, vbs.state,
                                 store.getEPEngine().getEpStats(),
                                 store.getEPEngine().getCheckpointConfig(),
                                 shard, vbs.highSeqno, vbs.lastSnapStart,
                                 vbs.lastSnapEnd, table, cb, config,
                                 vbs.state, 1, vbs.purgeSeqno, vbs.maxCas));

            if(vbs.state == vbucket_state_active && !cleanShutdown) {
                if (static_cast<uint64_t>(vbs.highSeqno) == vbs.lastSnapEnd) {
                    vb->failovers->createEntry(vbs.lastSnapEnd);
                } else {
                    vb->failovers->createEntry(vbs.lastSnapStart);
                }
            }

            store.vbMap.addBucket(vb);
        }

        // Pass the open checkpoint Id for each vbucket.
        vb->checkpointManager.setOpenCheckpointId(vbs.checkpointId + 1);
        // Pass the max deleted seqno for each vbucket.
        vb->ht.setMaxDeletedRevSeqno(vbs.maxDeletedSeqno);
        // For each vbucket, set its latest checkpoint Id that was
        // successfully persisted.
        store.vbMap.setPersistenceCheckpointId(vbid, vbs.checkpointId);
        // For each vbucket, set the last persisted seqno checkpoint
        store.vbMap.setPersistenceSeqno(vbid, vbs.highSeqno);
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::EstimateDatabaseItemCount);
    }
}


void Warmup::scheduleEstimateDatabaseItemCount()
{
    threadtask_count = 0;
    estimateTime = 0;
    estimatedItemCount = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupEstimateDatabaseItemCount(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }
}

void Warmup::estimateDatabaseItemCount(uint16_t shardId)
{
    hrtime_t st = gethrtime();
    size_t item_count = 0;

    const std::vector<uint16_t> &vbs = shardVbIds[shardId];
    std::vector<uint16_t>::const_iterator it = vbs.begin();
    for (; it != vbs.end(); ++it) {
        size_t vbItemCount = store.getROUnderlyingByShard(shardId)->
                                                        getItemCount(*it);
        RCPtr<VBucket> vb = store.getVBucket(*it);
        if (vb) {
            vb->ht.numTotalItems = vbItemCount;
        }
        item_count += vbItemCount;
    }

    estimatedItemCount.fetch_add(item_count);
    estimateTime.fetch_add(gethrtime() - st);

    if (++threadtask_count == store.vbMap.getNumShards()) {
        if (store.getItemEvictionPolicy() == VALUE_ONLY) {
            transition(WarmupState::KeyDump);
        } else {
            transition(WarmupState::CheckForAccessLog);
        }
    }
}

void Warmup::scheduleKeyDump()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupKeyDump(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }

}

void Warmup::keyDumpforShard(uint16_t shardId)
{
    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    LoadStorageKVPairCallback *load_cb =
            new LoadStorageKVPairCallback(store, false, state.getState());
    std::shared_ptr<Callback<GetValue> > cb(load_cb);
    std::shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback());

    std::vector<uint16_t>::iterator itr = shardVbIds[shardId].begin();

    for (; itr != shardVbIds[shardId].end(); ++itr) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, *itr, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    ValueFilter::KEYS_ONLY);
        if (ctx) {
            auto errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }

    shardKeyDumpStatus[shardId] = true;

    if (++threadtask_count == store.vbMap.getNumShards()) {
        bool success = false;
        for (size_t i = 0; i < store.vbMap.getNumShards(); i++) {
            if (shardKeyDumpStatus[i]) {
                success = true;
            } else {
                success = false;
                break;
            }
        }

        if (success) {
            transition(WarmupState::CheckForAccessLog);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to dump keys, falling back to full dump");
            transition(WarmupState::LoadingKVPairs);
        }
    }
}

void Warmup::scheduleCheckForAccessLog()
{
    ExTask task = new WarmupCheckforAccessLog(store, this);
    ExecutorPool::get()->schedule(task, READER_TASK_IDX);
}

void Warmup::checkForAccessLog()
{
    metadata.store(gethrtime() - startTime);
    LOG(EXTENSION_LOG_NOTICE, "metadata loaded in %s",
        hrtime2text(metadata.load()).c_str());

    if (store.maybeEnableTraffic()) {
        transition(WarmupState::Done);
    }

    size_t accesslogs = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::string curr = store.accessLog[i]->getLogFile();
        std::string old = store.accessLog[i]->getLogFile();
        old.append(".old");
        if (access(curr.c_str(), F_OK) == 0 ||
            access(old.c_str(), F_OK) == 0) {
            accesslogs++;
        }
    }
    if (accesslogs == store.vbMap.shards.size()) {
        transition(WarmupState::LoadingAccessLog);
    } else {
        if (store.getItemEvictionPolicy() == VALUE_ONLY) {
            transition(WarmupState::LoadingData);
        } else {
            transition(WarmupState::LoadingKVPairs);
        }
    }

}

void Warmup::scheduleLoadingAccessLog()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupLoadAccessLog(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }
}

void Warmup::loadingAccessLog(uint16_t shardId)
{
    LoadStorageKVPairCallback *load_cb =
        new LoadStorageKVPairCallback(store, true, state.getState());
    bool success = false;
    hrtime_t stTime = gethrtime();
    if (store.accessLog[shardId]->exists()) {
        try {
            store.accessLog[shardId]->open();
            if (doWarmup(*(store.accessLog[shardId]),
                         shardVbStates[shardId], *load_cb) != (size_t)-1) {
                success = true;
            }
        } catch (MutationLog::ReadException &e) {
            corruptAccessLog = true;
            LOG(EXTENSION_LOG_WARNING, "Error reading warmup access log:  %s",
                    e.what());
        }
    }

    if (!success) {
        // Do we have the previous file?
        std::string nm = store.accessLog[shardId]->getLogFile();
        nm.append(".old");
        MutationLog old(nm);
        if (old.exists()) {
            try {
                old.open();
                if (doWarmup(old, shardVbStates[shardId],
                             *load_cb) != (size_t)-1) {
                    success = true;
                }
            } catch (MutationLog::ReadException &e) {
                corruptAccessLog = true;
                LOG(EXTENSION_LOG_WARNING, "Error reading old access log:  %s",
                        e.what());
            }
        }
    }

    size_t numItems = store.getEPEngine().getEpStats().warmedUpValues;
    if (success && numItems) {
        LOG(EXTENSION_LOG_NOTICE,
            "%" PRIu64 " items loaded from access log, completed in %s",
            uint64_t(numItems),
            hrtime2text((gethrtime() - stTime) / 1000).c_str());
    } else {
        size_t estimatedCount= store.getEPEngine().getEpStats().warmedUpKeys;
        setEstimatedWarmupCount(estimatedCount);
    }

    delete load_cb;
    if (++threadtask_count == store.vbMap.getNumShards()) {
        if (!store.maybeEnableTraffic()) {
            transition(WarmupState::LoadingData);
        } else {
            transition(WarmupState::Done);
        }

    }
}

size_t Warmup::doWarmup(MutationLog &lf, const std::map<uint16_t,
                        vbucket_state> &vbmap, Callback<GetValue> &cb)
{
    MutationLogHarvester harvester(lf, &store.getEPEngine());
    std::map<uint16_t, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVBucket(it->first);
    }

    // To constrain the number of elements from the access log we have to keep
    // alive (there may be millions of items per-vBucket), process it
    // a batch at a time.
    hrtime_t log_load_duration{};
    hrtime_t log_apply_duration{};
    WarmupCookie cookie(&store, cb);

    auto alog_iter = lf.begin();
    do {
        // Load a chunk of the access log file
        hrtime_t start = gethrtime();
        alog_iter = harvester.loadBatch(alog_iter, config.getWarmupBatchSize());
        log_load_duration += (gethrtime() - start);

        // .. then apply it to the store.
        hrtime_t apply_start = gethrtime();
        if (store.multiBGFetchEnabled()) {
            harvester.apply(&cookie, &batchWarmupCallback);
        } else {
            harvester.apply(&cookie, &warmupCallback);
        }
        log_apply_duration += (gethrtime() - apply_start);
    } while (alog_iter != lf.end());

    size_t total = harvester.total();
    setEstimatedWarmupCount(total);
    LOG(EXTENSION_LOG_DEBUG, "Completed log read in %s with %ld entries",
        hrtime2text(log_load_duration).c_str(), total);

    LOG(EXTENSION_LOG_DEBUG,
        "Populated log in %s with(l: %ld, s: %ld, e: %ld)",
        hrtime2text(log_apply_duration).c_str(), cookie.loaded, cookie.skipped,
        cookie.error);

    return cookie.loaded;
}

void Warmup::scheduleLoadingKVPairs()
{
    // We reach here only if keyDump didn't return SUCCESS or if
    // in case of Full Eviction. Either way, set estimated value
    // count equal to the estimated item count, as very likely no
    // keys have been warmed up at this point.
    setEstimatedWarmupCount(estimatedItemCount);

    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupLoadingKVPairs(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }

}

void Warmup::loadKVPairsforShard(uint16_t shardId)
{
    bool maybe_enable_traffic = false;
    scan_error_t errorCode = scan_success;

    if (store.getItemEvictionPolicy() == FULL_EVICTION) {
        maybe_enable_traffic = true;
    }

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    LoadStorageKVPairCallback *load_cb =
        new LoadStorageKVPairCallback(store, maybe_enable_traffic,
                                      state.getState());
    std::shared_ptr<Callback<GetValue> > cb(load_cb);
    std::shared_ptr<Callback<CacheLookup> >
        cl(new LoadValueCallback(store.vbMap, state.getState()));

    std::vector<uint16_t>::iterator itr = shardVbIds[shardId].begin();
    for (; itr != shardVbIds[shardId].end(); ++itr) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, *itr, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    ValueFilter::VALUES_DECOMPRESSED);
        if (ctx) {
            errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }
    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::Done);
    }
}

void Warmup::scheduleLoadingData()
{
    size_t estimatedCount = store.getEPEngine().getEpStats().warmedUpKeys;
    setEstimatedWarmupCount(estimatedCount);

    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = new WarmupLoadingData(store, i, this);
        ExecutorPool::get()->schedule(task, READER_TASK_IDX);
    }
}

void Warmup::loadDataforShard(uint16_t shardId)
{
    scan_error_t errorCode = scan_success;

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    LoadStorageKVPairCallback *load_cb =
        new LoadStorageKVPairCallback(store, true, state.getState());
    std::shared_ptr<Callback<GetValue> > cb(load_cb);
    std::shared_ptr<Callback<CacheLookup> >
        cl(new LoadValueCallback(store.vbMap, state.getState()));

    std::vector<uint16_t>::iterator itr = shardVbIds[shardId].begin();
    for (; itr != shardVbIds[shardId].end(); ++itr) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, *itr, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    ValueFilter::VALUES_DECOMPRESSED);
        if (ctx) {
            errorCode = kvstore->scan(ctx);
            kvstore->destroyScanContext(ctx);
            if (errorCode == scan_again) { // ENGINE_ENOMEM
                // skip loading remaining VBuckets as memory limit was reached
                break;
            }
        }
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::Done);
    }
}

void Warmup::scheduleCompletion() {
    ExTask task = new WarmupCompletion(store, this);
    ExecutorPool::get()->schedule(task, READER_TASK_IDX);
}

void Warmup::done()
{
    if (setComplete()) {
        setWarmupTime();
        store.warmupCompleted();
        LOG(EXTENSION_LOG_NOTICE, "warmup completed in %s",
                                   hrtime2text(warmup.load()).c_str());
    }
}

void Warmup::step() {
    switch (state.getState()) {
        case WarmupState::Initialize:
            scheduleInitialize();
            break;
        case WarmupState::CreateVBuckets:
            scheduleCreateVBuckets();
            break;
        case WarmupState::EstimateDatabaseItemCount:
            scheduleEstimateDatabaseItemCount();
            break;
        case WarmupState::KeyDump:
            scheduleKeyDump();
            break;
        case WarmupState::CheckForAccessLog:
            scheduleCheckForAccessLog();
            break;
        case WarmupState::LoadingAccessLog:
            scheduleLoadingAccessLog();
            break;
        case WarmupState::LoadingKVPairs:
            scheduleLoadingKVPairs();
            break;
        case WarmupState::LoadingData:
            scheduleLoadingData();
            break;
        case WarmupState::Done:
            scheduleCompletion();
            break;
        default:
            throw std::logic_error("Warmup::step: illegal warmup state:" +
                                   std::to_string(state.getState()));
    }
}

void Warmup::transition(int to, bool force) {
    int old = state.getState();
    if (old != WarmupState::Done) {
        state.transition(to, force);
        step();
    }
}

template <typename T>
void Warmup::addStat(const char *nm, const T &val, ADD_STAT add_stat,
                     const void *c) const {
    std::string name = "ep_warmup";
    if (nm != NULL) {
        name.append("_");
        name.append(nm);
    }

    std::stringstream value;
    value << val;
    add_casted_stat(name.data(), value.str().data(), add_stat, c);
}

void Warmup::addStats(ADD_STAT add_stat, const void *c) const
{
    if (store.getEPEngine().getConfiguration().isWarmup()) {
        EPStats &stats = store.getEPEngine().getEpStats();
        addStat(NULL, "enabled", add_stat, c);
        const char *stateName = state.toString();
        addStat("state", stateName, add_stat, c);
        if (warmupComplete.load()) {
            addStat("thread", "complete", add_stat, c);
        } else {
            addStat("thread", "running", add_stat, c);
        }
        addStat("key_count", stats.warmedUpKeys, add_stat, c);
        addStat("value_count", stats.warmedUpValues, add_stat, c);
        addStat("dups", stats.warmDups, add_stat, c);
        addStat("oom", stats.warmOOM, add_stat, c);
        addStat("min_memory_threshold",
                stats.warmupMemUsedCap * 100.0, add_stat, c);
        addStat("min_item_threshold",
                stats.warmupNumReadCap * 100.0, add_stat, c);

        hrtime_t md_time = metadata.load();
        if (md_time > 0) {
            addStat("keys_time", md_time / 1000, add_stat, c);
        }

        hrtime_t w_time = warmup.load();
        if (w_time > 0) {
            addStat("time", w_time / 1000, add_stat, c);
        }

        size_t itemCount = estimatedItemCount.load();
        if (itemCount == std::numeric_limits<size_t>::max()) {
            addStat("estimated_key_count", "unknown", add_stat, c);
        } else {
            hrtime_t e_time = estimateTime.load();
            if (e_time != 0) {
                addStat("estimate_time", e_time / 1000, add_stat, c);
            }
            addStat("estimated_key_count", itemCount, add_stat, c);
        }

        if (corruptAccessLog) {
            addStat("access_log", "corrupt", add_stat, c);
        }

        size_t warmupCount = estimatedWarmupCount.load();
        if (warmupCount ==  std::numeric_limits<size_t>::max()) {
            addStat("estimated_value_count", "unknown", add_stat, c);
        } else {
            addStat("estimated_value_count", warmupCount, add_stat, c);
        }
   } else {
        addStat(NULL, "disabled", add_stat, c);
    }
}

/* In the case of CouchKVStore, all vbucket states of all the shards are stored
 * in a single instance. ForestKVStore stores only the vbucket states specific
 * to that shard. Hence the vbucket states of all the shards need to be
 * retrieved */
uint16_t Warmup::getNumKVStores()
{
    Configuration& config = store.getEPEngine().getConfiguration();
    if (config.getBackend().compare("couchdb") == 0) {
        return 1;
    } else if (config.getBackend().compare("forestdb") == 0) {
        return config.getMaxNumShards();
    }

    return 0;
}

void Warmup::populateShardVbStates()
{
    uint16_t numKvs = getNumKVStores();

    for (size_t i = 0; i < numKvs; i++) {
        std::vector<vbucket_state *> allVbStates =
                     store.getROUnderlyingByShard(i)->listPersistedVbuckets();
        for (uint16_t vb = 0; vb < allVbStates.size(); vb++) {
            if (!allVbStates[vb] || allVbStates[vb]->state == vbucket_state_dead) {
                continue;
            }
            std::map<uint16_t, vbucket_state> &shardVB =
                shardVbStates[vb % store.vbMap.getNumShards()];
            shardVB.insert(std::pair<uint16_t, vbucket_state>(vb,
                                                          *(allVbStates[vb])));
        }
    }

    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::vector<uint16_t> activeVBs, replicaVBs;
        std::map<uint16_t, vbucket_state>::const_iterator it;
        for (it = shardVbStates[i].begin(); it != shardVbStates[i].end(); ++it) {
            uint16_t vbid = it->first;
            vbucket_state vbs = it->second;
            if (vbs.state == vbucket_state_active) {
                activeVBs.push_back(vbid);
            } else if (vbs.state == vbucket_state_replica) {
                replicaVBs.push_back(vbid);
            }
        }

        // Push one active VB to the front.
        // When the ratio of RAM to VBucket is poor (big vbuckets) this will
        // ensure we at least bring active data in before replicas eat RAM.
        if (!activeVBs.empty()) {
            shardVbIds[i].push_back(activeVBs.back());
            activeVBs.pop_back();
        }

        // Now the VB lottery can begin.
        // Generate a psudeo random, weighted list of active/replica vbuckets.
        // The random seed is the shard ID so that re-running warmup
        // for the same shard and vbucket set always gives the same output and keeps
        // nodes of the cluster more equal after a warmup.

        std::mt19937 twister(i);
        // Give 'true' (aka active) 60% of the time
        // Give 'false' (aka replica) 40% of the time.
        std::bernoulli_distribution distribute(0.6);
        std::array<std::vector<uint16_t>*, 2> activeReplicaSource = {{&activeVBs,
                                                                      &replicaVBs}};

        while (!activeVBs.empty() || !replicaVBs.empty()) {
            const bool active = distribute(twister);
            int num = active ? 0 : 1;
            if (!activeReplicaSource[num]->empty()) {
                shardVbIds[i].push_back(activeReplicaSource[num]->back());
                activeReplicaSource[num]->pop_back();
            } else {
                // Once active or replica set is empty, just drain the other one.
                num = num ^ 1;
                while (!activeReplicaSource[num]->empty()) {
                    shardVbIds[i].push_back(activeReplicaSource[num]->back());
                    activeReplicaSource[num]->pop_back();
                }
            }
        }
    }
}
