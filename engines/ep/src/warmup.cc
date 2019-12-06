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

#include "bucket_logger.h"
#include "callbacks.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "common.h"
#include "connmap.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "executorpool.h"
#include "failover-table.h"
#include "item.h"
#include "mutation_log.h"
#include "statwriter.h"
#include "vb_visitors.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <phosphor/phosphor.h>
#include <platform/dirutils.h>
#include <platform/timeutils.h>
#include <utilities/logtags.h>

#include <array>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>

struct WarmupCookie {
    WarmupCookie(EPBucket* s, StatusCallback<GetValue>& c)
        : cb(c), epstore(s), loaded(0), skipped(0), error(0) { /* EMPTY */
    }
    StatusCallback<GetValue>& cb;
    EPBucket* epstore;
    size_t loaded;
    size_t skipped;
    size_t error;
};

void logWarmupStats(EPBucket& epstore) {
    EPStats& stats = epstore.getEPEngine().getEpStats();
    std::chrono::duration<double, std::chrono::seconds::period> seconds =
            epstore.getWarmup()->getTime();
    double keys_per_seconds = stats.warmedUpValues / seconds.count();
    double megabytes = stats.getPreciseTotalMemoryUsed() / 1.0e6;
    double megabytes_per_seconds = megabytes / seconds.count();
    EP_LOG_INFO(
            "Warmup completed: {} keys and {} values loaded in {} ({} keys/s), "
            "mem_used now at {} MB ({} MB/s)",
            stats.warmedUpKeys,
            stats.warmedUpValues,
            cb::time2text(
                    std::chrono::nanoseconds(epstore.getWarmup()->getTime())),
            keys_per_seconds,
            megabytes,
            megabytes_per_seconds);
}

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Helper class used to insert data into the epstore                     //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public StatusCallback<GetValue> {
public:
    LoadStorageKVPairCallback(EPBucket& ep,
                              bool maybeEnableTraffic,
                              WarmupState::State warmupState);

    void callback(GetValue& val) override;

private:
    bool shouldEject() const;

    void purge();

    VBucketMap& vbuckets;
    EPStats& stats;
    EPBucket& epstore;
    time_t startTime;
    bool hasPurged;

    /// If true, call EPBucket::maybeEnableTraffic() after each KV pair loaded.
    const bool maybeEnableTraffic;

    WarmupState::State warmupState;
};

class LoadValueCallback : public StatusCallback<CacheLookup> {
public:
    LoadValueCallback(VBucketMap& vbMap, WarmupState::State warmupState)
        : vbuckets(vbMap), warmupState(warmupState) {
    }

    void callback(CacheLookup& lookup) override;

private:
    VBucketMap& vbuckets;
    WarmupState::State warmupState;
};

// Warmup Tasks ///////////////////////////////////////////////////////////////

class WarmupInitialize : public GlobalTask {
public:
    WarmupInitialize(EPBucket& st, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupInitialize, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return "Warmup - initialize";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Typically takes single-digits ms.
        return std::chrono::milliseconds(50);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupInitialize");
        _warmup->initialize();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupCreateVBuckets : public GlobalTask {
public:
    WarmupCreateVBuckets(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupCreateVBuckets, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - creating vbuckets: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // VB creation typically takes some 10s of milliseconds.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCreateVBuckets");
        _warmup->createVBuckets(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingCollectionCounts : public GlobalTask {
public:
    WarmupLoadingCollectionCounts(EPBucket& st, uint16_t sh, Warmup& w)
        : GlobalTask(&st.getEPEngine(),
                     TaskId::WarmupLoadingCollectionCounts,
                     0,
                     false),
          shardId(sh),
          warmup(w) {
        warmup.addToTaskSet(uid);
    }

    std::string getDescription() override {
        return "Warmup - loading collection counts: shard " +
               std::to_string(shardId);
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // This task has to open each VB's data-file and (certainly for
        // couchstore) read a small document per defined collection
        return std::chrono::seconds(10);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingCollectionCounts");
        warmup.loadCollectionStatsForShard(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
};

class WarmupEstimateDatabaseItemCount : public GlobalTask {
public:
    WarmupEstimateDatabaseItemCount(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(),
                     TaskId::WarmupEstimateDatabaseItemCount,
                     0,
                     false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - estimate item count: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Typically takes a few 10s of milliseconds (need to open kstore files
        // and read statistics.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarpupEstimateDatabaseItemCount");
        _warmup->estimateDatabaseItemCount(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

/**
 * Warmup task which loads any prepared SyncWrites which are not yet marked
 * as Committed (or Aborted) from disk.
 */
class WarmupLoadPreparedSyncWrites : public GlobalTask {
public:
    WarmupLoadPreparedSyncWrites(EventuallyPersistentEngine* engine,
                                 uint16_t shard,
                                 Warmup& warmup)
        : GlobalTask(engine, TaskId::WarmupLoadPreparedSyncWrites, 0, false),
          shardId(shard),
          warmup(warmup),
          description("Warmup - loading prepared SyncWrites: shard " +
                      std::to_string(shardId)){};

    std::string getDescription() override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is a function of how many prepared sync writes exist in the
        // buckets for this shard - can be minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::minutes(10);
    }

    bool run() override {
        TRACE_EVENT1("ep-engine/task",
                     "WarmupLoadPreparedSyncWrites",
                     "shard",
                     shardId);
        warmup.loadPreparedSyncWrites(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
    const std::string description;
};

/**
 * Warmup task which moves all warmed-up VBuckets into the bucket's vbMap
 */
class WarmupPopulateVBucketMap : public GlobalTask {
public:
    WarmupPopulateVBucketMap(EPBucket& st, uint16_t shard, Warmup& warmup)
        : GlobalTask(&st.getEPEngine(),
                     TaskId::WarmupPopulateVBucketMap,
                     0,
                     false),
          shardId(shard),
          warmup(warmup),
          description("Warmup - populate VB Map: shard " +
                      std::to_string(shardId)){};

    std::string getDescription() override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is expected to be quick, we're just adding pointers to a map
        // with some locking
        return std::chrono::milliseconds(1);
    }

    bool run() override {
        TRACE_EVENT1(
                "ep-engine/task", "WarmupPopulateVBucketMap", "shard", shardId);
        warmup.populateVBucketMap(shardId);
        warmup.removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t shardId;
    Warmup& warmup;
    const std::string description;
};

class WarmupKeyDump : public GlobalTask {
public:
    WarmupKeyDump(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupKeyDump, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - key dump: shard " + std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is a function of the number of keys in the database; can be
        // many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() override {
        TRACE_EVENT1("ep-engine/task", "WarmupKeyDump", "shard", _shardId);
        _warmup->keyDumpforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupCheckforAccessLog : public GlobalTask {
public:
    WarmupCheckforAccessLog(EPBucket& st, Warmup* w)
        : GlobalTask(
                  &st.getEPEngine(), TaskId::WarmupCheckforAccessLog, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return "Warmup - check for access log";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Checking for the access log is a disk task (so can take a variable
        // amount of time), however it should be relatively quick as we are
        // just checking files exist.
        return std::chrono::milliseconds(100);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCheckForAccessLog");
        _warmup->checkForAccessLog();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupLoadAccessLog : public GlobalTask {
public:
    WarmupLoadAccessLog(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadAccessLog, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading access log: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is a function of the number of keys in the access log files;
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadAccessLog");
        _warmup->loadingAccessLog(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingKVPairs : public GlobalTask {
public:
    WarmupLoadingKVPairs(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingKVPairs, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading KV Pairs: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is a function of the number of documents which can
        // be held in RAM (and need to be laoded from disk),
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingKVPairs");
        _warmup->loadKVPairsforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupLoadingData : public GlobalTask {
public:
    WarmupLoadingData(EPBucket& st, uint16_t sh, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingData, 0, false),
          _shardId(sh),
          _warmup(w),
          _description("Warmup - loading data: shard " +
                       std::to_string(_shardId)) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return _description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime is a function of the number of documents which can
        // be held in RAM (and need to be laoded from disk),
        // can be many minutes in large datasets.
        // Given this large variation; set max duration to a "way out" value
        // which we don't expect to see.
        return std::chrono::hours(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupLoadingData");
        _warmup->loadDataforShard(_shardId);
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
    const std::string _description;
};

class WarmupCompletion : public GlobalTask {
public:
    WarmupCompletion(EPBucket& st, Warmup* w)
        : GlobalTask(&st.getEPEngine(), TaskId::WarmupCompletion, 0, false),
          _warmup(w) {
        _warmup->addToTaskSet(uid);
    }

    std::string getDescription() override {
        return "Warmup - completion";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // This task should be very quick - just the final warmup steps.
        return std::chrono::milliseconds(1);
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "WarmupCompletion");
        _warmup->done();
        _warmup->removeFromTaskSet(uid);
        return false;
    }

private:
    Warmup* _warmup;
};

static bool batchWarmupCallback(Vbid vbId,
                                const std::set<StoredDocKey>& fetches,
                                void* arg) {
    WarmupCookie *c = static_cast<WarmupCookie *>(arg);

    if (!c->epstore->maybeEnableTraffic()) {
        vb_bgfetch_queue_t items2fetch;
        for (auto& key : fetches) {
            // Access log only records Committed keys, therefore construct
            // DiskDocKey with pending == false.
            DiskDocKey diskKey{key, /*prepared*/ false};
            // Deleted below via a unique_ptr in the next loop
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items2fetch[diskKey];
            bg_itm_ctx.isMetaOnly = GetMetaOnly::No;
            bg_itm_ctx.bgfetched_list.emplace_back(
                    std::make_unique<VBucketBGFetchItem>(nullptr, false));
            bg_itm_ctx.bgfetched_list.back()->value = &bg_itm_ctx.value;
        }

        c->epstore->getROUnderlying(vbId)->getMulti(vbId, items2fetch);

        // applyItem controls the  mode this loop operates in.
        // true we will attempt the callback (attempt a HashTable insert)
        // false we don't attempt the callback
        // in both cases the loop must delete the VBucketBGFetchItem we
        // allocated above.
        bool applyItem = true;
        for (auto& items : items2fetch) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = items.second;
            std::unique_ptr<VBucketBGFetchItem> fetchedItem(
                    std::move(bg_itm_ctx.bgfetched_list.back()));
            if (applyItem) {
                GetValue& val = *fetchedItem->value;
                if (val.getStatus() == ENGINE_SUCCESS) {
                    // NB: callback will delete the GetValue's Item
                    c->cb.callback(val);
                } else {
                    EP_LOG_WARN(
                            "Warmup failed to load data for {}"
                            " key{{{}}} error = {}",
                            vbId,
                            cb::UserData{items.first.to_string()},
                            val.getStatus());
                    c->error++;
                }

                if (c->cb.getStatus() == ENGINE_SUCCESS) {
                    c->loaded++;
                } else {
                    // Failed to apply an Item, so fail the rest
                    applyItem = false;
                }
            } else {
                c->skipped++;
            }
        }

        return true;
    } else {
        c->skipped++;
        return false;
    }
}

const char *WarmupState::toString(void) const {
    return getStateDescription(state.load());
}

const char* WarmupState::getStateDescription(State st) const {
    switch (st) {
    case State::Initialize:
        return "initialize";
    case State::CreateVBuckets:
        return "creating vbuckets";
    case State::LoadingCollectionCounts:
        return "loading collection counts";
    case State::EstimateDatabaseItemCount:
        return "estimating database item count";
    case State::LoadPreparedSyncWrites:
        return "loading prepared SyncWrites";
    case State::PopulateVBucketMap:
        return "populating vbucket map";
    case State::KeyDump:
        return "loading keys";
    case State::CheckForAccessLog:
        return "determine access log availability";
    case State::LoadingAccessLog:
        return "loading access log";
    case State::LoadingKVPairs:
        return "loading k/v pairs";
    case State::LoadingData:
        return "loading data";
    case State::Done:
        return "done";
    }
    return "Illegal state";
}

void WarmupState::transition(State to, bool allowAnystate) {
    if (allowAnystate || legalTransition(to)) {
        EP_LOG_DEBUG("Warmup transition from state \"{}\" to \"{}\"",
                     getStateDescription(state.load()),
                     getStateDescription(to));
        state.store(to);
    } else {
        // Throw an exception to make it possible to test the logic ;)
        std::stringstream ss;
        ss << "Illegal state transition from \"" << *this << "\" to "
           << getStateDescription(to) << "(" << int(to) << ")";
        throw std::runtime_error(ss.str());
    }
}

bool WarmupState::legalTransition(State to) const {
    switch (state.load()) {
    case State::Initialize:
        return (to == State::CreateVBuckets);
    case State::CreateVBuckets:
        return (to == State::LoadingCollectionCounts);
    case State::LoadingCollectionCounts:
        return (to == State::EstimateDatabaseItemCount);
    case State::EstimateDatabaseItemCount:
        return (to == State::LoadPreparedSyncWrites);
    case State::LoadPreparedSyncWrites:
        return (to == State::PopulateVBucketMap);
    case State::PopulateVBucketMap:
        return (to == State::KeyDump || to == State::CheckForAccessLog);
    case State::KeyDump:
        return (to == State::LoadingKVPairs || to == State::CheckForAccessLog);
    case State::CheckForAccessLog:
        return (to == State::LoadingAccessLog || to == State::LoadingData ||
                to == State::LoadingKVPairs || to == State::Done);
    case State::LoadingAccessLog:
        return (to == State::Done || to == State::LoadingData);
    case State::LoadingKVPairs:
        return (to == State::Done);
    case State::LoadingData:
        return (to == State::Done);
    case State::Done:
        return false;
    }

    return false;
}

std::ostream& operator <<(std::ostream &out, const WarmupState &state)
{
    out << state.toString();
    return out;
}

LoadStorageKVPairCallback::LoadStorageKVPairCallback(
        EPBucket& ep, bool maybeEnableTraffic, WarmupState::State warmupState)
    : vbuckets(ep.vbMap),
      stats(ep.getEPEngine().getEpStats()),
      epstore(ep),
      startTime(ep_real_time()),
      hasPurged(false),
      maybeEnableTraffic(maybeEnableTraffic),
      warmupState(warmupState) {
}

void LoadStorageKVPairCallback::callback(GetValue &val) {
    // This callback method is responsible for deleting the Item
    std::unique_ptr<Item> i(std::move(val.item));

    // Don't attempt to load the system event documents.
    if (i->getKey().getCollectionID() == CollectionID::System) {
        return;
    }

    // Prepared SyncWrites are ignored here  -
    // they are handled in the earlier warmup State::LoadPreparedSyncWrites
    if (i->isPending()) {
        return;
    }

    bool stopLoading = false;
    if (i != NULL && !epstore.getWarmup()->isComplete()) {
        VBucketPtr vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            setStatus(ENGINE_NOT_MY_VBUCKET);
            return;
        }
        bool succeeded(false);
        int retry = 2;
        do {
            if (i->getCas() == static_cast<uint64_t>(-1)) {
                if (val.isPartial()) {
                    i->setCas(0);
                } else {
                    i->setCas(vb->nextHLCCas());
                }
            }

            EPVBucket* epVb = dynamic_cast<EPVBucket*>(vb.get());
            if (!epVb) {
                setStatus(ENGINE_NOT_MY_VBUCKET);
                return;
            }

            const auto res =
                    epVb->insertFromWarmup(*i, shouldEject(), val.isPartial());
            switch (res) {
            case MutationStatus::NoMem:
                if (retry == 2) {
                    if (hasPurged) {
                        if (++stats.warmOOM == 1) {
                            EP_LOG_WARN(
                                    "Warmup dataload failure: max_size too "
                                    "low.");
                        }
                    } else {
                        EP_LOG_WARN(
                                "Emergency startup purge to free space for "
                                "load.");
                        purge();
                    }
                } else {
                    EP_LOG_WARN("Cannot store an item after emergency purge.");
                    ++stats.warmOOM;
                }
                break;
            case MutationStatus::InvalidCas:
                EP_LOG_DEBUG(
                        "Value changed in memory before restore from disk. "
                        "Ignored disk value for: key{{{}}}.",
                        i->getKey().c_str());
                ++stats.warmDups;
                succeeded = true;
                break;
            case MutationStatus::NotFound:
                succeeded = true;
                break;
            default:
                throw std::logic_error(
                        "LoadStorageKVPairCallback::callback: "
                        "Unexpected result from HashTable::insert: " +
                        std::to_string(static_cast<uint16_t>(res)));
            }
        } while (!succeeded && retry-- > 0);

        if (maybeEnableTraffic) {
            stopLoading = epstore.maybeEnableTraffic();
        }

        switch (warmupState) {
        case WarmupState::State::KeyDump:
            if (stats.warmOOM) {
                epstore.getWarmup()->setOOMFailure();
                stopLoading = true;
            } else {
                ++stats.warmedUpKeys;
            }
            break;
        case WarmupState::State::LoadingData:
        case WarmupState::State::LoadingAccessLog:
            if (epstore.getItemEvictionPolicy() == EvictionPolicy::Full) {
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
            logWarmupStats(epstore);
        }
        EP_LOG_INFO(
                "Engine warmup is complete, request to stop "
                "loading remaining database");
        setStatus(ENGINE_ENOMEM);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

bool LoadStorageKVPairCallback::shouldEject() const {
    return stats.getEstimatedTotalMemoryUsed() >= stats.mem_low_wat;
}

void LoadStorageKVPairCallback::purge() {
    class EmergencyPurgeVisitor : public VBucketVisitor,
                                  public HashTableVisitor {
    public:
        EmergencyPurgeVisitor(EPBucket& store) : epstore(store) {
        }

        void visitBucket(const VBucketPtr& vb) override {
            if (vBucketFilter(vb->getId())) {
                currentBucket = vb;
                vb->ht.visit(*this);
            }
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            StoredValue* vPtr = &v;
            currentBucket->ht.unlocked_ejectItem(
                    lh, vPtr, epstore.getItemEvictionPolicy());
            return true;
        }

    private:
        EPBucket& epstore;
        VBucketPtr currentBucket;
    };

    auto vbucketIds(vbuckets.getBuckets());
    EmergencyPurgeVisitor epv(epstore);
    for (auto vbid : vbucketIds) {
        VBucketPtr vb = vbuckets.getBucket(vbid);
        if (vb) {
            epv.visitBucket(vb);
        }
    }
    hasPurged = true;
}

void LoadValueCallback::callback(CacheLookup &lookup)
{
    if (warmupState != WarmupState::State::LoadingData) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    // Prepared SyncWrites are ignored in the normal LoadValueCallback -
    // they are handled in an earlier warmup phase so return ENGINE_KEY_EEXISTS
    // to indicate this key should be skipped.
    if (lookup.getKey().isPrepared()) {
        setStatus(ENGINE_KEY_EEXISTS);
        return;
    }

    VBucketPtr vb = vbuckets.getBucket(lookup.getVBucketId());
    if (!vb) {
        return;
    }

    // We explicitly want the committedSV (if exists).
    auto res = vb->ht.findOnlyCommitted(lookup.getKey().getDocKey());
    if (res.storedValue && res.storedValue->isResident()) {
        // Already resident in memory - skip loading from disk.
        setStatus(ENGINE_KEY_EEXISTS);
        return;
    }

    // Otherwise - item value not in hashTable - continue with disk load.
    setStatus(ENGINE_SUCCESS);
}

//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Implementation of the warmup class                                    //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

Warmup::Warmup(EPBucket& st, Configuration& config_)
    : store(st),
      config(config_),
      shardVbStates(store.vbMap.getNumShards()),
      shardVbIds(store.vbMap.getNumShards()),
      warmedUpVbuckets(config.getMaxVbuckets()) {
}

Warmup::~Warmup() = default;

void Warmup::addToTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.insert(taskId);
}

void Warmup::removeFromTaskSet(size_t taskId) {
    LockHolder lh(taskSetMutex);
    taskSet.erase(taskId);
}

void Warmup::setEstimatedWarmupCount(size_t to)
{
    estimatedWarmupCount.store(to);
}

size_t Warmup::getEstimatedItemCount() const {
    return estimatedItemCount.load();
}

void Warmup::start() {
    step();
}

void Warmup::stop() {
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
    transition(WarmupState::State::Done, true);
    done();
}

void Warmup::scheduleInitialize()
{
    ExTask task = std::make_shared<WarmupInitialize>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::initialize()
{
    {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        warmupStart.time = std::chrono::steady_clock::now();
    }

    std::map<std::string, std::string> session_stats;
    store.getOneROUnderlying()->getPersistedStats(session_stats);


    std::map<std::string, std::string>::const_iterator it =
        session_stats.find("ep_force_shutdown");

    if (it == session_stats.end() || it->second.compare("false") != 0) {
        cleanShutdown = false;
    }

    populateShardVbStates();
    transition(WarmupState::State::CreateVBuckets);
}

void Warmup::scheduleCreateVBuckets()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupCreateVBuckets>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::createVBuckets(uint16_t shardId) {
    size_t maxEntries = store.getEPEngine().getMaxFailoverEntries();

    // Iterate over all VBucket states defined for this shard, creating VBucket
    // objects if they do not already exist.
    for (const auto& itr : shardVbStates[shardId]) {
        Vbid vbid = itr.first;
        const vbucket_state& vbs = itr.second;

        // Collections and sync-repl requires that the VBucket datafiles have
        // 'namespacing' applied to the key space
        if (!vbs.supportsNamespaces) {
            EP_LOG_CRITICAL(
                    "Warmup::createVBuckets aborting warmup as {} datafile "
                    "is unusable, name-spacing is not enabled.",
                    vbid);
            return;
        }

        VBucketPtr vb = store.getVBucket(vbid);
        if (!vb) {
            std::unique_ptr<FailoverTable> table;
            if (vbs.transition.failovers.empty()) {
                table = std::make_unique<FailoverTable>(maxEntries);
            } else {
                table = std::make_unique<FailoverTable>(
                        vbs.transition.failovers, maxEntries, vbs.highSeqno);
            }
            KVShard* shard = store.getVBuckets().getShardByVbId(vbid);

            std::unique_ptr<Collections::VB::Manifest> manifest;
            if (config.isCollectionsEnabled()) {
                manifest = std::make_unique<Collections::VB::Manifest>(
                        store.getROUnderlyingByShard(shardId)
                                ->getCollectionsManifest(vbid));
            } else {
                manifest = std::make_unique<Collections::VB::Manifest>();
            }

            vb = store.makeVBucket(vbid,
                                   vbs.transition.state,
                                   shard,
                                   std::move(table),
                                   std::make_unique<NotifyNewSeqnoCB>(store),
                                   std::move(manifest),
                                   vbs.transition.state,
                                   vbs.highSeqno,
                                   vbs.lastSnapStart,
                                   vbs.lastSnapEnd,
                                   vbs.purgeSeqno,
                                   vbs.maxCas,
                                   vbs.hlcCasEpochSeqno,
                                   vbs.mightContainXattrs,
                                   vbs.transition.replicationTopology,
                                   vbs.maxVisibleSeqno);

            if (vbs.transition.state == vbucket_state_active &&
                !cleanShutdown) {
                if (static_cast<uint64_t>(vbs.highSeqno) == vbs.lastSnapEnd) {
                    vb->failovers->createEntry(vbs.lastSnapEnd);
                } else {
                    vb->failovers->createEntry(vbs.lastSnapStart);
                }
                auto entry = vb->failovers->getLatestEntry();
                EP_LOG_INFO(
                        "Warmup::createVBuckets: {} created new failover entry "
                        "with uuid:{} and seqno:{} due to unclean shutdown",
                        vbid,
                        entry.vb_uuid,
                        entry.by_seqno);
            }
            EPBucket* bucket = &this->store;
            vb->setFreqSaturatedCallback(
                    [bucket]() { bucket->wakeItemFreqDecayerTask(); });

            // Add the new vbucket to our local map, it will later be added
            // to the bucket's vbMap once the vbuckets are fully initialised
            // from KVStore data
            warmedUpVbuckets.insert(std::make_pair(vbid.get(), vb));
        }

        // Initial checkpoint for an active vbucket has an ID of 2 (see
        // VBucket::setState which does the same when a new vbucket is created)
        if (vbs.state == vbucket_state_active) {
            vb->checkpointManager->setOpenCheckpointId(2);
        }

        // Pass the max deleted seqno for each vbucket.
        vb->ht.setMaxDeletedRevSeqno(vbs.maxDeletedSeqno);

        // For each vbucket, set the last persisted seqno checkpoint
        vb->setPersistenceSeqno(vbs.highSeqno);
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::State::LoadingCollectionCounts);
    }
}

void Warmup::processCreateVBucketsComplete() {
    std::unique_lock<std::mutex> lock(pendingCookiesMutex);
    createVBucketsComplete = true;
    if (!pendingCookies.empty()) {
        EP_LOG_INFO(
                "Warmup::processCreateVBucketsComplete unblocking {} cookie(s)",
                pendingCookies.size());
        while (!pendingCookies.empty()) {
            const void* c = pendingCookies.front();
            pendingCookies.pop_front();
            // drop lock to avoid lock inversion
            lock.unlock();
            store.getEPEngine().notifyIOComplete(c, ENGINE_SUCCESS);
            lock.lock();
        }
    }
}

bool Warmup::maybeWaitForVBucketWarmup(const void* cookie) {
    std::lock_guard<std::mutex> lg(pendingCookiesMutex);
    if (!createVBucketsComplete) {
        pendingCookies.push_back(cookie);
        return true;
    }
    return false;
}

void Warmup::scheduleLoadingCollectionCounts() {
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadingCollectionCounts>(
                store, i, *this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadCollectionStatsForShard(uint16_t shardId) {
    // get each VB in the shard and iterate its collections manifest
    // load the _local doc count value

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    // Iterate the VBs in the shard
    for (const auto vbid : shardVbIds[shardId]) {
        auto itr = warmedUpVbuckets.find(vbid.get());
        if (itr == warmedUpVbuckets.end()) {
            continue;
        }

        auto wh = itr->second->getManifest().wlock();
        auto kvstoreContext = kvstore->makeFileHandle(vbid);
        // For each collection in the VB, get its stats
        for (auto& collection : wh) {
            auto stats = kvstore->getCollectionStats(*kvstoreContext,
                                                     collection.first);
            collection.second.setDiskCount(stats.itemCount);
            collection.second.setPersistedHighSeqno(stats.highSeqno);
            // Set the in memory high seqno - might be 0 in the case of the
            // default collection so we have to reset the monotonic value
            collection.second.resetHighSeqno(stats.highSeqno);
        }
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::State::EstimateDatabaseItemCount);
    }
}

void Warmup::scheduleEstimateDatabaseItemCount()
{
    threadtask_count = 0;
    estimateTime.store(std::chrono::steady_clock::duration::zero());
    estimatedItemCount = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupEstimateDatabaseItemCount>(
                store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::estimateDatabaseItemCount(uint16_t shardId)
{
    auto st = std::chrono::steady_clock::now();
    size_t item_count = 0;

    for (const auto vbid : shardVbIds[shardId]) {
        size_t vbItemCount = store.getROUnderlyingByShard(shardId)->
                                                        getItemCount(vbid);
        const auto* vbState =
                store.getROUnderlyingByShard(shardId)->getVBucketState(vbid);
        Expects(vbState);

        // We don't want to include the number of prepares on disk in the number
        // of items in the vBucket/Bucket that is displayed to the user so
        // subtract the number of prepares from the number of on disk items.
        vbItemCount -= vbState->onDiskPrepares;

        auto itr = warmedUpVbuckets.find(vbid.get());
        if (itr != warmedUpVbuckets.end()) {
            itr->second->setNumTotalItems(vbItemCount);
        }
        item_count += vbItemCount;
    }

    estimatedItemCount.fetch_add(item_count);
    estimateTime.fetch_add(std::chrono::steady_clock::now() - st);

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::State::LoadPreparedSyncWrites);
    }
}

void Warmup::scheduleLoadPreparedSyncWrites() {
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadPreparedSyncWrites>(
                &store.getEPEngine(), i, *this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadPreparedSyncWrites(uint16_t shardId) {
    for (const auto vbid : shardVbIds[shardId]) {
        auto itr = warmedUpVbuckets.find(vbid.get());
        if (itr == warmedUpVbuckets.end()) {
            continue;
        }

        // Our EPBucket function will do the load for us as we re-use the code
        // for rollback.
        auto& vb = *(itr->second);
        folly::SharedMutex::WriteHolder vbStateLh(vb.getStateLock());

        auto result = store.loadPreparedSyncWrites(vbStateLh, vb);
        store.getEPEngine()
                .getEpStats()
                .warmupItemsVisitedWhilstLoadingPrepares += result.itemsVisited;
        store.getEPEngine().getEpStats().warmedUpPrepares +=
                result.preparesLoaded;
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::State::PopulateVBucketMap);
    }
}

void Warmup::schedulePopulateVBucketMap() {
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task =
                std::make_shared<WarmupPopulateVBucketMap>(store, i, *this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::populateVBucketMap(uint16_t shardId) {
    for (const auto vbid : shardVbIds[shardId]) {
        auto itr = warmedUpVbuckets.find(vbid.get());
        if (itr != warmedUpVbuckets.end()) {
            store.vbMap.addBucket(itr->second);
        }
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        warmedUpVbuckets.clear();
        // Once we have populated the VBMap we can allow setVB state changes
        processCreateVBucketsComplete();
        if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
            transition(WarmupState::State::KeyDump);
        } else {
            transition(WarmupState::State::CheckForAccessLog);
        }
    }
}

void Warmup::scheduleKeyDump()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupKeyDump>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }

}

void Warmup::keyDumpforShard(uint16_t shardId)
{
    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, false, state.getState());
    auto cl = std::make_shared<NoLookupCallback>();

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
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

    if (++threadtask_count == store.vbMap.getNumShards()) {
        transition(WarmupState::State::CheckForAccessLog);
    }
}

void Warmup::scheduleCheckForAccessLog()
{
    ExTask task = std::make_shared<WarmupCheckforAccessLog>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::checkForAccessLog()
{
    {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        metadata.store(std::chrono::steady_clock::now() - warmupStart.time);
    }
    EP_LOG_INFO("metadata loaded in {}",
                cb::time2text(std::chrono::nanoseconds(metadata.load())));

    if (store.maybeEnableTraffic()) {
        transition(WarmupState::State::Done);
    }

    size_t accesslogs = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::string curr = store.accessLog[i].getLogFile();
        std::string old = store.accessLog[i].getLogFile();
        old.append(".old");
        if (cb::io::isFile(curr) || cb::io::isFile(old)) {
            accesslogs++;
        }
    }
    if (accesslogs == store.vbMap.shards.size()) {
        transition(WarmupState::State::LoadingAccessLog);
    } else {
        if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
            transition(WarmupState::State::LoadingData);
        } else {
            transition(WarmupState::State::LoadingKVPairs);
        }
    }

}

void Warmup::scheduleLoadingAccessLog()
{
    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadAccessLog>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadingAccessLog(uint16_t shardId)
{
    LoadStorageKVPairCallback load_cb(store, true, state.getState());
    bool success = false;
    auto stTime = std::chrono::steady_clock::now();
    if (store.accessLog[shardId].exists()) {
        try {
            store.accessLog[shardId].open();
            if (doWarmup(store.accessLog[shardId],
                         shardVbStates[shardId],
                         load_cb) != (size_t)-1) {
                success = true;
            }
        } catch (MutationLog::ReadException &e) {
            corruptAccessLog = true;
            EP_LOG_WARN("Error reading warmup access log:  {}", e.what());
        }
    }

    if (!success) {
        // Do we have the previous file?
        std::string nm = store.accessLog[shardId].getLogFile();
        nm.append(".old");
        MutationLog old(nm);
        if (old.exists()) {
            try {
                old.open();
                if (doWarmup(old, shardVbStates[shardId], load_cb) !=
                    (size_t)-1) {
                    success = true;
                }
            } catch (MutationLog::ReadException &e) {
                corruptAccessLog = true;
                EP_LOG_WARN("Error reading old access log:  {}", e.what());
            }
        }
    }

    size_t numItems = store.getEPEngine().getEpStats().warmedUpValues;
    if (success && numItems) {
        EP_LOG_INFO("{} items loaded from access log, completed in {}",
                    uint64_t(numItems),
                    cb::time2text(std::chrono::steady_clock::now() - stTime));
    } else {
        size_t estimatedCount= store.getEPEngine().getEpStats().warmedUpKeys;
        setEstimatedWarmupCount(estimatedCount);
    }

    if (++threadtask_count == store.vbMap.getNumShards()) {
        if (!store.maybeEnableTraffic()) {
            transition(WarmupState::State::LoadingData);
        } else {
            transition(WarmupState::State::Done);
        }

    }
}

size_t Warmup::doWarmup(MutationLog& lf,
                        const std::map<Vbid, vbucket_state>& vbmap,
                        StatusCallback<GetValue>& cb) {
    MutationLogHarvester harvester(lf, &store.getEPEngine());
    std::map<Vbid, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVBucket(it->first);
    }

    // To constrain the number of elements from the access log we have to keep
    // alive (there may be millions of items per-vBucket), process it
    // a batch at a time.
    std::chrono::nanoseconds log_load_duration{};
    std::chrono::nanoseconds log_apply_duration{};
    WarmupCookie cookie(&store, cb);

    auto alog_iter = lf.begin();
    do {
        // Load a chunk of the access log file
        auto start = std::chrono::steady_clock::now();
        alog_iter = harvester.loadBatch(alog_iter, config.getWarmupBatchSize());
        log_load_duration += (std::chrono::steady_clock::now() - start);

        // .. then apply it to the store.
        auto apply_start = std::chrono::steady_clock::now();
        harvester.apply(&cookie, &batchWarmupCallback);
        log_apply_duration += (std::chrono::steady_clock::now() - apply_start);
    } while (alog_iter != lf.end());

    size_t total = harvester.total();
    setEstimatedWarmupCount(total);
    EP_LOG_DEBUG("Completed log read in {} with {} entries",
                 cb::time2text(log_load_duration),
                 total);

    EP_LOG_DEBUG("Populated log in {} with(l: {}, s: {}, e: {})",
                 cb::time2text(log_apply_duration),
                 cookie.loaded,
                 cookie.skipped,
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
        ExTask task = std::make_shared<WarmupLoadingKVPairs>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }

}

void Warmup::loadKVPairsforShard(uint16_t shardId)
{
    bool maybe_enable_traffic = false;
    scan_error_t errorCode = scan_success;

    if (store.getItemEvictionPolicy() == EvictionPolicy::Full) {
        maybe_enable_traffic = true;
    }

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, maybe_enable_traffic, state.getState());
    auto cl =
            std::make_shared<LoadValueCallback>(store.vbMap, state.getState());

    ValueFilter valFilter = store.getValueFilterForCompressionMode();

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    valFilter);
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
        transition(WarmupState::State::Done);
    }
}

void Warmup::scheduleLoadingData()
{
    size_t estimatedCount = store.getEPEngine().getEpStats().warmedUpKeys;
    setEstimatedWarmupCount(estimatedCount);

    threadtask_count = 0;
    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        ExTask task = std::make_shared<WarmupLoadingData>(store, i, this);
        ExecutorPool::get()->schedule(task);
    }
}

void Warmup::loadDataforShard(uint16_t shardId)
{
    scan_error_t errorCode = scan_success;

    KVStore* kvstore = store.getROUnderlyingByShard(shardId);
    auto cb = std::make_shared<LoadStorageKVPairCallback>(
            store, true, state.getState());
    auto cl =
            std::make_shared<LoadValueCallback>(store.vbMap, state.getState());

    ValueFilter valFilter = store.getValueFilterForCompressionMode();

    for (const auto vbid : shardVbIds[shardId]) {
        ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, 0,
                                                    DocumentFilter::NO_DELETES,
                                                    valFilter);
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
        transition(WarmupState::State::Done);
    }
}

void Warmup::scheduleCompletion() {
    ExTask task = std::make_shared<WarmupCompletion>(store, this);
    ExecutorPool::get()->schedule(task);
}

void Warmup::done()
{
    if (setComplete()) {
        setWarmupTime();
        store.warmupCompleted();
        logWarmupStats(store);
    }
}

void Warmup::step() {
    switch (state.getState()) {
    case WarmupState::State::Initialize:
        scheduleInitialize();
        return;
    case WarmupState::State::CreateVBuckets:
        scheduleCreateVBuckets();
        return;
    case WarmupState::State::LoadingCollectionCounts:
        scheduleLoadingCollectionCounts();
        return;
    case WarmupState::State::EstimateDatabaseItemCount:
        scheduleEstimateDatabaseItemCount();
        return;
    case WarmupState::State::PopulateVBucketMap:
        schedulePopulateVBucketMap();
        return;
    case WarmupState::State::LoadPreparedSyncWrites:
        scheduleLoadPreparedSyncWrites();
        return;
    case WarmupState::State::KeyDump:
        scheduleKeyDump();
        return;
    case WarmupState::State::CheckForAccessLog:
        scheduleCheckForAccessLog();
        return;
    case WarmupState::State::LoadingAccessLog:
        scheduleLoadingAccessLog();
        return;
    case WarmupState::State::LoadingKVPairs:
        scheduleLoadingKVPairs();
        return;
    case WarmupState::State::LoadingData:
        scheduleLoadingData();
        return;
    case WarmupState::State::Done:
        scheduleCompletion();
        return;
    }
    throw std::logic_error("Warmup::step: illegal warmup state:" +
                           std::to_string(int(state.getState())));
}

void Warmup::transition(WarmupState::State to, bool force) {
    auto old = state.getState();
    if (old != WarmupState::State::Done) {
        state.transition(to, force);
        step();
    }
}

template <typename T>
void addStat(const char* nm,
             const T& val,
             const AddStatFn& add_stat,
             const void* c) {
    std::string name = "ep_warmup";
    if (nm != NULL) {
        name.append("_");
        name.append(nm);
    }

    std::stringstream value;
    value << val;
    add_casted_stat(name.data(), value.str().data(), add_stat, c);
}

void Warmup::addStats(const AddStatFn& add_stat, const void* c) const {
    using namespace std::chrono;

    EPStats& stats = store.getEPEngine().getEpStats();
    addStat(NULL, "enabled", add_stat, c);
    const char* stateName = state.toString();
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
            stats.warmupMemUsedCap * 100.0,
            add_stat,
            c);
    addStat("min_item_threshold", stats.warmupNumReadCap * 100.0, add_stat, c);

    auto md_time = metadata.load();
    if (md_time > md_time.zero()) {
        addStat("keys_time",
                duration_cast<microseconds>(md_time).count(),
                add_stat,
                c);
    }

    auto w_time = warmup.load();
    if (w_time > w_time.zero()) {
        addStat("time",
                duration_cast<microseconds>(w_time).count(),
                add_stat,
                c);
    }

    size_t itemCount = estimatedItemCount.load();
    if (itemCount == std::numeric_limits<size_t>::max()) {
        addStat("estimated_key_count", "unknown", add_stat, c);
    } else {
        auto e_time = estimateTime.load();
        if (e_time != e_time.zero()) {
            addStat("estimate_time",
                    duration_cast<microseconds>(e_time).count(),
                    add_stat,
                    c);
        }
        addStat("estimated_key_count", itemCount, add_stat, c);
    }

    if (corruptAccessLog) {
        addStat("access_log", "corrupt", add_stat, c);
    }

    size_t warmupCount = estimatedWarmupCount.load();
    if (warmupCount == std::numeric_limits<size_t>::max()) {
        addStat("estimated_value_count", "unknown", add_stat, c);
    } else {
        addStat("estimated_value_count", warmupCount, add_stat, c);
    }
}

/* In the case of CouchKVStore, all vbucket states of all the shards
 * are stored in a single instance. Others (e.g. RocksDBKVStore) store
 * only the vbucket states specific to that shard. Hence the vbucket
 * states of all the shards need to be retrieved */
uint16_t Warmup::getNumKVStores()
{
    Configuration& config = store.getEPEngine().getConfiguration();
    if (config.getBackend().compare("couchdb") == 0) {
        return 1;
    } else if (config.getBackend().compare("rocksdb") == 0 ||
               config.getBackend().compare("magma") == 0) {
        return store.vbMap.getNumShards();
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
            if (!allVbStates[vb]) {
                continue;
            }
            std::map<Vbid, vbucket_state>& shardVB =
                    shardVbStates[vb % store.vbMap.getNumShards()];
            shardVB.insert(std::pair<Vbid, vbucket_state>(Vbid(vb),
                                                          *(allVbStates[vb])));
        }
    }

    for (size_t i = 0; i < store.vbMap.shards.size(); i++) {
        std::vector<Vbid> activeVBs, otherVBs;
        std::map<Vbid, vbucket_state>::const_iterator it;
        for (auto it : shardVbStates[i]) {
            Vbid vbid = it.first;
            vbucket_state vbs = it.second;
            if (vbs.transition.state == vbucket_state_active) {
                activeVBs.push_back(vbid);
            } else {
                otherVBs.push_back(vbid);
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
        // Give 'false' (aka other) 40% of the time.
        std::bernoulli_distribution distribute(0.6);
        std::array<std::vector<Vbid>*, 2> activeReplicaSource = {
                {&activeVBs, &otherVBs}};

        while (!activeVBs.empty() || !otherVBs.empty()) {
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
