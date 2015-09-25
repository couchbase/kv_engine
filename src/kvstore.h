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

#ifndef SRC_KVSTORE_H_
#define SRC_KVSTORE_H_ 1

#include "config.h"

#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "item.h"
#include "configuration.h"

class PersistenceCallback;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const void *c, bool meta_only) :
        cookie(c), initTime(gethrtime()), metaDataOnly(meta_only)
    { }
    ~VBucketBGFetchItem() {}

    void delValue() {
        delete value.getValue();
        value.setValue(NULL);
    }

    GetValue value;
    const void * cookie;
    hrtime_t initTime;
    bool metaDataOnly;
};

const size_t CONFLICT_RES_META_LEN = 1;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

typedef struct {
    std::list<VBucketBGFetchItem *> bgfetched_list;
    bool isMetaOnly;
} vb_bgfetch_item_ctx_t;

typedef unordered_map<std::string, vb_bgfetch_item_ctx_t> vb_bgfetch_queue_t;
typedef std::pair<std::string, VBucketBGFetchItem *> bgfetched_item_t;

/**
 * Compaction context to perform compaction
 */

typedef struct {
    uint64_t revSeqno;
    std::string keyStr;
} expiredItemCtx;

typedef struct {
    uint64_t purge_before_ts;
    uint64_t purge_before_seq;
    uint64_t max_purged_seq;
    uint8_t  drop_deletes;
    uint32_t curr_time;
    shared_ptr<Callback<std::string&, bool&> > bloomFilterCallback;
    shared_ptr<Callback<std::string&, uint64_t&> > expiryCallback;
} compaction_ctx;

/**
 * Result of database mutation operations.
 *
 * This is a pair where .first is the number of rows affected, and
 * .second is true if it is an insertion.
 *
 * .first will be -1 if there was an error performing the update.
 *
 * .first will be 0 if the update did not error, but did not occur.
 * This would generally be considered a fatal condition (in practice,
 * it requires you to be firing an update at a missing rowid).
 */
typedef std::pair<int, bool> mutation_result;

typedef union {
    Callback <mutation_result> *setCb;
    Callback <int> *delCb;
} MutationRequestCallback;

typedef struct RollbackResult {
    RollbackResult(bool _success, uint64_t _highSeqno, uint64_t _snapStartSeqno,
                   uint64_t _snapEndSeqno)
        : success(_success), highSeqno(_highSeqno),
          snapStartSeqno(_snapStartSeqno), snapEndSeqno(_snapEndSeqno) {}

    bool success;
    uint64_t highSeqno;
    uint64_t snapStartSeqno;
    uint64_t snapEndSeqno;
} RollbackResult;

struct vbucket_state {
    vbucket_state(vbucket_state_t _state, uint64_t _chkid,
                  uint64_t _maxDelSeqNum, int64_t _highSeqno,
                  uint64_t _purgeSeqno, uint64_t _lastSnapStart,
                  uint64_t _lastSnapEnd, uint64_t _maxCas,
                  uint64_t _driftCounter, std::string& _failovers) :
        state(_state), checkpointId(_chkid), maxDeletedSeqno(_maxDelSeqNum),
        highSeqno(_highSeqno), purgeSeqno(_purgeSeqno),
        lastSnapStart(_lastSnapStart), lastSnapEnd(_lastSnapEnd),
        maxCas(_maxCas), driftCounter(_driftCounter),failovers(_failovers) { }

    vbucket_state(const vbucket_state& vbstate) {
        state = vbstate.state;
        checkpointId = vbstate.checkpointId;
        maxDeletedSeqno = vbstate.maxDeletedSeqno;
        highSeqno = vbstate.highSeqno;
        failovers.assign(vbstate.failovers);
        purgeSeqno = vbstate.purgeSeqno;
        lastSnapStart = vbstate.lastSnapStart;
        lastSnapEnd = vbstate.lastSnapEnd;
        maxCas = vbstate.maxCas;
        driftCounter = vbstate.driftCounter;
    }

    friend bool operator== (const vbucket_state& vbstate1,
                            const vbucket_state& vbstate2) {
       if (vbstate1.state == vbstate2.state &&
           vbstate1.checkpointId == vbstate2.checkpointId &&
           vbstate1.maxDeletedSeqno == vbstate2.maxDeletedSeqno &&
           vbstate1.highSeqno == vbstate2.highSeqno &&
           vbstate1.purgeSeqno == vbstate2.purgeSeqno &&
           vbstate1.lastSnapStart == vbstate2.lastSnapStart &&
           vbstate1.lastSnapEnd == vbstate2.lastSnapEnd &&
           vbstate1.maxCas == vbstate2.maxCas &&
           vbstate1.driftCounter == vbstate2.driftCounter &&
           vbstate1.failovers.compare(vbstate2.failovers) == 0) {
           return true;
       }

       return false;
    }

    std::string toJSON() const;

    vbucket_state_t state;
    uint64_t checkpointId;
    uint64_t maxDeletedSeqno;
    int64_t highSeqno;
    uint64_t purgeSeqno;
    uint64_t lastSnapStart;
    uint64_t lastSnapEnd;
    uint64_t maxCas;
    int64_t driftCounter;
    std::string failovers;
};

struct DBFileInfo {
    DBFileInfo() :
        itemCount(0), fileSize(0), spaceUsed(0) { }

    uint64_t itemCount;
    uint64_t fileSize;
    uint64_t spaceUsed;
};

enum scan_error_t {
    scan_success,
    scan_again,
    scan_failed
};

enum class DocumentFilter {
    ALL_ITEMS,
    NO_DELETES,
    ONLY_DELETES
};

enum class ValueFilter {
    KEYS_ONLY,
    VALUES_COMPRESSED,
    VALUES_DECOMPRESSED
};

class ScanContext {
public:
    ScanContext(shared_ptr<Callback<GetValue> > cb,
                shared_ptr<Callback<CacheLookup> > cl,
                uint16_t vb, size_t id, uint64_t start,
                uint64_t end, DocumentFilter _docFilter,
                ValueFilter _valFilter)
    : callback(cb), lookup(cl), lastReadSeqno(0), startSeqno(start),
      maxSeqno(end), scanId(id), vbid(vb), docFilter(_docFilter),
      valFilter(_valFilter) {}

    ~ScanContext() {}

    const shared_ptr<Callback<GetValue> > callback;
    const shared_ptr<Callback<CacheLookup> > lookup;

    uint64_t lastReadSeqno;
    const uint64_t startSeqno;
    const uint64_t maxSeqno;
    const size_t scanId;
    const uint16_t vbid;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
};

// First bool is true if an item exists in VB DB file.
// second bool is true if the operation is SET (i.e., insert or update).
typedef std::pair<bool, bool> kstat_entry_t;

struct KVStatsCtx{
    KVStatsCtx() : vbucket(std::numeric_limits<uint16_t>::max()),
                   fileSpaceUsed(0), fileSize(0) {}

    uint16_t vbucket;
    size_t fileSpaceUsed;
    size_t fileSize;
    unordered_map<std::string, kstat_entry_t> keyStats;
};

typedef struct KVStatsCtx kvstats_ctx;

/**
 * Type of vbucket map.
 *
 * key is the vbucket identifier.
 * value is a pair of string representation of the vbucket state and
 * its latest checkpoint Id persisted.
 */
typedef std::map<uint16_t, vbucket_state> vbucket_map_t;

/**
 * Properties of the storage layer.
 *
 * If concurrent filesystem access is possible, maxConcurrency() will
 * be greater than one.  One will need to determine whether more than
 * one writer is possible as well as whether more than one reader is
 * possible.
 */
class StorageProperties {
public:

    StorageProperties(bool evb, bool evd, bool pd, bool eget)
        : efficientVBDump(evb), efficientVBDeletion(evd),
          persistedDeletions(pd), efficientGet(eget) {}

    //! True if we can efficiently dump a single vbucket.
    bool hasEfficientVBDump() const { return efficientVBDump; }
    //! True if we can efficiently delete a vbucket all at once.
    bool hasEfficientVBDeletion() const { return efficientVBDeletion; }

    //! True if we can persisted deletions to disk.
    bool hasPersistedDeletions() const { return persistedDeletions; }

    //! True if we can batch-process multiple get operations at once.
    bool hasEfficientGet() const { return efficientGet; }

private:
    bool efficientVBDump;
    bool efficientVBDeletion;
    bool persistedDeletions;
    bool efficientGet;
};

class RollbackCB;
class Configuration;

class KVStoreConfig {
public:
    KVStoreConfig(Configuration& config, uint16_t shardId);

    KVStoreConfig(uint16_t _maxVBuckets, uint16_t _maxShards,
                  std::string& _dbname, std::string& _backend,
                  uint16_t _shardId);

    uint16_t getMaxVBuckets() {
        return maxVBuckets;
    }

    uint16_t getMaxShards() {
        return maxShards;
    }

    std::string getDBName() {
        return dbname;
    }

    std::string getBackend() {
        return backend;
    }

    uint16_t getShardId() {
        return shardId;
    }

private:
    uint16_t maxVBuckets;
    uint16_t maxShards;
    std::string dbname;
    std::string backend;
    uint16_t shardId;
};

class IORequest {
public:
    IORequest(uint16_t vbId, MutationRequestCallback &cb, bool del,
              const std::string &itmKey);

    virtual ~IORequest() { }

    bool isDelete() {
        return deleteItem;
    }

    uint16_t getVBucketId() {
        return vbucketId;
    }

    hrtime_t getDelta() {
        return (gethrtime() - start)/1000;
    }

    Callback<mutation_result>* getSetCallback(void) {
        return callback.setCb;
    }

    Callback<int>* getDelCallback(void) {
        return callback.delCb;
    }

    const std::string& getKey(void) const {
        return key;
    }

protected:
    uint16_t vbucketId;
    bool deleteItem;
    MutationRequestCallback callback;
    hrtime_t start;
    std::string key;
};

/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    KVStore(KVStoreConfig &config, bool read_only = false)
        : configuration(config), readOnly(read_only) { }

    virtual ~KVStore() {}

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addStats(const std::string &prefix, ADD_STAT add_stat, const void *c) {
        (void)prefix;
        (void)add_stat;
        (void)c;
    }

    /**
     * Show kvstore specific timing stats.
     *
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addTimingStats(const std::string &, ADD_STAT, const void *) {
    }

    /**
     * Resets kvstore specific stats
     */
    virtual void resetStats() {
    }

    /**
     * Reset the store to a clean state.
     */
    virtual void reset(uint16_t shardId) = 0;

    /**
     * Begin a transaction (if not already in one).
     *
     * @return false if we cannot begin a transaction
     */
    virtual bool begin() = 0;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return false if the commit fails
     */
    virtual bool commit(Callback<kvstats_ctx> *cb) = 0;

    /**
     * Rollback the current transaction.
     */
    virtual void rollback() = 0;

    /**
     * Get the properties of the underlying storage.
     */
    virtual StorageProperties getStorageProperties() = 0;

    /**
     * Set an item into the kv store.
     */
    virtual void set(const Item &item,
                     Callback<mutation_result> &cb) = 0;

    /**
     * Get an item from the kv store.
     */
    virtual void get(const std::string &key, uint16_t vb,
                     Callback<GetValue> &cb, bool fetchDelete = false) = 0;

    virtual void getWithHeader(void *dbHandle, const std::string &key,
                               uint16_t vb, Callback<GetValue> &cb,
                               bool fetchDelete = false) = 0;
    /**
     * Get multiple items if supported by the kv store
     */
    virtual void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) {
        (void) itms; (void) vb;
        throw std::runtime_error("Backend does not support getMulti()");
    }

    /**
     * Delete an item from the kv store.
     */
    virtual void del(const Item &itm, Callback<int> &cb) = 0;

    /**
     * Delete a given vbucket database.
     */
    virtual void delVBucket(uint16_t vbucket) = 0;

    /**
     * Get a list of all persisted vbuckets (with their states).
     */
    virtual std::vector<vbucket_state *> listPersistedVbuckets(void) = 0;


    /**
     * Get a list of all persisted engine and tap stats. This API is mainly
     * invoked during warmup to get the engine stats from the previous session.
     *
     * @param stats map instance where the engine stats from the previous
     * session is stored.
     */
    virtual void getPersistedStats(std::map<std::string, std::string> &stats) {
        (void) stats;
    }

    /**
     * Persist a snapshot of a collection of stats.
     */
    virtual bool snapshotStats(const std::map<std::string, std::string> &m) = 0;

    /**
     * Snapshot vbucket state
     * @param vbucketId id of the vbucket that needs to be snapshotted
     * @param vbstate   state of the vbucket
     * @param cb        stats callback
     * @param persist   whether state needs to be persisted to disk
     */
    virtual bool snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                                 Callback<kvstats_ctx> *cb,
                                 bool persist = true) = 0;

    /**
     * Compact a vbucket file.
     */
    virtual bool compactVBucket(const uint16_t vbid,
                                compaction_ctx *c,
                                Callback<kvstats_ctx> &kvcb) = 0;

    virtual vbucket_state *getVBucketState(uint16_t vbid) = 0;

    virtual size_t getNumPersistedDeletes(uint16_t) {
        return 0;
    }

    /**
     * This method will return information about the file whose id
     * is passed in as an argument. The information returned contains
     * the item count, file size and space used.
     */
    virtual DBFileInfo getDbFileInfo(uint16_t dbFileId) = 0;

    virtual size_t getNumItems(uint16_t, uint64_t, uint64_t) {
        return 0;
    }

    virtual RollbackResult rollback(uint16_t vbid, uint64_t rollbackseqno,
                                    shared_ptr<RollbackCB> cb) = 0;

    /**
     * This method is called before persisting a batch of data if you'd like to
     * do stuff to them that might improve performance at the IO layer.
     */
    void optimizeWrites(std::vector<queued_item> &items) {
        if (isReadOnly()) {
            throw std::logic_error("KVStore::optimizeWrites: Not valid on a "
                    "read-only object");
        }
        if (items.empty()) {
            return;
        }

        CompareQueuedItemsBySeqnoAndKey cq;
        std::sort(items.begin(), items.end(), cq);
    }

    std::list<PersistenceCallback *>& getPersistenceCbList() {
        return pcbs;
    }

    /**
     * This method is called after persisting a batch of data to perform any
     * pending tasks on the underlying KVStore instance.
     */
    virtual void pendingTasks() = 0;

    uint64_t getLastPersistedSeqno(uint16_t vbid) {
        vbucket_state *state = cachedVBStates[vbid];
        if (state) {
            return state->highSeqno;
        }
        return 0;
    }

    bool isReadOnly(void) {
        return readOnly;
    }

    KVStoreConfig& getConfig(void) {
        return configuration;
    }

    virtual ENGINE_ERROR_CODE getAllKeys(uint16_t vbid,
                            std::string &start_key, uint32_t count,
                            shared_ptr<Callback<uint16_t&, char*&> > cb) = 0;

    virtual ScanContext* initScanContext(shared_ptr<Callback<GetValue> > cb,
                                         shared_ptr<Callback<CacheLookup> > cl,
                                         uint16_t vbid, uint64_t startSeqno,
                                         DocumentFilter options,
                                         ValueFilter valOptions) = 0;

    virtual scan_error_t scan(ScanContext* sctx) = 0;

    virtual void destroyScanContext(ScanContext* ctx) = 0;

protected:
    KVStoreConfig &configuration;
    bool readOnly;
    std::vector<vbucket_state *> cachedVBStates;
    std::list<PersistenceCallback *> pcbs;
    void createDataDir(const std::string& dbname);
    std::string updateCachedVBState(uint16_t vbid, const vbucket_state& vbState);
};

/**
 * The KVStoreFactory creates the correct KVStore instance(s) when
 * needed by EPStore.
 */
class KVStoreFactory {
public:

    /**
     * Create a KVStore with the given type.
     *
     * @param config    engine configuration
     * @param read_only true if the kvstore instance is for read operations only
     */
    static KVStore *create(KVStoreConfig &config, bool read_only = false);
};

/**
 * Callback class used by DcpConsumer, for rollback operation
 */
class RollbackCB : public Callback<GetValue> {
public:
    RollbackCB() : dbHandle(NULL) { }

    virtual void callback(GetValue &val) = 0;

    void setDbHeader(void *db) {
        dbHandle = db;
    }

protected:
    void *dbHandle;
};



#endif  // SRC_KVSTORE_H_
