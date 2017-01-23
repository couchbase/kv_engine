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

#include <cJSON.h>
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <relaxed_atomic.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "configuration.h"
#include "item.h"
#include "logger.h"

class KVStore;
class PersistenceCallback;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const void *c, bool meta_only) :
        cookie(c), initTime(gethrtime()), metaDataOnly(meta_only)
    { }
    VBucketBGFetchItem(const GetValue& value_, const void* c,
                       const hrtime_t& init_time, bool meta_only)
        : value(value_),
          cookie(c),
          initTime(init_time),
          metaDataOnly(meta_only) {}

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

static const int64_t INITIAL_DRIFT = -140737488355328; //lowest possible 48-bit integer

struct vb_bgfetch_item_ctx_t {
    // These need to be here due to MSVC2013 which otherwise would generate
    // an incorrect move constructor.
    vb_bgfetch_item_ctx_t() {
    }
    vb_bgfetch_item_ctx_t(vb_bgfetch_item_ctx_t&& other)
        : bgfetched_list(std::move(other.bgfetched_list)),
          isMetaOnly(other.isMetaOnly) {
    }

    vb_bgfetch_item_ctx_t& operator=(vb_bgfetch_item_ctx_t&& other) {
        this->bgfetched_list = std::move(other.bgfetched_list);
        this->isMetaOnly = other.isMetaOnly;
        return *this;
    }
    std::list<std::unique_ptr<VBucketBGFetchItem>> bgfetched_list;
    bool isMetaOnly;
};

typedef std::unordered_map<StoredDocKey, vb_bgfetch_item_ctx_t> vb_bgfetch_queue_t;
typedef std::pair<StoredDocKey, const VBucketBGFetchItem*> bgfetched_item_t;

/**
 * Compaction context to perform compaction
 */

typedef struct {
    uint64_t revSeqno;
    std::string keyStr;
} expiredItemCtx;

typedef uint16_t DBFileId;

typedef std::shared_ptr<Callback<uint16_t&, const DocKey&, bool&> > BloomFilterCBPtr;
typedef std::shared_ptr<Callback<uint16_t&, const DocKey&, uint64_t&, time_t&> > ExpiredItemsCBPtr;

typedef struct {
    uint64_t purge_before_ts;
    uint64_t purge_before_seq;
    //mapping of <key: vbucket id, value: max purged sequence number>
    std::unordered_map<uint16_t, uint64_t> max_purged_seq;
    KVStore *store;
    uint8_t  drop_deletes;
    DBFileId db_file_id;
    uint32_t curr_time;
    BloomFilterCBPtr bloomFilterCallback;
    ExpiredItemsCBPtr expiryCallback;
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


class NoLookupCallback : public Callback<CacheLookup> {
public:
    NoLookupCallback() {}
    ~NoLookupCallback() {}
    void callback(CacheLookup&) {}
};

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
    vbucket_state() = default;

    vbucket_state(vbucket_state_t _state, uint64_t _chkid,
                  uint64_t _maxDelSeqNum, int64_t _highSeqno,
                  uint64_t _purgeSeqno, uint64_t _lastSnapStart,
                  uint64_t _lastSnapEnd, uint64_t _maxCas,
                  std::string _failovers) :
        state(_state),
        checkpointId(_chkid),
        maxDeletedSeqno(_maxDelSeqNum),
        highSeqno(_highSeqno),
        purgeSeqno(_purgeSeqno),
        lastSnapStart(_lastSnapStart),
        lastSnapEnd(_lastSnapEnd),
        maxCas(_maxCas),
        failovers(std::move(_failovers)) { }

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
    }

    std::string toJSON() const;

    bool needsToBePersisted(const vbucket_state& vbstate) {
        /**
         * The vbucket state information is to be persisted
         * only if a change is detected in the state or the
         * failovers fields.
         */
        if (state != vbstate.state ||
            failovers.compare(vbstate.failovers) != 0) {
            return true;
        }
        return false;
    }

    void reset() {
        checkpointId = 0;
        maxDeletedSeqno = 0;
        highSeqno = 0;
        purgeSeqno = 0;
        lastSnapStart = 0;
        lastSnapEnd = 0;
        maxCas = 0;
        failovers.assign("[{\"id\":0, \"seq\":0}]");
    }

    vbucket_state_t state;
    uint64_t checkpointId;
    uint64_t maxDeletedSeqno;
    int64_t highSeqno;
    uint64_t purgeSeqno;
    uint64_t lastSnapStart;
    uint64_t lastSnapEnd;
    uint64_t maxCas;
    std::string failovers;
};

struct DBFileInfo {
    DBFileInfo() :
        fileSize(0), spaceUsed(0) { }

    DBFileInfo(uint64_t fileSize_, uint64_t spaceUsed_)
        : fileSize(fileSize_), spaceUsed(spaceUsed_) {}

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
    NO_DELETES
};

enum class ValueFilter {
    KEYS_ONLY,
    VALUES_COMPRESSED,
    VALUES_DECOMPRESSED
};

enum class VBStatePersist {
    VBSTATE_CACHE_UPDATE_ONLY,       //Update only cached state in-memory
    VBSTATE_PERSIST_WITHOUT_COMMIT,  //Persist without committing to disk
    VBSTATE_PERSIST_WITH_COMMIT      //Persist with commit to disk
};

class ScanContext {
public:
    ScanContext(std::shared_ptr<Callback<GetValue> > cb,
                std::shared_ptr<Callback<CacheLookup> > cl,
                uint16_t vb, size_t id, uint64_t start,
                uint64_t end, DocumentFilter _docFilter,
                ValueFilter _valFilter, uint64_t _documentCount)
    : callback(cb), lookup(cl), lastReadSeqno(0), startSeqno(start),
      maxSeqno(end), scanId(id), vbid(vb), docFilter(_docFilter),
      valFilter(_valFilter), documentCount(_documentCount),
      logger(&global_logger) {}

    ~ScanContext() {}

    const std::shared_ptr<Callback<GetValue> > callback;
    const std::shared_ptr<Callback<CacheLookup> > lookup;

    uint64_t lastReadSeqno;
    const uint64_t startSeqno;
    const uint64_t maxSeqno;
    const size_t scanId;
    const uint16_t vbid;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
    const uint64_t documentCount;

    Logger* logger;
};

// First bool is true if an item exists in VB DB file.
// second bool is true if the operation is SET (i.e., insert or update).
typedef std::pair<bool, bool> kstat_entry_t;

struct KVStatsCtx{
    KVStatsCtx() : vbucket(std::numeric_limits<uint16_t>::max()) {}

    uint16_t vbucket;
    std::unordered_map<StoredDocKey, kstat_entry_t> keyStats;
};

typedef struct KVStatsCtx kvstats_ctx;

struct FileStats {
public:
    FileStats() :
        readSeekHisto(ExponentialGenerator<size_t>(1, 2), 50),
        readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        totalBytesRead(0),
        totalBytesWritten(0) { }

    //Read time length
    Histogram<hrtime_t> readTimeHisto;
    //Distance from last read
    Histogram<size_t> readSeekHisto;
    //Size of read
    Histogram<size_t> readSizeHisto;
    //Write time length
    Histogram<hrtime_t> writeTimeHisto;
    //Write size
    Histogram<size_t> writeSizeHisto;
    //Time spent in sync
    Histogram<hrtime_t> syncTimeHisto;

    // total bytes read from disk.
    std::atomic<size_t> totalBytesRead;
    // Total bytes written to disk.
    std::atomic<size_t> totalBytesWritten;

    void reset() {
        readTimeHisto.reset();
        readSeekHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        syncTimeHisto.reset();
        totalBytesRead = 0;
        totalBytesWritten = 0;
    }
};

/**
 * Stats and timings for KVStore
 */
class KVStoreStats {

public:
    /**
     * Default constructor
     */
    KVStoreStats() :
      docsCommitted(0),
      numOpen(0),
      numClose(0),
      numLoadedVb(0),
      numGetFailure(0),
      numSetFailure(0),
      numDelFailure(0),
      numOpenFailure(0),
      numVbSetFailure(0),
      io_num_read(0),
      io_num_write(0),
      io_read_bytes(0),
      io_write_bytes(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    KVStoreStats(const KVStoreStats &copyFrom) {}

    void reset() {
        docsCommitted = 0;
        numOpen = 0;
        numClose = 0;
        numLoadedVb = 0;
        numGetFailure = 0;
        numSetFailure = 0;
        numDelFailure = 0;
        numOpenFailure = 0;
        numVbSetFailure = 0;

        readTimeHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        delTimeHisto.reset();
        compactHisto.reset();
        snapshotHisto.reset();
        commitHisto.reset();
        saveDocsHisto.reset();
        batchSize.reset();
        fsStats.reset();
    }

    // the number of docs committed
    Couchbase::RelaxedAtomic<size_t> docsCommitted;
    // the number of open() calls
    Couchbase::RelaxedAtomic<size_t> numOpen;
    // the number of close() calls
    Couchbase::RelaxedAtomic<size_t> numClose;
    // the number of vbuckets loaded
    Couchbase::RelaxedAtomic<size_t> numLoadedVb;

    //stats tracking failures
    Couchbase::RelaxedAtomic<size_t> numGetFailure;
    Couchbase::RelaxedAtomic<size_t> numSetFailure;
    Couchbase::RelaxedAtomic<size_t> numDelFailure;
    Couchbase::RelaxedAtomic<size_t> numOpenFailure;
    Couchbase::RelaxedAtomic<size_t> numVbSetFailure;

    //! Number of read related io operations
    Couchbase::RelaxedAtomic<size_t> io_num_read;
    //! Number of write related io operations
    Couchbase::RelaxedAtomic<size_t> io_num_write;
    //! Number of bytes read
    Couchbase::RelaxedAtomic<size_t> io_read_bytes;
    //! Number of bytes written (key + value + application rev metadata)
    Couchbase::RelaxedAtomic<size_t> io_write_bytes;

    /* for flush and vb delete, no error handling in KVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    Histogram<hrtime_t> readTimeHisto;
    // How big are our reads?
    Histogram<size_t> readSizeHisto;
    // How long it takes us to complete a write
    Histogram<hrtime_t> writeTimeHisto;
    // How big are our writes?
    Histogram<size_t> writeSizeHisto;
    // Time spent in delete() calls.
    Histogram<hrtime_t> delTimeHisto;
    // Time spent in commit
    Histogram<hrtime_t> commitHisto;
    // Time spent in compaction
    Histogram<hrtime_t> compactHisto;
    // Time spent in saving documents to disk
    Histogram<hrtime_t> saveDocsHisto;
    // Batch size while saving documents
    Histogram<size_t> batchSize;
    //Time spent in vbucket snapshot
    Histogram<hrtime_t> snapshotHisto;

    // Stats from the underlying OS file operations
    FileStats fsStats;

    // Underlying stats for OS file operations during compaction
    FileStats fsStatsCompaction;
};

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

    enum class EfficientVBDump {
        Yes,
        No
    };

    enum class EfficientVBDeletion {
        Yes,
        No
    };

    enum class PersistedDeletion {
        Yes,
        No
    };

    enum class EfficientGet {
        Yes,
        No
    };

    enum class ConcurrentWriteCompact {
        Yes,
        No
    };

    StorageProperties(EfficientVBDump evb, EfficientVBDeletion evd, PersistedDeletion pd,
                      EfficientGet eget, ConcurrentWriteCompact cwc)
        : efficientVBDump(evb), efficientVBDeletion(evd),
          persistedDeletions(pd), efficientGet(eget),
          concWriteCompact(cwc) {}

    /* True if we can efficiently dump a single vbucket */
    bool hasEfficientVBDump() const {
        return (efficientVBDump == EfficientVBDump::Yes);
    }

    /* True if we can efficiently delete a vbucket all at once */
    bool hasEfficientVBDeletion() const {
        return (efficientVBDeletion == EfficientVBDeletion::Yes);
    }

    /* True if we can persist deletions to disk */
    bool hasPersistedDeletions() const {
        return (persistedDeletions == PersistedDeletion::Yes);
    }

    /* True if we can batch-process multiple get operations at once */
    bool hasEfficientGet() const {
        return (efficientGet == EfficientGet::Yes);
    }

    /* True if the underlying storage supports concurrent writing
     * and compacting */
    bool hasConcWriteCompact() const {
        return (concWriteCompact == ConcurrentWriteCompact::Yes);
    }

private:
    EfficientVBDump efficientVBDump;
    EfficientVBDeletion efficientVBDeletion;
    PersistedDeletion persistedDeletions;
    EfficientGet efficientGet;
    ConcurrentWriteCompact concWriteCompact;
};

class RollbackCB;
class Configuration;

class KVStoreConfig {
public:
    /**
     * This constructor intialises the object from a central
     * ep-engine Configuration instance.
     */
    KVStoreConfig(Configuration& config, uint16_t shardId);

    /**
     * This constructor sets the mandatory config options
     *
     * Optional config options are set using a separate method
     */
    KVStoreConfig(uint16_t _maxVBuckets,
                  uint16_t _maxShards,
                  const std::string& _dbname,
                  const std::string& _backend,
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

    Logger& getLogger() {
        return *logger;
    }

    /**
     * Indicates whether or not underlying file operations will be
     * buffered by the storage engine used.
     *
     * Only recognised by CouchKVStore
     */
    bool getBuffered() {
        return buffered;
    }

    /**
     * Used to override the default logger object
     */
    KVStoreConfig& setLogger(Logger& _logger);

    /**
     * Used to override the default buffering behaviour.
     *
     * Only recognised by CouchKVStore
     */
    KVStoreConfig& setBuffered(bool _buffered);

private:
    uint16_t maxVBuckets;
    uint16_t maxShards;
    std::string dbname;
    std::string backend;
    uint16_t shardId;
    Logger* logger;
    bool buffered;
};

class IORequest {
public:
    IORequest(uint16_t vbId, MutationRequestCallback &cb, bool del,
              const DocKey itmKey);

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

    const StoredDocKey& getKey(void) const {
        return key;
    }

protected:
    uint16_t vbucketId;
    bool deleteItem;
    MutationRequestCallback callback;
    hrtime_t start;
    StoredDocKey key;
    size_t dataSize;
};

/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    KVStore(KVStoreConfig &config, bool read_only = false)
        : configuration(config), readOnly(read_only) {}

    virtual ~KVStore() {}

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    void addStats(ADD_STAT add_stat, const void *c);

    /**
     * Request the specified statistic name from the kvstore.
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    virtual bool getStat(const char* name, size_t& value) {
        return false;
    }

    /**
     * Show kvstore specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addTimingStats(ADD_STAT add_stat, const void* c);

    /**
     * Resets kvstore specific stats
     */
    void resetStats() {
        st.reset();
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
    virtual bool commit() = 0;

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
    virtual void get(const DocKey& key, uint16_t vb,
                     Callback<GetValue> &cb, bool fetchDelete = false) = 0;

    virtual void getWithHeader(void *dbHandle, const DocKey& key,
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
     * Get the number of vbuckets in a single database file
     *
     * returns - the number of vbuckets in the file
     */
    virtual uint16_t getNumVbsPerFile(void) = 0;

    /**
     * Delete an item from the kv store.
     */
    virtual void del(const Item &itm, Callback<int> &cb) = 0;

    /**
     * Delete a given vbucket database instance from underlying storage
     *
     * @param vbucket vbucket id
     * return true, if vbucket deletion was successful. Else, false.
     */
    virtual bool delVBucket(uint16_t vbucket) = 0;

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
    bool snapshotStats(const std::map<std::string, std::string> &m);

    /**
     * Snapshot vbucket state
     * @param vbucketId id of the vbucket that needs to be snapshotted
     * @param vbstate   state of the vbucket
     * @param cb        stats callback
     * @param options   options for persisting the state
     */
    virtual bool snapshotVBucket(uint16_t vbucketId,
                                 const vbucket_state &vbstate,
                                 VBStatePersist options) = 0;

    /**
     * Compact a database file.
     */
    virtual bool compactDB(compaction_ctx *c) = 0;

    /**
     * Return the database file id from the compaction request
     * @param compact_req request structure for compaction
     *
     * return database file id
     */
    virtual uint16_t getDBFileId(const protocol_binary_request_compact_db& req) = 0;

    virtual vbucket_state *getVBucketState(uint16_t vbid) = 0;

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for.
     * @returns the number of deletes which are persisted
     * @throws std::runtime_error (and subclasses) if it was not possible to
     *         obtain a count of persisted deletes.
     */
    virtual size_t getNumPersistedDeletes(uint16_t vbid) = 0;

    /**
     * This method will return information about the file whose id
     * is passed in as an argument. The information returned contains
     * the item count, file size and space used.
     *
     * @throws std::runtime_error (and subclasses) if it was not possible to
     *         obtain the DB file info.
     */
    virtual DBFileInfo getDbFileInfo(uint16_t dbFileId) = 0;

    /**
     * This method will return file size and space used for the
     * entire KV store
     */
    virtual DBFileInfo getAggrDbFileInfo() = 0;

    virtual size_t getNumItems(uint16_t, uint64_t, uint64_t) {
        return 0;
    }

    /**
     * This method will return the total number of items in the vbucket
     *
     * vbid - vbucket id
     */
    virtual size_t getItemCount(uint16_t vbid) = 0;

    virtual RollbackResult rollback(uint16_t vbid, uint64_t rollbackseqno,
                                    std::shared_ptr<RollbackCB> cb) = 0;

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

    KVStoreStats& getKVStoreStat(void) {
        return st;
    }

    virtual ENGINE_ERROR_CODE getAllKeys(uint16_t vbid,
                            const DocKey start_key, uint32_t count,
                            std::shared_ptr<Callback<const DocKey&>> cb) = 0;

    virtual ScanContext* initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                         std::shared_ptr<Callback<CacheLookup> > cl,
                                         uint16_t vbid, uint64_t startSeqno,
                                         DocumentFilter options,
                                         ValueFilter valOptions) = 0;

    virtual scan_error_t scan(ScanContext* sctx) = 0;

    virtual void destroyScanContext(ScanContext* ctx) = 0;

protected:

    /* all stats */
    KVStoreStats st;
    KVStoreConfig& configuration;
    bool readOnly;
    std::vector<vbucket_state *> cachedVBStates;
    /* non-deleted docs in each file, indexed by vBucket.
       RelaxedAtomic to allow stats access without lock. */
    std::vector<Couchbase::RelaxedAtomic<size_t>> cachedDocCount;
    Couchbase::RelaxedAtomic<uint16_t> cachedValidVBCount;
    std::list<PersistenceCallback *> pcbs;

protected:

    void createDataDir(const std::string& dbname);
    template <typename T>
    void addStat(const std::string& prefix, const char* nm, T& val,
                 ADD_STAT add_stat, const void* c);

    /**
     * Updates the cached state for a vbucket
     *
     * @param vbid the vbucket id
     * @param vbState the new state information for the vbucket
     *
     * @return true if the cached vbucket state is updated
     */
    bool updateCachedVBState(uint16_t vbid, const vbucket_state& vbState);
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

inline const std::string getJSONObjString(const cJSON *i) {
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        throw std::invalid_argument("getJSONObjString: type of object (" +
                                    std::to_string(i->type) +
                                    ") is not cJSON_String");
    }
    return i->valuestring;
}


#endif  // SRC_KVSTORE_H_
