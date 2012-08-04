#ifndef COUCH_KVSTORE_H
#define COUCH_KVSTORE_H 1

#include "libcouchstore/couch_db.h"
#include "kvstore.hh"
#include "item.hh"
#include "histo.hh"
#include "stats.hh"
#include "configuration.hh"
#include "couch-kvstore/couch-notifier.hh"
#include "couch-kvstore/couch-fs-stats.hh"

#define COUCHSTORE_NO_OPTIONS 0

/**
 * Stats and timings for couchKVStore
 */
class CouchKVStoreStats {

public:
    CouchKVStoreStats() :
      docsCommitted(0), numOpen(0), numClose(0),
      numLoadedVb(0), numGetFailure(0), numSetFailure(0),
      numDelFailure(0), numOpenFailure(0), numVbSetFailure(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    // the number of docs committed
    Atomic<size_t> docsCommitted;
    // the number of open() calls
    Atomic<size_t> numOpen;
    // the number of close() calls
    Atomic<size_t> numClose;
    // the number of vbuckets loaded
    Atomic<size_t> numLoadedVb;

    //stats tracking failures
    Atomic<size_t> numGetFailure;
    Atomic<size_t> numSetFailure;
    Atomic<size_t> numDelFailure;
    Atomic<size_t> numOpenFailure;
    Atomic<size_t> numVbSetFailure;
    Atomic<size_t> numCommitRetry;

    /* for flush and vb delete, no error handling in CouchKVStore, such
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
    // Time spent in couchstore commit
    Histogram<hrtime_t> commitHisto;
    // Time spent in couchstore commit retry
    Histogram<hrtime_t> commitRetryHisto;
    // Time spent in couchstore save documents
    Histogram<hrtime_t> saveDocsHisto;

    // Stats from the underlying OS file operations done by couchstore.
    CouchstoreStats fsStats;
};

class EventuallyPersistentEngine;
class EPStats;

typedef union {
    Callback <mutation_result> *setCb;
    Callback <int> *delCb;
} CouchRequestCallback;

const size_t COUCHSTORE_METADATA_SIZE(2 * sizeof(uint32_t) + sizeof(uint64_t));

class CouchRequest
{
public:
    CouchRequest(const Item &it, int rev, CouchRequestCallback &cb, bool del);

    uint16_t getVBucketId(void) {
        return vbucketId;
    }
    int getRevNum(void) {
        return fileRevNum;
    }
    Doc *getDbDoc(void) {
        if (deleteItem) {
            return NULL;
        } else {
            return &dbDoc;
        }
    }
    DocInfo *getDbDocInfo(void) {
        return &dbDocInfo;
    }
    Callback<mutation_result> *getSetCallback(void) {
        return callback.setCb;
    }
    Callback<int> *getDelCallback(void) {
        return callback.delCb;
    }
    int64_t getItemId(void) {
        return itemId;
    }
    hrtime_t getDelta() {
        return (gethrtime() - start) / 1000;
    }
    size_t getNBytes() {
        return valuelen;
    }
    bool   isDelete() {
        return deleteItem;
    };

    const std::string& getKey(void) const {
        return key;
    }

private :
    value_t value;
    size_t valuelen;
    uint8_t meta[COUCHSTORE_METADATA_SIZE];
    uint16_t vbucketId;
    int fileRevNum;
    std::string key;
    Doc dbDoc;
    DocInfo dbDocInfo;
    int64_t itemId;
    bool deleteItem;
    CouchRequestCallback callback;

    hrtime_t start;
};

/**
 * Couchstore kv-store
 */
class CouchKVStore : public KVStore
{
public:
    /**
     * Build it!
     */
    CouchKVStore(EventuallyPersistentEngine &theEngine,
                 bool read_only = false);
    CouchKVStore(const CouchKVStore &from);

    /**
     * Cleanup.
     */
    virtual ~CouchKVStore() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset(void);

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(void) {
        assert(!isReadOnly());
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(void);

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) {
        assert(!isReadOnly());
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties(void);

    /**
     * Overrides set().
     */
    void set(const Item &item, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb, Callback<GetValue> &cb);

    /**
     * Overrides getMulti().
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms);

    /**
     * Overrides del().
     */
    void del(const Item &itm, uint64_t rowid,
             Callback<int> &cb);

    bool delVBucket(uint16_t vbucket);

    vbucket_map_t listPersistedVbuckets(void);

    void getPersistedStats(std::map<std::string, std::string> &stats);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);

    /**
    * Take a snapshot of the vbucket states in the main DB.
    */
    bool snapshotVBuckets(const vbucket_map_t &m);

    /**
     * Overrides dump
     */
    void dump(shared_ptr<Callback<GetValue> > cb);
    void dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb);
    void dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb);
    void dumpDeleted(uint16_t vb,  shared_ptr<Callback<GetValue> > cb);
    bool isKeyDumpSupported() {
        return true;
    }

    /**
     * Overrides warmup
     */
    size_t warmup(MutationLog &lf,
                  const std::map<uint16_t, vbucket_state> &vbmap,
                  Callback<GetValue> &cb,
                  Callback<size_t> &estimate);

    /**
     * Overrides getEstimatedItemCount
     */
    bool getEstimatedItemCount(size_t &items);

    void addStats(const std::string &prefix, ADD_STAT add_stat, const void *c);
    void addTimingStats(const std::string &prefix, ADD_STAT add_stat,
                        const void *c);
    void optimizeWrites(std::vector<queued_item> &items);
    void processTxnSizeChange(size_t txn_size) {
        (void) txn_size;
    }
    void setVBBatchCount(size_t batch_count) {
        (void) batch_count;
    }
    void destroyInvalidVBuckets(bool destroyOnlyOne = false) {
        (void) destroyOnlyOne;
    }

    static int recordDbDump(Db *db, DocInfo *docinfo, void *ctx);
    static int recordDbStat(Db *db, DocInfo *docinfo, void *ctx);
    static int getMultiCb(Db *db, DocInfo *docinfo, void *ctx);
    static void readVBState(Db *db, uint16_t vbId, vbucket_state &vbState);

    couchstore_error_t fetchDoc(Db *db, DocInfo *docinfo,
                                GetValue &docValue, uint16_t vbId,
                                bool metaOnly);
    ENGINE_ERROR_CODE couchErr2EngineErr(couchstore_error_t errCode);

    CouchKVStoreStats &getCKVStoreStat(void) { return st; }

protected:
    void loadDB(shared_ptr<Callback<GetValue> > cb, bool keysOnly,
                std::vector<uint16_t> *vbids,
                couchstore_docinfos_options options=COUCHSTORE_NO_OPTIONS);
    bool setVBucketState(uint16_t vbucketId, vbucket_state vbstate,
                         bool stateChanged = true, bool newfile = false);

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T &val,
                 ADD_STAT add_stat, const void *c);

private:
    void operator=(const CouchKVStore &from);

    void open();
    void close();
    bool commit2couchstore(void);
    void queueItem(CouchRequest *req);

    bool getDbFile(uint16_t vbucketId, std::string &dbFileName);

    int checkNewRevNum(std::string &dbname, bool newFile = false);

    void populateFileNameMap(std::vector<std::string> &filenames);
    void getFileNameMap(std::vector<uint16_t> *vbids, std::string &dirname,
                        std::map<uint16_t, int> &filemap);
    void updateDbFileMap(uint16_t vbucketId, int newFileRev,
                         bool insertImmediately = false);
    void remVBucketFromDbFileMap(uint16_t vbucketId);
    couchstore_error_t  openDB(uint16_t vbucketId, uint16_t fileRev, Db **db,
                               uint64_t options, uint16_t *newFileRev = NULL);
    couchstore_error_t saveDocs(uint16_t vbid, int rev, Doc **docs,
                                DocInfo **docinfos, int docCount);
    void commitCallback(CouchRequest **committedReqs, int numReqs,
                        couchstore_error_t errCode);
    couchstore_error_t saveVBState(Db *db, vbucket_state &vbState);
    void setDocsCommitted(uint16_t docs);
    void closeDatabaseHandle(Db *db);

    EventuallyPersistentEngine &engine;
    EPStats &epStats;
    Configuration &configuration;
    const std::string dbname;
    CouchNotifier *couchNotifier;
    std::map<uint16_t, int>dbFileMap;
    std::vector<CouchRequest *> pendingReqsQ;
    size_t pendingCommitCnt;
    bool intransaction;

    /* all stats */
    CouchKVStoreStats   st;
    couch_file_ops statCollectingFileOps;
    /* vbucket state cache*/
    vbucket_map_t cachedVBStates;
};

#endif /* COUCHSTORE_KVSTORE_H */
