#ifndef COUCH_KVSTORE_H
#define COUCH_KVSTORE_H 1

#include "couch_db.h"
#include "kvstore.hh"
#include "item.hh"
#include "stats.hh"
#include "configuration.hh"
#include "mc-kvstore/mc-engine.hh"
#include "tools/cJSON.h"

class EventuallyPersistentEngine;
class EPStats;

typedef union {
    Callback <mutation_result> *setCb;
    Callback <int> *delCb;
} CouchRequestCallback;

class LoadCallback {
public:
    LoadCallback(shared_ptr<Callback<GetValue> > &data,
                 shared_ptr<RememberingCallback<bool> > &w) :
        cb(data), complete(w) {
    }

    shared_ptr<Callback<GetValue> > cb;
    shared_ptr<RememberingCallback<bool> > complete;
};

const size_t COUCHSTORE_METADATA_SIZE (2*sizeof(uint32_t) + sizeof(uint64_t));

class CouchRequest {
public:
    CouchRequest(const Item &it, int rev, CouchRequestCallback &cb, bool del);

    uint16_t getVBucketId(void) { return vbucketId; }
    int getRevNum(void) { return fileRevNum; }
    Doc *getDbDoc(void) { return &dbDoc; }
    DocInfo *getDbDocInfo(void) { return &dbDocInfo; }
    Callback<mutation_result> *getSetCallback(void) { return callback.setCb; }
    Callback<int> *getDelCallback(void) { return callback.delCb; }
    int64_t getItemId(void) { return itemId; }
    hrtime_t getDelta() { return (gethrtime() - start) / 1000; }
    size_t getNBytes() { return valuelen; }
    bool   isDelete() { return deleteItem; };

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
class CouchKVStore : public KVStore {
public:
    /**
     * Build it!
     */
    CouchKVStore(EventuallyPersistentEngine &theEngine);
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
    void set(const Item &item, uint16_t vb_version, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb, uint16_t vbver, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    void del(const Item &itm, uint64_t rowid,
             uint16_t vbver, Callback<int> &cb);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);

    vbucket_map_t listPersistedVbuckets(void);

    /**
     * Change the vbucket state in the main DB.
     */
    void vbStateChanged(uint16_t vbucket, vbucket_state_t newState);

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
    bool isKeyDumpSupported() {
        return true;
    }

    virtual void addStats(const std::string &prefix, ADD_STAT add_stat, const void *c);
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

    static int recordDbDump(Db* db, DocInfo* docinfo, void *ctx);
    static int recordDbStat(Db* db, DocInfo* docinfo, void *ctx);
    static void readVBState(Db *db, uint16_t vbId, vbucket_state &vbState);

protected:
    void loadDB(shared_ptr<LoadCallback> cb, bool keysOnly,
                std::vector<uint16_t> *vbids);
    bool setVBucketState(uint16_t vbucketId, vbucket_state_t state, uint64_t checkpointId);
    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T val,
                 ADD_STAT add_stat, const void *c);

private:
    void operator=(const CouchKVStore &from);

    void open();
    void close();
    bool commit2couchstore(void);
    void queue(CouchRequest &req);

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
    void commitCallback(CouchRequest **committedReqs, int numReqs, int errCode);
    couchstore_error_t saveVBState(Db *db, vbucket_state &vbState);
    void setDocsCommitted(uint16_t docs);


    EventuallyPersistentEngine &engine;
    EPStats &epStats;
    Configuration &configuration;
    MemcachedEngine *mc;
    std::map<uint16_t, int>dbFileMap;
    std::list<CouchRequest *> pendingReqsQ;
    size_t pendingCommitCnt;
    bool intransaction;

    // stat: the number of docs committed
    uint16_t  docsCommitted;
};

#endif /* COUCHSTORE_KVSTORE_H */
