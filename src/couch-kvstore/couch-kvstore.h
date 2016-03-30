/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#ifndef SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
#define SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_ 1

#include "config.h"
#include "libcouchstore/couch_db.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "configuration.h"
#include "couch-kvstore/couch-fs-stats.h"
#include <platform/histogram.h>
#include <platform/strerror.h>
#include "logger.h"
#include "item.h"
#include "kvstore.h"
#include "tasks.h"
#include "atomicqueue.h"

#define COUCHSTORE_NO_OPTIONS 0

/**
 * Stats and timings for couchKVStore
 */
class CouchKVStoreStats {

public:
    /**
     * Default constructor
     */
    CouchKVStoreStats() :
      docsCommitted(0), numOpen(0), numClose(0),
      numLoadedVb(0), numGetFailure(0), numSetFailure(0),
      numDelFailure(0), numOpenFailure(0), numVbSetFailure(0),
      io_num_read(0), io_num_write(0), io_read_bytes(0), io_write_bytes(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    void reset() {
        docsCommitted.store(0);
        numOpen.store(0);
        numClose.store(0);
        numLoadedVb.store(0);
        numGetFailure.store(0);
        numSetFailure.store(0);
        numDelFailure.store(0);
        numOpenFailure.store(0);
        numVbSetFailure.store(0);

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
    AtomicValue<size_t> docsCommitted;
    // the number of open() calls
    AtomicValue<size_t> numOpen;
    // the number of close() calls
    AtomicValue<size_t> numClose;
    // the number of vbuckets loaded
    AtomicValue<size_t> numLoadedVb;

    //stats tracking failures
    AtomicValue<size_t> numGetFailure;
    AtomicValue<size_t> numSetFailure;
    AtomicValue<size_t> numDelFailure;
    AtomicValue<size_t> numOpenFailure;
    AtomicValue<size_t> numVbSetFailure;

    //! Number of read related io operations
    AtomicValue<size_t> io_num_read;
    //! Number of write related io operations
    AtomicValue<size_t> io_num_write;
    //! Number of bytes read
    AtomicValue<size_t> io_read_bytes;
    //! Number of bytes written (key + value + application rev metadata)
    AtomicValue<size_t> io_write_bytes;

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
    // Time spent in couchstore compaction
    Histogram<hrtime_t> compactHisto;
    // Time spent in couchstore save documents
    Histogram<hrtime_t> saveDocsHisto;
    // Batch size of saveDocs calls
    Histogram<size_t> batchSize;
    //Time spent in vbucket snapshot
    Histogram<hrtime_t> snapshotHisto;

    // Stats from the underlying OS file operations done by couchstore.
    CouchstoreStats fsStats;
    // Underlying stats for OS file operations during compaction.
    CouchstoreStats fsStatsCompaction;
};

class EventuallyPersistentEngine;

// Additional 3 Bytes for flex meta, datatype and conflict resolution mode
const size_t COUCHSTORE_METADATA_SIZE(2 * sizeof(uint32_t) + sizeof(uint64_t) +
                                      FLEX_DATA_OFFSET + EXT_META_LEN +
                                      CONFLICT_RES_META_LEN);

/**
 * Class representing a document to be persisted in couchstore.
 */
class CouchRequest : public IORequest
{
public:
    /**
     * Constructor
     *
     * @param it Item instance to be persisted
     * @param rev vbucket database revision number
     * @param cb persistence callback
     * @param del flag indicating if it is an item deletion or not
     */
    CouchRequest(const Item &it, uint64_t rev, MutationRequestCallback &cb,
                 bool del);

    /**
     * Get the revision number of the vbucket database file
     * where the document is persisted
     *
     * @return revision number of the corresponding vbucket database file
     */
    uint64_t getRevNum(void) {
        return fileRevNum;
    }

    /**
     * Get the couchstore Doc instance of a document to be persisted
     *
     * @return pointer to the couchstore Doc instance of a document
     */
    void *getDbDoc(void) {
        if (deleteItem) {
            return NULL;
        } else {
            return &dbDoc;
        }
    }

    /**
     * Get the couchstore DocInfo instance of a document to be persisted
     *
     * @return pointer to the couchstore DocInfo instance of a document
     */
    DocInfo *getDbDocInfo(void) {
        return &dbDocInfo;
    }

    /**
     * Get the length of a document body to be persisted
     *
     * @return length of a document body
     */
    size_t getNBytes() {
        return dbDocInfo.rev_meta.size + dbDocInfo.size;
    }

private :
    value_t value;
    uint8_t meta[COUCHSTORE_METADATA_SIZE];
    uint64_t fileRevNum;
    Doc dbDoc;
    DocInfo dbDocInfo;
};

/**
 * KVStore with couchstore as the underlying storage system
 */
class CouchKVStore : public KVStore
{
public:
    /**
     * Constructor
     *
     * @param stats     Engine stats
     * @param config    Configuration information
     * @param read_only flag indicating if this kvstore instance is for read-only operations
     */
    CouchKVStore(KVStoreConfig &config, bool read_only = false);

    /**
     * Copy constructor
     *
     * @param from the source kvstore instance
     */
    CouchKVStore(const CouchKVStore &from);

    /**
     * Deconstructor
     */
    ~CouchKVStore();

    void initialize();

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(void) override {
        if (isReadOnly()) {
            throw std::logic_error("CouchKVStore::begin: Not valid on a "
                    "read-only object.");
        }
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return true if the commit is completed successfully.
     */
    bool commit() override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) override {
        if (isReadOnly()) {
            throw std::logic_error("CouchKVStore::rollback: Not valid on a "
                    "read-only object.");
        }
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void) override;

    /**
     * Insert or update a given document.
     *
     * @param itm instance representing the document to be inserted or updated
     * @param cb callback instance for SET
     */
    void set(const Item &itm, Callback<mutation_result> &cb) override;

    /**
     * Retrieve the document with a given key from the underlying storage system.
     *
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param cb callback instance for GET
     * @param fetchDelete True if we want to retrieve a deleted item if it not
     *        purged yet.
     */
    void get(const std::string &key, uint16_t vb, Callback<GetValue> &cb,
             bool fetchDelete = false) override;

    void getWithHeader(void *dbHandle, const std::string &key,
                       uint16_t vb, Callback<GetValue> &cb,
                       bool fetchDelete = false) override;

    /**
     * Retrieve the multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) override;

    /**
     * Get the number of vbuckets in a single database file
     *
     * returns - the number of vbuckets in a database file
     */
    uint16_t getNumVbsPerFile(void) override {
        return 1;
    }

    /**
     * Delete a given document from the underlying storage system.
     *
     * @param itm instance representing the document to be deleted
     * @param cb callback instance for DELETE
     */
    void del(const Item &itm, Callback<int> &cb) override;

    /**
     * Delete a given vbucket database instance from the underlying storage system
     *
     * @param vbucket vbucket id
     * @param recreate flag to re-create vbucket after deletion
     */
    void delVBucket(uint16_t vbucket) override;

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state vector instance where key is vbucket id and
     * value is vbucket state
     */
   std::vector<vbucket_state *>  listPersistedVbuckets(void) override;

    /**
     * Retrieve ths list of persisted engine stats
     *
     * @param stats map instance where the persisted engine stats will be added
     */
    void getPersistedStats(std::map<std::string, std::string> &stats) override;

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vbucketId - vbucket id
     * @param vbstate   - vbucket state
     * @param options   - options used for persisting the state to disk
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                         VBStatePersist options) override;

     /**
     * Compact a database file in the underlying storage system.
     *
     * @param ctx - compaction context that holds the identifier of the
                    underlying database file, options and callbacks
                    that need to invoked.
     * @return true if successful
     */
    bool compactDB(compaction_ctx *ctx) override;

    /**
     * Return the database file id from the compaction request
     * @param compact_req request structure for compaction
     *
     * return database file id
     */
    uint16_t getDBFileId(const protocol_binary_request_compact_db& req) override {
        return ntohs(req.message.header.request.vbucket);
    }

    vbucket_state *getVBucketState(uint16_t vbid) override;

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for
     */
    size_t getNumPersistedDeletes(uint16_t vbid) override;

    /**
     * Get the vbucket pertaining stats from a vbucket database file
     *
     * @param vbid The vbucket of the file to get the number of docs for
     */
    DBFileInfo getDbFileInfo(uint16_t vbid) override;

    /**
     * Get the file statistics for the underlying KV store
     *
     * return cumulative file size and space usage for the KV store
     */
    DBFileInfo getAggrDbFileInfo() override;

    /**
     * Get the number of non-deleted items from a vbucket database file
     *
     * @param vbid The vbucket of the file to get the number of docs for
     * @param min_seq The sequence number to start the count from
     * @param max_seq The sequence number to stop the count at
     */
    size_t getNumItems(uint16_t vbid, uint64_t min_seq, uint64_t max_seq) override;

    /**
     * This method will return the total number of items in the vbucket. Unlike
     * the getNumItems function that returns items within a specified range of
     * sequence numbers, this will return all the items in the vbucket.
     *
     * vbid - vbucket id
     */
    size_t getItemCount(uint16_t vbid) override;

    /**
     * Do a rollback to the specified seqNo on the particular vbucket
     *
     * @param vbid The vbucket of the file that's to be rolled back
     * @param rollbackSeqno The sequence number upto which the engine needs
     * to be rolled back
     * @param cb getvalue callback
     */
    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override;

    /**
     * Perform pending tasks after persisting dirty items
     */
    void pendingTasks() override;

    /**
     * Add all the kvstore stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addStats(const std::string &prefix, ADD_STAT add_stat, const void *cookie) override;

    /**
     * Add all the kvstore timings stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addTimingStats(const std::string &prefix, ADD_STAT add_stat,
                        const void *c) override;

    bool getStat(const char* name, size_t& value) override;

    /**
     * Resets couchstore stats
     */
    void resetStats() override {
        st.reset();
    }

    static int recordDbDump(Db *db, DocInfo *docinfo, void *ctx);
    static int recordDbStat(Db *db, DocInfo *docinfo, void *ctx);
    static int getMultiCb(Db *db, DocInfo *docinfo, void *ctx);
    ENGINE_ERROR_CODE readVBState(Db *db, uint16_t vbId);

    couchstore_error_t fetchDoc(Db *db, DocInfo *docinfo,
                                GetValue &docValue, uint16_t vbId,
                                bool metaOnly, bool fetchDelete = false);
    ENGINE_ERROR_CODE couchErr2EngineErr(couchstore_error_t errCode);

    CouchKVStoreStats &getCKVStoreStat(void) { return st; }

    uint64_t getLastPersistedSeqno(uint16_t vbid);

    /**
     * Get all_docs API, to return the list of all keys in the store
     */
    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, std::string &start_key,
                                 uint32_t count,
                                 std::shared_ptr<Callback<uint16_t&, char*&> > cb) override;

    ScanContext* initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                 std::shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

private:
    bool setVBucketState(uint16_t vbucketId, vbucket_state &vbstate,
                         VBStatePersist options, bool reset=false);

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T &val,
                 ADD_STAT add_stat, const void *c);

    void operator=(const CouchKVStore &from);

    void close();
    bool commit2couchstore();

    uint64_t checkNewRevNum(std::string &dbname, bool newFile = false);
    void populateFileNameMap(std::vector<std::string> &filenames,
                             std::vector<uint16_t> *vbids);
    void remVBucketFromDbFileMap(uint16_t vbucketId);
    void updateDbFileMap(uint16_t vbucketId, uint64_t newFileRev);
    couchstore_error_t openDB(uint16_t vbucketId, uint64_t fileRev, Db **db,
                              uint64_t options, uint64_t *newFileRev = NULL,
                              bool reset=false, FileOpsInterface* ops = nullptr);
    couchstore_error_t openDB_retry(std::string &dbfile, uint64_t options,
                                    FileOpsInterface *ops,
                                    Db **db, uint64_t *newFileRev);
    couchstore_error_t saveDocs(uint16_t vbid, uint64_t rev, Doc **docs,
                                DocInfo **docinfos, size_t docCount,
                                kvstats_ctx &kvctx);
    void commitCallback(std::vector<CouchRequest *> &committedReqs,
                        kvstats_ctx &kvctx,
                        couchstore_error_t errCode);
    couchstore_error_t saveVBState(Db *db, vbucket_state &vbState);
    void setDocsCommitted(uint16_t docs);
    void closeDatabaseHandle(Db *db);

    /**
     * Unlink selected couch file, which will be removed by the OS,
     * once all its references close.
     */
    void unlinkCouchFile(uint16_t vbucket, uint64_t fRev);

    /**
     * Remove compact file
     *
     * @param dbname
     * @param vbucket id
     * @param current db rev number
     */
    void removeCompactFile(const std::string &dbname, uint16_t vbid,
                           uint64_t currentRev);

    void removeCompactFile(const std::string &filename);

    const std::string dbname;

    // Map of the fileRev for each vBucket. Using RelaxedAtomic so
    // stats gathering (doDcpVbTakeoverStats) can get a snapshot
    // without having to lock.
    std::vector<Couchbase::RelaxedAtomic<uint64_t>> dbFileRevMap;

    uint16_t numDbFiles;
    std::vector<CouchRequest *> pendingReqsQ;
    bool intransaction;

    /* all stats */
    CouchKVStoreStats   st;

    /**
     * FileOpsInterface implementation for couchstore which tracks
     * all bytes read/written by couchstore *except* compaction.
     *
     * Backed by this->st.fsStats
     */
    std::unique_ptr<FileOpsInterface> statCollectingFileOps;

    /**
     * FileOpsInterface implementation for couchstore which tracks
     * all bytes read/written by couchstore just for compaction
     *
     * Backed by this->st.fsStatsCompaction
     */
    std::unique_ptr<FileOpsInterface> statCollectingFileOpsCompaction;

    /* deleted docs in each file, indexed by vBucket. RelaxedAtomic
       to allow stats access witout lock */
    std::vector<Couchbase::RelaxedAtomic<size_t>> cachedDeleteCount;
    std::vector<Couchbase::RelaxedAtomic<uint64_t>> cachedFileSize;
    std::vector<Couchbase::RelaxedAtomic<uint64_t>> cachedSpaceUsed;
    /* pending file deletions */
    AtomicQueue<std::string> pendingFileDeletions;

    AtomicValue<size_t> backfillCounter;
    std::map<size_t, Db*> backfills;
    Mutex backfillLock;

    Logger& logger;
};

#endif  // SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
