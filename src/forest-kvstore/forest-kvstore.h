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

#ifndef SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_
#define SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_ 1

#include "libforestdb/forestdb.h"
#include "kvstore.h"

/**
 * KVStore with ForestDB as the underlying storage system
 */
class ForestKVStore : public KVStore
{
    public:
    /**
     * Constructor
     *
     * @param config    Configuration information
     */
    ForestKVStore(KVStoreConfig &config);

    /**
     * Copy constructor
     *
     * @param from the source kvstore instance
     */
    ForestKVStore(const ForestKVStore &from);

    /**
     * Destructor
     */
    ~ForestKVStore();

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId) {
        return;
    }

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(void) {
        cb_assert(!isReadOnly());
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return true if the commit is completed successfully.
     */
    bool commit(Callback<kvstats_ctx> *cb, uint64_t snapStartSeqno,
                uint64_t snapEndSeqno, uint64_t maxCas,
                uint64_t driftCounter);

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) {
        cb_assert(!isReadOnly());
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void);

    /**
     * Insert or update a given document.
     *
     * @param itm instance representing the document to be inserted or updated
     * @param cb callback instance for SET
     */
    void set(const Item &itm, Callback<mutation_result> &cb);

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
             bool fetchDelete = false);

    void getWithHeader(void *dbHandle, const std::string &key,
                       uint16_t vb, Callback<GetValue> &cb,
                       bool fetchDelete = false);

    /**
     * Retrieve multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms);

    /**
     * Delete a given document from the underlying storage system.
     *
     * @param itm instance representing the document to be deleted
     * @param cb callback instance for DELETE
     */
    void del(const Item &itm, Callback<int> &cb);

    /**
     * Delete a given vbucket database instance from the
     * underlying storage system
     *
     * @param vbucket vbucket id
     */
    void delVBucket(uint16_t vbucket);

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state vector instance where key is vbucket id and
     * value is vbucket state
     */
    std::vector<vbucket_state *>  listPersistedVbuckets(void);

    /**
     * Persist a snapshot of the engine stats in the underlying storage.
     *
     * @param engine_stats map instance that contains all the engine stats
     * @return true if the snapshot is done successfully
     */
    bool snapshotStats(const std::map<std::string, std::string> &engine_stats);

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vbucketId vbucket id
     * @param vbstate vbucket state
     * @param cb - callback for updating kv stats
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                         Callback<kvstats_ctx> *cb);

    /**
     * Compact a vbucket in the underlying storage system.
     *
     * @param vbid   - which vbucket needs to be compacted
     * @param hook_ctx - details of vbucket which needs to be compacted
     * @param cb - callback to help process newly expired items
     * @param kvcb - callback to update kvstore stats
     * @return true if successful
     */
    bool compactVBucket(const uint16_t vbid, compaction_ctx *cookie,
                        Callback<kvstats_ctx> &kvcb);

    /**
     * Do a rollback to the specified sequence number on the particular vbucket
     *
     * @param vbid          The vbucket id to be rolled back
     * @param rollbackSeqno The sequence number to which the engine needs
     *                      to be rolled back
     * @param cb            callback function to be invoked
     */
    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            shared_ptr<RollbackCB> cb);

    void pendingTasks() {
        return;
    }

    uint64_t getLastPersistedSeqno(uint16_t vbid) {
        return 0;
    }

    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, std::string &start_key,
                                 uint32_t count,
                                 shared_ptr<Callback<uint16_t&, char*&> > cb) {
        return ENGINE_SUCCESS;
    }

    ScanContext *initScanContext(shared_ptr<Callback<GetValue> > cb,
                                 shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 bool keysOnly, bool noDeletes,
                                 bool deletesOnly) {
        return NULL;
    }

    scan_error_t scan(ScanContext *sctx) {
        return scan_success;
    }

    void destroyScanContext(ScanContext *ctx) {
        return;
    }

private:
    bool intransaction;
    const std::string dbname;
    void close();
    std::vector<vbucket_state *> cachedVBStates;
};

#endif  // SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_
