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

#include "configuration.h"
#include "stats.h"
#include "tasks.h"
#include "vbucket.h"

/**
 * Result of database mutation operations.
 *
 * This is a pair where .first is the number of rows affected, and
 * .second is the ID that was generated (if any).  .second will be 0
 * on updates (not generating an ID).
 *
 * .first will be -1 if there was an error performing the update.
 *
 * .first will be 0 if the update did not error, but did not occur.
 * This would generally be considered a fatal condition (in practice,
 * it requires you to be firing an update at a missing rowid).
 */
typedef std::pair<int, int64_t> mutation_result;

struct vbucket_state {
    vbucket_state() { }
    vbucket_state(vbucket_state_t _state, uint64_t _chkid,
                  uint64_t _maxDelSeqNum, int64_t _highSeqno) :
        state(_state), checkpointId(_chkid), maxDeletedSeqno(_maxDelSeqNum),
        highSeqno(_highSeqno) { }

    vbucket_state_t state;
    uint64_t checkpointId;
    uint64_t maxDeletedSeqno;
    int64_t highSeqno;
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
 * Properites of the storage layer.
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

/**
 * Database strategy
 */
enum db_type {
    single_db,           //!< single database strategy
    multi_db,            //!< multi-database strategy
    single_mt_db,        //!< single database, multi-table strategy
    multi_mt_db,         //!< multi-database, multi-table strategy
    multi_mt_vb_db       //!< multi-db, multi-table strategy sharded by vbucket
};

/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    KVStore(bool read_only = false) : readOnly(read_only) { }

    virtual ~KVStore() {}

    virtual size_t getEstimatedItemCount(std::vector<uint16_t> &vbs);


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
    virtual void reset() = 0;

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
    virtual void get(const std::string &key, uint64_t rowid,
                     uint16_t vb,
                     Callback<GetValue> &cb) = 0;

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
    virtual void del(const Item &itm, uint64_t rowid,
                     Callback<int> &cb) = 0;

    /**
     * Delete a given vbucket database.
     */
    virtual bool delVBucket(uint16_t vbucket, bool recreate = false) = 0;

    /**
     * Get a list of all persisted vbuckets (with their states).
     */
    virtual vbucket_map_t listPersistedVbuckets(void) = 0;

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
     * Snapshot vbucket states.
     */
    virtual bool snapshotVBuckets(const vbucket_map_t &m) = 0;

    /**
     * Compact a vbucket file.
     */
    virtual bool compactVBucket(const uint16_t vbid,
                                compaction_ctx *c,
                                Callback<compaction_ctx> &cb) = 0;

    /**
     * Pass all stored data through the given callback.
     */
    virtual void dump(shared_ptr<Callback<GetValue> > cb,
                      shared_ptr<Callback<CacheLookup> > cl) = 0;

    /**
     * Pass all stored data for specified keys through the given callback.
     */
    virtual void dump(std::vector<uint16_t> &vbids,
                      shared_ptr<Callback<GetValue> > cb,
                      shared_ptr<Callback<CacheLookup> > cl) = 0;

    /**
     * Pass all stored data for the given vbucket through the given
     * callback.
     */
    virtual void dump(uint16_t vbid, shared_ptr<Callback<GetValue> > cb,
                      shared_ptr<Callback<CacheLookup> > cl) = 0;

    /**
     * Check if the kv-store supports a dumping all of the keys
     * @return true you may call dumpKeys() to do a prefetch
     *              of the keys
     */
    virtual bool isKeyDumpSupported() {
        return false;
    }

    /**
     * Dump the keys from a given set of vbuckets
     * @param vbids the vbuckets to dump
     * @param cb the callback to fire for each document
     */
    virtual void dumpKeys(std::vector<uint16_t> &vbids, shared_ptr<Callback<GetValue> > cb) {
        (void)vbids; (void)cb;
        throw std::runtime_error("Backed does not support dumpKeys()");
    }

    virtual void dumpDeleted(uint16_t vbid, shared_ptr<Callback<GetValue> > cb) {
        (void) vbid; (void) cb;
        throw std::runtime_error("Backend does not support dumpDeleted()");
    }

    virtual size_t getNumPersistedDeletes(uint16_t) {
        return 0;
    }

    virtual size_t getNumItems(uint16_t) {
        return 0;
    }

    /**
     * This method is called before persisting a batch of data if you'd like to
     * do stuff to them that might improve performance at the IO layer.
     */
    virtual void optimizeWrites(std::vector<queued_item> &items) {
        (void)items;
        // EMPTY
    }

    bool isReadOnly(void) {
        return readOnly;
    }

protected:
    bool readOnly;

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
     * @param stats the engine stats
     * @param config the engine configuration
     * @param read_only true if the kvstore instance is for read operations only
     */
    static KVStore *create(EPStats &stats, Configuration &config,
                           bool read_only = false);
};

#endif  // SRC_KVSTORE_H_
