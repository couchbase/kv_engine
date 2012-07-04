/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_HH
#define KVSTORE_HH 1

#include <map>
#include <string>
#include <utility>

#include <cstring>

#include "stats.hh"
#include "item.hh"
#include "queueditem.hh"
#include "mutation_log.hh"

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
    vbucket_state_t state;
    uint64_t checkpointId;
    uint32_t maxDeletedSeqno;
};

/**
 * Type of vbucket map.
 *
 * key.first is the vbucket identifier.
 * key.second is the vbucket version
 * value is a pair of string representation of the vbucket state and
 * its latest checkpoint Id persisted.
 */
typedef std::map<std::pair<uint16_t, uint16_t>, vbucket_state> vbucket_map_t;

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

    StorageProperties(size_t c, size_t r, size_t w, bool evb, bool evd, bool pd)
        : maxc(c), maxr(r), maxw(w), efficientVBDump(evb),
          efficientVBDeletion(evd), persistedDeletions(pd) {}

    //! The maximum number of active queries.
    size_t maxConcurrency()   const { return maxc; }
    //! Maximum number of active read-only connections.
    size_t maxReaders()       const { return maxr; }
    //! Maximum number of active connections for read and write.
    size_t maxWriters()       const { return maxw; }
    //! True if we can efficiently dump a single vbucket.
    bool hasEfficientVBDump() const { return efficientVBDump; }
    //! True if we can efficiently delete a vbucket all at once.
    bool hasEfficientVBDeletion() const { return efficientVBDeletion; }
    //! True if we can persisted deletions to disk.
    bool hasPersistedDeletions() const { return persistedDeletions; }

private:
    size_t maxc;
    size_t maxr;
    size_t maxw;
    bool efficientVBDump;
    bool efficientVBDeletion;
    bool persistedDeletions;
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
    KVStore() : engine(NULL) { }


    virtual ~KVStore() {}

    virtual bool getEstimatedItemCount(size_t &items);


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
    virtual void set(const Item &item, uint16_t vb_version,
                     Callback<mutation_result> &cb) = 0;

    /**
     * Get an item from the kv store.
     */
    virtual void get(const std::string &key, uint64_t rowid,
                     uint16_t vb, uint16_t vbver,
                     Callback<GetValue> &cb) = 0;

    /**
     * Delete an item from the kv store.
     */
    virtual void del(const Item &itm, uint64_t rowid,
                     uint16_t vbver, Callback<int> &cb) = 0;

    /**
     * Bulk delete some versioned records from a vbucket.
     */
    virtual bool delVBucket(uint16_t vbucket, uint16_t vb_version) = 0;

    /**
     * Bulk delete some versioned records from a vbucket.
     */
    virtual bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                            std::pair<int64_t, int64_t> row_range) = 0;

    /**
     * Get a list of all persisted vbuckets (with their versions and states).
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
     * Pass all stored data through the given callback.
     */
    virtual void dump(shared_ptr<Callback<GetValue> > cb) = 0;

    /**
     * Pass all stored data for the given vbucket through the given
     * callback.
     */
    virtual void dump(uint16_t vbid, shared_ptr<Callback<GetValue> > cb) = 0;

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
    virtual void dumpKeys(const std::vector<uint16_t> &vbids, shared_ptr<Callback<GetValue> > cb) {
        (void)vbids; (void)cb;
        throw std::runtime_error("Backed does not support dumpKeys()");
    }

    virtual void dumpDeleted(uint16_t vbid, shared_ptr<Callback<GetValue> > cb) {
        (void) vbid; (void) cb;
        throw std::runtime_error("Backend does not support dumpDeleted()");
    }

    /**
     * Get the number of data shards in this kvstore.
     */
    virtual size_t getNumShards() {
        return 1;
    }

    /**
     * get the shard ID for the given queued item.
     */
    virtual size_t getShardId(const QueuedItem &i) {
        (void)i;
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

    virtual void processTxnSizeChange(size_t txn_size) {
        (void)txn_size;
    }

    virtual void setVBBatchCount(size_t batch_count) {
        (void)batch_count;
    }

    /**
     * Remove invalid vbuckets from the underlying storage engine.
     * @param destroyOnlyOne True if this run should remove only one invalid vbucket.
     * This can be set to true if we want to delete all invalid vbuckets over the time.
     */
    virtual void destroyInvalidVBuckets(bool destroyOnlyOne = false) = 0;


    /**
     * Warm up the cache by using the given mutation log (this is actually an access log),
     * The default implementaiton of the warmup warmup will scan the access file and load
     * each key in a sequence. Each backend may overload this function with a more optimal
     * version.
     *
     * NOTE: this operation block until all warmup is complete
     *
     * @param lf the access log file
     * @param vbmap A map containing the map of vb id and version to warm up
     * @param cb callback used to load objects into the cache
     * @param estimate is a callback used to push out the estimated number of
     *                 items going to be warmed up
     * @return number of items loaded
     */
    virtual size_t warmup(MutationLog &lf,
                          const std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &vb,
                          Callback<GetValue> &cb,
                          Callback<size_t> &estimate);

    void setEngine(EventuallyPersistentEngine *theEngine) {
        engine = theEngine;
    }

    EventuallyPersistentEngine *getEngine(void) { return engine; }

protected:
    EventuallyPersistentEngine *engine;

};

/**
 * The KVStoreFactory creates the correct KVStore instance(s) when
 * needed by EPStore.
 */
class KVStoreFactory {
public:

    /**
     * Create a KVStore with the given properties.
     *
     * @param type the type of DB to set up
     * @param stats the server stats
     * @param conf type-specific parameters
     */
    static KVStore *create(EventuallyPersistentEngine &theEngine);
};

#endif // KVSTORE_HH
