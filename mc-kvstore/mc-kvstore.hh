/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MC_KVSTORE_H
#define MC_KVSTORE_H 1

#include "kvstore.hh"
#include "item.hh"
#include "stats.hh"

#include "mc-kvstore/mc-engine.hh"

class EventuallyPersistentEngine;
class EPStats;
class MCKVStoreTestEnvironment;
/**
 * A persistence store that stores stuff via memcached protocol.
 */
class MCKVStore : public KVStore {
public:

    MCKVStore(MCKVStoreTestEnvironment &mock);

    /**
     * Build it!
     */
    MCKVStore(EventuallyPersistentEngine &theEngine);

    /**
     * Copying opens a new underlying DB.
     */
    MCKVStore(const MCKVStore &from);

    virtual void addStats(const std::string &prefix, ADD_STAT add_stat, const void *c);

    /**
     * Cleanup.
     */
    virtual ~MCKVStore() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset();

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() {
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
    void rollback() {
        if(intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties();

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

    bool isKeyDumpSupported() {
        return true;
    }

    void dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb);

    void optimizeWrites(std::vector<queued_item> &items);

    void processTxnSizeChange(size_t txn_size);

    void setVBBatchCount(size_t batch_count);

    void destroyInvalidVBuckets(bool destroyOnlyOne = false) {
        (void) destroyOnlyOne;
    }

private:

    EPStats &stats;

    void open();

    void close() {
        intransaction = false;
        delete mc;
        mc = NULL;
    }

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T val,
                 ADD_STAT add_stat, const void *c);

    bool intransaction;


    // Disallow assignment.
    void operator=(const MCKVStore &from);

    MemcachedEngine *mc;
    Configuration &config;
    EventuallyPersistentEngine &engine;
    size_t vbBatchCount;
    size_t vbBatchSize;
};

#endif /* MC_KVSTORE_H */
