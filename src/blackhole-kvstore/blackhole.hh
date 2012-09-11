/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef BLACKHOLE_KVSTORE_H
#define BLACKHOLE_KVSTORE_H 1

#include "kvstore.hh"

/**
 * A black hole kv-store
 */
class BlackholeKVStore : public KVStore {
public:
    /**
     * Build it!
     */
    BlackholeKVStore(bool read_only = false);

    /**
     * Cleanup.
     */
    virtual ~BlackholeKVStore();

    /**
     * Reset database to a clean state.
     */
    void reset(void);

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(void);

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(void);

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void);

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
     * Overrides del().
     */
    void del(const Item &itm, uint64_t rowid,
             Callback<int> &cb);

    bool delVBucket(uint16_t vbucket, bool recreate);

    vbucket_map_t listPersistedVbuckets(void);


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

private:
};

#endif /* Blackhole_KVSTORE_H */
