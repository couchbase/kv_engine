/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MC_KVSTORE_H
#define MC_KVSTORE_H 1

#include <map>
#include <vector>

#include "kvstore.hh"
#include "item.hh"
#include "stats.hh"

class EventuallyPersistentEngine;
class EPStats;

/**
 * A persistence store that stores stuff via memcached protocol.
 */
class MCKVStore : public KVStore {
public:

    /**
     * Build it!
     */
    MCKVStore(EventuallyPersistentEngine &theEngine);

    /**
     * Copying opens a new underlying DB.
     */
    MCKVStore(const MCKVStore &from);

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
    bool commit() {
        if (intransaction) {
            intransaction = false;
        }
        // !intransaction == not in a transaction == committed
        return !intransaction;
    }

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
    void del(const std::string &key, uint64_t rowid,
             uint16_t vb, uint16_t vbver,
             Callback<int> &cb);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);

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
    void dump(Callback<GetValue> &cb);

    void dump(uint16_t vb, Callback<GetValue> &cb);

private:

    void insert(const Item &itm, uint16_t vb_version, Callback<mutation_result> &cb);
    void update(const Item &itm, uint16_t vb_version, Callback<mutation_result> &cb);

    EPStats &stats;

    void open() {
        // Wake Up!
        intransaction = false;
    }

    void close() {
        intransaction = false;
    }

    bool intransaction;


    // Disallow assignment.
    void operator=(const MCKVStore &from);
    EventuallyPersistentEngine &engine;
};

#endif /* MC_KVSTORE_H */
