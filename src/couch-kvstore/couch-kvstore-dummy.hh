#ifndef COUCH_KVSTORE_DUMMY_H
#define COUCH_KVSTORE_DUMMY_H 1

#ifdef HAVE_LIBCOUCHSTORE
#error "This header file should only be included if you don't have libcouchstore"
#endif

#include "kvstore.hh"

class EventuallyPersistentEngine;
class EPStats;

/**
 * THis is a dummy implementation of the couchkvstore just to satisfy the
 * linker without a too advanced Makefile (for builds without libcouchkvstore)
 */
class CouchKVStore : public KVStore
{
public:
    CouchKVStore(EventuallyPersistentEngine &theEngine, bool read_only = false);
    CouchKVStore(const CouchKVStore &from);
    void reset();
    bool begin();
    bool commit();
    void rollback();
    StorageProperties getStorageProperties();
    void set(const Item &item,
             Callback<mutation_result> &cb);
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb,
             Callback<GetValue> &cb);
    void del(const Item &itm, uint64_t rowid,
             Callback<int> &cb);
    bool delVBucket(uint16_t vbucket);
    vbucket_map_t listPersistedVbuckets(void);
    bool snapshotStats(const std::map<std::string, std::string> &m);
    bool snapshotVBuckets(const vbucket_map_t &m);
    void dump(shared_ptr<Callback<GetValue> > cb);
    void dump(uint16_t vbid, shared_ptr<Callback<GetValue> > cb);
    void destroyInvalidVBuckets(bool destroyOnlyOne = false);
};

#endif
