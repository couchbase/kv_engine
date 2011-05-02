/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include "mc-kvstore.hh"

MCKVStore::MCKVStore(EPStats &st) : KVStore(),
                                    stats(st), intransaction(false) {
    open();
}

MCKVStore::MCKVStore(const MCKVStore &from) : KVStore(from),
    stats(from.stats), intransaction(false) {
    open();
}

void MCKVStore::insert(const Item &itm, uint16_t vb_version,
                       Callback<mutation_result> &cb) {
    (void)itm;
    (void)vb_version;
    (void)cb;
    abort();
}

void MCKVStore::update(const Item &itm, uint16_t vb_version,
                       Callback<mutation_result> &cb) {
    (void)itm;
    (void)vb_version;
    (void)cb;
    abort();
}

vbucket_map_t MCKVStore::listPersistedVbuckets() {
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    abort();
    return rv;
}

void MCKVStore::set(const Item &itm, uint16_t vb_version,
                    Callback<mutation_result> &cb) {
    if (itm.getId() <= 0) {
        insert(itm, vb_version, cb);
    } else {
        update(itm, vb_version, cb);
    }
}

void MCKVStore::get(const std::string &key, uint64_t rowid,
                    uint16_t vb, uint16_t vbver, Callback<GetValue> &cb) {
    (void)key;
    (void)rowid;
    (void)vb;
    (void)vbver;
    (void)cb;
    abort();
}

void MCKVStore::reset() {
    abort();
}

void MCKVStore::del(const std::string &key, uint64_t rowid,
                    uint16_t vb, uint16_t vbver,
                    Callback<int> &cb) {
    (void)key;
    (void)rowid;
    (void)vb;
    (void)vbver;
    (void)cb;
    abort();
}

bool MCKVStore::delVBucket(uint16_t vbucket, uint16_t vb_version,
                           std::pair<int64_t, int64_t> row_range) {
    (void)vbucket;
    (void)vb_version;
    (void)row_range;

    bool rv = true;
    abort();
    return rv;
}

bool MCKVStore::delVBucket(uint16_t vbucket, uint16_t vb_version) {
    (void)vbucket;
    (void)vb_version;

    bool rv = true;
    abort();
    return rv;
}

bool MCKVStore::snapshotVBuckets(const vbucket_map_t &m) {
    (void)m;
    abort();
}

bool MCKVStore::snapshotStats(const std::map<std::string, std::string> &m) {
    (void)m;
    abort();
}

void MCKVStore::dump(Callback<GetValue> &cb) {
    (void)cb;
    abort();
}

void MCKVStore::dump(uint16_t vb, Callback<GetValue> &cb) {
    (void)vb;
    (void)cb;
    abort();
}


StorageProperties MCKVStore::getStorageProperties() {
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}
