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

#include "forest-kvstore/forest-kvstore.h"
#include <sys/stat.h>

ForestKVStore::ForestKVStore(KVStoreConfig &config) :
    dbname(config.getDBName()) {

    /* create the data directory */
    createDataDir(dbname);
}

ForestKVStore::ForestKVStore(const ForestKVStore &copyFrom) :
    dbname(copyFrom.dbname) {

    /* create the data directory */
    createDataDir(dbname);
}

ForestKVStore::~ForestKVStore() {

}

void ForestKVStore::close() {
    intransaction = false;
}

void ForestKVStore::delVBucket(uint16_t vbucket) {

}

void ForestKVStore::getWithHeader(void *dbHandle, const std::string &key,
                                  uint16_t vb, Callback<GetValue> &cb,
                                  bool fetchDelete) {

}

bool ForestKVStore::commit(Callback<kvstats_ctx> *cb, uint64_t snapStartSeqno,
                           uint64_t snapEndSeqno, uint64_t maxCas,
                           uint64_t driftCounter) {
    return true;
}

StorageProperties ForestKVStore::getStorageProperties(void) {
    StorageProperties rv(true, true, true, true);
    return rv;
}

void ForestKVStore::set(const Item &itm, Callback<mutation_result> &cb) {

}

void ForestKVStore::get(const std::string &key, uint16_t vb,
                        Callback<GetValue> &cb, bool fetchDelete) {

}

void ForestKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) {

}

void ForestKVStore::del(const Item &itm, Callback<int> &cb) {

}

std::vector<vbucket_state *> ForestKVStore::listPersistedVbuckets(void) {
    return cachedVBStates;
}

bool ForestKVStore::snapshotStats(const std::map<std::string,
                                  std::string> &engine_stats) {
    return true;
}

bool ForestKVStore::snapshotVBucket(uint16_t vbucketId,
                                    vbucket_state &vbstate,
                                    Callback<kvstats_ctx> *cb) {
    return true;
}

bool ForestKVStore::compactVBucket(const uint16_t vbid, compaction_ctx *cookie,
                                   Callback<kvstats_ctx> &kvcb) {
    return true;
}

RollbackResult ForestKVStore::rollback(uint16_t vbid, uint64_t rollbackSeqno,
                                       shared_ptr<RollbackCB> cb) {
   return RollbackResult(true, 0, 0, 0);
}
