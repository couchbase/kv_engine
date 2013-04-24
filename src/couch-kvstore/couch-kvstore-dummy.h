/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_COUCH_KVSTORE_COUCH_KVSTORE_DUMMY_H_
#define SRC_COUCH_KVSTORE_COUCH_KVSTORE_DUMMY_H_ 1

#ifdef HAVE_LIBCOUCHSTORE
#error "This header file should only be included if you don't have libcouchstore"
#endif

#include "config.h"

#include <map>
#include <string>

#include "kvstore.h"

class EPStats;

/**
 * THis is a dummy implementation of the couchkvstore just to satisfy the
 * linker without a too advanced Makefile (for builds without libcouchkvstore)
 */
class CouchKVStore : public KVStore
{
public:
    CouchKVStore(EPStats &stats, Configuration &config, bool read_only = false);
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
};

#endif  // SRC_COUCH_KVSTORE_COUCH_KVSTORE_DUMMY_H_
