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
#include <platform/dirutils.h>

using namespace CouchbaseDirectoryUtilities;

ForestKVStore::ForestKVStore(KVStoreConfig &config) :
    configuration(config), intransaction(false),
    dbname(configuration.getDBName()), dbFileRevNum(1) {

    /* create the data directory */
    createDataDir(dbname);
    fdb_status status;

    uint16_t shardId = config.getShardId();
    uint16_t maxVbuckets = config.getMaxVBuckets();
    uint16_t maxShards = config.getMaxShards();

    std::stringstream dbFile;
    dbFile << dbname << "/" << shardId << ".fdb";

    std::stringstream prefix;
    prefix << shardId << ".fdb";

    std::vector<std::string> files = findFilesContaining(dbname,
                                     prefix.str().c_str());

    std::vector<std::string>::iterator fileItr;

    for (fileItr = files.begin(); fileItr != files.end(); ++fileItr) {
        const std::string &filename = *fileItr;
        size_t secondDot = filename.rfind(".");
        std::string revNumStr = filename.substr(secondDot + 1);
        char *ptr = NULL;
        uint64_t revNum = strtoull(revNumStr.c_str(), &ptr, 10);
        if (revNum == 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Invalid revision number obtained for database file");
            abort();
        }

        if (revNum > dbFileRevNum) {
            dbFileRevNum = revNum;
        }
    }

    dbFile << "." << dbFileRevNum;

    fileConfig = fdb_get_default_config();
    kvsConfig = fdb_get_default_kvs_config();

    status = fdb_open(&dbFileHandle, dbFile.str().c_str(), &fileConfig);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Opening the database file instance failed with error: %s\n",
            fdb_error_msg(status));
        abort();
    }

    fdb_kvs_handle *kvsHandle = NULL;
    /* Initialize ForestDB KV store instances for all the vbuckets
     * that belong to this shard */
    for (uint16_t i = shardId; i < maxVbuckets; i += maxShards) {
        writeHandleMap.insert(std::make_pair(i, kvsHandle));
        readHandleMap.insert(std::make_pair(i, kvsHandle));
    }

    /* Initialize the handle for the vbucket state information */
    status = fdb_kvs_open(dbFileHandle, &vbStateHandle, "vbstate",
                          &kvsConfig);

    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Opening the vbucket state KV store instance failed "
            "with error: %s\n", fdb_error_msg(status));
        abort();
    }
    /* TODO: populate vbucket state information */
}

fdb_config ForestKVStore::getFileConfig() {
    return fileConfig;
}

fdb_kvs_config ForestKVStore::getKVConfig() {
    return kvsConfig;
}

ForestKVStore::ForestKVStore(const ForestKVStore &copyFrom) :
    KVStore(copyFrom), configuration(copyFrom.configuration),
    intransaction(false), dbname(copyFrom.dbname) {

    /* create the data directory */
    createDataDir(dbname);
}

ForestKVStore::~ForestKVStore() {
   close();

   /* Close all the KV store instances */
   unordered_map<uint16_t, fdb_kvs_handle *>::iterator handleItr;
   for (handleItr = writeHandleMap.begin(); handleItr != writeHandleMap.end();
        handleItr++) {
       fdb_kvs_handle *kvsHandle = handleItr->second;
       fdb_kvs_close(kvsHandle);
   }

   writeHandleMap.clear();

   for (handleItr = readHandleMap.begin(); handleItr != readHandleMap.end();
        handleItr++) {
       fdb_kvs_handle *kvsHandle = handleItr->second;
       fdb_kvs_close(kvsHandle);
   }

   readHandleMap.clear();

   /* Close the database file instance */
   fdb_close(dbFileHandle);
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

DBFileInfo ForestKVStore::getDbFileInfo(uint16_t dbId) {
    DBFileInfo dbInfo;
    return dbInfo;
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
