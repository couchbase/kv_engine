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
#include <vbucket.h>
#include <cJSON.h>

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

    cachedVBStates.reserve(config.getMaxVBuckets());
    for (uint16_t i = shardId; i < maxVbuckets; i += maxShards) {
        readVBState(i);
    }
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

   /* delete all the cached vbucket states */
   std::vector<vbucket_state *>::iterator stateItr;
   for (stateItr = cachedVBStates.begin(); stateItr != cachedVBStates.end();
        stateItr++) {
        vbucket_state *vbstate = *stateItr;
        if (vbstate) {
            delete vbstate;
            *stateItr = NULL;
        }
   }

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

static const std::string getJSONObjString(const cJSON *i) {
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        abort();
    }
    return i->valuestring;
}

void ForestKVStore::readVBState(uint16_t vbId) {
    fdb_status status;
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    std::string failovers("[{\"id\":0,\"seq\":0}]");
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t driftCounter = INITIAL_DRIFT;

    char keybuf[20];
    fdb_doc *statDoc;
    sprintf(keybuf, "partition%d", vbId);
    fdb_doc_create(&statDoc, (void *)keybuf, strlen(keybuf), NULL, 0, NULL, 0);
    status = fdb_get(vbStateHandle, statDoc);

    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_DEBUG,
            "Warning: failed to retrieve stat info for vBucket=%d "
            "error=%s", vbId, fdb_error_msg(status));
    } else {
        cJSON *jsonObj = cJSON_Parse((char *)statDoc->body);

        if (!jsonObj) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to parse the vbstat json doc for vbucket %d: %s",
                vbId, (char *)statDoc->body);
            fdb_doc_free(statDoc);
            abort();
        }

        const std::string vb_state = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "state"));

        const std::string checkpoint_id = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj,"checkpoint_id"));

        const std::string max_deleted_seqno = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "max_deleted_seqno"));

        const std::string snapStart = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "snap_start"));

        const std::string snapEnd = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "snap_end"));

        const std::string maxCasValue = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "max_cas"));

        const std::string driftCount = getJSONObjString(
                                 cJSON_GetObjectItem(jsonObj, "drift_counter"));

        cJSON *failover_json = cJSON_GetObjectItem(jsonObj, "failover_table");
        if (vb_state.compare("") == 0 || checkpoint_id.compare("") == 0
               || max_deleted_seqno.compare("") == 0) {
             LOG(EXTENSION_LOG_WARNING,
                 "Warning: state JSON doc for vbucket %d is in the wrong format: %s",
                 vbId, (char *)statDoc->body);
        } else {
            state = VBucket::fromString(vb_state.c_str());
            parseUint64(max_deleted_seqno.c_str(), &maxDeletedSeqno);
            parseUint64(checkpoint_id.c_str(), &checkpointId);

            if (snapStart.compare("")) {
                parseUint64(snapStart.c_str(), &lastSnapStart);
            }

            if (snapEnd.compare("")) {
                parseUint64(snapEnd.c_str(), &lastSnapEnd);
            }

            if (maxCasValue.compare("")) {
                parseUint64(maxCasValue.c_str(), &maxCas);
            }

            if (driftCount.compare("")) {
                parseInt64(driftCount.c_str(), &driftCounter);
            }

            if (failover_json) {
                char* json = cJSON_PrintUnformatted(failover_json);
                failovers.assign(json);
                free(json);
            }
        }

        cJSON_Delete(jsonObj);
    }

    cachedVBStates[vbId] = new vbucket_state(state, checkpointId,
                                             maxDeletedSeqno, 0, 0,
                                             lastSnapStart, lastSnapEnd,
                                             maxCas, driftCounter,
                                             failovers);
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
