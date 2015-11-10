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

#include "common.h"

#include <sys/stat.h>
#include <platform/dirutils.h>
#include <vbucket.h>
#include <JSON_checker.h>
#include <locks.h>

using namespace CouchbaseDirectoryUtilities;

Mutex ForestKVStore::initLock;
int ForestKVStore::numGlobalFiles = 0;

void ForestKVStore::initForestDb() {
    LockHolder lh(initLock);
    if (numGlobalFiles == 0) {
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        fdb_status status = fdb_init(&fileConfig);
        if (status != FDB_RESULT_SUCCESS) {
            throw std::logic_error("ForestKVStore::initForestDb: failed "
                    "with status:" + std::to_string(status));
        }
        ObjectRegistry::onSwitchThread(epe);
    }
    ++numGlobalFiles;
}

void ForestKVStore::shutdownForestDb() {
   LockHolder lh(initLock);
   if (--numGlobalFiles == 0) {
       EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
       fdb_status status = fdb_shutdown();
       if (status != FDB_RESULT_SUCCESS) {
           LOG(EXTENSION_LOG_WARNING,
               "Shutting down forestdb failed with error: %s",
               fdb_error_msg(status));
       }
       ObjectRegistry::onSwitchThread(epe);
   }
}

ForestKVStore::ForestKVStore(KVStoreConfig &config) :
    KVStore(config), intransaction(false),
    dbname(config.getDBName()), dbFileRevNum(1) {

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

    initForestDb();

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

    // Initialize the handle for the vbucket state information
    status = fdb_kvs_open_default(dbFileHandle, &vbStateHandle, &kvsConfig);

    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Opening the vbucket state KV store instance failed "
            "with error: %s\n", fdb_error_msg(status));
        abort();
    }

    cachedVBStates.reserve(maxVbuckets);
    for (uint16_t i = 0; i < maxVbuckets; ++i) {
        cachedVBStates.push_back((vbucket_state *)NULL);
        if (i == shardId) {
            readVBState(i);
            shardId += maxShards; // jump to next vbucket in shard
        }
    }

    cachedDocCount.assign(maxVbuckets, Couchbase::RelaxedAtomic<size_t>(-1));
    cachedDeleteCount.assign(maxVbuckets, Couchbase::RelaxedAtomic<size_t>(-1));
}

fdb_config ForestKVStore::getFileConfig() {
    return fileConfig;
}

fdb_kvs_config ForestKVStore::getKVConfig() {
    return kvsConfig;
}

ForestKVStore::ForestKVStore(const ForestKVStore &copyFrom) :
    KVStore(copyFrom), intransaction(false),
    dbname(copyFrom.dbname) {

    /* create the data directory */
    createDataDir(dbname);
    LockHolder lh(initLock);
    ++numGlobalFiles;
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
   std::unordered_map<uint16_t, fdb_kvs_handle *>::iterator handleItr;
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

   shutdownForestDb();
}

void ForestKVStore::reset(uint16_t vbucketId) {
    vbucket_state *state = cachedVBStates[vbucketId];
    if (!state) {
        throw std::invalid_argument("ForestKVStore::reset::No entry "
                "in cached states for vbucket " +
                std::to_string(vbucketId));
    }

    state->reset();

    cachedDocCount[vbucketId] = 0;
    cachedDeleteCount[vbucketId] = 0;

    fdb_kvs_handle *kvsHandle = getKvsHandle(vbucketId, handleType::READER);
    fdb_status status;

    if (kvsHandle != nullptr) {
        status = fdb_kvs_close(kvsHandle);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::reset:fdb_kvs_close API call failed for "
                "vbucket %" PRIu16" with error: %s", vbucketId,
                fdb_error_msg(status));
        }
    }

    readHandleMap[vbucketId] = NULL;

    kvsHandle = getKvsHandle(vbucketId, handleType::WRITER);

    if (kvsHandle != nullptr) {
        status = fdb_kvs_close(kvsHandle);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "ForestKVStore::reset:fdb_kvs_close API call failed for "
                "vbucket %" PRIu16" with error: %s", vbucketId,
                fdb_error_msg(status));
        }
    }

    writeHandleMap[vbucketId] = NULL;

    char kvsName[20];
    sprintf(kvsName,"partition%" PRIu16, vbucketId);

    status = fdb_kvs_remove(dbFileHandle, kvsName);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "ForestKVStore::reset: ForestDB KV Store remove failed for "
            "vbucket :%" PRIu16" with error: %s", vbucketId,
            fdb_error_msg(status));
    }

    std::string stateStr = state->toJSON();

    if (!stateStr.empty()) {
        char keybuf[20];
        fdb_doc statDoc;
        memset(&statDoc, 0, sizeof(statDoc));
        sprintf(keybuf, "partition%d", vbucketId);
        statDoc.key = keybuf;
        statDoc.keylen = strlen(keybuf);
        statDoc.meta = NULL;
        statDoc.metalen = 0;
        statDoc.body = const_cast<char *>(stateStr.c_str());
        statDoc.bodylen = stateStr.length();
        fdb_status status = fdb_set(vbStateHandle, &statDoc);

        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::reset:Failed to save "
                "vbucket state for vbucket=%" PRIu16" error=%s", vbucketId,
                fdb_error_msg(status));
        }
    }
}

ForestRequest::ForestRequest(const Item &it, MutationRequestCallback &cb ,bool del)
    : IORequest(it.getVBucketId(), cb, del, it.getKey()),
      status(MUTATION_SUCCESS) { }

ForestRequest::~ForestRequest() {
}

void ForestKVStore::close() {
    intransaction = false;
}

ENGINE_ERROR_CODE ForestKVStore::readVBState(uint16_t vbId) {
    fdb_status status = FDB_RESULT_SUCCESS;
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    std::string failovers;
    int64_t highSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t driftCounter = INITIAL_DRIFT;

    fdb_kvs_info kvsInfo;
    fdb_kvs_handle *kvsHandle = getKvsHandle(vbId, handleType::READER);
    if (kvsHandle != nullptr) {
        status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
        //TODO: Update the purge sequence number
        if (status == FDB_RESULT_SUCCESS) {
            highSeqno = static_cast<int64_t>(kvsInfo.last_seqnum);
        } else {
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::readVBState: Failed to "
                "read KV Store info for vbucket: %d with error: %s", vbId,
                fdb_error_msg(status));
            return forestErr2EngineErr(status);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "ForestKVStore::readVBState: Failed to "
            "get reader handle for vbucket: %d", vbId);
    }

    char keybuf[20];
    fdb_doc *statDoc;
    sprintf(keybuf, "partition%d", vbId);
    fdb_doc_create(&statDoc, (void *)keybuf, strlen(keybuf), NULL, 0, NULL, 0);
    status = fdb_get(vbStateHandle, statDoc);

    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_DEBUG,
            "Failed to retrieve stat info for vBucket=%d "
            "with error=%s", vbId , fdb_error_msg(status));
    } else {
        cJSON *jsonObj = cJSON_Parse((char *)statDoc->body);

        if (!jsonObj) {
            fdb_doc_free(statDoc);
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::readVBState: Failed to "
                "parse the vbstat json doc for vbucket: %d: %s", vbId,
                (char *)statDoc->body);
            return forestErr2EngineErr(status);
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
                 "ForestKVStore::readVBState: State JSON "
                 "doc for vbucket: %d is in the wrong format: %s,"
                 "vb state: %s, checkpoint id: %s and max deleted seqno: %s",
                 vbId, (char *)statDoc->body, vb_state.c_str(),
                 checkpoint_id.c_str(), max_deleted_seqno.c_str());
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

    if (failovers.empty()) {
        failovers.assign("[{\"id\":0,\"seq\":0}]");
    }

    delete cachedVBStates[vbId];
    cachedVBStates[vbId] = new vbucket_state(state, checkpointId,
                                             maxDeletedSeqno, highSeqno, 0,
                                             lastSnapStart, lastSnapEnd,
                                             maxCas, driftCounter,
                                             failovers);
    fdb_doc_free(statDoc);
    return forestErr2EngineErr(status);
}

void ForestKVStore::delVBucket(uint16_t vbucket) {
    fdb_kvs_handle *kvsHandle = getKvsHandle(vbucket, handleType::READER);
    fdb_status status;

    if (kvsHandle != nullptr) {
        status = fdb_kvs_close(kvsHandle);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "fdb_kvs_close API call failed for vbucket %d with error: %s "
                "in ForestKVStore::delVBucket\n", vbucket, fdb_error_msg(status));
        }
    }

    readHandleMap[vbucket] = NULL;

    kvsHandle = getKvsHandle(vbucket, handleType::WRITER);

    if (kvsHandle != nullptr) {
        status = fdb_kvs_close(kvsHandle);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "fdb_kvs_close API call failed for vbucket %d with error: %s "
                "in ForestKVStore::delVBucket\n", vbucket, fdb_error_msg(status));
        }
    }

    writeHandleMap[vbucket] = NULL;

    char kvsName[20];
    sprintf(kvsName,"partition%d", vbucket);

    status = fdb_kvs_remove(dbFileHandle, kvsName);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "ForestDB KV Store remove failed for vbucket "
            "%d with error: %s\n", vbucket,
            fdb_error_msg(status));
    }

    if (cachedVBStates[vbucket]) {
        delete cachedVBStates[vbucket];
    }

    std::string failovers("[{\"id\":0, \"seq\":0}]");
    cachedVBStates[vbucket] = new vbucket_state(vbucket_state_dead, 0, 0, 0, 0,
                                                0, 0, 0, INITIAL_DRIFT,
                                                failovers);

    vbucket_state *state = cachedVBStates[vbucket];
    std::string stateStr = state->toJSON();

    if (!stateStr.empty()) {
        fdb_doc statDoc;
        memset(&statDoc, 0, sizeof(statDoc));
        statDoc.key = kvsName;
        statDoc.keylen = strlen(kvsName);
        statDoc.meta = NULL;
        statDoc.metalen = 0;
        statDoc.body = const_cast<char *>(stateStr.c_str());
        statDoc.bodylen = stateStr.length();
        fdb_status status = fdb_set(vbStateHandle, &statDoc);

        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "ForestKVStore::delVBucket: Failed to "
                    "save vbucket state for vbucket=%" PRIu16" error=%s",
                    vbucket, fdb_error_msg(status));
        }
    }
}

ENGINE_ERROR_CODE ForestKVStore::forestErr2EngineErr(fdb_status errCode) {
    switch (errCode) {
    case FDB_RESULT_SUCCESS:
        return ENGINE_SUCCESS;
    case FDB_RESULT_ALLOC_FAIL:
        return ENGINE_ENOMEM;
    case FDB_RESULT_KEY_NOT_FOUND:
        return ENGINE_KEY_ENOENT;
    case FDB_RESULT_NO_SUCH_FILE:
    case FDB_RESULT_NO_DB_HEADERS:
    default:
        // same as the general error return code of
        // EventuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
}

void ForestKVStore::getWithHeader(void *dbHandle, const std::string &key,
                                  uint16_t vb, Callback<GetValue> &cb,
                                  bool fetchDelete) {
    fdb_file_handle *dbFileHandle = (fdb_file_handle *)dbHandle;
    RememberingCallback<GetValue> *rc =
                       static_cast<RememberingCallback<GetValue> *>(&cb);
    bool getMetaOnly = rc && rc->val.isPartial();
    GetValue rv;
    fdb_kvs_handle *kvsHandle = NULL;
    fdb_status status;
    char kvsName[20];
    sprintf(kvsName, "partition%d", vb);

    std::unordered_map<uint16_t, fdb_kvs_handle*>::iterator found =
                                                       readHandleMap.find(vb);

    //There will always be a handle in the handle map for readers. It will be
    //initialized to NULL if one hasn't been initialized yet.
    if (found == readHandleMap.end()) {
        throw std::invalid_argument("ForestKVStore::getWithHeader: Failed "
                "to find vb (which is " + std::to_string(vb) +
                ") in readHandleMap");
    }
    if (!(found->second)) {
        status = fdb_kvs_open(dbFileHandle, &kvsHandle, kvsName, &kvsConfig);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Opening the KV store instance for vbucket %d failed "
                "with error: %s", vb, fdb_error_msg(status));
            rv.setStatus(forestErr2EngineErr(status));
            cb.callback(rv);
            return;
        }
        found->second = kvsHandle;
    }
    else {
        kvsHandle = found->second;
    }

    fdb_doc rdoc;
    memset(&rdoc, 0, sizeof(rdoc));
    rdoc.key = const_cast<char *>(key.c_str());
    rdoc.keylen = key.length();

    if (!getMetaOnly) {
        status = fdb_get(kvsHandle, &rdoc);
    } else {
        status = fdb_get_metaonly(kvsHandle, &rdoc);
    }

    if (status != FDB_RESULT_SUCCESS) {
        if (!getMetaOnly) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to retrieve metadata from "
                "database, vbucketId:%d key:%s error:%s\n",
                vb, key.c_str(), fdb_error_msg(status));
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to retrieve key value from database,"
                "vbucketId:%d key:%s error:%s deleted:%s", vb, key.c_str(),
                fdb_error_msg(status), rdoc.deleted ? "yes" : "no");
        }
    } else {
        rv = docToItem(kvsHandle, &rdoc, vb, getMetaOnly, fetchDelete);
    }

    rdoc.key = NULL;
    fdb_free_block(rdoc.meta);
    fdb_free_block(rdoc.body);

    rv.setStatus(forestErr2EngineErr(status));
    cb.callback(rv);
}

GetValue ForestKVStore::docToItem(fdb_kvs_handle *kvsHandle, fdb_doc *rdoc,
                                  uint16_t vbId, bool metaOnly, bool fetchDelete) {
    char *metadata = (char *)rdoc->meta;
    uint64_t cas;
    uint64_t rev_seqno;
    uint32_t exptime;
    uint32_t texptime;
    uint32_t itemFlags;
    uint8_t ext_meta[EXT_META_LEN];
    uint8_t ext_len;
    uint8_t conf_res_mode = 0;

    //TODO: handle metadata upgrade?
    memcpy(&cas, metadata, 8);
    memcpy(&exptime, metadata + 8, 4);
    memcpy(&texptime, metadata + 12, 4);
    memcpy(&itemFlags, metadata + 16, 4);
    memcpy(&rev_seqno, metadata + 20, 8);
    memcpy(ext_meta, metadata + 29, EXT_META_LEN);
    memcpy(&conf_res_mode, metadata + 30, CONFLICT_RES_META_LEN);
    ext_len = EXT_META_LEN;

    cas = ntohll(cas);
    exptime = ntohl(exptime);
    texptime = ntohl(texptime);
    rev_seqno = ntohll(rev_seqno);

    Item *it = NULL;
    if (metaOnly || (fetchDelete && rdoc->deleted)) {
        it = new Item((char *)rdoc->key, rdoc->keylen, itemFlags,
                      exptime, NULL, 0, ext_meta, ext_len, cas,
                      (uint64_t)rdoc->seqnum, vbId);
        if (rdoc->deleted) {
            it->setDeleted();
        }
    } else {
        size_t valuelen = rdoc->bodylen;
        void *valuePtr = rdoc->body;

        if (checkUTF8JSON((const unsigned char *)valuePtr, valuelen)) {
            ext_meta[0] = PROTOCOL_BINARY_DATATYPE_JSON;
        } else {
            ext_meta[0] = PROTOCOL_BINARY_RAW_BYTES;
        }

        it = new Item((char *)rdoc->key, rdoc->keylen, itemFlags,
                      exptime, valuePtr, valuelen, ext_meta, ext_len,
                      cas, (uint64_t)rdoc->seqnum, vbId);
    }

    it->setConflictResMode(
                   static_cast<enum conflict_resolution_mode>(conf_res_mode));
    it->setRevSeqno(rev_seqno);
    return GetValue(it);
}

vbucket_state * ForestKVStore::getVBucketState(uint16_t vbucketId) {
    return cachedVBStates[vbucketId];
}

static void commitCallback(std::vector<ForestRequest *> &committedReqs) {
    size_t commitSize = committedReqs.size();

    for (size_t index = 0; index < commitSize; index++) {
        int rv = committedReqs[index]->getStatus();
        if (committedReqs[index]->isDelete()) {
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            //TODO: For now, all mutations are passed in as insertions.
            //This needs to be revisited in order to update stats.
            mutation_result p(rv, true);
            committedReqs[index]->getSetCallback()->callback(p);
        }
    }
}

bool ForestKVStore::save2forestdb(Callback<kvstats_ctx> *cb) {
    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return true;
    }

    fdb_status status = fdb_commit(dbFileHandle, FDB_COMMIT_NORMAL);
    if (status != FDB_RESULT_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "fdb_commit failed with error: %s", fdb_error_msg(status));
        return false;
    }

    commitCallback(pendingReqsQ);

    for (size_t i = 0; i < pendingCommitCnt; i++) {
        delete pendingReqsQ[i];
    }

    pendingReqsQ.clear();
    return true;
}

bool ForestKVStore::commit(Callback<kvstats_ctx> *cb) {
    if (intransaction) {
        if (save2forestdb(cb)) {
            intransaction = false;
        }
    }

    return !intransaction;
}

StorageProperties ForestKVStore::getStorageProperties(void) {
    StorageProperties rv(true, true, true, true);
    return rv;
}

fdb_kvs_handle* ForestKVStore::getKvsHandle(uint16_t vbucketId, handleType htype) {
    char kvsName[20];
    sprintf(kvsName, "partition%d", vbucketId);
    fdb_kvs_handle *kvsHandle = NULL;
    fdb_status status;
    std::unordered_map<uint16_t, fdb_kvs_handle*>::iterator found;

    switch (htype) {
        case handleType::WRITER:
            found = writeHandleMap.find(vbucketId);
            if (found == writeHandleMap.end()) {
                throw std::invalid_argument("ForestKVStore::getKvsHandle: Failed "
                    "to find vb (which is " + std::to_string(vbucketId) +
                    ") in writeHandleMap");
            }
            break;
        case handleType::READER:
            found = readHandleMap.find(vbucketId);
            if (found == readHandleMap.end()) {
                throw std::invalid_argument("ForestKVStore::getKvsHandle: Failed "
                    "to find vb (which is " + std::to_string(vbucketId) +
                    ") in readHandleMap");
            }
            break;
    }

    if (!(found->second)) {
        status = fdb_kvs_open(dbFileHandle, &kvsHandle, kvsName, &kvsConfig);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Opening the KV store instance for vbucket %d failed "
                "with error: %s", vbucketId, fdb_error_msg(status));
            return NULL;
        }
        found->second = kvsHandle;
    } else {
        kvsHandle = found->second;
    }

    return kvsHandle;
}

static int8_t getMutationStatus(fdb_status errCode) {
    switch(errCode) {
        case FDB_RESULT_SUCCESS:
            return MUTATION_SUCCESS;
        case FDB_RESULT_NO_DB_HEADERS:
        case FDB_RESULT_NO_SUCH_FILE:
        case FDB_RESULT_KEY_NOT_FOUND:
            return DOC_NOT_FOUND;
        default:
            return MUTATION_FAILED;
    }
}

static void populateMetaData(const Item &itm, uint8_t *meta, bool deletion) {
    uint64_t cas = htonll(itm.getCas());
    uint64_t rev_seqno = htonll(itm.getRevSeqno());
    uint32_t flags = itm.getFlags();
    uint32_t exptime = itm.getExptime();
    uint32_t texptime = 0;
    uint8_t confresmode = static_cast<uint8_t>(itm.getConflictResMode());

    if (deletion) {
        texptime = ep_real_time();
    }

    exptime = htonl(exptime);
    texptime = htonl(texptime);

    memcpy(meta, &cas, 8);
    memcpy(meta + 8, &exptime, 4);
    memcpy(meta + 12, &texptime, 4);
    memcpy(meta + 16, &flags, 4);
    memcpy(meta + 20, &rev_seqno, 8);

    *(meta + 28) = FLEX_META_CODE;

    if (deletion) {
        *(meta + 29) = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        memcpy(meta + 29, itm.getExtMeta(), itm.getExtMetaLen());
    }

    memcpy(meta + 30, &confresmode, CONFLICT_RES_META_LEN);
}

void ForestKVStore::set(const Item &itm, Callback<mutation_result> &cb) {
    if (isReadOnly()) {
        throw std::logic_error("ForestKVStore::set: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("ForestKVStore::set: intransaction must be "
                        "true to perform a set operation.");
    }
    MutationRequestCallback requestcb;

    // each req will be de-allocated after commit
    requestcb.setCb = &cb;
    ForestRequest *req = new ForestRequest(itm, requestcb, false);
    fdb_doc setDoc;
    fdb_kvs_handle *kvsHandle = NULL;
    fdb_status status;
    uint8_t meta[FORESTDB_METADATA_SIZE];

    memset(meta, 0, sizeof(meta));
    populateMetaData(itm, meta, false);

    setDoc.key = const_cast<char *>(itm.getKey().c_str());
    setDoc.keylen = itm.getNKey();
    setDoc.meta = meta;
    setDoc.metalen = sizeof(meta);
    setDoc.body = const_cast<char *>(itm.getData());
    setDoc.bodylen = itm.getNBytes();
    setDoc.deleted = false;

    fdb_doc_set_seqnum(&setDoc, itm.getBySeqno());
    kvsHandle = getKvsHandle(req->getVBucketId(), handleType::WRITER);
    if (kvsHandle) {
        status = fdb_set(kvsHandle, &setDoc);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "fdb_set failed for key: %s and "
                "vbucketId: %d with error: %s", req->getKey().c_str(),
                req->getVBucketId(), fdb_error_msg(status));
            req->setStatus(getMutationStatus(status));
        }
        setDoc.body = NULL;
        setDoc.bodylen = 0;
    } else {
        LOG(EXTENSION_LOG_WARNING, "Failed to open KV store instance "
            "for key: %s and vbucketId: %d", req->getKey().c_str(),
            req->getVBucketId());
        req->setStatus(MUTATION_FAILED);
    }

    pendingReqsQ.push_back(req);
}

void ForestKVStore::get(const std::string &key, uint16_t vb,
                        Callback<GetValue> &cb, bool fetchDelete) {
    getWithHeader(dbFileHandle, key, vb, cb, fetchDelete);
}

void ForestKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) {
    bool meta_only = true;
    vb_bgfetch_queue_t::iterator itr = itms.begin();
    for (; itr != itms.end(); ++itr) {
        vb_bgfetch_item_ctx_t &bg_itm_ctx = (*itr).second;
        meta_only = bg_itm_ctx.isMetaOnly;

        RememberingCallback<GetValue> gcb;
        if (meta_only) {
            gcb.val.setPartial();
        }

        const std::string &key = (*itr).first;
        get(key, vb, gcb);
        ENGINE_ERROR_CODE status = gcb.val.getStatus();
        if (status != ENGINE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "Failed to retrieve key: %s",
                key.c_str());
        }

        std::list<VBucketBGFetchItem *> &fetches = bg_itm_ctx.bgfetched_list;
        std::list<VBucketBGFetchItem *>:: iterator fitr = fetches.begin();

        for (fitr = fetches.begin(); fitr != fetches.end(); ++fitr) {
            (*fitr)->value = gcb.val;
        }
        meta_only = true;
    }
}

void ForestKVStore::del(const Item &itm, Callback<int> &cb) {
    if (isReadOnly()) {
        throw std::logic_error("ForestKVStore::del: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("ForestKVStore::del: intransaction must be "
                        "true to perform a delete operation.");
    }
    MutationRequestCallback requestcb;
    requestcb.delCb = &cb;
    ForestRequest *req = new ForestRequest(itm, requestcb, true);
    fdb_doc delDoc;
    fdb_kvs_handle *kvsHandle = NULL;
    fdb_status status;
    uint8_t meta[FORESTDB_METADATA_SIZE];

    memset(meta, 0, sizeof(meta));
    populateMetaData(itm, meta, true);

    delDoc.key = const_cast<char *>(itm.getKey().c_str());
    delDoc.keylen = itm.getNKey();
    delDoc.meta = meta;
    delDoc.metalen = sizeof(meta);
    delDoc.deleted = true;

    fdb_doc_set_seqnum(&delDoc, itm.getBySeqno());
    kvsHandle = getKvsHandle(req->getVBucketId(), handleType::WRITER);
    if (kvsHandle) {
        status = fdb_del(kvsHandle, &delDoc);
        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "fdb_del failed for key: %s and "
                "vbucketId: %d with error: %s",  req->getKey().c_str(),
                req->getVBucketId(), fdb_error_msg(status));
            req->setStatus(getMutationStatus(status));
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "Failure to open KV store instance "
            "for key: %s and vbucketId: %d", req->getKey().c_str(),
            req->getVBucketId());
        req->setStatus(MUTATION_FAILED);
    }

    pendingReqsQ.push_back(req);
}

std::vector<vbucket_state *> ForestKVStore::listPersistedVbuckets(void) {
    return cachedVBStates;
}

size_t ForestKVStore::getNumPersistedDeletes(uint16_t vbid) {
    size_t delCount = cachedDeleteCount[vbid];
    if (delCount != (size_t) -1) {
        return delCount;
    }

    fdb_kvs_handle *kvsHandle = getKvsHandle(vbid, handleType::READER);
    if (!kvsHandle) {
        std::string err("ForestKVStore::getNumPersistedDeletes: Failed to "
                        "get reader KV store handle for vbucket:" +
                        std::to_string(static_cast<int>(vbid)));
        throw std::invalid_argument(err);
    }

    fdb_kvs_info kvsInfo;
    fdb_status status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getNumPersistedDeletes:Failed to "
            "retrieve KV store info with error:" +
            std::string(fdb_error_msg(status)) + " for vbucket id:" +
            std::to_string(static_cast<int>(vbid)));
        throw std::runtime_error(err);
    }

    return kvsInfo.deleted_count;
}

DBFileInfo ForestKVStore::getDbFileInfo(uint16_t vbId) {
    DBFileInfo dbInfo;
    fdb_file_info fileInfo;
    fdb_kvs_info kvsInfo;
    fdb_kvs_handle *kvsHandle = NULL;
    fdb_status status;

    status = fdb_get_file_info(dbFileHandle, &fileInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getDbFileInfo:Failed to retrieve "
            "file info with error:" + std::string(fdb_error_msg(status)) +
            " for vbucket id:" + std::to_string(static_cast<int>(vbId)));
        throw std::runtime_error(err);
    }

    kvsHandle = getKvsHandle(vbId, handleType::READER);
    if (!kvsHandle) {
        std::string err("ForestKVStore::getDbFileInfo:Failed to get reader "
            "KV store handle for vbucket:" +
            std::to_string(static_cast<int>(vbId)));
        throw std::invalid_argument(err);
    }

    status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getDbFileInfo:Failed to retrieve "
            "KV store info with error:" + std::string(fdb_error_msg(status)) +
            " vbucket id:" + std::to_string(static_cast<int>(vbId)));
        throw std::runtime_error(err);
    }

    dbInfo.itemCount = kvsInfo.doc_count;
    dbInfo.fileSize =  fileInfo.file_size/fileInfo.num_kv_stores;
    dbInfo.spaceUsed = kvsInfo.space_used;

    return dbInfo;
}

bool ForestKVStore::snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                                    Callback<kvstats_ctx> *cb, bool persist) {

    std::string stateStr = updateCachedVBState(vbucketId, vbstate);

    if (!stateStr.empty() && persist) {
        char keybuf[20];
        fdb_doc statDoc;
        memset(&statDoc, 0, sizeof(statDoc));
        sprintf(keybuf, "partition%d", vbucketId);
        statDoc.key = keybuf;
        statDoc.keylen = strlen(keybuf);
        statDoc.meta = NULL;
        statDoc.metalen = 0;
        statDoc.body = const_cast<char *>(stateStr.c_str());
        statDoc.bodylen = stateStr.length();
        fdb_status status = fdb_set(vbStateHandle, &statDoc);

        if (status != FDB_RESULT_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "Failed to save vbucket state for "
                    "vbucket=%d error=%s", vbucketId, fdb_error_msg(status));
            return false;
        }
    }

    return true;
}

ScanContext* ForestKVStore::initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                           std::shared_ptr<Callback<CacheLookup> > cl,
                                           uint16_t vbid, uint64_t startSeqno,
                                           DocumentFilter options,
                                           ValueFilter valOptions) {
    fdb_kvs_handle *kvsHandle = NULL;

    kvsHandle = getKvsHandle(vbid, handleType::READER);
    if (!kvsHandle) {
        std::string err("ForestKVStore::initScanContext: Failed to get reader "
                        "KV store handle for vbucket:" +
                        std::to_string(static_cast<int>(vbid)));
        throw std::invalid_argument(err);
    }

    fdb_kvs_info kvsInfo;
    fdb_status status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::initScanContext:Failed to retrieve "
            "KV store info with error:" + std::string(fdb_error_msg(status)) +
            "vbucket id:" + std::to_string(static_cast<int>(vbid)));
        throw std::runtime_error(err);
    }

    size_t backfillId = backfillCounter++;
    size_t count = getNumItems(kvsHandle,
                               startSeqno,
                               std::numeric_limits<uint64_t>::max());

    LockHolder lh(backfillLock);
    backfills[backfillId] = kvsHandle;

    return new ScanContext(cb, cl, vbid, backfillId, startSeqno,
                           (uint64_t)kvsInfo.last_seqnum, options,
                           valOptions, count);
}

scan_error_t ForestKVStore::scan(ScanContext *ctx) {
     if (!ctx) {
         return scan_failed;
     }

     if (ctx->lastReadSeqno == ctx->maxSeqno) {
         return scan_success;
     }

     LockHolder lh(backfillLock);
     auto itr = backfills.find(ctx->scanId);
     if (itr == backfills.end()) {
         return scan_failed;
     }

     fdb_kvs_handle *kvsHandle = itr->second;
     lh.unlock();

     fdb_iterator_opt_t options;
     fdb_iterator *fdb_iter = NULL;
     fdb_status status;
     fdb_doc *rdoc;

     std::shared_ptr<Callback<GetValue> > cb = ctx->callback;
     std::shared_ptr<Callback<CacheLookup> > cl = ctx->lookup;

     bool validFilter = false;
     switch (ctx->docFilter) {
          case DocumentFilter::NO_DELETES:
              options = FDB_ITR_NO_DELETES;
              validFilter = true;
              break;
          case DocumentFilter::ALL_ITEMS:
              options = FDB_ITR_NONE;
              validFilter = true;
              break;
          case DocumentFilter::ONLY_DELETES:
              break;
     }

     if (!validFilter) {
         std::string err("ForestKVStore::scan: Illegal document filter!" +
                         std::to_string(static_cast<int>(ctx->docFilter)));
         throw std::logic_error(err);
     }

     fdb_seqnum_t start = (fdb_seqnum_t)ctx->startSeqno;

     status = fdb_iterator_sequence_init(kvsHandle, &fdb_iter, start,
                                         0, options);
     if (status != FDB_RESULT_SUCCESS) {
         LOG(EXTENSION_LOG_WARNING,
             "ForestDB iterator initialization failed with error "
             "message: %s", fdb_error_msg(status));
         return scan_failed;
     }

     status = fdb_doc_create(&rdoc, NULL, 0, NULL, 0, NULL, 0);
     if (status != FDB_RESULT_SUCCESS) {
         LOG(EXTENSION_LOG_WARNING,
             "ForestDB doc creation failed with error: %s",
             fdb_error_msg(status));
         fdb_iterator_close(fdb_iter);
         return scan_failed;
     }
     // Pre-allocate key and meta data as their max sizes are known.
     rdoc->key = malloc(MAX_KEY_LENGTH);
     rdoc->meta = malloc(FORESTDB_METADATA_SIZE);
     // Document body will be allocated by fdb_iterator_get API below.
     rdoc->body = NULL;
     do {
         status = fdb_iterator_get(fdb_iter, &rdoc);
         if (status != FDB_RESULT_SUCCESS) {
             LOG(EXTENSION_LOG_WARNING,
                 "fdb_iterator_get failed with error: %s",
                 fdb_error_msg(status));
             fdb_doc_free(rdoc);
             fdb_iterator_close(fdb_iter);
             return scan_failed;
         }
         uint8_t *metadata = (uint8_t *)rdoc->meta;
         std::string docKey((char *)rdoc->key, rdoc->keylen);

         CacheLookup lookup(docKey, (uint64_t) rdoc->seqnum, ctx->vbid);
         cl->callback(lookup);
         if (cl->getStatus() == ENGINE_KEY_EEXISTS) {
             ctx->lastReadSeqno = static_cast<uint64_t>(rdoc->seqnum);
             free(rdoc->body);
             rdoc->body = NULL;
             continue;
         } else if (cl->getStatus() == ENGINE_ENOMEM) {
             fdb_doc_free(rdoc);
             fdb_iterator_close(fdb_iter);
             return scan_again;
         }

         uint64_t cas;
         uint32_t exptime;
         uint32_t texptime;
         uint32_t itemflags;
         uint64_t rev_seqno;
         uint8_t ext_meta[EXT_META_LEN] = {0};
         uint8_t ext_len;
         uint8_t conf_res_mode = 0;

         memcpy(&cas, metadata, 8);
         memcpy(&exptime, metadata + 8, 4);
         memcpy(&texptime, metadata + 12, 4);
         memcpy(&itemflags, metadata + 16, 4);
         memcpy(&rev_seqno, metadata + 20, 8);
         memcpy(ext_meta, metadata + 29, EXT_META_LEN);
         memcpy(&conf_res_mode, metadata + 30, CONFLICT_RES_META_LEN);
         ext_len = EXT_META_LEN;

         cas = ntohll(cas);
         exptime = ntohl(exptime);
         texptime = ntohl(texptime);
         rev_seqno = ntohll(rev_seqno);

         uint64_t bySeqno = static_cast<uint64_t>(rdoc->seqnum);

         Item *it = new Item(rdoc->key, rdoc->keylen, itemflags, (time_t)exptime,
                             rdoc->body, rdoc->bodylen, ext_meta, ext_len, cas,
                             bySeqno, ctx->vbid, rev_seqno);

         if (rdoc->deleted) {
             it->setDeleted();
         }

         it->setConflictResMode(
                      static_cast<enum conflict_resolution_mode>(conf_res_mode));

         bool onlyKeys = (ctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;
         GetValue rv(it, ENGINE_SUCCESS, -1, onlyKeys);
         cb->callback(rv);

         free(rdoc->body);
         rdoc->body = NULL;

         if (cb->getStatus() == ENGINE_ENOMEM) {
             fdb_doc_free(rdoc);
             fdb_iterator_close(fdb_iter);
             return scan_again;
         }

         ctx->lastReadSeqno = bySeqno;
     } while (fdb_iterator_next(fdb_iter) == FDB_RESULT_SUCCESS);

     fdb_doc_free(rdoc);
     rdoc = NULL;
     fdb_iterator_close(fdb_iter);

     return scan_success;
}

void ForestKVStore::destroyScanContext(ScanContext* ctx) {
    if (!ctx) {
        return;
    }

    LockHolder lh(backfillLock);
    auto itr = backfills.find(ctx->scanId);
    if (itr != backfills.end()) {
        backfills.erase(itr);
    }

    delete ctx;
}

bool ForestKVStore::compactDB(compaction_ctx *ctx, Callback<kvstats_ctx> &kvcb) {
    return false;
}

size_t ForestKVStore::getNumItems(uint16_t vbid, uint64_t min_seq,
                                  uint64_t max_seq) {
    fdb_kvs_handle *kvsHandle = NULL;
    kvsHandle = getKvsHandle(vbid, handleType::READER);
    return getNumItems(kvsHandle, min_seq, max_seq);
}

size_t ForestKVStore::getNumItems(fdb_kvs_handle* kvsHandle,
                                  uint64_t min_seq,
                                  uint64_t max_seq) {
    size_t totalCount = 0;
    fdb_iterator* fdb_iter = nullptr;
    fdb_status status = fdb_iterator_sequence_init(kvsHandle, &fdb_iter, min_seq,
                                                   max_seq, FDB_ITR_NONE);
    if (status != FDB_RESULT_SUCCESS) {
        std::string err("ForestKVStore::getNumItems: ForestDB iterator "
            "initialization failed with error: " +
             std::string(fdb_error_msg(status)));
        throw std::runtime_error(err);
    }

    do {
        totalCount++;
    } while (fdb_iterator_next(fdb_iter) == FDB_RESULT_SUCCESS);

    fdb_iterator_close(fdb_iter);

    return totalCount;
}

RollbackResult ForestKVStore::rollback(uint16_t vbid, uint64_t rollbackSeqno,
                                       std::shared_ptr<RollbackCB> cb) {
   fdb_kvs_handle *kvsHandle = NULL;
   fdb_kvs_info kvsInfo;

   kvsHandle = getKvsHandle(vbid, handleType::WRITER);
   if (!kvsHandle) {
       LOG(EXTENSION_LOG_WARNING,
           "Failed to get reader KV store handle for vbucket "
           "id :%d and rollback sequence number: %" PRIu64, vbid,
           rollbackSeqno);
       return RollbackResult(false, 0, 0, 0);
   }

   fdb_status status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
   if (status != FDB_RESULT_SUCCESS) {
       LOG(EXTENSION_LOG_WARNING,
           "Failed to retrieve KV store info with error: %s for "
           "vbucket id: %d and rollback sequence number: %" PRIu64,
           fdb_error_msg(status), vbid, rollbackSeqno);
       return RollbackResult(false, 0, 0 ,0);
   }

   fdb_iterator *fdb_iter;
   fdb_iterator_opt_t options;
   options = FDB_ITR_NONE;

   uint64_t totalCount = kvsInfo.doc_count;

   fdb_snapshot_info_t *markers;
   uint64_t num_markers;
   status = fdb_get_all_snap_markers(dbFileHandle, &markers, &num_markers);
   if (status != FDB_RESULT_SUCCESS) {
       LOG(EXTENSION_LOG_WARNING,
           "Failed to retrieve snapshot markers from ForestDB file "
           "with error: %s for vbucket id: %d and rollback sequence number: "
           "%" PRIu64, fdb_error_msg(status), vbid, rollbackSeqno);
       return RollbackResult(false, 0, 0, 0);
   }

   int64_t kvIndex = 0;
   // search the snap marker to find out the index of the KV store
   for (; kvIndex < markers[0].num_kvs_markers; kvIndex++) {
       if (strcmp(markers[0].kvs_markers[kvIndex].kv_store_name, kvsInfo.name) == 0) {
           break;
       }
   }

   uint64_t currentSeqno = 0;
   //Now search each marker in that index to find the correct snapshot
   for (uint64_t snapIndex = 0; snapIndex < num_markers; snapIndex++) {
       currentSeqno = markers[snapIndex].kvs_markers[kvIndex].seqnum;
       if (currentSeqno <= rollbackSeqno) {
           break;
       }
   }

   uint64_t lastSeqno = kvsInfo.last_seqnum;
   status = fdb_iterator_sequence_init(kvsHandle, &fdb_iter, currentSeqno,
                                       lastSeqno, options);
   if (status != FDB_RESULT_SUCCESS) {
       LOG(EXTENSION_LOG_WARNING,
           "ForestDB iterator initialization failed with error message: %s "
           "for vbucket id: %d and rollback sequence number: %" PRIu64,
           fdb_error_msg(status), vbid, rollbackSeqno);
       return RollbackResult(false, 0, 0, 0);
   }

   uint64_t rollbackCount = 0;
   std::vector<std::string> rollbackKeys;
   fdb_doc *rdoc = NULL;
   fdb_doc_create(&rdoc, NULL, 0, NULL, 0, NULL, 0);
   rdoc->key = malloc(MAX_KEY_LENGTH);
   rdoc->meta = malloc(FORESTDB_METADATA_SIZE);
   do {
       status = fdb_iterator_get_metaonly(fdb_iter, &rdoc);
       if (status != FDB_RESULT_SUCCESS) {
           LOG(EXTENSION_LOG_WARNING,
               "ForestDB iterator get meta failed with error: %s "
               "for vbucket id: %d and rollback sequence number: %" PRIu64,
               fdb_error_msg(status), vbid, rollbackSeqno);
           fdb_doc_free(rdoc);
           fdb_iterator_close(fdb_iter);
           return RollbackResult(false, 0, 0, 0);
       }
       rollbackCount++;
       std::string str((char *)rdoc->key, rdoc->keylen);
       rollbackKeys.push_back(str);
   } while (fdb_iterator_next(fdb_iter) == FDB_RESULT_SUCCESS);

   fdb_doc_free(rdoc);
   fdb_iterator_close(fdb_iter);

   if ((totalCount/2) <= rollbackCount) {
       return RollbackResult(false, 0, 0, 0);
   }

   cb->setDbHeader(kvsHandle);

   status = fdb_rollback(&kvsHandle, currentSeqno);
   if (status != FDB_RESULT_SUCCESS) {
       LOG(EXTENSION_LOG_WARNING,
           "ForestDB rollback failed on vbucket: %d and rollback "
           "sequence number: %" PRIu64 "with error: %s",
           vbid, currentSeqno, fdb_error_msg(status));
       return RollbackResult(false, 0, 0, 0);
   }

   writeHandleMap[vbid] = kvsHandle;

   /* This is currently a workaround to update the rolled back keys
    * in memory. ForestDB doesn't provide a way to retrieve the
    * rolled back keys. So, we try to retrieve the rolled back keys
    * which are cached in rollbackKeys. If the key isn't available,
    * it is deleted in memory, otherwise the the original value is
    * restored. */

   for (auto it = rollbackKeys.begin(); it != rollbackKeys.end(); ++it) {
       RememberingCallback<GetValue> gcb;
       get(*it, vbid, gcb);
       if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
           Item *itm = new Item(*it, vbid, queue_op_del, 0, 0);
           gcb.val.setValue(itm);
       }
       cb->callback(gcb.val);
   }

   status = fdb_get_kvs_info(kvsHandle, &kvsInfo);
   if (status != FDB_RESULT_SUCCESS) {
       LOG(EXTENSION_LOG_WARNING,
           "Failed to retrieve KV store info after rollback with error: %s "
           "for vbucket id: %d and rollback sequence number: %" PRIu64,
           fdb_error_msg(status), vbid, rollbackSeqno);
       return RollbackResult(false, 0, 0 ,0);
   }

   readVBState(vbid);

   cachedDocCount[vbid] = kvsInfo.doc_count;
   cachedDeleteCount[vbid] = kvsInfo.deleted_count;

   vbucket_state *vb_state = cachedVBStates[vbid];
   return RollbackResult(true, vb_state->highSeqno,
                         vb_state->lastSnapStart, vb_state->lastSnapEnd);
}
