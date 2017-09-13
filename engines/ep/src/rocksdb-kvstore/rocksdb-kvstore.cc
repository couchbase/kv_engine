/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "config.h"

#include "rocksdb-kvstore.h"

#include "ep_time.h"

#include "kvstore_config.h"
#include "kvstore_priv.h"

#include <rocksdb/convenience.h>

#include <string.h>
#include <algorithm>
#include <limits>

#include "vbucket.h"

namespace rockskv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to RocksDB and when reading a Document
// from RocksDB.
class MetaData {
public:
    MetaData()
        : deleted(0),
          version(0),
          datatype(0),
          flags(0),
          valueSize(0),
          exptime(0),
          cas(0),
          revSeqno(0),
          bySeqno(0){};
    MetaData(bool deleted,
             uint8_t version,
             uint8_t datatype,
             uint32_t flags,
             uint32_t valueSize,
             time_t exptime,
             uint64_t cas,
             uint64_t revSeqno,
             int64_t bySeqno)
        : deleted(deleted),
          version(version),
          datatype(datatype),
          flags(flags),
          valueSize(valueSize),
          exptime(exptime),
          cas(cas),
          revSeqno(revSeqno),
          bySeqno(bySeqno){};

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
    uint8_t deleted : 1;
    uint8_t version : 7;
    uint8_t datatype;
    uint32_t flags;
    uint32_t valueSize;
    time_t exptime;
    uint64_t cas;
    uint64_t revSeqno;
    int64_t bySeqno;
#pragma pack()
};
} // namespace rockskv

/**
 * Class representing a document to be persisted in RocksDB.
 */
class RocksRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param del Flag indicating if it is an item deletion or not
     */
    RocksRequest(const Item& item, MutationRequestCallback& callback)
        : IORequest(item.getVBucketId(),
                    callback,
                    item.isDeleted(),
                    item.getKey()),
          docBody(item.getValue()) {
        docMeta = rockskv::MetaData(
                item.isDeleted(),
                0,
                item.getDataType(),
                item.getFlags(),
                item.getNBytes(),
                item.isDeleted() ? ep_real_time() : item.getExptime(),
                item.getCas(),
                item.getRevSeqno(),
                item.getBySeqno());
    }

    const rockskv::MetaData& getDocMeta() {
        return docMeta;
    }

    // Get a rocksdb::Slice wrapping the Document MetaData
    rocksdb::Slice getDocMetaSlice() {
        return rocksdb::Slice(reinterpret_cast<char*>(&docMeta),
                              sizeof(docMeta));
    }

    // Get a rocksdb::Slice wrapping the Document Body
    rocksdb::Slice getDocBodySlice() {
        const char* data = docBody ? docBody->getData() : nullptr;
        size_t size = docBody ? docBody->valueSize() : 0;
        return rocksdb::Slice(data, size);
    }

private:
    rockskv::MetaData docMeta;
    value_t docBody;
};

RocksDBKVStore::RocksDBKVStore(KVStoreConfig& config)
    : KVStore(config),
      in_transaction(false),
      scanCounter(0),
      logger(config.getLogger()) {
    cachedVBStates.resize(configuration.getMaxVBuckets());
    writeOptions.sync = true;
    open();
}

RocksDBKVStore::~RocksDBKVStore() {
    close();
}

void RocksDBKVStore::open() {
    // The RocksDB Options is a set of DBOptions and ColumnFamilyOptions.
    // Together they cover all RocksDB available parameters.
    auto status = rocksdb::GetDBOptionsFromString(
            rocksdb::Options(), configuration.getRocksDBOptions(), &rdbOptions);
    if (!status.ok()) {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::open: GetDBOptionsFromString "
                            "error: ") +
                status.getState());
    }
    status = rocksdb::GetColumnFamilyOptionsFromString(
            rdbOptions, configuration.getRocksDBCFOptions(), &rdbOptions);
    if (!status.ok()) {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::open: "
                            "GetColumnFamilyOptionsFromString error: ") +
                status.getState());
    }

    // The `max_background_compactions` or `max_background_flushes` options
    // are deprecated but still available in case the RocksDB default is not
    // optimal. We need the following further configuration for applying them.
    // RocksDB will set `max_background_jobs` to
    // `max_background_compactions + max_background_flushes` in the case where
    // the user sets at least one of `max_background_compactions` or
    // `max_background_flushes`. Thus, in that case the `max_background_jobs`
    // option is ignored.
    // In the case where only one of `max_background_compactions` and
    // `max_background_flushes` is set, RocksDB sets the other to 1 and we do
    // not need to call `env->SetBackgroundThreads()` for that one because the
    // default size of both Thread Pools is 1.
    auto defaultOptions = rocksdb::Options();
    if (rdbOptions.max_background_compactions !=
        defaultOptions.max_background_compactions) {
        rocksdb::Env::Default()->SetBackgroundThreads(
                rdbOptions.max_background_compactions, rocksdb::Env::LOW);
    }
    if (rdbOptions.max_background_flushes !=
        defaultOptions.max_background_flushes) {
        rocksdb::Env::Default()->SetBackgroundThreads(
                rdbOptions.max_background_flushes, rocksdb::Env::HIGH);
    }

    // Set the provided `RocksDBCFOptions` as the base configuration for all
    // Column Families.
    defaultCFOptions = rocksdb::ColumnFamilyOptions(rdbOptions);
    seqnoCFOptions = rocksdb::ColumnFamilyOptions(rdbOptions);
    localCFOptions = rocksdb::ColumnFamilyOptions(rdbOptions);

    rdbOptions.create_if_missing = true;
    rdbOptions.create_missing_column_families = true;

    seqnoCFOptions.comparator = &vbidSeqnoComparator;

    /* Use a listener to set the appropriate engine in the
     * flusher threads RocksDB creates. We need the flusher threads to
     * account for news/deletes against the appropriate bucket. */
    auto fsl = std::make_shared<FlushStartListener>(
            ObjectRegistry::getCurrentEngine());
    rdbOptions.listeners.emplace_back(fsl);

    const std::string dbdir = configuration.getDBName();

    cb::io::mkdirp(dbdir);

    const std::string dbname =
            dbdir + "/rocksdb." + std::to_string(configuration.getShardId());

    std::vector<rocksdb::ColumnFamilyDescriptor> families{
            rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName,
                                            defaultCFOptions),

            rocksdb::ColumnFamilyDescriptor("vbid_seqno_to_key",
                                            seqnoCFOptions),
            rocksdb::ColumnFamilyDescriptor("_local", localCFOptions)};

    std::vector<rocksdb::ColumnFamilyHandle*> handles;

    rocksdb::DB* dbPtr;
    rocksdb::Status s =
            rocksdb::DB::Open(rdbOptions, dbname, families, &handles, &dbPtr);

    if (s.ok()) {
        db.reset(dbPtr);
    } else {
        throw std::runtime_error(
                "RocksDBKVStore::open: failed to open database '" + dbname +
                "': " + s.ToString());
    }

    defaultFamilyHandle.reset(handles[0]);
    seqnoFamilyHandle.reset(handles[1]);
    localFamilyHandle.reset(handles[2]);

    // Attempt to read persisted vb states
    std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(rocksdb::ReadOptions(), localFamilyHandle.get()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        uint16_t vb = std::stoi(it->key().ToString().substr(
                getVbstatePrefix().length(), std::string::npos));
        readVBState(vb);
    }
}

void RocksDBKVStore::close() {
    defaultFamilyHandle.reset();
    seqnoFamilyHandle.reset();
    localFamilyHandle.reset();
    db.reset();
    in_transaction = false;
}

bool RocksDBKVStore::begin() {
    in_transaction = true;
    return in_transaction;
}

bool RocksDBKVStore::commit(const Item* collectionsManifest) {
    // This behaviour is to replicate the one in Couchstore.
    // If `commit` is called when not in transaction, just return true.
    if (!in_transaction) {
        return true;
    }

    if (pendingReqs.size() == 0) {
        in_transaction = false;
        return true;
    }

    // Swap `pendingReqs` with the temporary `commitBatch` so that we can
    // shorten the scope of the lock.
    std::vector<std::unique_ptr<RocksRequest>> commitBatch;
    {
        std::lock_guard<std::mutex> lock(writeLock);
        std::swap(pendingReqs, commitBatch);
    }

    bool success = true;
    auto vbid = commitBatch[0]->getVBucketId();
    KVStatsCtx statsCtx(configuration);
    statsCtx.vbucket = vbid;

    // Flush all documents to disk
    auto status = saveDocs(vbid, collectionsManifest, commitBatch);
    if (!status.ok()) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::commit: saveDocs error:%d, "
                   "vb:%" PRIu16,
                   status.code(),
                   vbid);
        success = false;
    }

    commitCallback(statsCtx, status, commitBatch);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
    }

    return success;
}

static int getMutationStatus(rocksdb::Status status) {
    switch (status.code()) {
    case rocksdb::Status::Code::kOk:
        return MUTATION_SUCCESS;
    case rocksdb::Status::Code::kNotFound:
        // This return value causes ep-engine to drop the failed flush
        return DOC_NOT_FOUND;
    case rocksdb::Status::Code::kBusy:
        // This return value causes ep-engine to keep re-queueing the failed
        // flush
        return MUTATION_FAILED;
    default:
        throw std::runtime_error(
                std::string("getMutationStatus: RocksDB error:") +
                std::string(status.getState()));
    }
}

void RocksDBKVStore::commitCallback(
        KVStatsCtx& kvctx,
        rocksdb::Status status,
        const std::vector<std::unique_ptr<RocksRequest>>& commitBatch) {
    for (const auto& request : commitBatch) {
        auto dataSize = request->getDocMetaSlice().size() +
                        request->getDocBodySlice().size();
        const auto& key = request->getKey();
        /* update ep stats */
        ++st.io_num_write;
        st.io_write_bytes += (key.size() + dataSize);

        auto rv = getMutationStatus(status);
        if (request->isDelete()) {
            if (status.code()) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(request->getDelta() / 1000);
            }
            if (rv != -1) {
                // TODO: Should set `rv` to 1 or 0 depending on if this is a
                // delete to an existing (1) or non-existing (0) item. However,
                // to achieve this we would need to perform a Get to RocksDB
                // which is costly. For now just assume that the item did exist.
                rv = 1;
            }
            request->getDelCallback()->callback(rv);
        } else {
            if (status.code()) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(request->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + key.size());
            }
            // TODO: Should set `mr.second` to true or false depending on if
            // this is an insertion (true) or an update of an existing item
            // (false). However, to achieve this we would need to perform a Get
            // to RocksDB which is costly. For now just assume that the item
            // did not exist.
            mutation_result mr = std::make_pair(1, true);
            request->getSetCallback()->callback(mr);
        }
    }
}

void RocksDBKVStore::rollback() {
    if (in_transaction) {
        in_transaction = false;
    }
}

std::vector<vbucket_state*> RocksDBKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void RocksDBKVStore::set(const Item& item, Callback<mutation_result>& cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "RocksDBKVStore::set: in_transaction must be true to perform a "
                "set operation.");
    }
    MutationRequestCallback callback;
    callback.setCb = &cb;
    pendingReqs.push_back(std::make_unique<RocksRequest>(item, callback));
}

GetValue RocksDBKVStore::get(const DocKey& key, uint16_t vb, bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue RocksDBKVStore::getWithHeader(void* dbHandle,
                                       const DocKey& key,
                                       uint16_t vb,
                                       GetMetaOnly getMetaOnly,
                                       bool fetchDelete) {
    std::string k(mkKeyStr(vb, key));
    std::string value;

    // TODO RDB: use a PinnableSlice to avoid some memcpy
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), k, &value);
    if (!s.ok()) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }
    return makeGetValue(vb, key, value, getMetaOnly);
}

void RocksDBKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) {
    // TODO RDB: RocksDB supports a multi get which we should use here.
    for (auto& it : itms) {
        auto& key = it.first;
        std::string vbAndKey(mkKeyStr(vb, it.first));
        std::string value;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), vbAndKey, &value);
        if (s.ok()) {
            it.second.value =
                    makeGetValue(vb, key, value, it.second.isMetaOnly);
            GetValue* rv = &it.second.value;
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value = rv;
            }
        } else {
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value->setStatus(ENGINE_KEY_ENOENT);
            }
        }
    }
}

void RocksDBKVStore::reset(uint16_t vbucketId) {
    if (db) {
        // TODO RDB:  Implement.
    }
}

void RocksDBKVStore::del(const Item& item, Callback<int>& cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "RocksDBKVStore::del: in_transaction must be true to perform a "
                "delete operation.");
    }
    // TODO: Deleted items remain as tombstones, but are not yet expired,
    // they will accumuate forever.
    MutationRequestCallback callback;
    callback.delCb = &cb;
    pendingReqs.push_back(std::make_unique<RocksRequest>(item, callback));
}

static bool matches_prefix(rocksdb::Slice s, size_t len, const char* p) {
    return s.size() >= len && std::memcmp(p, s.data(), len) == 0;
}

void RocksDBKVStore::delVBucket(uint16_t vb, uint64_t vb_version) {
    std::lock_guard<std::mutex> lg(writeLock);
    rocksdb::WriteBatch delBatch;

    const char* prefix(reinterpret_cast<const char*>(&vb));
    std::string start(prefix, sizeof(vb));

    // We must delete both all the documents for the VB,
    // and all the vbid:seqno=>vbid:key mappings
    std::vector<rocksdb::ColumnFamilyHandle*> CFHandles{
            defaultFamilyHandle.get(), seqnoFamilyHandle.get()};

    // makes use of the fact that keys in both CFs are vbid prefixed
    // if we move to a db per vb this will just be dropping the whole DB.
    for (auto* handle : CFHandles) {
        std::unique_ptr<rocksdb::Iterator> it(
                db->NewIterator(rocksdb::ReadOptions(), handle));

        for (it->Seek(start);
             it->Valid() && matches_prefix(it->key(), sizeof(vb), prefix);
             it->Next()) {
            delBatch.Delete(handle, it->key());
        }
    }
    rocksdb::Status s = db->Write(writeOptions, &delBatch);
    cb_assert(s.ok());
}

bool RocksDBKVStore::snapshotVBucket(uint16_t vbucketId,
                                     const vbucket_state& vbstate,
                                     VBStatePersist options) {
    // TODO RDB: Refactor out behaviour common to this and CouchKVStore
    auto start = ProcessClock::now();

    if (updateCachedVBState(vbucketId, vbstate) &&
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        if (!saveVBState(vbstate, vbucketId).ok()) {
            logger.log(EXTENSION_LOG_WARNING,
                       "RocksDBKVStore::snapshotVBucket: saveVBState failed "
                       "state:%s, vb:%" PRIu16,
                       VBucket::toString(vbstate.state),
                       vbucketId);
            return false;
        }
    }

    LOG(EXTENSION_LOG_DEBUG,
        "RocksDBKVStore::snapshotVBucket: Snapshotted vbucket:%" PRIu16
        " state:%s",
        vbucketId,
        vbstate.toJSON().c_str());

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
                                 ProcessClock::now() - start)
                                 .count());

    return true;
}

bool RocksDBKVStore::snapshotStats(const std::map<std::string, std::string>&) {
    // TODO RDB:  Implement
    return true;
}

void RocksDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO RDB:  implement
}

size_t RocksDBKVStore::getNumShards() {
    return configuration.getMaxShards();
}

StorageProperties RocksDBKVStore::getStorageProperties(void) {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         // TODO RDB: Not strictly true, multiGet
                         // does not yet use the underlying multi get
                         // of RocksDB
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

std::string RocksDBKVStore::mkKeyStr(uint16_t vbid, const DocKey& k) {
    size_t headerSize = sizeof(uint16_t) + k.size();

    std::string buffer;
    buffer.reserve(headerSize);

    buffer.append(reinterpret_cast<char*>(&vbid), sizeof(vbid));
    buffer.append(const_cast<char*>(reinterpret_cast<const char*>(k.data())),
                  k.size());

    return buffer;
}

void RocksDBKVStore::grokKeySlice(const rocksdb::Slice& s,
                                  uint16_t* v,
                                  std::string* k) {
    assert(s.size() > sizeof(uint16_t));
    std::memcpy(v, s.data(), sizeof(uint16_t));
    k->assign(s.data() + sizeof(uint16_t), s.size() - sizeof(uint16_t));
}

std::string RocksDBKVStore::mkSeqnoStr(uint16_t vbid, int64_t seqno) {
    size_t size = sizeof(uint16_t) + sizeof(int64_t);

    std::string buffer;
    buffer.reserve(size);

    buffer.append(reinterpret_cast<char*>(&vbid), sizeof(vbid));
    buffer.append(reinterpret_cast<char*>(&seqno), sizeof(seqno));

    return buffer;
}

void RocksDBKVStore::grokSeqnoSlice(const rocksdb::Slice& s,
                                    uint16_t* vb,
                                    int64_t* seqno) {
    assert(s.size() == sizeof(uint16_t) + sizeof(int64_t));
    std::memcpy(vb, s.data(), sizeof(uint16_t));
    std::memcpy(seqno, s.data() + sizeof(uint16_t), sizeof(int64_t));
}

std::unique_ptr<Item> RocksDBKVStore::makeItem(uint16_t vb,
                                               const DocKey& key,
                                               const rocksdb::Slice& s,
                                               GetMetaOnly getMetaOnly) {
    assert(s.size() >= sizeof(rockskv::MetaData));

    const char* data = s.data();

    rockskv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

    auto item = std::make_unique<Item>(key,
                                       meta.flags,
                                       meta.exptime,
                                       includeValue ? data : nullptr,
                                       includeValue ? meta.valueSize : 0,
                                       meta.datatype,
                                       meta.cas,
                                       meta.bySeqno,
                                       vb,
                                       meta.revSeqno);

    if (meta.deleted) {
        item->setDeleted();
    }

    return item;
}

GetValue RocksDBKVStore::makeGetValue(uint16_t vb,
                                      const DocKey& key,
                                      const std::string& value,
                                      GetMetaOnly getMetaOnly) {
    rocksdb::Slice sval(value);
    return GetValue(
            makeItem(vb, key, sval, getMetaOnly), ENGINE_SUCCESS, -1, 0);
}

void RocksDBKVStore::readVBState(uint16_t vbid) {
    // Largely copied from CouchKVStore
    // TODO RDB: refactor out sections common to CouchKVStore
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    int64_t highSeqno = readHighSeqnoFromDisk(vbid);
    std::string failovers;
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    bool mightContainXattrs = false;

    std::string lDocKey = getVbstatePrefix() + std::to_string(vbid);
    std::string statjson;

    rocksdb::Status s = db->Get(rocksdb::ReadOptions(),
                                localFamilyHandle.get(),
                                lDocKey,
                                &statjson);

    if (!s.ok()) {
        if (s.IsNotFound()) {
            logger.log(EXTENSION_LOG_NOTICE,
                       "RocksDBKVStore::readVBState: '_local/vbstate.%" PRIu16
                       "' not found",
                       vbid);
        } else {
            logger.log(EXTENSION_LOG_WARNING,
                       "RocksDBKVStore::readVBState: error getting vbstate"
                       " error:%s, vb:%" PRIu16,
                       s.getState(),
                       vbid);
        }
    } else {
        cJSON* jsonObj = cJSON_Parse(statjson.c_str());
        if (!jsonObj) {
            logger.log(EXTENSION_LOG_WARNING,
                       "RocksKVStore::readVBState: Failed to "
                       "parse the vbstat json doc for vb:%" PRIu16 ", json:%s",
                       vbid,
                       statjson.c_str());
        }

        const std::string vb_state =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "state"));
        const std::string checkpoint_id =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "checkpoint_id"));
        const std::string max_deleted_seqno = getJSONObjString(
                cJSON_GetObjectItem(jsonObj, "max_deleted_seqno"));
        const std::string snapStart =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "snap_start"));
        const std::string snapEnd =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "snap_end"));
        const std::string maxCasValue =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "max_cas"));
        const std::string hlcCasEpoch =
                getJSONObjString(cJSON_GetObjectItem(jsonObj, "hlc_epoch"));
        mightContainXattrs = getJSONObjBool(
                cJSON_GetObjectItem(jsonObj, "might_contain_xattrs"));

        cJSON* failover_json = cJSON_GetObjectItem(jsonObj, "failover_table");
        if (vb_state.compare("") == 0 || checkpoint_id.compare("") == 0 ||
            max_deleted_seqno.compare("") == 0) {
            logger.log(EXTENSION_LOG_WARNING,
                       "RocksDBKVStore::readVBState: State"
                       " JSON doc for vb:%" PRIu16
                       " is in the wrong format:%s, "
                       "vb state:%s, checkpoint id:%s and max deleted seqno:%s",
                       vbid,
                       statjson.c_str(),
                       vb_state.c_str(),
                       checkpoint_id.c_str(),
                       max_deleted_seqno.c_str());
        } else {
            state = VBucket::fromString(vb_state.c_str());
            maxDeletedSeqno = std::stoull(max_deleted_seqno);
            checkpointId = std::stoull(checkpoint_id);

            if (snapStart.compare("") == 0) {
                lastSnapStart = highSeqno;
            } else {
                lastSnapStart = std::stoull(snapStart.c_str());
            }

            if (snapEnd.compare("") == 0) {
                lastSnapEnd = highSeqno;
            } else {
                lastSnapEnd = std::stoull(snapEnd.c_str());
            }

            if (maxCasValue.compare("") != 0) {
                maxCas = std::stoull(maxCasValue.c_str());
            }

            if (!hlcCasEpoch.empty()) {
                hlcCasEpochSeqno = std::stoull(hlcCasEpoch);
            }

            if (failover_json) {
                char* json = cJSON_PrintUnformatted(failover_json);
                failovers.assign(json);
                cJSON_Free(json);
            }
        }
        cJSON_Delete(jsonObj);
    }

    cachedVBStates[vbid] = std::make_unique<vbucket_state>(state,
                                                           checkpointId,
                                                           maxDeletedSeqno,
                                                           highSeqno,
                                                           purgeSeqno,
                                                           lastSnapStart,
                                                           lastSnapEnd,
                                                           maxCas,
                                                           hlcCasEpochSeqno,
                                                           mightContainXattrs,
                                                           failovers);
}

rocksdb::Status RocksDBKVStore::saveVBState(const vbucket_state& vbState,
                                            uint16_t vbid) {
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state) << "\""
              << ",\"checkpoint_id\": \"" << vbState.checkpointId << "\""
              << ",\"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno
              << "\"";
    if (!vbState.failovers.empty()) {
        jsonState << ",\"failover_table\": " << vbState.failovers;
    }
    jsonState << ",\"snap_start\": \"" << vbState.lastSnapStart << "\""
              << ",\"snap_end\": \"" << vbState.lastSnapEnd << "\""
              << ",\"max_cas\": \"" << vbState.maxCas << "\""
              << ",\"hlc_epoch\": \"" << vbState.hlcCasEpochSeqno << "\"";

    if (vbState.mightContainXattrs) {
        jsonState << ",\"might_contain_xattrs\": true";
    } else {
        jsonState << ",\"might_contain_xattrs\": false";
    }

    jsonState << "}";

    std::string lDocKey = getVbstatePrefix() + std::to_string(vbid);

    rocksdb::Status s = db->Put(writeOptions,
                                localFamilyHandle.get(),
                                rocksdb::Slice(lDocKey),
                                rocksdb::Slice(jsonState.str()));
    return s;
}

rocksdb::Status RocksDBKVStore::saveDocs(
        uint16_t vbid,
        const Item* collectionsManifest,
        const std::vector<std::unique_ptr<RocksRequest>>& commitBatch) {
    auto reqsSize = commitBatch.size();
    if (reqsSize == 0) {
        st.docsCommitted = 0;
        return rocksdb::Status::OK();
    }

    auto& vbstate = cachedVBStates[vbid];
    if (vbstate == nullptr) {
        throw std::logic_error("RocksDBKVStore::saveDocs: cachedVBStates[" +
                               std::to_string(vbid) + "] is NULL");
    }

    rocksdb::Status status;
    int64_t maxDBSeqno = 0;
    rocksdb::WriteBatch batch;

    for (const auto& request : commitBatch) {
        int64_t bySeqno = request->getDocMeta().bySeqno;
        maxDBSeqno = std::max(maxDBSeqno, bySeqno);

        status = addRequestToWriteBatch(batch, request.get());
        if (!status.ok()) {
            logger.log(EXTENSION_LOG_WARNING,
                       "RocksDBKVStore::saveDocs: addRequestToWriteBatch "
                       "error:%d, vb:%" PRIu16,
                       status.code(),
                       vbid);
            return status;
        }
    }

    status = saveVBState(*vbstate, vbid);
    if (!status.ok()) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::saveDocs: saveVBState error:%d",
                   status.code());
        return status;
    }

    auto begin = ProcessClock::now();
    status = db->Write(writeOptions, &batch);
    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
                               ProcessClock::now() - begin)
                               .count());
    if (!status.ok()) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::saveDocs: rocksdb::DB::Write error:%d, "
                   "vb:%" PRIu16,
                   status.code(),
                   vbid);
        return status;
    }

    st.batchSize.add(reqsSize);
    st.docsCommitted = reqsSize;

    // Check and update last seqno
    int64_t lastSeqno = readHighSeqnoFromDisk(vbid);
    if (maxDBSeqno != lastSeqno) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::saveDocs: Seqno in db header (%" PRIu64
                   ") is not matched with what was persisted (%" PRIu64
                   ") for vb:%" PRIu16,
                   lastSeqno,
                   maxDBSeqno,
                   vbid);
    }
    vbstate->highSeqno = lastSeqno;

    return rocksdb::Status::OK();
}

rocksdb::Status RocksDBKVStore::addRequestToWriteBatch(
        rocksdb::WriteBatch& batch, RocksRequest* request) {
    uint16_t vbid = request->getVBucketId();

    auto key = mkKeyStr(vbid, request->getKey());
    rocksdb::Slice keySlice(key);
    rocksdb::SliceParts keySliceParts(&keySlice, 1);

    rocksdb::Slice docSlices[] = {request->getDocMetaSlice(),
                                  request->getDocBodySlice()};
    rocksdb::SliceParts valueSliceParts(docSlices, 2);

    auto bySeqno = mkSeqnoStr(vbid, request->getDocMeta().bySeqno);
    // We use the `saveDocsHisto` to track the time spent on
    // `rocksdb::WriteBatch::Put()`.
    auto begin = ProcessClock::now();
    auto status = batch.Put(keySliceParts, valueSliceParts);
    if (!status.ok()) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::saveDocs: rocksdb::WriteBatch::Put "
                   "[ColumnFamily: \'default\']  error:%d, "
                   "vb:%" PRIu16,
                   status.code(),
                   vbid);
        return status;
    }
    status = batch.Put(seqnoFamilyHandle.get(), bySeqno, key);
    if (!status.ok()) {
        logger.log(EXTENSION_LOG_WARNING,
                   "RocksDBKVStore::saveDocs: rocksdb::WriteBatch::Put "
                   "[ColumnFamily: \'seqno\']  error:%d, "
                   "vb:%" PRIu16,
                   status.code(),
                   vbid);
        return status;
    }
    st.saveDocsHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
                                 ProcessClock::now() - begin)
                                 .count());

    return rocksdb::Status::OK();
}

int64_t RocksDBKVStore::readHighSeqnoFromDisk(uint16_t vbid) {
    std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(rocksdb::ReadOptions(), seqnoFamilyHandle.get()));

    // Seek to the highest seqno=>key mapping stored for the vbid
    std::string start = mkSeqnoStr(vbid, std::numeric_limits<int64_t>::max());
    it->SeekForPrev(start);

    if (it->Valid()) {
        uint16_t vb;
        int64_t seqno;

        rocksdb::Slice seqnoSlice = it->key();
        grokSeqnoSlice(seqnoSlice, &vb, &seqno);

        if (vb == vbid) {
            return seqno;
        }
    }

    return 0;
}

std::string RocksDBKVStore::getVbstatePrefix() {
    return "vbstate.";
}

ScanContext* RocksDBKVStore::initScanContext(
        std::shared_ptr<Callback<GetValue> > cb,
        std::shared_ptr<Callback<CacheLookup> > cl,
        uint16_t vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;
    scanSnapshots.emplace(scanId, SnapshotPtr(db->GetSnapshot(), *db));
    // As we cannot efficiently determine how many documents this scan will
    // find, we approximate this value with the seqno difference + 1
    // as scan is supposed to be inclusive at both ends,
    // seqnos 2 to 4 covers 3 docs not 4 - 2 = 2

    uint64_t endSeqno = cachedVBStates[vbid]->highSeqno;
    return new ScanContext(cb,
                           cl,
                           vbid,
                           scanId,
                           startSeqno,
                           endSeqno,
                           options,
                           valOptions,
                           /* documentCount */ endSeqno - startSeqno + 1,
                           configuration);
}

scan_error_t RocksDBKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    uint64_t start = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        start = ctx->lastReadSeqno + 1;
    }

    GetMetaOnly isMetaOnly = ctx->valFilter == ValueFilter::KEYS_ONLY
                                     ? GetMetaOnly::Yes
                                     : GetMetaOnly::No;

    rocksdb::ReadOptions snapshotOpts{rocksdb::ReadOptions()};
    snapshotOpts.snapshot = scanSnapshots.at(ctx->scanId).get();

    std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(snapshotOpts, seqnoFamilyHandle.get()));

    std::string startStr = mkSeqnoStr(ctx->vbid, start);
    it->Seek(startStr);

    std::string endStr = mkSeqnoStr(ctx->vbid, ctx->maxSeqno);

    auto isPastEnd = [&endStr, this](rocksdb::Slice seqSlice) {
        return vbidSeqnoComparator.Compare(seqSlice, endStr) == 1;
    };

    for (; it->Valid() && !isPastEnd(it->key()); it->Next()) {
        uint16_t vb;
        int64_t seqno;

        rocksdb::Slice seqnoSlice = it->key();
        grokSeqnoSlice(seqnoSlice, &vb, &seqno);

        rocksdb::Slice keySlice = it->value();

        std::string valueStr;
        rocksdb::Status s = db->Get(snapshotOpts, keySlice, &valueStr);

        if (!s.ok()) {
            // TODO RDB: Old seqnos are never removed from the db!
            // If the item does not exist (s.isNotFound())
            // the seqno => key mapping could be removed; not even
            // a tombstone remains of that item.
            continue;
        }

        rocksdb::Slice valSlice(valueStr);

        // TODO RDB: Deal with collections
        DocKey key(reinterpret_cast<const uint8_t*>(keySlice.data() +
                                                    sizeof(uint16_t)),
                   keySlice.size() - sizeof(uint16_t),
                   DocNamespace::DefaultCollection);

        std::unique_ptr<Item> itm =
                makeItem(ctx->vbid, key, valSlice, isMetaOnly);

        if (itm->getBySeqno() > seqno) {
            // TODO RDB: Old seqnos are never removed from the db!
            // If the item has a newer seqno now, the stale
            // seqno => key mapping could be removed
            continue;
        } else if (itm->getBySeqno() < seqno) {
            throw std::logic_error(
                    "RocksDBKVStore::scan: index has a higher seqno"
                    "than the document in a snapshot!");
        }

        bool includeDeletes =
                (ctx->docFilter == DocumentFilter::NO_DELETES) ? false : true;
        bool onlyKeys =
                (ctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;

        if (!includeDeletes && itm->getOperation() == queue_op::del) {
            continue;
        }
        int64_t byseqno = itm->getBySeqno();
        CacheLookup lookup(key, byseqno, ctx->vbid);
        ctx->lookup->callback(lookup);

        int status = ctx->lookup->getStatus();

        if (status == ENGINE_KEY_EEXISTS) {
            ctx->lastReadSeqno = byseqno;
            continue;
        } else if (status == ENGINE_ENOMEM) {
            return scan_again;
        }

        GetValue rv(std::move(itm), ENGINE_SUCCESS, -1, onlyKeys);
        ctx->callback->callback(rv);
        status = ctx->callback->getStatus();

        if (status == ENGINE_ENOMEM) {
            return scan_again;
        }

        ctx->lastReadSeqno = byseqno;
    }

    cb_assert(it->status().ok()); // Check for any errors found during the scan

    return scan_success;
}

void RocksDBKVStore::destroyScanContext(ScanContext* ctx) {
    // TODO RDB: Might be nice to have the snapshot in the ctx and
    // release it on destruction
    auto it = scanSnapshots.find(ctx->scanId);
    if (it != scanSnapshots.end()) {
        scanSnapshots.erase(it);
    }
    delete ctx;
}
