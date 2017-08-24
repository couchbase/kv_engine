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

#include "kvstore_config.h"
#include "kvstore_priv.h"

#include <string.h>
#include <algorithm>
#include <limits>

#include "vbucket.h"

static const size_t DEFAULT_VAL_SIZE(64 * 1024);

RocksDBKVStore::RocksDBKVStore(KVStoreConfig& config)
    : KVStore(config),
      valBuffer(NULL),
      valSize(0),
      scanCounter(0),
      logger(config.getLogger()) {
    cachedVBStates.reserve(configuration.getMaxVBuckets());
    cachedVBStates.assign(configuration.getMaxVBuckets(), nullptr);

    writeOptions.sync = true;

    adjustValBuffer(DEFAULT_VAL_SIZE);
    open();
}

RocksDBKVStore::~RocksDBKVStore() {
    close();
    free(valBuffer);
}

void RocksDBKVStore::open() {
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
                                            rocksdb::ColumnFamilyOptions()),

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
    batch.reset();
    defaultFamilyHandle.reset();
    seqnoFamilyHandle.reset();
    localFamilyHandle.reset();
    db.reset();
}

bool RocksDBKVStore::begin() {
    if (!batch) {
        batch = std::make_unique<rocksdb::WriteBatch>();
    }
    return bool(batch);
}

bool RocksDBKVStore::commit(const Item* collectionsManifest) {
    std::lock_guard<std::mutex> lg(writeLock);
    if (batch) {
        rocksdb::Status s = db->Write(writeOptions, batch.get());
        if (s.ok()) {
            batch.reset();
        }
    }
    return !batch;
}

void RocksDBKVStore::rollback() {
    batch.reset();
}

void RocksDBKVStore::adjustValBuffer(const size_t to) {
    // Save room for the flags, exp, etc...
    size_t needed(sizeof(ItemMetaData) +
                  /* deleted flag */ sizeof(uint8_t) +
                  /* bySeqno */ sizeof(uint64_t) +
                  /* datatype */ sizeof(uint8_t) +
                  /* value len */ sizeof(uint32_t) + to);

    if (valBuffer == NULL || valSize < needed) {
        void* buf = realloc(valBuffer, needed);
        if (buf) {
            valBuffer = static_cast<char*>(buf);
            valSize = needed;
        }
    }
}

std::vector<vbucket_state*> RocksDBKVStore::listPersistedVbuckets() {
    return cachedVBStates;
}

void RocksDBKVStore::set(const Item& itm, Callback<mutation_result>& cb) {
    storeItem(itm);

    // TODO RDB: This callback should not really be called until
    // after the batch is committed.
    std::pair<int, bool> p(1, true);
    cb.callback(p);
}

void RocksDBKVStore::storeItem(const Item& itm) {
    // TODO RDB: Consider using SliceParts to avoid copying if
    // possible.
    uint16_t vbid = itm.getVBucketId();
    auto k = mkKeyStr(vbid, itm.getKey());
    auto v = mkValSlice(itm);
    auto seq = mkSeqnoStr(vbid, itm.getBySeqno());

    // TODO RDB: check status.
    rocksdb::Status s1 = batch->Put(k, v);
    rocksdb::Status s2 = batch->Put(seqnoFamilyHandle.get(), seq, k);
    cb_assert(s1.ok());
    cb_assert(s2.ok());
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

void RocksDBKVStore::del(const Item& itm, Callback<int>& cb) {
    // TODO RDB: Deleted items remain as tombstones, but are not yet
    // expired - they will accumuate forever.
    storeItem(itm);

    int rv(1);
    cb.callback(rv);
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
        if (!saveVBState(vbstate, vbucketId)) {
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

rocksdb::Slice RocksDBKVStore::mkValSlice(const Item& item) {
    // Serialize an Item to the format to write to RocksDB.
    // Using the following layout:
    //    uint64_t           cas          ]
    //    uint64_t           revSeqno     ] ItemMetaData
    //    uint32_t           flags        ]
    //    uint32_t           exptime      ]
    // TODO RDB: Wasting a whole byte for deleted
    //    uint8_t            deleted
    //    uint64_t           bySeqno
    //    uint8_t            datatype
    //    uint32_t           value_len
    //    uint8_t[value_len] value

    adjustValBuffer(item.getNBytes());
    char* dest = valBuffer;
    std::memcpy(dest, &item.getMetaData(), sizeof(ItemMetaData));
    dest += sizeof(ItemMetaData);

    uint8_t deleted = item.isDeleted();
    std::memcpy(dest, &deleted, sizeof(deleted));
    dest += sizeof(deleted);

    const int64_t bySeqno{item.getBySeqno()};
    std::memcpy(dest, &bySeqno, sizeof(bySeqno));
    dest += sizeof(uint64_t);

    const uint8_t datatype{item.getDataType()};
    std::memcpy(dest, &datatype, sizeof(datatype));
    dest += sizeof(uint8_t);

    const uint32_t valueLen = item.getNBytes();
    std::memcpy(dest, &valueLen, sizeof(valueLen));
    dest += sizeof(valueLen);
    if (valueLen) {
        std::memcpy(dest, item.getValue()->getData(), valueLen);
    }
    dest += valueLen;

    return rocksdb::Slice(valBuffer, dest - valBuffer);
}

std::unique_ptr<Item> RocksDBKVStore::grokValSlice(uint16_t vb,
                                                   const DocKey& key,
                                                   const rocksdb::Slice& s,
                                                   GetMetaOnly getMetaOnly) {
    // Reverse of mkValSlice - deserialize back into an Item.

    assert(s.size() >= sizeof(ItemMetaData) + sizeof(uint64_t) +
                               sizeof(uint8_t) + sizeof(uint32_t));

    ItemMetaData meta;
    const char* src = s.data();
    std::memcpy(&meta, src, sizeof(meta));
    src += sizeof(meta);

    uint8_t deleted;
    std::memcpy(&deleted, src, sizeof(deleted));
    src += sizeof(deleted);

    int64_t bySeqno;
    std::memcpy(&bySeqno, src, sizeof(bySeqno));
    src += sizeof(bySeqno);

    uint8_t datatype;
    std::memcpy(&datatype, src, sizeof(datatype));
    src += sizeof(datatype);

    uint32_t valueLen;
    std::memcpy(&valueLen, src, sizeof(valueLen));
    src += sizeof(valueLen);

    uint8_t extMeta[EXT_META_LEN];
    extMeta[0] = datatype;

    bool includeValue = getMetaOnly == GetMetaOnly::No && valueLen;

    auto item = std::make_unique<Item>(key,
                                       meta.flags,
                                       meta.exptime,
                                       includeValue ? src : nullptr,
                                       includeValue ? valueLen : 0,
                                       extMeta,
                                       EXT_META_LEN,
                                       meta.cas,
                                       bySeqno,
                                       vb,
                                       meta.revSeqno);

    if (deleted) {
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
            grokValSlice(vb, key, sval, getMetaOnly), ENGINE_SUCCESS, -1, 0);
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

    delete cachedVBStates[vbid];
    cachedVBStates[vbid] = new vbucket_state(state,
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

bool RocksDBKVStore::saveVBState(const vbucket_state& vbState, uint16_t vbid) {
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
    return s.ok();
}

int64_t RocksDBKVStore::readHighSeqnoFromDisk(uint16_t vbid) {
    rocksdb::Iterator* it =
            db->NewIterator(rocksdb::ReadOptions(), seqnoFamilyHandle.get());

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
    snapshotOpts.snapshot = db->GetSnapshot();

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
                grokValSlice(ctx->vbid, key, valSlice, isMetaOnly);

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
