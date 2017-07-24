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

#include <string.h>
#include <algorithm>

static const size_t DEFAULT_VAL_SIZE(64 * 1024);

RocksDBKVStore::RocksDBKVStore(KVStoreConfig& config)
    : KVStore(config), valBuffer(NULL), valSize(0), scanCounter(0) {
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

    /* Use a listener to set the appropriate engine in the
     * flusher threads RocksDB creates. We need the flusher threads to
     * account for news/deletes against the appropriate bucket. */
    FlushStartListener* fsl =
            new FlushStartListener(ObjectRegistry::getCurrentEngine());

    rdbOptions.listeners.emplace_back(fsl);

    const std::string dbdir = configuration.getDBName();

    cb::io::mkdirp(dbdir);

    const std::string dbname =
            dbdir + "/rocksdb." + std::to_string(configuration.getShardId());

    rocksdb::DB* dbPtr;
    rocksdb::Status s = rocksdb::DB::Open(rdbOptions, dbname, &dbPtr);

    if (s.ok()) {
        db.reset(dbPtr);
    } else {
        throw std::runtime_error(
                "RocksDBKVStore::open: failed to open database '" + dbname +
                "': " + s.ToString());
    }
}

void RocksDBKVStore::close() {
    batch.reset();
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
    // TODO RDB:  Something useful.
    // std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    std::vector<vbucket_state*> rv;
    return rv;
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

    // TODO RDB: check status.
    rocksdb::Status s = batch->Put(k, v);
    cb_assert(s.ok());
}

GetValue RocksDBKVStore::get(const DocKey& key, uint16_t vb, bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue RocksDBKVStore::getWithHeader(
        void* dbHandle,
        const DocKey& key,
        uint16_t vb,
        GetMetaOnly getMetaOnly, // TODO RDB: get meta only
        bool fetchDelete) {
    std::string k(mkKeyStr(vb, key));
    std::string value;

    // TODO RDB: use a PinnableSlice to avoid some memcpy
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), k, &value);
    if (!s.ok()) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }
    return makeGetValue(vb, key, value);
}

void RocksDBKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) {
    // TODO RDB: RocksDB supports a multi get which we should use here.
    for (auto& it : itms) {
        auto& key = it.first;
        std::string vbAndKey(mkKeyStr(vb, it.first));
        std::string value;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), vbAndKey, &value);
        if (s.ok()) {
            it.second.value = makeGetValue(vb, key, value);
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
    std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(rocksdb::ReadOptions()));
    const char* prefix(reinterpret_cast<const char*>(&vb));
    std::string start(prefix, sizeof(vb));
    rocksdb::WriteBatch delBatch;
    for (it->Seek(start);
         it->Valid() && matches_prefix(it->key(), sizeof(vb), prefix);
         it->Next()) {
        delBatch.Delete(it->key());
    }
    rocksdb::Status s = db->Write(writeOptions, &delBatch);
    cb_assert(s.ok());
}

bool RocksDBKVStore::snapshotVBucket(uint16_t vbucketId,
                                     const vbucket_state& vbstate,
                                     VBStatePersist options) {
    // TODO RDB:  Implement
    return true;
}

bool RocksDBKVStore::snapshotStats(const std::map<std::string, std::string>&) {
    // TODO RDB:  Implement
    return true;
}

void RocksDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO RDB:  implement
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
                                                   const rocksdb::Slice& s) {
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

    auto item = std::make_unique<Item>(key,
                                       meta.flags,
                                       meta.exptime,
                                       src,
                                       valueLen,
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
                                      const std::string& value) {
    rocksdb::Slice sval(value);
    return GetValue(grokValSlice(vb, key, sval), ENGINE_SUCCESS, -1, 0);
}

ScanContext* RocksDBKVStore::initScanContext(
        std::shared_ptr<Callback<GetValue> > cb,
        std::shared_ptr<Callback<CacheLookup> > cl,
        uint16_t vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    // TODO RDB vmx 2016-10-29: implement
    size_t scanId = scanCounter++;
    return new ScanContext(cb,
                           cl,
                           vbid,
                           scanId,
                           startSeqno,
                           99999999,
                           options,
                           valOptions,
                           999999,
                           configuration);
}
