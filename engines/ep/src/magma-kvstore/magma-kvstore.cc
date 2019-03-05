/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "magma-kvstore.h"
#include "bucket_logger.h"
#include "ep_time.h"
#include "kvstore_priv.h"
#include "magma-kvstore_config.h"
#include "vbucket.h"

#include <string.h>
#include <algorithm>
#include <limits>

namespace magmakv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to Magma and when reading a Document
// from Magma.
class MetaData {
public:
    MetaData()
        : bySeqno(0),
          cas(0),
          exptime(0),
          revSeqno(0),
          flags(0),
          valueSize(0),
          deleted(0),
          version(0),
          datatype(0){};
    MetaData(bool deleted,
             uint8_t version,
             uint8_t datatype,
             uint32_t flags,
             uint32_t valueSize,
             time_t exptime,
             uint64_t cas,
             uint64_t revSeqno,
             int64_t bySeqno)
        : bySeqno(bySeqno),
          cas(cas),
          exptime(exptime),
          revSeqno(revSeqno),
          flags(flags),
          valueSize(valueSize),
          deleted(deleted),
          version(version),
          datatype(datatype){};

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
    int64_t bySeqno;
    uint64_t cas;
    time_t exptime;
    uint64_t revSeqno;
    uint32_t flags;
    uint32_t valueSize;
    uint8_t deleted : 1;
    uint8_t version : 7;
    uint8_t datatype;
#pragma pack()
};
} // namespace magmakv

/**
 * Class representing a document to be persisted in Magma.
 */
class MagmaRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param del Flag indicating if it is an item deletion or not
     */
    MagmaRequest(const Item& item, MutationRequestCallback& callback)
        : IORequest(item.getVBucketId(),
                    callback,
                    item.isDeleted(),
                    DiskDocKey{item}),
          docBody(item.getValue()),
          updatedExistingItem(false) {
        docMeta = magmakv::MetaData(
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

    magmakv::MetaData& getDocMeta() {
        return docMeta;
    }

    int64_t getBySeqno() const {
        return docMeta.bySeqno;
    }

    size_t getKeyLen() const {
        return getKey().size();
    }

    size_t getBodySize() const {
        return docBody ? docBody->valueSize() : 0;
    }

    const void* getBodyData() const {
        return docBody ? docBody->getData() : nullptr;
    }

    bool wasCreate() const {
        return !updatedExistingItem;
    }

    void markAsUpdated() {
        updatedExistingItem = true;
    }

private:
    magmakv::MetaData docMeta;
    value_t docBody;
    bool updatedExistingItem;
};

class KVMagma {
public:
    KVMagma(const Vbid vb, const std::string path) : vbid(vb.get()) {
        // open magma
    }

    int SetOrDel(MagmaRequest* req) {
        if (req->isDelete()) {
            ; // TODO should we have a merge delta ops for old value?
        }
        return 0;
    }

    int Get(const DiskDocKey& key, void** value, int* valueLen) {
        return 0;
    }

    Vbid vbid;
    int magmaHandleId;
};

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration)
    : KVStore(configuration),
      vbDB(configuration.getMaxVBuckets()),
      in_transaction(false),
      magmaPath(configuration.getDBName() + "/magma."),
      scanCounter(0),
      logger(configuration.getLogger()) {
    {
        // TODO: storage-team 2018-10-10 Must support dynamic
        // reconfiguration of memtables Quota when bucket RAM
        // is modified.
        const auto memtablesQuota = configuration.getBucketQuota() /
                                    configuration.getMaxShards() *
                                    configuration.getMagmaMemQuotaRatio();
        const int commitPoints = configuration.getMagmaMaxCommitPoints();
        const size_t writeCache = configuration.getMagmaMaxWriteCache();
        const size_t minValueSize = configuration.getMagmaMinValueSize();
        const int numFlushers = configuration.getMagmaNumFlushers();
        const int numCompactors = configuration.getMagmaNumCompactors();
        const size_t walBufferSize = configuration.getMagmaWalBufferSize();

        (void)memtablesQuota;
        (void)commitPoints;
        (void)writeCache;
        (void)minValueSize;
        (void)numFlushers;
        (void)numCompactors;
        (void)walBufferSize;
    }
    cachedVBStates.resize(configuration.getMaxVBuckets());

    createDataDir(configuration.getDBName());

    // Read persisted VBs state
    auto vbids = discoverVBuckets();
    for (auto vbid : vbids) {
        KVMagma db(vbid, magmaPath);
        // TODO: may need to read stashed magma state files for caching.
        // Update stats
        ++st.numLoadedVb;
    }
}

MagmaKVStore::~MagmaKVStore() {
}

std::string MagmaKVStore::getVBDBSubdir(Vbid vbid) {
    return magmaPath + std::to_string(vbid.get());
}

std::vector<Vbid> MagmaKVStore::discoverVBuckets() {
    std::vector<Vbid> vbids;
    auto vbDirs = cb::io::findFilesContaining(magmaPath, "");
    for (const auto& dir : vbDirs) {
        size_t lastDotIndex = dir.rfind(".");
        size_t vbidLength = dir.size() - lastDotIndex - 1;
        std::string vbidStr = dir.substr(lastDotIndex + 1, vbidLength);
        Vbid vbid(std::stoi(vbidStr.c_str()));
        // Take in account only VBuckets managed by this Shard
        if ((vbid.get() % configuration.getMaxShards()) ==
            configuration.getShardId()) {
            vbids.push_back(vbid);
        }
    }
    return vbids;
}

bool MagmaKVStore::begin(std::unique_ptr<TransactionContext> txCtx) {
    in_transaction = true;
    transactionCtx = std::move(txCtx);
    return in_transaction;
}

bool MagmaKVStore::commit(Collections::VB::Flush& collectionsFlush) {
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
    std::vector<std::unique_ptr<MagmaRequest>> commitBatch;
    {
        std::lock_guard<std::mutex> lock(writeLock);
        std::swap(pendingReqs, commitBatch);
    }

    bool success = true;
    auto vbid = commitBatch[0]->getVBucketId();

    // Flush all documents to disk
    auto status = saveDocs(vbid, collectionsFlush, commitBatch);
    if (status) {
        logger.warn(
                "MagmaKVStore::commit: saveDocs error:{}, "
                "vb:{}",
                status,
                vbid);
        success = false;
    }

    commitCallback(status, commitBatch);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
        transactionCtx.reset();
    }

    return success;
}

void MagmaKVStore::commitCallback(
        int status,
        const std::vector<std::unique_ptr<MagmaRequest>>& commitBatch) {
    for (const auto& req : commitBatch) {
        if (!status) {
            ++st.numSetFailure;
        } else {
            st.writeTimeHisto.add(req->getDelta() / 1000);
            st.writeSizeHisto.add(req->getKeyLen() + req->getBodySize());
        }
        // TODO: Should set `mr.second` to true or false depending on if
        // this is an insertion (true) or an update of an existing item
        // (false). However, to achieve this we would need to perform a lookup
        // which is costly. For now just assume that the item
        // did not exist. Later maybe use hyperlog for a better answer?
        mutation_result mr = std::make_pair(1, req->wasCreate());
        req->getSetCallback()->callback(*transactionCtx, mr);
    }
}

void MagmaKVStore::rollback() {
    if (in_transaction) {
        in_transaction = false;
        transactionCtx.reset();
    }
}

StorageProperties MagmaKVStore::getStorageProperties() {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

std::vector<vbucket_state*> MagmaKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void MagmaKVStore::set(const Item& item,
                       Callback<TransactionContext, mutation_result>& cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::set: in_transaction must be true to perform a "
                "set operation.");
    }
    MutationRequestCallback callback;
    callback.setCb = &cb;
    pendingReqs.push_back(std::make_unique<MagmaRequest>(item, callback));
}

GetValue MagmaKVStore::get(const DiskDocKey& key, Vbid vb, bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue MagmaKVStore::getWithHeader(void* dbHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly,
                                     bool fetchDelete) {
    void* value = nullptr;
    int valueLen = 0;
    KVMagma db(vb, magmaPath);
    int status = db.Get(key, &value, &valueLen);
    if (status < 0) {
        logger.warn(
                "MagmaKVStore::getWithHeader: magma::DB::Lookup error:{}, "
                "vb:{}",
                status,
                vb);
    }
    std::string valStr(reinterpret_cast<char*>(value), valueLen);
    return makeGetValue(vb, key, valStr, getMetaOnly);
}

void MagmaKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
    KVMagma db(vb, magmaPath);
    for (auto& it : itms) {
        auto& key = it.first;
        void* value = nullptr;
        int valueLen = 0;
        int status = db.Get(key, &value, &valueLen);
        if (status < 0) {
            logger.warn(
                    "MagmaKVStore::getMulti: magma::DB::Lookup error:{}, "
                    "vb:{}",
                    status,
                    vb);
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value->setStatus(ENGINE_KEY_ENOENT);
            }
            continue;
        }
        std::string valStr(reinterpret_cast<char*>(value), valueLen);
        it.second.value = makeGetValue(vb, key, valStr, it.second.isMetaOnly);
        GetValue* rv = &it.second.value;
        for (auto& fetch : it.second.bgfetched_list) {
            fetch->value = rv;
        }
    }
}

void MagmaKVStore::reset(Vbid vbucketId) {
    // TODO storage-team 2018-10-9 need to implement
}

void MagmaKVStore::del(const Item& item,
                       Callback<TransactionContext, int>& cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::del: in_transaction must be true to perform a "
                "delete operation.");
    }
    // TODO: Deleted items remain as tombstones, but are not yet expired,
    // they will accumuate forever.
    MutationRequestCallback callback;
    callback.delCb = &cb;
    pendingReqs.push_back(std::make_unique<MagmaRequest>(item, callback));
}

void MagmaKVStore::delVBucket(Vbid vbid, uint64_t vb_version) {
    std::lock_guard<std::mutex> lg(writeLock);
    // TODO: check if needs lock on `openDBMutex`.
    vbDB[vbid.get()].reset();
    // Just destroy the DB in the sub-folder for vbid
    auto dbname = getVBDBSubdir(vbid);
    // DESTROY DB...
}

bool MagmaKVStore::snapshotVBucket(Vbid vbucketId,
                                   const vbucket_state& vbstate,
                                   VBStatePersist options) {
    // TODO Refactor out behaviour common to this and CouchKVStore
    auto start = std::chrono::steady_clock::now();

    if (updateCachedVBState(vbucketId, vbstate) &&
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
    }

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

bool MagmaKVStore::snapshotStats(const std::map<std::string, std::string>&) {
    // TODO storage-team 2018-10-9 need to implement
    return true;
}

void MagmaKVStore::destroyInvalidVBuckets(bool) {
    // TODO storage-team 2018-10-9 need to implement
}

size_t MagmaKVStore::getNumShards() const {
    return configuration.getMaxShards();
}

std::unique_ptr<Item> MagmaKVStore::makeItem(Vbid vb,
                                             const DiskDocKey& key,
                                             const std::string& value,
                                             GetMetaOnly getMetaOnly) {
    Expects(value.size() >= sizeof(magmakv::MetaData));

    const char* data = value.c_str();

    magmakv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

    auto item = std::make_unique<Item>(key.getDocKey(),
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

GetValue MagmaKVStore::makeGetValue(Vbid vb,
                                    const DiskDocKey& key,
                                    const std::string& value,
                                    GetMetaOnly getMetaOnly) {
    return GetValue(
            makeItem(vb, key, value, getMetaOnly), ENGINE_SUCCESS, -1, 0);
}

void MagmaKVStore::readVBState(const KVMagma& db) {
    // Largely copied from CouchKVStore
    // TODO refactor out sections common to CouchKVStore
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    int64_t highSeqno = readHighSeqnoFromDisk(db);
    std::string failovers;
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    bool mightContainXattrs = false;

    auto key = getVbstateKey();
    std::string vbstate;
    auto vbid = db.vbid;
    cachedVBStates[vbid.get()] =
            std::make_unique<vbucket_state>(state,
                                            checkpointId,
                                            maxDeletedSeqno,
                                            highSeqno,
                                            purgeSeqno,
                                            lastSnapStart,
                                            lastSnapEnd,
                                            maxCas,
                                            hlcCasEpochSeqno,
                                            mightContainXattrs,
                                            failovers,
                                            false);
}

int MagmaKVStore::saveDocs(
        Vbid vbid,
        Collections::VB::Flush& collectionsFlush,
        const std::vector<std::unique_ptr<MagmaRequest>>& commitBatch) {
    auto reqsSize = commitBatch.size();
    if (reqsSize == 0) {
        st.docsCommitted = 0;
        return 0;
    }

    auto& vbstate = cachedVBStates[vbid.get()];
    if (vbstate == nullptr) {
        throw std::logic_error("MagmaKVStore::saveDocs: cachedVBStates[" +
                               std::to_string(vbid.get()) + "] is NULL");
    }

    int64_t lastSeqno = 0;
    int status = 0;

    auto begin = std::chrono::steady_clock::now();
    {
        KVMagma db(vbid, magmaPath);

        for (const auto& request : commitBatch) {
            status = db.SetOrDel(request.get());
            if (status < 0) {
                logger.warn(
                        "MagmaKVStore::saveDocs: magma::DB::Insert error:{}, "
                        "vb:{}",
                        status,
                        vbid);
            }
            if (request->getBySeqno() > lastSeqno) {
                lastSeqno = request->getBySeqno();
            }
        }
    }

    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));
    if (status) {
        logger.warn(
                "MagmaKVStore::saveDocs: magma::DB::Write error:{}, "
                "vb:%d",
                status,
                vbid.get());
        return status;
    }

    vbstate->highSeqno = lastSeqno;

    return status;
}

int64_t MagmaKVStore::readHighSeqnoFromDisk(const KVMagma& db) {
    return 0;
}

std::string MagmaKVStore::getVbstateKey() {
    return "vbstate";
}

ScanContext* MagmaKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;

    // As we cannot efficiently determine how many documents this scan will
    // find, we approximate this value with the seqno difference + 1
    // as scan is supposed to be inclusive at both ends,
    // seqnos 2 to 4 covers 3 docs not 4 - 2 = 2

    uint64_t endSeqno = cachedVBStates[vbid.get()]->highSeqno;
    return new ScanContext(cb,
                           cl,
                           vbid,
                           scanId,
                           startSeqno,
                           endSeqno,
                           0, /*TODO MAGMA: pass the read purge-seqno */
                           options,
                           valOptions,
                           /* documentCount */ endSeqno - startSeqno + 1,
                           configuration,
                           {/* TODO: add collections in magma */});
}

scan_error_t MagmaKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    auto startSeqno = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        startSeqno = ctx->lastReadSeqno + 1;
    }
    (void)startSeqno;

    return scan_success;
}

void MagmaKVStore::destroyScanContext(ScanContext* ctx) {
    // TODO Might be nice to have the snapshot in the ctx and
    // release it on destruction
}
