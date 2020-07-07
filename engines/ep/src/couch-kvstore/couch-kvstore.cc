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

#include "couch-kvstore/couch-kvstore.h"
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "couch-kvstore-config.h"
#include "diskdockey.h"
#include "ep_time.h"
#include "getkeys.h"
#include "item.h"
#include "kvstore_config.h"
#include "persistence_callback.h"
#include "rollback_result.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <JSON_checker.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <gsl/gsl>

#include <memory>
#include <shared_mutex>
#include <utility>

static int bySeqnoScanCallback(Db* db, DocInfo* docinfo, void* ctx);
static int getMultiCallback(Db* db, DocInfo* docinfo, void* ctx);

static bool endWithCompact(const std::string &filename) {
    const std::string suffix{".compact"};
    const auto pos = filename.rfind(suffix);
    return pos != std::string::npos && (pos + suffix.size()) == filename.size();
}

static void discoverDbFiles(const std::string &dir,
                            std::vector<std::string> &v) {
    auto files = cb::io::findFilesContaining(dir, ".couch");
    std::vector<std::string>::iterator ii;
    for (ii = files.begin(); ii != files.end(); ++ii) {
        if (!endWithCompact(*ii)) {
            v.push_back(*ii);
        }
    }
}

static bool allDigit(std::string &input) {
    size_t numchar = input.length();
    for(size_t i = 0; i < numchar; ++i) {
        if (!isdigit(input[i])) {
            return false;
        }
    }
    return true;
}

static std::string couchkvstore_strerrno(Db *db, couchstore_error_t err) {
    std::array<char, 256> msg;
    switch (err) {
    case COUCHSTORE_ERROR_OPEN_FILE:
    case COUCHSTORE_ERROR_READ:
    case COUCHSTORE_ERROR_WRITE:
    case COUCHSTORE_ERROR_FILE_CLOSE:
        if (couchstore_last_os_error(db, msg.data(), msg.size()) ==
            COUCHSTORE_SUCCESS) {
            return {msg.data()};
        }
        return {"<error retrieving last_os_error>"};

    case COUCHSTORE_ERROR_CORRUPT:
    case COUCHSTORE_ERROR_CHECKSUM_FAIL:
        if (couchstore_last_internal_error(db, msg.data(), msg.size()) ==
            COUCHSTORE_SUCCESS) {
            return {msg.data()};
        }
        return {"<error retrieving last_internal_error>"};

    default:
        return {"none"};
    }
}

static DiskDocKey makeDiskDocKey(sized_buf buf) {
    return DiskDocKey{buf.buf, buf.size};
}

static sized_buf to_sized_buf(const DiskDocKey& key) {
    return {const_cast<char*>(reinterpret_cast<const char*>(key.data())),
            key.size()};
}

/**
 * Helper function to create an Item from couchstore DocInfo & related types.
 */
static std::unique_ptr<Item> makeItemFromDocInfo(Vbid vbid,
                                                 const DocInfo& docinfo,
                                                 const MetaData& metadata,
                                                 sized_buf value) {
    // Strip off the DurabilityPrepare namespace (if present) from the persisted
    // dockey before we create the in-memory item.
    auto item = std::make_unique<Item>(makeDiskDocKey(docinfo.id).getDocKey(),
                                       metadata.getFlags(),
                                       metadata.getExptime(),
                                       value.buf,
                                       value.size,
                                       metadata.getDataType(),
                                       metadata.getCas(),
                                       docinfo.db_seq,
                                       vbid,
                                       docinfo.rev_seq);
    if (docinfo.deleted) {
        item->setDeleted(metadata.getDeleteSource());
    }

    if (metadata.getVersionInitialisedFrom() == MetaData::Version::V3) {
        // Metadata is from a SyncWrite - update the Item appropriately.
        switch (metadata.getDurabilityOp()) {
        case queue_op::pending_sync_write:
            // From disk we return an infinite timeout; as this could
            // refer to an already-committed SyncWrite and hence timeout
            // must be ignored.
            item->setPendingSyncWrite({metadata.getDurabilityLevel(),
                                       cb::durability::Timeout::Infinity()});
            if (metadata.isPreparedSyncDelete()) {
                item->setDeleted(DeleteSource::Explicit);
            }
            break;
        case queue_op::commit_sync_write:
            item->setCommittedviaPrepareSyncWrite();
            item->setPrepareSeqno(metadata.getPrepareSeqno());
            break;
        case queue_op::abort_sync_write:
            item->setAbortSyncWrite();
            item->setPrepareSeqno(metadata.getPrepareSeqno());
            break;
        default:
            throw std::logic_error("makeItemFromDocInfo: Invalid queue_op:" +
                                   to_string(metadata.getDurabilityOp()));
        }
    }
    return item;
}

struct GetMultiCbCtx {
    GetMultiCbCtx(CouchKVStore& c, Vbid v, vb_bgfetch_queue_t& f)
        : cks(c), vbId(v), fetches(f) {
    }

    CouchKVStore &cks;
    Vbid vbId;
    vb_bgfetch_queue_t &fetches;
};

struct AllKeysCtx {
    AllKeysCtx(std::shared_ptr<StatusCallback<const DiskDocKey&>> callback,
               uint32_t cnt)
        : cb(std::move(callback)), count(cnt) {
    }

    std::shared_ptr<StatusCallback<const DiskDocKey&>> cb;
    uint32_t count;
};

couchstore_content_meta_flags CouchRequest::getContentMeta(const Item& it) {
    couchstore_content_meta_flags rval;

    if (mcbp::datatype::is_json(it.getDataType())) {
        rval = COUCH_DOC_IS_JSON;
    } else {
        rval = COUCH_DOC_NON_JSON_MODE;
    }

    if (it.getNBytes() > 0 && !mcbp::datatype::is_snappy(it.getDataType())) {
        //Compress only if a value exists and is not already compressed
        rval |= COUCH_DOC_IS_COMPRESSED;
    }

    return rval;
}

CouchRequest::CouchRequest(queued_item it)
    : IORequest(std::move(it)), value(item->getValue()) {
    dbDoc.id = to_sized_buf(getKey());

    if (item->getNBytes()) {
        dbDoc.data.buf = const_cast<char *>(value->getData());
        dbDoc.data.size = item->getNBytes();
    } else {
        dbDoc.data.buf = nullptr;
        dbDoc.data.size = 0;
    }
    meta.setCas(item->getCas());
    meta.setFlags(item->getFlags());
    meta.setExptime(item->getExptime());
    meta.setDataType(item->getDataType());

    const auto isDurabilityOp =
            (item->getOperation() == queue_op::pending_sync_write ||
             item->getOperation() == queue_op::commit_sync_write ||
             item->getOperation() == queue_op::abort_sync_write);

    if (isDurabilityOp) {
        meta.setDurabilityOp(item->getOperation());
    }

    if (item->isPending()) {
        // Note: durability timeout /isn't/ persisted as part of a pending
        // SyncWrite. This is because if we ever read it back from disk
        // during warmup (i.e. the commit_sync_write was never persisted), we
        // don't know if the SyncWrite was actually already committed; as such
        // to ensure consistency the pending SyncWrite *must* eventually commit
        // (or sit in pending forever).
        const auto level = item->getDurabilityReqs().getLevel();
        meta.setPrepareProperties(level, item->isDeleted());
    }

    if (item->isCommitSyncWrite() || item->isAbort()) {
        meta.setCompletedProperties(item->getPrepareSeqno());
    }

    dbDocInfo.db_seq = item->getBySeqno();

    // Now allocate space to hold the meta and get it ready for storage
    dbDocInfo.rev_meta.size = MetaData::getMetaDataSize(
            isDurabilityOp ? MetaData::Version::V3 : MetaData::Version::V1);
    dbDocInfo.rev_meta.buf = meta.prepareAndGetForPersistence();

    dbDocInfo.rev_seq = item->getRevSeqno();

    if (item->isDeleted() && !item->isPending()) {
        // Prepared SyncDeletes are not marked as deleted.
        dbDocInfo.deleted = 1;
        meta.setDeleteSource(item->deletionSource());
    } else {
        dbDocInfo.deleted = 0;
    }
    dbDocInfo.id = dbDoc.id;
    dbDocInfo.content_meta = getContentMeta(*item);
}

CouchRequest::~CouchRequest() = default;

namespace Collections {
static constexpr const char* manifestName = "_local/collections/manifest";
static constexpr const char* openCollectionsName = "_local/collections/open";
static constexpr const char* scopesName = "_local/scope/open";
static constexpr const char* droppedCollectionsName =
        "_local/collections/dropped";
} // namespace Collections

CouchKVStore::CouchKVStore(CouchKVStoreConfig& config)
    : CouchKVStore(config, *couchstore_get_default_file_ops()) {
}

CouchKVStore::CouchKVStore(CouchKVStoreConfig& config,
                           FileOpsInterface& ops,
                           bool readOnly,
                           std::shared_ptr<RevisionMap> revMap)
    : KVStore(readOnly),
      configuration(config),
      dbname(config.getDBName()),
      dbFileRevMap(std::move(revMap)),
      logger(config.getLogger()),
      base_ops(ops) {
    // todo: consider refactor of construction to separate out RW/RO
    if (!readOnly) {
        // Must post-initialise the vector behind the folly::Synchonised
        std::vector<MonotonicRevision> revMap(config.getMaxVBuckets());
        dbFileRevMap->swap(revMap);
    }

    createDataDir(dbname);
    statCollectingFileOps = getCouchstoreStatsOps(st.fsStats, base_ops);
    statCollectingFileOpsCompaction = getCouchstoreStatsOps(
        st.fsStatsCompaction, base_ops);

    // init db file map with default revision number, 1
    numDbFiles = configuration.getMaxVBuckets();

    // pre-allocate lookup maps (vectors) given we have a relatively
    // small, fixed number of vBuckets.
    cachedDocCount.assign(numDbFiles, cb::RelaxedAtomic<size_t>(0));
    cachedDeleteCount.assign(numDbFiles, cb::RelaxedAtomic<size_t>(-1));
    cachedFileSize.assign(numDbFiles, cb::RelaxedAtomic<uint64_t>(0));
    cachedSpaceUsed.assign(numDbFiles, cb::RelaxedAtomic<uint64_t>(0));
    cachedVBStates.resize(numDbFiles);

    initialize();
}

CouchKVStore::CouchKVStore(CouchKVStoreConfig& config, FileOpsInterface& ops)
    : CouchKVStore(config,
                   ops,
                   false /*readonly*/,
                   std::make_shared<RevisionMap>()) {
}

/**
 * Make a read-only CouchKVStore from this object
 */
std::unique_ptr<CouchKVStore> CouchKVStore::makeReadOnlyStore() {
    // Not using make_unique due to the private constructor we're calling
    return std::unique_ptr<CouchKVStore>(
            new CouchKVStore(configuration, dbFileRevMap));
}

CouchKVStore::CouchKVStore(CouchKVStoreConfig& config,
                           std::shared_ptr<RevisionMap> dbFileRevMap)
    : CouchKVStore(config,
                   *couchstore_get_default_file_ops(),
                   true /*readonly*/,
                   dbFileRevMap) {
}

void CouchKVStore::initialize() {
    std::vector<Vbid> vbids;
    std::vector<std::string> files;
    discoverDbFiles(dbname, files);
    populateFileNameMap(files, &vbids);

    couchstore_error_t errorCode;

    for (auto id : vbids) {
        DbHolder db(*this);
        errorCode = openDB(id, db, COUCHSTORE_OPEN_FLAG_RDONLY);
        bool abort = false;
        if (errorCode == COUCHSTORE_SUCCESS) {
            auto readStatus = readVBStateAndUpdateCache(db, id).status;
            if (readStatus == ReadVBStateStatus::Success) {
                /* update stat */
                ++st.numLoadedVb;
            } else if (readStatus != ReadVBStateStatus::CorruptSnapshot) {
                logger.warn(
                        "CouchKVStore::initialize: readVBState"
                        " readVBState:{}, name:{}/{}.couch.{}",
                        int(readStatus),
                        dbname,
                        id.get(),
                        db.getFileRev());
                abort = true;
            }
        } else {
            logger.warn(
                    "CouchKVStore::initialize: openDB"
                    " error:{}, name:{}/{}.couch.{}",
                    couchstore_strerror(errorCode),
                    dbname,
                    id.get(),
                    db.getFileRev());
            abort = true;
        }

        // Abort couch-kvstore initialisation for two cases:
        // 1) open fails
        // 2) readVBState returns !Success and !CorruptSnapshot. Note the error
        // CorruptSnapshot is generated for replica/pending vbuckets only and
        // we want to continue as if the vbucket does not exist so it gets
        // rebuilt from the active.
        if (abort) {
            throw std::runtime_error(
                    "CouchKVStore::initialize: no vbstate for " +
                    id.to_string());
        }

        // Setup cachedDocCount
        cachedDocCount[id.get()] =
                cb::couchstore::getHeader(*db.getDb()).docCount;

        if (!isReadOnly()) {
            removeCompactFile(dbname, id);
        }
    }
}

CouchKVStore::~CouchKVStore() {
    close();
}

void CouchKVStore::reset(Vbid vbucketId) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::reset: Not valid on a read-only "
                        "object.");
    }

    vbucket_state* state = getVBucketState(vbucketId);
    if (state) {
        state->reset();

        cachedDocCount[vbucketId.get()] = 0;
        cachedDeleteCount[vbucketId.get()] = 0;
        cachedFileSize[vbucketId.get()] = 0;
        cachedSpaceUsed[vbucketId.get()] = 0;

        // Unlink the current revision and then increment it to ensure any
        // pending delete doesn't delete us. Note that the expectation is that
        // some higher level per VB lock is required to prevent data-races here.
        // KVBucket::vb_mutexes is used in this case.
        unlinkCouchFile(vbucketId, getDbRevision(vbucketId));
        prepareToCreateImpl(vbucketId);

        writeVBucketState(vbucketId, *state);
    } else {
        throw std::invalid_argument(
                "CouchKVStore::reset: No entry in cached "
                "states for " +
                vbucketId.to_string());
    }
}

void CouchKVStore::set(queued_item item) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::set: Not valid on a read-only "
                        "object.");
    }
    if (!inTransaction) {
        throw std::invalid_argument(
                "CouchKVStore::set: inTransaction must be "
                "true to perform a set operation.");
    }

    // each req will be de-allocated after commit
    pendingReqsQ.emplace_back(std::move(item));
}

GetValue CouchKVStore::get(const DiskDocKey& key, Vbid vb) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vb, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        logger.warn("CouchKVStore::get: openDB error:{}, {}",
                    couchstore_strerror(errCode),
                    vb);
        return GetValue(nullptr, couchErr2EngineErr(errCode));
    }

    GetValue gv = getWithHeader(db, key, vb, GetMetaOnly::No);
    return gv;
}

GetValue CouchKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly) {
    // const_cast away here, the lower level couchstore does not use const
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(
            const_cast<KVFileHandle&>(kvFileHandle));
    return getWithHeader(couchKvHandle.getDbHolder(), key, vb, getMetaOnly);
}

GetValue CouchKVStore::getWithHeader(DbHolder& db,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly) {
    auto start = std::chrono::steady_clock::now();
    DocInfo *docInfo = nullptr;
    GetValue rv;

    sized_buf id = to_sized_buf(key);

    couchstore_error_t errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                                          id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (getMetaOnly == GetMetaOnly::No) {
            // log error only if this is non-xdcr case
            logger.warn(
                    "CouchKVStore::getWithHeader: couchstore_docinfo_by_id "
                    "error:{} [{}], {}",
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode),
                    vb);
        }
    } else {
        if (docInfo == nullptr) {
            throw std::logic_error("CouchKVStore::getWithHeader: "
                    "couchstore_docinfo_by_id returned success but docInfo "
                    "is NULL");
        }
        errCode = fetchDoc(db, docInfo, rv, vb, getMetaOnly);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::getWithHeader: fetchDoc error:{} [{}],"
                    " {}, deleted:{}",
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode),
                    vb,
                    docInfo->deleted ? "yes" : "no");
        }

        // record stats
        st.readTimeHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start));
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(key.size() + rv.item->getNBytes());
        }
    }

    if(errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
    }

    couchstore_free_docinfo(docInfo);
    rv.setStatus(couchErr2EngineErr(errCode));
    return rv;
}

void CouchKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
    if (itms.empty()) {
        return;
    }
    int numItems = itms.size();

    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vb, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::getMulti: openDB error:{}, "
                "{}, numDocs:{}",
                couchstore_strerror(errCode),
                vb,
                numItems);
        st.numGetFailure += numItems;
        for (auto& item : itms) {
            item.second.value.setStatus(ENGINE_NOT_MY_VBUCKET);
        }
        return;
    }

    size_t idx = 0;
    std::vector<sized_buf> ids(itms.size());
    for (auto& item : itms) {
        ids[idx] = to_sized_buf(item.first);
        ++idx;
    }

    GetMultiCbCtx ctx(*this, vb, itms);

    errCode = couchstore_docinfos_by_id(
            db, ids.data(), itms.size(), 0, getMultiCallback, &ctx);
    if (errCode != COUCHSTORE_SUCCESS) {
        st.numGetFailure += numItems;
        logger.warn(
                "CouchKVStore::getMulti: "
                "couchstore_docinfos_by_id error {} [{}], {}",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode),
                vb);
        for (auto& item : itms) {
            item.second.value.setStatus(couchErr2EngineErr(errCode));
        }
    }

    // If available, record how many reads() we did for this getMulti;
    // and the average reads per document.
    auto* stats = couchstore_get_db_filestats(db);
    if (stats != nullptr) {
        const auto readCount = stats->getReadCount();
        st.getMultiFsReadCount += readCount;
        st.getMultiFsReadHisto.add(readCount);
        st.getMultiFsReadPerDocHisto.add(readCount / itms.size());
    }
}

void CouchKVStore::getRange(Vbid vb,
                            const DiskDocKey& startKey,
                            const DiskDocKey& endKey,
                            const KVStore::GetRangeCb& cb) {
    DbHolder db(*this);
    auto errCode = openDB(vb, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        throw std::runtime_error("CouchKVStore::getRange: openDB error for " +
                                 vb.to_string() +
                                 " - couchstore returned error: " +
                                 couchstore_strerror(errCode));
    }

    // Trampoine's state. Cannot use lambda capture as couchstore expects a
    // C-style callback.
    struct TrampolineState {
        Vbid vb;
        const DiskDocKey& endKey;
        const KVStore::GetRangeCb& userFunc;
    };
    TrampolineState trampoline_state{vb, endKey, cb};

    // Trampoline to fetch the document value, and map C++ std::function to
    // C-style callback expected by couchstore.
    auto callback_trampoline = [](Db* db, DocInfo* docinfo, void* ctx) -> int {
        auto& state = *reinterpret_cast<TrampolineState*>(ctx);

        // Filter out (skip) any deleted items (couchstore_docinfos_by_id()
        // returns both alive and deleted items).
        if (docinfo->deleted) {
            return COUCHSTORE_SUCCESS;
        }

        // Note: Unfortunately couchstore compares the endKey *inclusively* -
        // i.e. it will return a document which has the key endKey. We only
        // want to perform an exclusive search [start, end).
        // Therefore, skip the last document if it's the same as the specified
        // end key.
        if (makeDiskDocKey(docinfo->id) == state.endKey) {
            return COUCHSTORE_SUCCESS;
        }

        // Create metadata from the rev_meta.
        std::unique_ptr<MetaData> metadata;
        try {
            metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);
        } catch (std::logic_error&) {
            return COUCHSTORE_ERROR_DB_NO_LONGER_VALID;
        }

        // Fetch document value.
        Doc* doc = nullptr;
        auto errCode = couchstore_open_doc_with_docinfo(
                db, docinfo, &doc, DECOMPRESS_DOC_BODIES);
        if (errCode != COUCHSTORE_SUCCESS) {
            // Failed to fetch document - cancel couchstore_docinfos_by_id
            // scan.
            return errCode;
        }

        state.userFunc(GetValue{
                makeItemFromDocInfo(state.vb, *docinfo, *metadata, doc->data)});
        couchstore_free_document(doc);
        return COUCHSTORE_SUCCESS;
    };

    const std::array<sized_buf, 2> range = {
            {to_sized_buf(startKey), to_sized_buf(endKey)}};
    errCode = couchstore_docinfos_by_id(db.getDb(),
                                        range.data(),
                                        range.size(),
                                        RANGES,
                                        callback_trampoline,
                                        &trampoline_state);
    if (errCode != COUCHSTORE_SUCCESS) {
        throw std::runtime_error(
                "CouchKVStore::getRange: docinfos_by_id failed for " +
                vb.to_string() + " - couchstore returned error: " +
                couchstore_strerror(errCode));
    }
}

void CouchKVStore::del(queued_item item) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::del: Not valid on a read-only "
                        "object.");
    }
    if (!inTransaction) {
        throw std::invalid_argument(
                "CouchKVStore::del: inTransaction must be "
                "true to perform a delete operation.");
    }

    pendingReqsQ.emplace_back(std::move(item));
}

void CouchKVStore::delVBucket(Vbid vbucket, uint64_t fileRev) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::delVBucket: Not valid on a "
                        "read-only object.");
    }

    unlinkCouchFile(vbucket, fileRev);
}

std::vector<vbucket_state *> CouchKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void CouchKVStore::getPersistedStats(std::map<std::string,
                                     std::string> &stats) {
    const auto fname = cb::io::sanitizePath(dbname + "/stats.json");
    if (!cb::io::isFile(fname)) {
        return;
    }

    std::string buffer;
    try {
        buffer = cb::io::loadFile(fname);
    } catch (const std::exception& exception) {
        logger.warn(
                "CouchKVStore::getPersistedStats: Failed to load the engine "
                "session stats due to IO exception \"{}\"",
                exception.what());
        return;
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(buffer);
    } catch (const nlohmann::json::exception& exception) {
        logger.warn(
                "CouchKVStore::getPersistedStats:"
                " Failed to parse the session stats json doc!!!: {}",
                exception.what());
        return;
    }

    for (auto it = json.begin(); it != json.end(); ++it) {
        stats[it.key()] = it.value().get<std::string>();
    }
}

static std::string getDBFileName(const std::string& dbname,
                                 Vbid vbid,
                                 uint64_t rev) {
    return dbname + "/" + std::to_string(vbid.get()) + ".couch." +
           std::to_string(rev);
}

/**
 * Notify the expiry callback that a document has expired
 *
 * @param info     document information for the expired item
 * @param metadata metadata of the document
 * @param item     buffer containing data and size
 * @param ctx      context for compaction
 * @param currtime current time
 */
static int notify_expired_item(DocInfo& info,
                               MetaData& metadata,
                               sized_buf item,
                               compaction_ctx& ctx,
                               time_t currtime) {
    sized_buf data{nullptr, 0};
    cb::compression::Buffer inflated;

    if (mcbp::datatype::is_xattr(metadata.getDataType())) {
        if (item.buf == nullptr) {
            // We need to pass on the entire document to the callback
            return COUCHSTORE_COMPACT_NEED_BODY;
        }

        // A document on disk is marked snappy in two ways.
        // 1) info.content_meta if the document was compressed by couchstore
        // 2) datatype snappy if the document was already compressed when stored
        if ((info.content_meta & COUCH_DOC_IS_COMPRESSED) ||
            mcbp::datatype::is_snappy(metadata.getDataType())) {
            using namespace cb::compression;

            if (!inflate(Algorithm::Snappy, {item.buf, item.size}, inflated)) {
                EP_LOG_WARN(
                        "time_purge_hook: failed to inflate document with "
                        "seqno {} revno: {}",
                        info.db_seq,
                        info.rev_seq);
                return COUCHSTORE_ERROR_CORRUPT;
            }
            // Now remove snappy bit
            metadata.setDataType(metadata.getDataType() &
                                 ~PROTOCOL_BINARY_DATATYPE_SNAPPY);
            data = {inflated.data(), inflated.size()};
        }
    }

    auto it = makeItemFromDocInfo(
            ctx.compactConfig.db_file_id, info, metadata, data);

    ctx.expiryCallback->callback(*it, currtime);

    return COUCHSTORE_SUCCESS;
}

static int time_purge_hook(Db* d,
                           DocInfo* info,
                           sized_buf item,
                           compaction_ctx* ctx) {
    if (info == nullptr) {
        // Compaction finished
        return couchstore_set_purge_seq(d, ctx->max_purged_seq);
    }

    if (info->rev_meta.size >= MetaData::getMetaDataSize(MetaData::Version::V0)) {
        auto metadata = MetaDataFactory::createMetaData(info->rev_meta);
        uint32_t exptime = metadata->getExptime();

        if (ctx->droppedKeyCb) {
            // We need to check both committed and prepared documents - if the
            // collection has been logically deleted then we need to discard
            // both types of keys.
            // As such use the docKey (i.e. without any DurabilityPrepare
            // namespace) when checking if logically deleted;
            auto diskKey = makeDiskDocKey(info->id);
            if (ctx->eraserContext->isLogicallyDeleted(diskKey.getDocKey(),
                                                       int64_t(info->db_seq))) {
                // Inform vb that the key@seqno is dropped
                try {
                    ctx->droppedKeyCb(diskKey,
                                      int64_t(info->db_seq),
                                      metadata->isAbort(),
                                      ctx->highCompletedSeqno);
                } catch (const std::exception& e) {
                    EP_LOG_WARN("time_purge_hook: droppedKeyCb exception: {}",
                                e.what());
                    return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
                }
                if (metadata->isPrepare()) {
                    ctx->stats.preparesPurged++;
                } else {
                    if (!info->deleted) {
                        ctx->stats.collectionsItemsPurged++;
                    } else {
                        ctx->stats.collectionsDeletedItemsPurged++;
                    }
                }
                return COUCHSTORE_COMPACT_DROP_ITEM;
            } else if (info->deleted) {
                ctx->eraserContext->processEndOfCollection(
                        diskKey.getDocKey(), SystemEvent(metadata->getFlags()));
            }
        }

        if (info->deleted) {
            const auto infoDb = cb::couchstore::getHeader(*d);
            if (info->db_seq != infoDb.updateSeqNum) {
                if (ctx->compactConfig.drop_deletes) { // all deleted items must
                                                       // be dropped ...
                    if (ctx->max_purged_seq < info->db_seq) {
                        ctx->max_purged_seq =
                                info->db_seq; // track max_purged_seq
                    }
                    ctx->stats.tombstonesPurged++;
                    return COUCHSTORE_COMPACT_DROP_ITEM;      // ...unconditionally
                }

                /**
                 * MB-30015: Found a tombstone whose expiry time is 0. Log this
                 * message because tombstones are expected to have a non-zero
                 * expiry time
                 */
                if (!exptime) {
                    EP_LOG_WARN(
                            "time_purge_hook: tombstone found with an"
                            " expiry time of 0");
                }

                if (exptime < ctx->compactConfig.purge_before_ts &&
                    (exptime || !ctx->compactConfig.retain_erroneous_tombstones) &&
                     (!ctx->compactConfig.purge_before_seq ||
                      info->db_seq <= ctx->compactConfig.purge_before_seq)) {
                    if (ctx->max_purged_seq < info->db_seq) {
                        ctx->max_purged_seq = info->db_seq;
                    }
                    ctx->stats.tombstonesPurged++;
                    return COUCHSTORE_COMPACT_DROP_ITEM;
                }
            }
        } else {
            // We can remove any prepares that have been completed. This works
            // because we send Mutations instead of Commits when streaming from
            // Disk so we do not need to send a Prepare message to keep things
            // consistent on a replica.
            if (metadata->isPrepare()) {
                if (info->db_seq <= ctx->highCompletedSeqno) {
                    ctx->stats.preparesPurged++;
                    return COUCHSTORE_COMPACT_DROP_ITEM;
                }

                // Just keep the item if not complete. We should not attempt to
                // expire a prepare and we do not want to update the bloom
                // filter for a prepare either as that could cause us to run
                // unnecessary BGFetches.
                return COUCHSTORE_COMPACT_KEEP_ITEM;
            }

            time_t currtime = ep_real_time();
            if (exptime && exptime < currtime && metadata->isCommit()) {
                int ret;
                metadata->setDeleteSource(DeleteSource::TTL);
                try {
                    ret = notify_expired_item(*info, *metadata, item,
                                             *ctx, currtime);
                } catch (const std::bad_alloc&) {
                    EP_LOG_WARN("time_purge_hook: memory allocation failed");
                    return COUCHSTORE_ERROR_ALLOC_FAIL;
                } catch (const std::exception& ex) {
                    EP_LOG_WARN("time_purge_hook: exception: {}", ex.what());
                    return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
                }

                if (ret != COUCHSTORE_SUCCESS) {
                    return ret;
                }
            }
        }
    }

    if (ctx->bloomFilterCallback) {
        bool deleted = info->deleted;
        auto key = makeDiskDocKey(info->id);

        try {
            auto vbid{ctx->compactConfig.db_file_id};
            ctx->bloomFilterCallback->callback(vbid, key.getDocKey(), deleted);
        } catch (std::runtime_error& re) {
            EP_LOG_WARN(
                    "time_purge_hook: exception occurred when invoking the "
                    "bloomfilter callback on {}"
                    " - Details: {}",
                    ctx->compactConfig.db_file_id,
                    re.what());
        }
    }

    return COUCHSTORE_COMPACT_KEEP_ITEM;
}

bool CouchKVStore::compactDB(std::shared_ptr<compaction_ctx> hook_ctx) {
    bool result = false;

    try {
        result = compactDBInternal(hook_ctx.get(), {});
    } catch (const std::exception& le) {
        EP_LOG_WARN(
                "CouchKVStore::compactDB: exception while performing "
                "compaction for {}"
                " - Details: {}",
                hook_ctx->compactConfig.db_file_id,
                le.what());
    }
    if (!result) {
        ++st.numCompactionFailure;
    }
    return result;
}

static FileInfo toFileInfo(const cb::couchstore::Header& info) {
    return FileInfo{
            info.docCount, info.deletedCount, info.fileSize, info.purgeSeqNum};
}

bool CouchKVStore::compactDBInternal(
        compaction_ctx* hook_ctx,
        cb::couchstore::CompactRewriteDocInfoCallback docinfo_hook) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::compactDB: Cannot perform "
                        "on a read-only instance.");
    }
    auto* def_iops = statCollectingFileOpsCompaction.get();
    DbHolder compactdb(*this);
    DbHolder targetDb(*this);
    couchstore_error_t         errCode = COUCHSTORE_SUCCESS;
    std::chrono::steady_clock::time_point start =
            std::chrono::steady_clock::now();
    std::string                 dbfile;
    std::string           compact_file;
    std::string               new_file;
    Vbid vbid = hook_ctx->compactConfig.db_file_id;

    TRACE_EVENT1("CouchKVStore", "compactDB", "vbid", vbid.get());

    // Open the source VBucket database file ...
    errCode = openDB(
            vbid, compactdb, (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY, def_iops);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::compactDB openDB error:{}, {}, fileRev:{}",
                    couchstore_strerror(errCode),
                    vbid,
                    compactdb.getFileRev());
        return false;
    }

    hook_ctx->eraserContext = std::make_unique<Collections::VB::EraserContext>(
            getDroppedCollections(*compactdb));

    uint64_t new_rev = compactdb.getFileRev() + 1;

    // Build the temporary vbucket.compact file name
    dbfile = getDBFileName(dbname, vbid, compactdb.getFileRev());
    compact_file = dbfile + ".compact";

    couchstore_open_flags flags(COUCHSTORE_COMPACT_FLAG_UPGRADE_DB);

    hook_ctx->stats.pre =
            toFileInfo(cb::couchstore::getHeader(*compactdb.getDb()));

    /**
     * This flag disables IO buffering in couchstore which means
     * file operations will trigger syscalls immediately. This has
     * a detrimental impact on performance and is only intended
     * for testing.
     */
    if(!configuration.getBuffered()) {
        flags |= COUCHSTORE_OPEN_FLAG_UNBUFFERED;
    }

    // Should automatic fsync() be configured for compaction?
    const auto periodicSyncBytes = configuration.getPeriodicSyncBytes();
    if (periodicSyncBytes != 0) {
        flags |= couchstore_encode_periodic_sync_flags(periodicSyncBytes);
    }

    // It would seem logical to grab the state from disk her (readVBState(...))
    // but we cannot do that. If we do, we may race with a concurrent scan as
    // readVBState will overwrite the cached vbucket_state. As such, just use
    // the cached vbucket_state.
    vbucket_state* vbState = getVBucketState(vbid);
    if (!vbState) {
        EP_LOG_WARN(
                "CouchKVStore::compactDBInternal ({}) Failed to obtain vbState "
                "for the highCompletedSeqno",
                vbid);
        return false;
    }
    hook_ctx->highCompletedSeqno = vbState->persistedCompletedSeqno;

    // Remove the destination file (in the case there is a leftover somewhere
    // IF one exist compact would start writing into the file, but if the
    // new compaction creates a smaller file people using the file may pick
    // up an old and stale file header at the end of the file!)
    // (couchstore don't remove the target file before starting compaction
    // as callers _may_ use the existence of those for locking purposes)
    removeCompactFile(compact_file);

    // Perform COMPACTION of vbucket.couch.rev into
    // vbucket.couch.rev.compact
    if (configuration.isPitrEnabled()) {
        std::chrono::nanoseconds timestamp =
                std::chrono::system_clock::now().time_since_epoch() -
                configuration.getPitrMaxHistoryAge();
        auto delta = std::chrono::duration_cast<std::chrono::nanoseconds>(
                configuration.getPitrGranularity());
        auto seconds =
                std::chrono::duration_cast<std::chrono::seconds>(timestamp);
        auto usec = std::chrono::duration_cast<std::chrono::microseconds>(
                timestamp - seconds);

        EP_LOG_INFO(
                "{}: Full compaction to {}, incremental with granularity of "
                "{} sec",
                vbid.to_string(),
                ISOTime::generatetimestamp(seconds.count(), usec.count()),
                configuration.getPitrGranularity().count());

        errCode = cb::couchstore::compact(
                *compactdb,
                compact_file.c_str(),
                flags,
                [hook_ctx](Db& db, DocInfo* docInfo, sized_buf value) -> int {
                    return time_purge_hook(&db, docInfo, value, hook_ctx);
                },
                std::move(docinfo_hook),
                def_iops,
                timestamp.count(),
                delta.count());
    } else {
        errCode = cb::couchstore::compact(
                *compactdb,
                compact_file.c_str(),
                flags,
                [hook_ctx](Db& db, DocInfo* docInfo, sized_buf value) -> int {
                    return time_purge_hook(&db, docInfo, value, hook_ctx);
                },
                std::move(docinfo_hook),
                def_iops);
    }
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::compactDB: cb::couchstore::compact() "
                "error:{} [{}], name:{}",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(compactdb, errCode),
                dbfile);
        // Remove the compacted file (in the case it was created and
        // partly written)
        removeCompactFile(compact_file);
        return false;
    }

    // Close the source Database File once compaction is done
    compactdb.close();

    // Rename the .compact file to one with the next revision number
    new_file = getDBFileName(dbname, vbid, new_rev);
    if (rename(compact_file.c_str(), new_file.c_str()) != 0) {
        logger.warn("CouchKVStore::compactDB: rename error:{}, old:{}, new:{}",
                    cb_strerror(),
                    compact_file,
                    new_file);

        removeCompactFile(compact_file);
        return false;
    }

    // Open the newly compacted VBucket database file in write mode, we will be
    // updating vbstate
    errCode = openSpecificDB(vbid, new_rev, targetDb, 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::compactDB: openDB#2 error:{}, file:{}, "
                "fileRev:{}",
                couchstore_strerror(errCode),
                new_file,
                targetDb.getFileRev());
        if (remove(new_file.c_str()) != 0) {
            logger.warn("CouchKVStore::compactDB: remove error:{}, path:{}",
                        cb_strerror(),
                        new_file);
        }
        return false;
    }

    PendingLocalDocRequestQueue localDocQueue;
    if (hook_ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        if (!hook_ctx->eraserContext->empty()) {
            std::stringstream ss;
            ss << "CouchKVStore::compactDB finalising dropped collections, "
               << "container should be empty" << *hook_ctx->eraserContext
               << std::endl;
            throw std::logic_error(ss.str());
        }
        // Need to ensure the 'dropped' list on disk is now gone
        localDocQueue.emplace_back(Collections::droppedCollectionsName,
                                   CouchLocalDocRequest::IsDeleted{});
    }

    auto info = cb::couchstore::getHeader(*targetDb.getDb());
    hook_ctx->stats.post = toFileInfo(info);

    cachedFileSize[vbid.get()] = info.fileSize;
    cachedSpaceUsed[vbid.get()] = info.spaceUsed;

    // also update cached state with dbinfo
    vbucket_state* state = getVBucketState(vbid);
    if (state) {
        state->highSeqno = info.updateSeqNum;
        state->purgeSeqno = info.purgeSeqNum;
        cachedDeleteCount[vbid.get()] = info.deletedCount;
        cachedDocCount[vbid.get()] = info.docCount;
        state->onDiskPrepares -= hook_ctx->stats.preparesPurged;
        // Must sync the modified state back
        localDocQueue.emplace_back("_local/vbstate", makeJsonVBState(*state));
    }

    if (!localDocQueue.empty()) {
        updateLocalDocuments(*targetDb.getDb(), localDocQueue);
        errCode = couchstore_commit(targetDb.getDb());
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::compactDB: failed to commit vbstate "
                    "errCode:{}",
                    couchstore_strerror(errCode));
        }
    }

    logger.debug("INFO: created new couch db file, name:{} rev:{}",
                 new_file,
                 new_rev);

    // Make our completion callback before writing the new file. We should
    // update our in memory state before we finalize on disk state so that we
    // don't have to worry about race conditions with things like the purge
    // seqno.
    if (hook_ctx->completionCallback) {
        hook_ctx->completionCallback(*hook_ctx);
    }

    // Update the global VBucket file map so all operations use the new file
    updateDbFileMap(vbid, new_rev);

    // Removing the stale couch file
    unlinkCouchFile(vbid, compactdb.getFileRev());

    st.compactHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

vbucket_state* CouchKVStore::getVBucketState(Vbid vbucketId) {
    return cachedVBStates[vbucketId.get()].get();
}

bool CouchKVStore::writeVBucketState(Vbid vbucketId,
                                     const vbucket_state& vbstate) {
    std::map<Vbid, uint64_t>::iterator mapItr;
    couchstore_error_t errorCode;

    DbHolder db(*this);
    errorCode = openDB(vbucketId, db, (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        logger.warn(
                "CouchKVStore::writeVBucketState: openDB error:{}, "
                "{}, fileRev:{}",
                couchstore_strerror(errorCode),
                vbucketId,
                db.getFileRev());
        return false;
    }

    errorCode = updateLocalDocument(
            *db, "_local/vbstate", makeJsonVBState(vbstate));
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        logger.warn(
                "CouchKVStore:writeVBucketState: updateLocalDocument "
                "error:{}, "
                "{}, fileRev:{}",
                couchstore_strerror(errorCode),
                vbucketId,
                db.getFileRev());
        return false;
    }

    errorCode = couchstore_commit(db);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        logger.warn(
                "CouchKVStore:writeVBucketState: couchstore_commit "
                "error:{} [{}], {}, rev:{}",
                couchstore_strerror(errorCode),
                couchkvstore_strerrno(db, errorCode),
                vbucketId,
                db.getFileRev());
        return false;
    }

    const auto info = cb::couchstore::getHeader(*db.getDb());
    cachedSpaceUsed[vbucketId.get()] = info.spaceUsed;
    cachedFileSize[vbucketId.get()] = info.fileSize;

    return true;
}

bool CouchKVStore::snapshotVBucket(Vbid vbucketId,
                                   const vbucket_state& vbstate) {
    if (isReadOnly()) {
        logger.warn(
                "CouchKVStore::snapshotVBucket: cannot be performed on a "
                "read-only KVStore instance");
        return false;
    }

    auto start = std::chrono::steady_clock::now();

    if (updateCachedVBState(vbucketId, vbstate)) {
        vbucket_state* vbs = getVBucketState(vbucketId);
        if (!writeVBucketState(vbucketId, *vbs)) {
            logger.warn(
                    "CouchKVStore::snapshotVBucket: writeVBucketState failed "
                    "state:{}, {}",
                    VBucket::toString(vbstate.transition.state),
                    vbucketId);
            return false;
        }
    }

    EP_LOG_DEBUG("CouchKVStore::snapshotVBucket: Snapshotted {} state:{}",
                 vbucketId,
                 vbstate);

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

StorageProperties CouchKVStore::getStorageProperties() {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::Yes,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::No,
                         StorageProperties::ByIdScan::Yes);
    return rv;
}

bool CouchKVStore::commit(VB::Commit& commitData) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::commit: Not valid on a read-only "
                        "object.");
    }

    if (inTransaction) {
        if (commit2couchstore(commitData)) {
            inTransaction = false;
            transactionCtx.reset();
        }
    }

    return !inTransaction;
}

bool CouchKVStore::getStat(const char* name, size_t& value)  {
    if (strcmp("failure_compaction", name) == 0) {
        value = st.numCompactionFailure.load();
        return true;
    } else if (strcmp("failure_get", name) == 0) {
        value = st.numGetFailure.load();
        return true;
    } else if (strcmp("io_document_write_bytes", name) == 0) {
        value = st.io_document_write_bytes;
        return true;
    } else if (strcmp("io_flusher_write_bytes", name) == 0) {
        value = st.fsStats.totalBytesWritten;
        return true;
    } else if (strcmp("io_total_read_bytes", name) == 0) {
        value = st.fsStats.totalBytesRead.load() +
                st.fsStatsCompaction.totalBytesRead.load();
        return true;
    } else if (strcmp("io_total_write_bytes", name) == 0) {
        value = st.fsStats.totalBytesWritten.load() +
                st.fsStatsCompaction.totalBytesWritten.load();
        return true;
    } else if (strcmp("io_compaction_read_bytes", name) == 0) {
        value = st.fsStatsCompaction.totalBytesRead;
        return true;
    } else if (strcmp("io_compaction_write_bytes", name) == 0) {
        value = st.fsStatsCompaction.totalBytesWritten;
        return true;
    } else if (strcmp("io_bg_fetch_read_count", name) == 0) {
        value = st.getMultiFsReadCount;
        return true;
    }

    return false;
}

void CouchKVStore::pendingTasks() {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::pendingTasks: Not valid on a "
                        "read-only object.");
    }

    // Swap out the contents of pendingFileDeletions to a temporary - we don't
    // want to hold the lock while performing IO.
    std::queue<std::string> filesToDelete;
    filesToDelete.swap(*pendingFileDeletions.wlock());

    while (!filesToDelete.empty()) {
        std::string filename_str = filesToDelete.front();
        if (remove(filename_str.c_str()) == -1) {
            logger.warn(
                    "CouchKVStore::pendingTasks: "
                    "remove error:{}, file{}",
                    errno,
                    filename_str);
            if (errno != ENOENT) {
                pendingFileDeletions->push(filename_str);
            }
        }
        filesToDelete.pop();
    }
}

std::unique_ptr<BySeqnoScanContext> CouchKVStore::initBySeqnoScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions,
        SnapshotSource source) {
    if (source == SnapshotSource::Historical) {
        throw std::runtime_error(
                "CouchKVStore::initBySeqnoScanContext: historicalSnapshot not "
                "implemented");
    }

    auto handle = makeFileHandle(vbid);

    if (!handle) {
        // makeFileHandle/openDb will of logged details of failure.
        logger.warn(
                "CouchKVStore::initBySeqnoScanContext: makeFileHandle failure "
                "{}",
                vbid.get());
        return nullptr;
    }
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(*handle);
    auto& db = couchKvHandle.getDbHolder();

    const auto info = cb::couchstore::getHeader(*db.getDb());
    uint64_t count = 0;
    auto errorCode = couchstore_changes_count(
            db, startSeqno, std::numeric_limits<uint64_t>::max(), &count);
    if (errorCode != COUCHSTORE_SUCCESS) {
        EP_LOG_WARN(
                "CouchKVStore::initBySeqnoScanContext:Failed to obtain changes "
                "count for {} rev:{} start_seqno:{} error: {}",
                vbid,
                db.getFileRev(),
                startSeqno,
                couchstore_strerror(errorCode));
        return nullptr;
    }

    auto readVbStateResult = readVBState(db, vbid);
    if (readVbStateResult.status != ReadVBStateStatus::Success) {
        EP_LOG_WARN(
                "CouchKVStore::initBySeqnoScanContext:Failed to obtain vbState "
                "for "
                "the highCompletedSeqno");
        return nullptr;
    }

    auto collectionsManifest = getDroppedCollections(*db);

    auto sctx = std::make_unique<BySeqnoScanContext>(std::move(cb),
                                                     std::move(cl),
                                                     vbid,
                                                     std::move(handle),
                                                     startSeqno,
                                                     info.updateSeqNum,
                                                     info.purgeSeqNum,
                                                     options,
                                                     valOptions,
                                                     count,
                                                     readVbStateResult.state,
                                                     collectionsManifest);
    sctx->logger = &logger;
    return sctx;
}

std::unique_ptr<ByIdScanContext> CouchKVStore::initByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        const std::vector<ByIdRange>& ranges,
        DocumentFilter options,
        ValueFilter valOptions) {
    auto handle = makeFileHandle(vbid);

    if (!handle) {
        // makeFileHandle/openDb will have logged details of failure.
        logger.warn(
                "CouchKVStore::initByIdScanContext (byName): makeFileHandle "
                "failure {}",
                vbid.get());
        return {};
    }
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(*handle);
    auto& db = couchKvHandle.getDbHolder();

    auto readVbStateResult = readVBState(db, vbid);
    if (readVbStateResult.status != ReadVBStateStatus::Success) {
        EP_LOG_WARN(
                "CouchKVStore::initByIdScanContext:Failed to obtain vbState "
                "for "
                "the highCompletedSeqno");
        return {};
    }

    const auto info = cb::couchstore::getHeader(*db.getDb());
    auto collectionsManifest = getDroppedCollections(*db);

    auto sctx = std::make_unique<ByIdScanContext>(std::move(cb),
                                                  std::move(cl),
                                                  vbid,
                                                  std::move(handle),
                                                  ranges,
                                                  options,
                                                  valOptions,
                                                  collectionsManifest,
                                                  info.updateSeqNum);
    sctx->logger = &logger;
    return sctx;
}

static couchstore_docinfos_options getDocFilter(const DocumentFilter& filter) {
    switch (filter) {
    case DocumentFilter::ALL_ITEMS:
    case DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS:
        return COUCHSTORE_NO_OPTIONS;
    case DocumentFilter::NO_DELETES:
        return COUCHSTORE_NO_DELETES;
    }

    std::string err("getDocFilter: Illegal document filter!" +
                    std::to_string(static_cast<int>(filter)));
    throw std::runtime_error(err);
}

scan_error_t CouchKVStore::scan(BySeqnoScanContext& ctx) {
    if (ctx.lastReadSeqno == ctx.maxSeqno) {
        return scan_success;
    }

    TRACE_EVENT_START2("CouchKVStore",
                       "scan",
                       "vbid",
                       ctx.vbid.get(),
                       "startSeqno",
                       ctx.startSeqno);

    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(*ctx.handle);
    auto& db = couchKvHandle.getDbHolder();

    uint64_t start = ctx.startSeqno;
    if (ctx.lastReadSeqno != 0) {
        start = ctx.lastReadSeqno + 1;
    }

    couchstore_error_t errorCode;
    errorCode = couchstore_changes_since(db,
                                         start,
                                         getDocFilter(ctx.docFilter),
                                         bySeqnoScanCallback,
                                         static_cast<void*>(&ctx));

    TRACE_EVENT_END1(
            "CouchKVStore", "scan", "lastReadSeqno", ctx.lastReadSeqno);

    if (errorCode != COUCHSTORE_SUCCESS) {
        if (errorCode == COUCHSTORE_ERROR_CANCEL) {
            return scan_again;
        } else {
            logger.warn(
                    "CouchKVStore::scan couchstore_changes_since "
                    "error:{} [{}]",
                    couchstore_strerror(errorCode),
                    couchkvstore_strerrno(db, errorCode));
            return scan_failed;
        }
    }
    return scan_success;
}

scan_error_t CouchKVStore::scan(ByIdScanContext& ctx) {
    TRACE_EVENT_START2("CouchKVStore",
                       "scan by id",
                       "vbid",
                       ctx.vbid.get(),
                       "ranges",
                       uint32_t(ctx.ranges.size()));

    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(*ctx.handle);
    auto& db = couchKvHandle.getDbHolder();

    couchstore_error_t errorCode = COUCHSTORE_SUCCESS;
    for (const auto& range : ctx.ranges) {
        std::array<sized_buf, 2> ids;
        ids[0] = sized_buf{const_cast<char*>(reinterpret_cast<const char*>(
                                   range.startKey.data())),
                           range.startKey.size()};
        ids[1] = sized_buf{const_cast<char*>(reinterpret_cast<const char*>(
                                   range.endKey.data())),
                           range.endKey.size()};

        errorCode = couchstore_docinfos_by_id(db,
                                              ids.data(),
                                              2,
                                              RANGES,
                                              bySeqnoScanCallback,
                                              static_cast<void*>(&ctx));
        if (errorCode != COUCHSTORE_SUCCESS) {
            break;
        }
    }
    TRACE_EVENT_END1(
            "CouchKVStore", "scan by id", "lastReadSeqno", ctx.lastReadSeqno);

    if (errorCode != COUCHSTORE_SUCCESS) {
        if (errorCode == COUCHSTORE_ERROR_CANCEL) {
            return scan_again;
        } else {
            logger.warn(
                    "CouchKVStore::scan couchstore_changes_since "
                    "error:{} [{}]",
                    couchstore_strerror(errorCode),
                    couchkvstore_strerrno(db, errorCode));
            return scan_failed;
        }
    }
    return scan_success;
}

cb::couchstore::Header CouchKVStore::getDbInfo(Vbid vbid) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        return cb::couchstore::getHeader(*db.getDb());
    } else {
        // open failed - map couchstore error code to exception.
        std::errc ec;
        switch (errCode) {
            case COUCHSTORE_ERROR_OPEN_FILE:
                ec = std::errc::no_such_file_or_directory; break;
            default:
                ec = std::errc::io_error; break;
        }
        throw std::system_error(
                std::make_error_code(ec),
                "CouchKVStore::getDbInfo: failed to open database file for " +
                        vbid.to_string() +
                        " rev = " + std::to_string(db.getFileRev()) +
                        " with error:" + couchstore_strerror(errCode));
    }
}

void CouchKVStore::close() {
    inTransaction = false;
}

void CouchKVStore::updateDbFileMap(Vbid vbucketId, uint64_t newFileRev) {
    try {
        (*dbFileRevMap->wlock()).at(vbucketId.get()) = newFileRev;
    } catch (std::out_of_range const&) {
        logger.warn(
                "CouchKVStore::updateDbFileMap: Cannot update db file map "
                "for an invalid vbucket, {}, rev:{}",
                vbucketId,
                newFileRev);
    }
}

uint64_t CouchKVStore::getDbRevision(Vbid vbucketId) {
    return (*dbFileRevMap->rlock())[vbucketId.get()];
}

couchstore_error_t CouchKVStore::openDB(Vbid vbucketId,
                                        DbHolder& db,
                                        couchstore_open_flags options,
                                        FileOpsInterface* ops) {
    // MB-27963: obtain read access whilst we open the file, updateDbFileMap
    // serialises on this mutex so we can be sure the fileRev we read should
    // still be a valid file once we hit sys_open
    auto lockedRevMap = dbFileRevMap->rlock();
    uint64_t fileRev = (*lockedRevMap)[vbucketId.get()];
    return openSpecificDB(vbucketId, fileRev, db, options, ops);
}

couchstore_error_t CouchKVStore::openSpecificDB(Vbid vbucketId,
                                                uint64_t fileRev,
                                                DbHolder& db,
                                                couchstore_open_flags options,
                                                FileOpsInterface* ops) {
    std::string dbFileName = getDBFileName(dbname, vbucketId, fileRev);
    db.setFileRev(fileRev); // save the rev so the caller can log it

    if(ops == nullptr) {
        ops = statCollectingFileOps.get();
    }

    couchstore_error_t errorCode = COUCHSTORE_SUCCESS;

    /**
     * This flag disables IO buffering in couchstore which means
     * file operations will trigger syscalls immediately. This has
     * a detrimental impact on performance and is only intended
     * for testing.
     */
    if(!configuration.getBuffered()) {
        options |= COUCHSTORE_OPEN_FLAG_UNBUFFERED;
    }

    /* get the flags that determine the tracing and validation of
     *  couchstore file operations
     */
    if (configuration.getCouchstoreTracingEnabled()) {
        options |= COUCHSTORE_OPEN_WITH_TRACING;
        if (!(options & COUCHSTORE_OPEN_FLAG_RDONLY)) {
            TRACE_INSTANT2("couchstore_write",
                           "openSpecificDB",
                           "vbucketId",
                           vbucketId.get(),
                           "fileRev",
                           fileRev);
        }
    }
    if (configuration.getCouchstoreWriteValidationEnabled()) {
        options |= COUCHSTORE_OPEN_WITH_WRITE_VALIDATION;
    }
    if (configuration.getCouchstoreMprotectEnabled()) {
        options |= COUCHSTORE_OPEN_WITH_MPROTECT;
    }

    errorCode = couchstore_open_db_ex(
            dbFileName.c_str(), options, ops, db.getDbAddress());

    /* update command statistics */
    st.numOpen++;
    if (errorCode) {
        st.numOpenFailure++;
        logger.warn(
                "CouchKVStore::openDB: error:{} [{}],"
                " name:{}, option:{}, fileRev:{}",
                couchstore_strerror(errorCode),
                cb_strerror(),
                dbFileName,
                options,
                fileRev);

        if (errorCode == COUCHSTORE_ERROR_NO_SUCH_FILE) {
            auto dotPos = dbFileName.find_last_of(".");
            if (dotPos != std::string::npos) {
                dbFileName = dbFileName.substr(0, dotPos);
            }
            auto files = cb::io::findFilesWithPrefix(dbFileName);
            logger.warn(
                    "CouchKVStore::openDB: No such file, found:{} alternative "
                    "files for {}",
                    files.size(),
                    dbFileName);
            for (const auto& f : files) {
                logger.warn("CouchKVStore::openDB: Found {}", f);
            }
        }
    }

    return errorCode;
}

void CouchKVStore::populateFileNameMap(std::vector<std::string>& filenames,
                                       std::vector<Vbid>* vbids) {
    std::vector<std::string>::iterator fileItr;

    for (fileItr = filenames.begin(); fileItr != filenames.end(); ++fileItr) {
        const std::string &filename = *fileItr;
        size_t secondDot = filename.rfind(".");
        std::string nameKey = filename.substr(0, secondDot);
        size_t firstDot = nameKey.rfind(".");
        size_t firstSlash = nameKey.rfind(cb::io::DirectorySeparator);

        std::string revNumStr = filename.substr(secondDot + 1);
        char* ptr = nullptr;
        uint64_t revNum = strtoull(revNumStr.c_str(), &ptr, 10);

        std::string vbIdStr = nameKey.substr(firstSlash + 1,
                                            (firstDot - firstSlash) - 1);
        if (allDigit(vbIdStr)) {
            int vbId = atoi(vbIdStr.c_str());
            if (vbids) {
                vbids->push_back(static_cast<Vbid>(vbId));
            }
            uint64_t old_rev_num = getDbRevision(Vbid(vbId));
            if (old_rev_num == revNum) {
                continue;
            } else if (old_rev_num < revNum) { // stale revision found
                updateDbFileMap(Vbid(vbId), revNum);
            } else { // stale file found (revision id has rolled over)
                old_rev_num = revNum;
            }

            const auto old_file =
                    getDBFileName(dbname, Vbid(uint16_t(vbId)), old_rev_num);
            if (cb::io::isFile(old_file)) {
                if (!isReadOnly()) {
                    if (remove(old_file.c_str()) == 0) {
                        logger.debug(
                                "CouchKVStore::populateFileNameMap: Removed "
                                "stale file:{}",
                                old_file);
                    } else {
                        logger.warn(
                                "CouchKVStore::populateFileNameMap: remove "
                                "error:{}, file:{}",
                                cb_strerror(),
                                old_file);
                    }
                } else {
                    logger.warn(
                            "CouchKVStore::populateFileNameMap: A read-only "
                            "instance of the underlying store "
                            "was not allowed to delete a stale file:{}",
                            old_file);
                }
            }
        } else {
            // skip non-vbucket database file, master.couch etc
            logger.debug(
                    "CouchKVStore::populateFileNameMap: Non-vbucket "
                    "database file, {}, skip adding "
                    "to CouchKVStore dbFileMap",
                    filename);
        }
    }
}

couchstore_error_t CouchKVStore::fetchDoc(Db* db,
                                          DocInfo* docinfo,
                                          GetValue& docValue,
                                          Vbid vbId,
                                          GetMetaOnly metaOnly) {
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;
    std::unique_ptr<MetaData> metadata;
    try {
        metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);
    } catch (std::logic_error&) {
        return COUCHSTORE_ERROR_DB_NO_LONGER_VALID;
    }

    if (metaOnly == GetMetaOnly::Yes) {
        auto it = makeItemFromDocInfo(vbId, *docinfo, *metadata, {nullptr, 0});

        docValue = GetValue(std::move(it));
        // update ep-engine IO stats
        ++st.io_bg_fetch_docs_read;
        st.io_bgfetch_doc_bytes += (docinfo->id.size + docinfo->rev_meta.size);
    } else {
        Doc *doc = nullptr;
        sized_buf value = {nullptr, 0};
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc,
                                                   DECOMPRESS_DOC_BODIES);
        if (errCode == COUCHSTORE_SUCCESS) {
            if (doc == nullptr) {
                throw std::logic_error("CouchKVStore::fetchDoc: doc is NULL");
            }

            if (doc->id.size > UINT16_MAX) {
                throw std::logic_error("CouchKVStore::fetchDoc: "
                            "doc->id.size (which is" +
                            std::to_string(doc->id.size) + ") is greater than "
                            + std::to_string(UINT16_MAX));
            }

            value = doc->data;

            if (metadata->getVersionInitialisedFrom() == MetaData::Version::V0) {
                throw std::runtime_error(
                        "CouchKVStore::fetchDoc: Encountered a document "
                        "with MetaData::Version::V0 generated by a 2.x version "
                        "Couchbase. " +
                        vbId.to_string() +
                        " seqno:" + std::to_string(docinfo->rev_seq));
            }
        } else if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND && docinfo->deleted) {
            // NOT_FOUND is expected for deleted documents, continue.
        } else {
            return errCode;
        }

        try {
            auto it = makeItemFromDocInfo(vbId, *docinfo, *metadata, value);
            docValue = GetValue(std::move(it));
        } catch (std::bad_alloc&) {
            couchstore_free_document(doc);
            return COUCHSTORE_ERROR_ALLOC_FAIL;
        }

        // update ep-engine IO stats
        ++st.io_bg_fetch_docs_read;
        st.io_bgfetch_doc_bytes +=
                (docinfo->id.size + docinfo->rev_meta.size + value.size);

        couchstore_free_document(doc);
    }
    return COUCHSTORE_SUCCESS;
}

/**
 * The bySeqnoScanCallback is the method provided to couchstore_changes_since
 * and couchstore_docinfos_by_id.
 *
 * @param db The database handle which the docinfo came from
 * @param docinfo Pointer to the document info
 * @param ctx A pointer to the ScanContext
 * @return See couch_db.h for the legal values
 */
static int bySeqnoScanCallback(Db* db, DocInfo* docinfo, void* ctx) {
    auto* sctx = static_cast<ScanContext*>(ctx);
    auto& cb = sctx->getValueCallback();
    auto& cl = sctx->getCacheCallback();

    Doc *doc = nullptr;
    sized_buf value{nullptr, 0};
    uint64_t byseqno = docinfo->db_seq;
    Vbid vbucketId = sctx->vbid;

    sized_buf key = docinfo->id;
    if (key.size > UINT16_MAX) {
        throw std::invalid_argument(
                "bySeqnoScanCallback: "
                "docinfo->id.size (which is " +
                std::to_string(key.size) + ") is greater than " +
                std::to_string(UINT16_MAX));
    }

    auto diskKey = makeDiskDocKey(docinfo->id);

    // Determine if the key is logically deleted, if it is we skip the key
    // Note that system event keys (like create scope) are never skipped here
    auto docKey = diskKey.getDocKey();
    if (!docKey.isInSystemCollection()) {
        if (sctx->docFilter !=
            DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
            if (sctx->collectionsContext.isLogicallyDeleted(docKey, byseqno)) {
                sctx->lastReadSeqno = byseqno;
                return COUCHSTORE_SUCCESS;
            }
        }

        CacheLookup lookup(diskKey, byseqno, vbucketId);

        cl.callback(lookup);
        if (cl.getStatus() == ENGINE_KEY_EEXISTS) {
            sctx->lastReadSeqno = byseqno;
            return COUCHSTORE_SUCCESS;
        } else if (cl.getStatus() == ENGINE_ENOMEM) {
            return COUCHSTORE_ERROR_CANCEL;
        }
    }

    auto metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);

    const bool keysOnly = sctx->valFilter == ValueFilter::KEYS_ONLY;
    if (!keysOnly) {
        couchstore_open_options openOptions = 0;

        /**
         * If the stored document has V0 metdata (no datatype)
         * or no special request is made to retrieve compressed documents
         * as is, then DECOMPRESS the document and update datatype
         */
        if (docinfo->rev_meta.size == metadata->getMetaDataSize(MetaData::Version::V0) ||
            sctx->valFilter == ValueFilter::VALUES_DECOMPRESSED) {
            openOptions = DECOMPRESS_DOC_BODIES;
        }

        auto errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc,
                                                        openOptions);

        if (errCode == COUCHSTORE_SUCCESS) {
            value = doc->data;
            if (doc->data.size) {
                if ((openOptions & DECOMPRESS_DOC_BODIES) == 0) {
                    // We always store the document bodies compressed on disk,
                    // but now the client _wanted_ to fetch the document
                    // in a compressed mode.
                    // We've never stored the "compressed" flag on disk
                    // (as we don't keep items compressed in memory).
                    // Update the datatype flag for this item to
                    // reflect that it is compressed so that the
                    // receiver of the object may notice (Note:
                    // this is currently _ONLY_ happening via DCP
                     auto datatype = metadata->getDataType();
                     metadata->setDataType(datatype | PROTOCOL_BINARY_DATATYPE_SNAPPY);
                } else if (metadata->getVersionInitialisedFrom() == MetaData::Version::V0) {
                    throw std::runtime_error(
                            "bySeqnoScanCallback: Encountered a "
                            "document with MetaData::Version::V0 generated by "
                            "a 2.x version Couchbase. " +
                            vbucketId.to_string() +
                            " seqno:" + std::to_string(docinfo->rev_seq));
                }
            } else {
                // No data, it cannot have a datatype!
                metadata->setDataType(PROTOCOL_BINARY_RAW_BYTES);
            }
        } else if (errCode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            sctx->logger->log(spdlog::level::level_enum::warn,
                              "bySeqnoScanCallback: "
                              "couchstore_open_doc_with_docinfo error:{} [{}], "
                              "{}, seqno:{}",
                              couchstore_strerror(errCode),
                              couchkvstore_strerrno(db, errCode),
                              vbucketId,
                              docinfo->rev_seq);
            return COUCHSTORE_SUCCESS;
        }
    }

    auto it = makeItemFromDocInfo(vbucketId, *docinfo, *metadata, value);
    GetValue rv(std::move(it), ENGINE_SUCCESS, -1, keysOnly);
    cb.callback(rv);

    couchstore_free_document(doc);

    if (cb.getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    sctx->lastReadSeqno = byseqno;
    return COUCHSTORE_SUCCESS;
}

bool CouchKVStore::commit2couchstore(VB::Commit& commitData) {
    bool success = true;

    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return success;
    }

    auto vbucket2flush = transactionCtx->vbid;

    TRACE_EVENT2("CouchKVStore",
                 "commit2couchstore",
                 "vbid",
                 vbucket2flush.get(),
                 "pendingCommitCnt",
                 pendingCommitCnt);

    std::vector<Doc*> docs(pendingCommitCnt);
    std::vector<DocInfo*> docinfos(pendingCommitCnt);

    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        auto& req = pendingReqsQ[i];
        docs[i] = req.getDbDoc();
        docinfos[i] = req.getDbDocInfo();
    }

    kvstats_ctx kvctx(commitData);
    // flush all
    couchstore_error_t errCode = saveDocs(vbucket2flush, docs, docinfos, kvctx);

    if (errCode) {
        success = false;
        logger.warn(
                "CouchKVStore::commit2couchstore: saveDocs error:{}, "
                "{}",
                couchstore_strerror(errCode),
                vbucket2flush);
    }

    if (postFlushHook) {
        postFlushHook();
    }

    commitCallback(pendingReqsQ, kvctx, errCode);

    pendingReqsQ.clear();
    return success;
}

// Callback when the btree is updated which we use for tracking create/update
// type statistics.
static void saveDocsCallback(const DocInfo* oldInfo,
                             const DocInfo* newInfo,
                             void* context) {
    auto* cbCtx = static_cast<kvstats_ctx*>(context);

    if (!newInfo) {
        // Should this even happen?
        return;
    }
    auto newKey = makeDiskDocKey(newInfo->id);

    // Update keyWasOnDisk for overall (not per-collection) stats
    if (oldInfo) {
        // Replacing a document
        if (!oldInfo->deleted) {
            cbCtx->keyWasOnDisk.insert(newKey);
        }
    }

    enum class DocMutationType { Insert, Update, Delete };
    DocMutationType onDiskMutationType = DocMutationType::Update;
    if (oldInfo) {
        if (!oldInfo->deleted) {
            if (newInfo->deleted) {
                // New is deleted, so decrement count
                onDiskMutationType = DocMutationType::Delete;
            }
        } else if (!newInfo->deleted) {
            // Adding an item
            onDiskMutationType = DocMutationType::Insert;
        }
    } else if (!newInfo->deleted) {
        // Adding an item
        onDiskMutationType = DocMutationType::Insert;
    }

    auto docKey = newKey.getDocKey();
    switch (onDiskMutationType) {
    case DocMutationType::Delete:
        if (newKey.isCommitted()) {
            cbCtx->commitData.collections.decrementDiskCount(docKey);
        } else {
            cbCtx->onDiskPrepareDelta--;
        }
        break;
    case DocMutationType::Insert:
        if (newKey.isCommitted()) {
            cbCtx->commitData.collections.incrementDiskCount(docKey);
        } else {
            cbCtx->onDiskPrepareDelta++;
        }
        break;
    case DocMutationType::Update:
        break;
    }

    // Do not need to update high seqno if we are calling this for a prepare and
    // it will error if we do so return early.
    if (!newKey.isCommitted()) {
        return;
    }

    // Set the highest seqno that we are persisting regardless of if it
    // is a mutation or deletion
    cbCtx->commitData.collections.setPersistedHighSeqno(
            docKey, newInfo->db_seq, newInfo->deleted);

    size_t oldSize = oldInfo ? oldInfo->physical_size : 0;
    size_t newSize = newInfo ? newInfo->physical_size : 0;

    ssize_t delta = newSize - oldSize;

    cbCtx->commitData.collections.updateDiskSize(docKey, delta);
}

/**
 * Returns the logical size of the data within the given Doc - i.e. the
 * "useful" data ep-engine is writing to disk for this document.
 * Used for Write Amplification calculation.
 */
static size_t calcLogicalDataSize(const Doc* doc, const DocInfo& info) {
    // key len + revision metadata (expiry, flags, etc) + value len (if not
    // deleted).
    return info.id.size + info.rev_meta.size + (doc ? doc->data.size : 0);
}

couchstore_error_t CouchKVStore::saveDocs(Vbid vbid,
                                          const std::vector<Doc*>& docs,
                                          std::vector<DocInfo*>& docinfos,
                                          kvstats_ctx& kvctx) {
    couchstore_error_t errCode;
    DbHolder db(*this);
    errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_CREATE);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::saveDocs: openDB error:{}, {}, rev:{}, "
                "numdocs:{}",
                couchstore_strerror(errCode),
                vbid,
                db.getFileRev(),
                uint64_t(docs.size()));
        return errCode;
    } else {
        uint64_t maxDBSeqno = 0;

        // Count of logical bytes written (key + ep-engine meta + value),
        // used to calculated Write Amplification.
        size_t docsLogicalBytes = 0;

        // Only do a couchstore_save_documents if there are docs
        if (!docs.empty()) {
            for (size_t idx = 0; idx < docs.size(); idx++) {
                maxDBSeqno = std::max(maxDBSeqno, docinfos[idx]->db_seq);

                // Accumulate the size of the useful data in this docinfo.
                docsLogicalBytes +=
                        calcLogicalDataSize(docs[idx], *docinfos[idx]);
            }

            auto cs_begin = std::chrono::steady_clock::now();

            uint64_t flags = COMPRESS_DOC_BODIES | COUCHSTORE_SEQUENCE_AS_IS;
            errCode = couchstore_save_documents_and_callback(
                    db,
                    docs.data(),
                    docinfos.data(),
                    (unsigned)docs.size(),
                    flags,
                    &saveDocsCallback,
                    &kvctx);

            st.saveDocsHisto.add(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - cs_begin));
            if (errCode != COUCHSTORE_SUCCESS) {
                logger.warn(
                        "CouchKVStore::saveDocs: couchstore_save_documents "
                        "error:{} [{}], {}, numdocs:{}",
                        couchstore_strerror(errCode),
                        couchkvstore_strerrno(db, errCode),
                        vbid,
                        uint64_t(docs.size()));
                return errCode;
            }
        }

        kvctx.commitData.collections.saveCollectionStats(
                std::bind(&CouchKVStore::saveCollectionStats,
                          this,
                          std::ref(*db),
                          std::placeholders::_1,
                          std::placeholders::_2));

        vbucket_state& state = kvctx.commitData.proposedVBState;
        state.onDiskPrepares += kvctx.onDiskPrepareDelta;
        pendingLocalReqsQ.emplace_back("_local/vbstate",
                                       makeJsonVBState(state));

        if (collectionsMeta.needsCommit) {
            updateCollectionsMeta(*db, kvctx.commitData.collections);
        }

        /// Update the local documents before we commit
        errCode = updateLocalDocuments(*db, pendingLocalReqsQ);
        if (errCode) {
            logger.warn(
                    "CouchKVStore::saveDocs: updateLocalDocuments size:{} "
                    "error:{} [{}]",
                    pendingLocalReqsQ.size(),
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode));
        }
        pendingLocalReqsQ.clear();

        auto cs_begin = std::chrono::steady_clock::now();

        errCode = couchstore_commit(db);
        st.commitHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - cs_begin));
        if (errCode) {
            logger.warn(
                    "CouchKVStore::saveDocs: couchstore_commit error:{} [{}]",
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode));
            return errCode;
        }

        st.batchSize.add(docs.size());

        // If available, record the write amplification we did for this commit -
        // i.e. for each byte of user data (key+value+meta) how many overhead
        // bytes were written.
        auto* stats = couchstore_get_db_filestats(db);
        if (stats != nullptr && docsLogicalBytes) {
            const auto writeBytes = stats->getWriteBytes();
            uint64_t writeAmp = (writeBytes * 10) / docsLogicalBytes;
            st.flusherWriteAmplificationHisto.addValue(writeAmp);
        }

        // retrieve storage system stats for file fragmentation computation
        const auto info = cb::couchstore::getHeader(*db.getDb());
        cachedSpaceUsed[vbid.get()] = info.spaceUsed;
        cachedFileSize[vbid.get()] = info.fileSize;
        cachedDeleteCount[vbid.get()] = info.deletedCount;
        cachedDocCount[vbid.get()] = info.docCount;

        // Check seqno if we wrote documents
        if (!docs.empty() && maxDBSeqno != info.updateSeqNum) {
            logger.warn(
                    "CouchKVStore::saveDocs: Seqno in db header ({})"
                    " is not matched with what was persisted ({})"
                    " for {}",
                    info.updateSeqNum,
                    maxDBSeqno,
                    vbid);
        }
        state.highSeqno = info.updateSeqNum;
    }

    /* update stat */
    if(errCode == COUCHSTORE_SUCCESS) {
        st.docsCommitted = docs.size();
    }

    return errCode;
}

void CouchKVStore::commitCallback(PendingRequestQueue& committedReqs,
                                  kvstats_ctx& kvctx,
                                  couchstore_error_t errCode) {
    const auto flushSuccess = (errCode == COUCHSTORE_SUCCESS);
    for (auto& committed : committedReqs) {
        const auto docLogicalSize = calcLogicalDataSize(
                committed.getDbDoc(), *committed.getDbDocInfo());
        ++st.io_num_write;
        st.io_document_write_bytes += docLogicalSize;

        const auto& key = committed.getKey();
        if (committed.isDelete()) {
            FlushStateDeletion state;
            if (flushSuccess) {
                if (kvctx.keyWasOnDisk.find(key) != kvctx.keyWasOnDisk.end()) {
                    // Deletion is for an existing item on disk
                    state = FlushStateDeletion::Delete;
                } else {
                    // Deletion is for a non-existing item on disk
                    state = FlushStateDeletion::DocNotFound;
                }
                st.delTimeHisto.add(committed.getDelta());
            } else {
                state = FlushStateDeletion::Failed;
                ++st.numDelFailure;
            }

            transactionCtx->deleteCallback(committed.getItem(), state);
        } else {
            FlushStateMutation state;
            if (flushSuccess) {
                if (kvctx.keyWasOnDisk.find(key) != kvctx.keyWasOnDisk.end()) {
                    // Mutation is for an existing item on disk
                    state = FlushStateMutation::Update;
                } else {
                    // Mutation is for a non-existing item on disk
                    state = FlushStateMutation::Insert;
                }
                st.writeTimeHisto.add(committed.getDelta());
                st.writeSizeHisto.add(docLogicalSize);
            } else {
                state = FlushStateMutation::Failed;
                ++st.numSetFailure;
            }

            transactionCtx->setCallback(committed.getItem(), state);
        }
    }
}

std::tuple<CouchKVStore::ReadVBStateStatus, uint64_t, uint64_t>
CouchKVStore::processVbstateSnapshot(Vbid vb,
                                     vbucket_state_t state,
                                     int64_t version,
                                     uint64_t snapStart,
                                     uint64_t snapEnd,
                                     uint64_t highSeqno) {
    ReadVBStateStatus status = ReadVBStateStatus::Success;

    // All upgrade paths we now expect start and end
    if (!(highSeqno >= snapStart && highSeqno <= snapEnd)) {
        // very likely MB-34173, log this occurrence.
        // log the state, range and version
        logger.warn(
                "CouchKVStore::processVbstateSnapshot {} {} with invalid "
                "snapshot range. Found version:{}, highSeqno:{}, start:{}, "
                "end:{}",
                vb,
                VBucket::toString(state),
                version,
                highSeqno,
                snapStart,
                snapEnd);

        if (state == vbucket_state_active) {
            // Reset the snapshot range to match what the flusher would
            // normally set, that is start and end equal the high-seqno
            snapStart = snapEnd = highSeqno;
        } else {
            // Flag that the VB is corrupt, it needs rebuilding
            status = ReadVBStateStatus::CorruptSnapshot;
            snapStart = 0, snapEnd = 0;
        }
    }

    return {status, snapStart, snapEnd};
}

CouchKVStore::ReadVBStateResult CouchKVStore::readVBState(Db* db, Vbid vbId) {
    sized_buf id;
    LocalDoc *ldoc = nullptr;
    ReadVBStateStatus status = ReadVBStateStatus::Success;
    // High sequence number and purge sequence number are stored automatically
    // by couchstore.
    int64_t highSeqno = 0;
    uint64_t purgeSeqno = 0;
    snapshot_info_t snapshot{0, {0, 0}};
    const auto info = cb::couchstore::getHeader(*db);
    highSeqno = info.updateSeqNum;
    purgeSeqno = info.purgeSeqNum;

    vbucket_state vbState;

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    auto couchStoreStatus =
            couchstore_open_local_document(db, (void*)id.buf, id.size, &ldoc);

    if (couchStoreStatus == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        logger.warn(
                "CouchKVStore::readVBState: '_local/vbstate' not found "
                "for {}",
                vbId);
    } else if (couchStoreStatus != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::readVBState: couchstore_open_local_document"
                " error:{}, {}",
                couchstore_strerror(couchStoreStatus),
                vbId);
        return {ReadVBStateStatus::CouchstoreError, {}};
    }

    // Proceed to read/parse the vbstate if success
    if (couchStoreStatus == COUCHSTORE_SUCCESS) {
        const std::string statjson(ldoc->json.buf, ldoc->json.size);

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(statjson);
        } catch (const nlohmann::json::exception& e) {
            couchstore_free_local_document(ldoc);
            logger.warn(
                    "CouchKVStore::readVBState: Failed to "
                    "parse the vbstat json doc for {}, json:{}, with "
                    "reason:{}",
                    vbId,
                    statjson,
                    e.what());
            return {ReadVBStateStatus::JsonInvalid, {}};
        }

        // Merge in the high_seqno & purge_seqno read previously from db info.
        json["high_seqno"] = std::to_string(highSeqno);
        json["purge_seqno"] = std::to_string(purgeSeqno);

        try {
            vbState = json;
        } catch (const nlohmann::json::exception& e) {
            couchstore_free_local_document(ldoc);
            logger.warn(
                    "CouchKVStore::readVBState: Failed to "
                    "convert the vbstat json doc for {} to vbState, json:{}, "
                    "with "
                    "reason:{}",
                    vbId,
                    json.dump(),
                    e.what());
            return {ReadVBStateStatus::JsonInvalid, {}};
        }

        // MB-17517: If the maxCas on disk was invalid then don't use it -
        // instead rebuild from the items we load from disk (i.e. as per
        // an upgrade from an earlier version).
        if (vbState.maxCas == static_cast<uint64_t>(-1)) {
            logger.warn(
                    "CouchKVStore::readVBState: Invalid max_cas "
                    "({:#x}) read from '{}' for {}. Resetting "
                    "max_cas to zero.",
                    vbState.maxCas,
                    id.buf,
                    vbId);
            vbState.maxCas = 0;
        }

        std::tie(status, vbState.lastSnapStart, vbState.lastSnapEnd) =
                processVbstateSnapshot(vbId,
                                       vbState.transition.state,
                                       vbState.version,
                                       vbState.lastSnapStart,
                                       vbState.lastSnapEnd,
                                       uint64_t(highSeqno));

        couchstore_free_local_document(ldoc);
    }

    return {status, vbState};
}

CouchKVStore::ReadVBStateResult CouchKVStore::readVBStateAndUpdateCache(
        Db* db, Vbid vbid) {
    auto res = readVBState(db, vbid);
    if (res.status == ReadVBStateStatus::Success) {
        cachedVBStates[vbid.get()] = std::make_unique<vbucket_state>(res.state);
    }
    return res;
}

std::string CouchKVStore::makeJsonVBState(const vbucket_state& vbState) {
    nlohmann::json j = vbState;
    // Strip out the high_seqno and purge_seqno - they are automatically
    // tracked by couchstore so unnecessary (and potentially confusing) to
    // store in vbstate document.
    j.erase("high_seqno");
    j.erase("purge_seqno");
    return j.dump();
}

void CouchKVStore::saveCollectionStats(Db& db,
                                       CollectionID cid,
                                       Collections::VB::PersistedStats stats) {
    // Write out the stats in BE to a local doc named after the collection
    // Using set-notation cardinality - |cid| which helps keep the keys small
    pendingLocalReqsQ.emplace_back("|" + cid.to_string() + "|",
                                   stats.getLebEncodedStats());
}

void CouchKVStore::deleteCollectionStats(CollectionID cid) {
    pendingLocalReqsQ.emplace_back("|" + cid.to_string() + "|",
                                   CouchLocalDocRequest::IsDeleted{});
}

std::optional<Collections::VB::PersistedStats> CouchKVStore::getCollectionStats(
        const KVFileHandle& kvFileHandle, CollectionID collection) {
    std::string docName = "|" + collection.to_string() + "|";

    const auto& db = static_cast<const CouchKVFileHandle&>(kvFileHandle);
    sized_buf id;
    id.buf = const_cast<char*>(docName.c_str());
    id.size = docName.size();

    LocalDocHolder lDoc;
    auto errCode = couchstore_open_local_document(
            db.getDb(), (void*)id.buf, id.size, lDoc.getLocalDocAddress());

    if (errCode != COUCHSTORE_SUCCESS) {
        // Could be a deleted collection, so not found not an issue
        if (errCode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.warn(
                    "CouchKVStore::getCollectionStats cid:{}"
                    "couchstore_open_local_document error:{}",
                    collection.to_string(),
                    couchstore_strerror(errCode));
        }

        return {};
    }

    return Collections::VB::PersistedStats(lDoc.getLocalDoc()->json.buf,
                                           lDoc.getLocalDoc()->json.size);
}

static int getMultiCallback(Db* db, DocInfo* docinfo, void* ctx) {
    if (docinfo == nullptr) {
        throw std::invalid_argument(
                "getMultiCallback: docinfo "
                "must be non-NULL");
    }
    if (ctx == nullptr) {
        throw std::invalid_argument(
                "getMultiCallback: ctx must "
                "be non-NULL");
    }

    auto *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    auto key = makeDiskDocKey(docinfo->id);
    KVStoreStats& st = cbCtx->cks.getKVStoreStat();

    auto qitr = cbCtx->fetches.find(key);
    if (qitr == cbCtx->fetches.end()) {
        // this could be a serious race condition in couchstore,
        // log a warning message and continue
        cbCtx->cks.getLogger().warn(
                "getMultiCallback: Couchstore returned invalid docinfo, no "
                "pending bgfetch has been issued for a key in {}, seqno:{}",
                cbCtx->vbId,
                docinfo->rev_seq);
        return 0;
    }

    vb_bgfetch_item_ctx_t& bg_itm_ctx = (*qitr).second;
    GetMetaOnly meta_only = bg_itm_ctx.isMetaOnly;

    couchstore_error_t errCode = cbCtx->cks.fetchDoc(
            db, docinfo, bg_itm_ctx.value, cbCtx->vbId, meta_only);
    if (errCode != COUCHSTORE_SUCCESS && (meta_only == GetMetaOnly::No)) {
        st.numGetFailure++;
    }

    bg_itm_ctx.value.setStatus(cbCtx->cks.couchErr2EngineErr(errCode));

    bool return_val_ownership_transferred = false;
    for (auto& fetch : bg_itm_ctx.bgfetched_list) {
        return_val_ownership_transferred = true;
        // populate return value for remaining fetch items with the
        // same seqid
        fetch->value = &bg_itm_ctx.value;
        st.readTimeHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - fetch->initTime));
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(bg_itm_ctx.value.item->getKey().size() +
                                 bg_itm_ctx.value.item->getNBytes());
        }
    }
    if (!return_val_ownership_transferred) {
        cbCtx->cks.getLogger().warn(
                "getMultiCallback called with zero items in bgfetched_list, "
                "{}, seqno:{}",
                cbCtx->vbId,
                docinfo->rev_seq);
    }

    return 0;
}

void CouchKVStore::closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_file(db);
    if (ret != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::closeDatabaseHandle: couchstore_close_file "
                "error:{} [{}]",
                couchstore_strerror(ret),
                couchkvstore_strerrno(db, ret));
    }
    ret = couchstore_free_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::closeDatabaseHandle: couchstore_free_db "
                "error:{} [{}]",
                couchstore_strerror(ret),
                couchkvstore_strerrno(nullptr, ret));
    }
    st.numClose++;
}

ENGINE_ERROR_CODE CouchKVStore::couchErr2EngineErr(couchstore_error_t errCode) {
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return ENGINE_SUCCESS;
    case COUCHSTORE_ERROR_ALLOC_FAIL:
        return ENGINE_ENOMEM;
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        return ENGINE_KEY_ENOENT;
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
    case COUCHSTORE_ERROR_NO_HEADER:
    default:
        // same as the general error return code of
        // EPBucket::getInternal
        return ENGINE_TMPFAIL;
    }
}

size_t CouchKVStore::getNumPersistedDeletes(Vbid vbid) {
    size_t delCount = cachedDeleteCount[vbid.get()];
    if (delCount != (size_t) -1) {
        return delCount;
    }

    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        const auto info = cb::couchstore::getHeader(*db.getDb());
        cachedDeleteCount[vbid.get()] = info.deletedCount;
        return info.deletedCount;
    } else {
        // open failed - map couchstore error code to exception.
        std::errc ec;
        switch (errCode) {
            case COUCHSTORE_ERROR_OPEN_FILE:
                ec = std::errc::no_such_file_or_directory; break;
            default:
                ec = std::errc::io_error; break;
        }
        throw std::system_error(
                std::make_error_code(ec),
                "CouchKVStore::getNumPersistedDeletes:"
                "Failed to open database file for " +
                        vbid.to_string() +
                        " rev = " + std::to_string(db.getFileRev()) +
                        " with error:" + couchstore_strerror(errCode));
    }
    return 0;
}

DBFileInfo CouchKVStore::getDbFileInfo(Vbid vbid) {
    const auto info = getDbInfo(vbid);
    return DBFileInfo{info.fileSize, info.spaceUsed};
}

DBFileInfo CouchKVStore::getAggrDbFileInfo() {
    DBFileInfo kvsFileInfo;
    /**
     * Iterate over all the vbuckets to get the total.
     * If the vbucket is dead, then its value would
     * be zero.
     */
    for (uint16_t vbid = 0; vbid < numDbFiles; vbid++) {
        kvsFileInfo.fileSize += cachedFileSize[vbid].load();
        kvsFileInfo.spaceUsed += cachedSpaceUsed[vbid].load();
    }
    return kvsFileInfo;
}

size_t CouchKVStore::getItemCount(Vbid vbid) {
    if (!isReadOnly()) {
        return cachedDocCount.at(vbid.get());
    }
    return getDbInfo(vbid).docCount;
}

RollbackResult CouchKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::unique_ptr<RollbackCB> cb) {
    DbHolder db(*this);

    // Open the vbucket's file and determine the latestSeqno persisted.
    auto errCode = openDB(vbid, db, (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY);
    const auto dbFileName = getDBFileName(dbname, vbid, db.getFileRev());

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::rollback: openDB error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName);
        return RollbackResult(false);
    }

    auto info = cb::couchstore::getHeader(*db.getDb());
    uint64_t latestSeqno = info.updateSeqNum;

    // Count how many updates are in the vbucket's file. We'll later compare
    // this with how many items must be discarded and hence decide if it is
    // better to discard everything and start from an empty vBucket.
    uint64_t totSeqCount = 0;
    errCode = couchstore_changes_count(db, 0, latestSeqno, &totSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::rollback: "
                "couchstore_changes_count(0, {}) error:{} [{}], "
                "{}, rev:{}",
                latestSeqno,
                couchstore_strerror(errCode),
                cb_strerror(),
                vbid,
                db.getFileRev());
        return RollbackResult(false);
    }

    // Open the vBucket file again; and search for a header which is
    // before the requested rollback point - the Rollback Header.
    auto newdb = std::make_unique<CouchKVFileHandle>(*this);
    errCode = openDB(vbid, newdb->getDbHolder(), 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::rollback: openDB#2 error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName);
        return RollbackResult(false);
    }

    while (info.updateSeqNum > rollbackSeqno) {
        errCode = couchstore_rewind_db_header(newdb->getDbHolder());
        if (errCode != COUCHSTORE_SUCCESS) {
            // rewind_db_header cleans up (frees DB) on error; so
            // release db in DbHolder to prevent a double-free.
            newdb->getDbHolder().releaseDb();
            logger.warn(
                    "CouchKVStore::rollback: couchstore_rewind_db_header "
                    "error:{} [{}], {}, latestSeqno:{}, rollbackSeqno:{}",
                    couchstore_strerror(errCode),
                    cb_strerror(),
                    vbid,
                    latestSeqno,
                    rollbackSeqno);
            //Reset the vbucket and send the entire snapshot,
            //as a previous header wasn't found.
            return RollbackResult(false);
        }
        info = cb::couchstore::getHeader(*newdb->getDbHolder().getDb());
    }

    // Count how many updates we need to discard to rollback to the Rollback
    // Header. If this is too many; then prefer to discard everything (than
    // have to patch up a large amount of in-memory data).
    uint64_t rollbackSeqCount = 0;
    errCode = couchstore_changes_count(
            db, info.updateSeqNum, latestSeqno, &rollbackSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::rollback: "
                "couchstore_changes_count#2({}, {}) "
                "error:{} [{}], {}, rev:{}",
                info.updateSeqNum,
                latestSeqno,
                couchstore_strerror(errCode),
                cb_strerror(),
                vbid,
                db.getFileRev());
        return RollbackResult(false);
    }

    // Allow rollbacks when we have fewer than 10 items even if the requested
    // rollback point is in the first half of our seqnos. This allows much
    // easier testing as we do not have to write x items before the majority of
    // tests to verify certain rollback behaviours.
    if (totSeqCount > 10 && (totSeqCount / 2) <= rollbackSeqCount) {
        //doresetVbucket flag set or rollback is greater than 50%,
        //reset the vbucket and send the entire snapshot
        return RollbackResult(false);
    }

    // We have decided to perform a rollback to the Rollback Header.
    // Iterate across the series of keys which have been updated /since/ the
    // Rollback Header; invoking a callback on each. This allows the caller to
    // then inspect the state of the given key in the Rollback Header, and
    // correct the in-memory view:
    // * If the key is not present in the Rollback header then delete it from
    //   the HashTable (if either didn't exist yet, or had previously been
    //   deleted in the Rollback header).
    // * If the key is present in the Rollback header then replace the in-memory
    // value with the value from the Rollback header.
    cb->setKVFileHandle(std::move(newdb));
    auto cl = std::make_unique<NoLookupCallback>();

    auto ctx = initBySeqnoScanContext(std::move(cb),
                                      std::move(cl),
                                      vbid,
                                      info.updateSeqNum + 1,
                                      DocumentFilter::ALL_ITEMS,
                                      ValueFilter::KEYS_ONLY,
                                      SnapshotSource::Head);
    if (!ctx) {
        return RollbackResult(false);
    }

    scan_error_t error = scan(*ctx);

    if (error != scan_success) {
        return RollbackResult(false);
    }

    // The RollbackCB owns the file handle and the ScanContext owns the callback
    const auto& rollbackCb =
            static_cast<const RollbackCB&>(ctx->getValueCallback());
    auto* handle = const_cast<CouchKVFileHandle*>(
            static_cast<const CouchKVFileHandle*>(
                    rollbackCb.getKVFileHandle()));
    if (readVBStateAndUpdateCache(handle->getDbHolder(), vbid).status !=
        ReadVBStateStatus::Success) {
        return RollbackResult(false);
    }
    cachedDeleteCount[vbid.get()] = info.deletedCount;
    cachedDocCount[vbid.get()] = info.docCount;

    // Append the rewinded header to the database file
    errCode = couchstore_commit(handle->getDbHolder());

    if (errCode != COUCHSTORE_SUCCESS) {
        return RollbackResult(false);
    }

    vbucket_state* vb_state = getVBucketState(vbid);
    return RollbackResult(true,
                          vb_state->highSeqno,
                          vb_state->lastSnapStart,
                          vb_state->lastSnapEnd);
}

int populateAllKeys(Db* db, DocInfo* docinfo, void* ctx) {
    auto* allKeysCtx = static_cast<AllKeysCtx*>(ctx);
    auto key = makeDiskDocKey(docinfo->id);
    allKeysCtx->cb->callback(key);
    return allKeysCtx->cb->getStatus() ? COUCHSTORE_ERROR_CANCEL
                                       : COUCHSTORE_SUCCESS;
}

ENGINE_ERROR_CODE
CouchKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& start_key,
        uint32_t count,
        std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if(errCode == COUCHSTORE_SUCCESS) {
        sized_buf ref = to_sized_buf(start_key);

        AllKeysCtx ctx(cb, count);
        errCode = couchstore_all_docs(db,
                                      &ref,
                                      COUCHSTORE_NO_DELETES,
                                      populateAllKeys,
                                      static_cast<void*>(&ctx));
        if (errCode == COUCHSTORE_SUCCESS ||
            errCode == COUCHSTORE_ERROR_CANCEL) {
            return ENGINE_SUCCESS;
        } else {
            logger.warn(
                    "CouchKVStore::getAllKeys: couchstore_all_docs "
                    "error:{} [{}] {}, rev:{}",
                    couchstore_strerror(errCode),
                    cb_strerror(),
                    vbid,
                    db.getFileRev());
        }
    } else {
        logger.warn("CouchKVStore::getAllKeys: openDB error:{}, {}, rev:{}",
                    couchstore_strerror(errCode),
                    vbid,
                    db.getFileRev());
    }
    return ENGINE_FAILED;
}

void CouchKVStore::unlinkCouchFile(Vbid vbucket, uint64_t fRev) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::unlinkCouchFile: Not valid on a "
                "read-only object.");
    }

    const auto fname =
            cb::io::sanitizePath(dbname + "/" + std::to_string(vbucket.get()) +
                                 ".couch." + std::to_string(fRev));
    logger.debug("CouchKVStore::unlinkCouchFile: {}, revision:{}, fname:{}",
                 vbucket,
                 fRev,
                 fname);

    if (remove(fname.c_str()) == -1) {
        logger.warn(
                "CouchKVStore::unlinkCouchFile: remove error:{}, "
                "{}, rev:{}, fname:{}",
                errno,
                vbucket,
                fRev,
                fname);

        if (errno != ENOENT) {
            std::string file_str = fname;
            pendingFileDeletions->push(file_str);
        }
    }
}

void CouchKVStore::removeCompactFile(const std::string& filename, Vbid vbid) {
    const auto compact_file =
            getDBFileName(filename, vbid, getDbRevision(vbid)) +
            ".compact";

    if (!isReadOnly()) {
        removeCompactFile(compact_file);
    } else {
        logger.warn(
                "CouchKVStore::removeCompactFile: A read-only instance of "
                "the underlying store was not allowed to delete a temporary"
                "file: {}",
                compact_file);
    }
}

void CouchKVStore::removeCompactFile(const std::string &filename) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::removeCompactFile: Not valid on "
                "a read-only object.");
    }

    if (cb::io::isFile(filename)) {
        if (remove(filename.c_str()) == 0) {
            logger.warn(
                    "CouchKVStore::removeCompactFile: Removed compact "
                    "filename:{}",
                    filename);
        }
        else {
            logger.warn(
                    "CouchKVStore::removeCompactFile: remove error:{}, "
                    "filename:{}",
                    cb_strerror(),
                    filename);

            if (errno != ENOENT) {
                pendingFileDeletions->push(filename);
            }
        }
    }
}

std::unique_ptr<KVFileHandle> CouchKVStore::makeFileHandle(Vbid vbid) {
    auto db = std::make_unique<CouchKVFileHandle>(*this);
    // openDB logs errors
    if (openDB(vbid, db->getDbHolder(), COUCHSTORE_OPEN_FLAG_RDONLY) !=
        COUCHSTORE_SUCCESS) {
        return {};
    }

    return std::move(db);
}

void CouchKVStore::prepareToCreateImpl(Vbid vbid) {
    if (!isReadOnly()) {
        (*dbFileRevMap->wlock())[vbid.get()]++;
    }
}

uint64_t CouchKVStore::prepareToDeleteImpl(Vbid vbid) {
    // Clear the stats so it looks empty (real deletion of the disk data occurs
    // later)
    cachedDocCount[vbid.get()] = 0;
    cachedDeleteCount[vbid.get()] = 0;
    cachedFileSize[vbid.get()] = 0;
    cachedSpaceUsed[vbid.get()] = 0;
    return getDbRevision(vbid);
}

CouchKVStore::LocalDocHolder CouchKVStore::readLocalDoc(Db& db,
                                                        std::string_view name) {
    sized_buf id;
    id.buf = const_cast<char*>(name.data());
    id.size = name.size();

    LocalDocHolder lDoc;
    auto errCode = couchstore_open_local_document(
            &db, (void*)id.buf, id.size, lDoc.getLocalDocAddress());
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.debug("CouchKVStore::readLocalDoc({}): doc not found", name);
        } else {
            logger.warn(
                    "CouchKVStore::readLocalDoc({}): "
                    "couchstore_open_local_document error:{}",
                    name,
                    couchstore_strerror(errCode));
        }

        return {};
    }

    return lDoc;
}

Collections::KVStore::Manifest CouchKVStore::getCollectionsManifest(Vbid vbid) {
    DbHolder db(*this);

    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        // openDB would of logged any critical error
        return Collections::KVStore::Manifest{
                Collections::KVStore::Manifest::Default{}};
    }

    auto manifest = readLocalDoc(*db.getDb(), Collections::manifestName);
    auto collections =
            readLocalDoc(*db.getDb(), Collections::openCollectionsName);
    auto scopes = readLocalDoc(*db.getDb(), Collections::scopesName);
    auto dropped =
            readLocalDoc(*db.getDb(), Collections::droppedCollectionsName);

    cb::const_byte_buffer empty;
    return Collections::KVStore::decodeManifest(
            manifest.getLocalDoc() ? manifest.getBuffer() : empty,
            collections.getLocalDoc() ? collections.getBuffer() : empty,
            scopes.getLocalDoc() ? scopes.getBuffer() : empty,
            dropped.getLocalDoc() ? dropped.getBuffer() : empty);
}

std::vector<Collections::KVStore::DroppedCollection>
CouchKVStore::getDroppedCollections(Vbid vbid) {
    DbHolder db(*this);

    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        return {};
    }

    return getDroppedCollections(*db.getDb());
}

std::vector<Collections::KVStore::DroppedCollection>
CouchKVStore::getDroppedCollections(Db& db) {
    auto dropped = readLocalDoc(db, Collections::droppedCollectionsName);

    if (!dropped.getLocalDoc()) {
        return {};
    }

    return Collections::KVStore::decodeDroppedCollections(dropped.getBuffer());
}

void CouchKVStore::updateCollectionsMeta(
        Db& db, Collections::VB::Flush& collectionsFlush) {
    updateManifestUid();

    // If the updateOpenCollections reads the dropped collections, it can pass
    // them via this optional to updateDroppedCollections, thus we only read
    // the dropped list once per update.
    std::optional<std::vector<Collections::KVStore::DroppedCollection>> dropped;

    if (!collectionsMeta.collections.empty() ||
        !collectionsMeta.droppedCollections.empty()) {
        dropped = updateOpenCollections(db);
    }

    if (!collectionsMeta.droppedCollections.empty()) {
        updateDroppedCollections(db, dropped);
        collectionsFlush.setNeedsPurge();
    }

    if (!collectionsMeta.scopes.empty() ||
        !collectionsMeta.droppedScopes.empty()) {
        updateScopes(db);
    }

    collectionsMeta.clear();
}

void CouchKVStore::updateManifestUid() {
    // write back, no read required
    pendingLocalReqsQ.emplace_back(
            Collections::manifestName,
            Collections::KVStore::encodeManifestUid(collectionsMeta));
}

std::vector<Collections::KVStore::DroppedCollection>
CouchKVStore::updateOpenCollections(Db& db) {
    auto droppedCollections = getDroppedCollections(db);
    auto collections = readLocalDoc(db, Collections::openCollectionsName);
    cb::const_byte_buffer empty;

    pendingLocalReqsQ.emplace_back(
            Collections::openCollectionsName,
            Collections::KVStore::encodeOpenCollections(
                    droppedCollections,
                    collectionsMeta,
                    collections.getLocalDoc() ? collections.getBuffer()
                                              : empty));
    return droppedCollections;
}

void CouchKVStore::updateDroppedCollections(
        Db& db,
        std::optional<std::vector<Collections::KVStore::DroppedCollection>>
                dropped) {
    for (const auto& drop : collectionsMeta.droppedCollections) {
        // Delete the 'stats' document for the collection
        deleteCollectionStats(drop.collectionId);
    }

    // If the input 'dropped' is not initialised we must read the dropped
    // collection data
    if (!dropped.has_value()) {
        dropped = getDroppedCollections(db);
    }

    pendingLocalReqsQ.emplace_back(
            Collections::droppedCollectionsName,
            Collections::KVStore::encodeDroppedCollections(collectionsMeta,
                                                           dropped.value()));
}

void CouchKVStore::updateScopes(Db& db) {
    auto scopes = readLocalDoc(db, Collections::scopesName);
    cb::const_byte_buffer empty;
    pendingLocalReqsQ.emplace_back(
            Collections::scopesName,
            encodeScopes(collectionsMeta,
                         scopes.getLocalDoc() ? scopes.getBuffer() : empty));
}

const KVStoreConfig& CouchKVStore::getConfig() const {
    return configuration;
}

vbucket_state CouchKVStore::readVBState(Vbid vbid) {
    DbHolder db(*this);
    const auto errorCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errorCode != COUCHSTORE_SUCCESS) {
        throw std::logic_error("CouchKVStore::readVBState: openDB for vbid:" +
                               to_string(vbid) + " failed with error " +
                               std::to_string(static_cast<int8_t>(errorCode)));
    }

    const auto res = readVBState(db, vbid);
    if (res.status != ReadVBStateStatus::Success) {
        throw std::logic_error(
                "CouchKVStore::readVBState: readVBState for vbid:" +
                to_string(vbid) + " failed with status " +
                std::to_string(static_cast<uint8_t>(res.status)));
    }

    return res.state;
}

couchstore_error_t CouchKVStore::updateLocalDocuments(
        Db& db, PendingLocalDocRequestQueue& queue) {
    if (queue.size() == 0) {
        return COUCHSTORE_SUCCESS;
    }

    // Build a vector of localdoc references from the pending updates
    std::vector<std::reference_wrapper<LocalDoc>> localDocuments;
    localDocuments.reserve(queue.size());

    for (auto& lDoc : queue) {
        localDocuments.emplace_back(lDoc.getLocalDoc());
    }

    auto errCode = cb::couchstore::saveLocalDocuments(db, localDocuments);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::updateLocalDocuments size:{} "
                "couchstore_save_local_documents "
                "error:{} [{}]",
                queue.size(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }
    return errCode;
}

couchstore_error_t CouchKVStore::updateLocalDocument(Db& db,
                                                     std::string_view name,
                                                     std::string_view value) {
    LocalDoc lDoc;
    lDoc.id.buf = const_cast<char*>(name.data());
    lDoc.id.size = name.size();
    lDoc.json.buf = const_cast<char*>(value.data());
    lDoc.json.size = value.size();
    lDoc.deleted = 0;
    couchstore_error_t errCode = couchstore_save_local_document(&db, &lDoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::updateLocalDocument "
                "couchstore_save_local_document name:{} "
                "error:{} [{}]",
                name,
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }

    return errCode;
}

CouchLocalDocRequest::CouchLocalDocRequest(std::string&& key,
                                           std::string&& value)
    : key(std::move(key)), value(std::move(value)) {
    setupKey();
    setupValue();
}

CouchLocalDocRequest::CouchLocalDocRequest(
        std::string&& key, const flatbuffers::DetachedBuffer& value)
    : key(std::move(key)),
      value(reinterpret_cast<const char*>(value.data()), value.size()) {
    setupKey();
    setupValue();
}

CouchLocalDocRequest::CouchLocalDocRequest(std::string&& key,
                                           CouchLocalDocRequest::IsDeleted)
    : key(std::move(key)) {
    setupKey();
    doc.deleted = 1;
}

void CouchLocalDocRequest::setupKey() {
    doc.id.buf = const_cast<char*>(key.data());
    doc.id.size = key.size();
}

void CouchLocalDocRequest::setupValue() {
    doc.json.buf = const_cast<char*>(value.data());
    doc.json.size = value.size();
}

LocalDoc& CouchLocalDocRequest::getLocalDoc() {
    return doc;
}

/* end of couch-kvstore.cc */
