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
#include "collections/kvstore_generated.h"
#include "common.h"
#include "diskdockey.h"
#include "ep_time.h"
#include "item.h"
#include "kvstore_config.h"
#include "rollback_result.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <gsl/gsl>
#include <shared_mutex>

extern "C" {
    static int recordDbDumpC(Db *db, DocInfo *docinfo, void *ctx)
    {
        return CouchKVStore::recordDbDump(db, docinfo, ctx);
    }
}

extern "C" {
    static int getMultiCbC(Db *db, DocInfo *docinfo, void *ctx)
    {
        return CouchKVStore::getMultiCb(db, docinfo, ctx);
    }
}

static std::string getStrError(Db *db) {
    const size_t max_msg_len = 256;
    char msg[max_msg_len];
    couchstore_last_os_error(db, msg, max_msg_len);
    std::string errorStr(msg);
    return errorStr;
}

/**
 * Determine the datatype for a blob. It is _highly_ unlikely that
 * this method is being called, as it would have to be for an item
 * which is read off the disk _before_ we started to write the
 * datatype to disk (we did that in a 3.x server).
 *
 * @param doc The document to check
 * @return JSON or RAW bytes
 */
static protocol_binary_datatype_t determine_datatype(sized_buf doc) {
    if (checkUTF8JSON(reinterpret_cast<uint8_t*>(doc.buf), doc.size)) {
        return PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        return PROTOCOL_BINARY_RAW_BYTES;
    }
}

static bool endWithCompact(const std::string &filename) {
    size_t pos = filename.find(".compact");
    if (pos == std::string::npos ||
                        (filename.size() - sizeof(".compact")) != pos) {
        return false;
    }
    return true;
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

static KVStore::MutationStatus getMutationStatus(couchstore_error_t errCode) {
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return KVStore::MutationStatus::Success;
    case COUCHSTORE_ERROR_NO_HEADER:
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        // this return causes ep engine to drop the failed flush
        // of an item since it does not know about the itme any longer
        return KVStore::MutationStatus::DocNotFound;
    default:
        // this return causes ep engine to keep requeuing the failed
        // flush of an item
        return KVStore::MutationStatus::Failed;
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
    switch (err) {
    case COUCHSTORE_ERROR_OPEN_FILE:
    case COUCHSTORE_ERROR_READ:
    case COUCHSTORE_ERROR_WRITE:
    case COUCHSTORE_ERROR_FILE_CLOSE:
        return getStrError(db);

    case COUCHSTORE_ERROR_CORRUPT:
    case COUCHSTORE_ERROR_CHECKSUM_FAIL: {
        char buffer[256];
        couchstore_last_internal_error(db, buffer, sizeof(buffer));
        return std::string(buffer);
    }
    default:
        return "none";
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
    AllKeysCtx(std::shared_ptr<Callback<const DiskDocKey&>> callback,
               uint32_t cnt)
        : cb(callback), count(cnt) {
    }

    std::shared_ptr<Callback<const DiskDocKey&>> cb;
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

CouchRequest::CouchRequest(const Item& it, MutationRequestCallback cb)
    : IORequest(it.getVBucketId(), std::move(cb), DiskDocKey{it}),
      value(it.getValue()) {
    dbDoc.id = to_sized_buf(key);

    if (it.getNBytes()) {
        dbDoc.data.buf = const_cast<char *>(value->getData());
        dbDoc.data.size = it.getNBytes();
    } else {
        dbDoc.data.buf = NULL;
        dbDoc.data.size = 0;
    }
    meta.setCas(it.getCas());
    meta.setFlags(it.getFlags());
    meta.setExptime(it.getExptime());
    meta.setDataType(it.getDataType());

    const auto isDurabilityOp =
            (it.getOperation() == queue_op::pending_sync_write ||
             it.getOperation() == queue_op::commit_sync_write ||
             it.getOperation() == queue_op::abort_sync_write);

    if (isDurabilityOp) {
        meta.setDurabilityOp(it.getOperation());
    }

    if (it.isPending()) {
        // Note: durability timeout /isn't/ persisted as part of a pending
        // SyncWrite. This is because if we ever read it back from disk
        // during warmup (i.e. the commit_sync_write was never persisted), we
        // don't know if the SyncWrite was actually already committed; as such
        // to ensure consistency the pending SyncWrite *must* eventually commit
        // (or sit in pending forever).
        const auto level = it.getDurabilityReqs().getLevel();
        meta.setPrepareProperties(level, it.isDeleted());
    }

    if (it.isCommitSyncWrite() || it.isAbort()) {
        meta.setCompletedProperties(it.getPrepareSeqno());
    }

    dbDocInfo.db_seq = it.getBySeqno();

    // Now allocate space to hold the meta and get it ready for storage
    dbDocInfo.rev_meta.size = MetaData::getMetaDataSize(
            isDurabilityOp ? MetaData::Version::V3 : MetaData::Version::V1);
    dbDocInfo.rev_meta.buf = meta.prepareAndGetForPersistence();

    dbDocInfo.rev_seq = it.getRevSeqno();
    dbDocInfo.size = dbDoc.data.size;

    if (it.isDeleted() && !it.isPending()) {
        // Prepared SyncDeletes are not marked as deleted.
        dbDocInfo.deleted = 1;
        meta.setDeleteSource(it.deletionSource());
    } else {
        dbDocInfo.deleted = 0;
    }
    dbDocInfo.id = dbDoc.id;
    dbDocInfo.content_meta = getContentMeta(it);
}

CouchRequest::~CouchRequest() = default;

namespace Collections {
static constexpr const char* manifestName = "_local/collections/manifest";
static constexpr const char* openCollectionsName = "_local/collections/open";
static constexpr const char* scopesName = "_local/scope/open";
static constexpr const char* droppedCollectionsName =
        "_local/collections/dropped";
} // namespace Collections

CouchKVStore::CouchKVStore(KVStoreConfig& config)
    : CouchKVStore(config, *couchstore_get_default_file_ops()) {
}

CouchKVStore::CouchKVStore(KVStoreConfig& config,
                           FileOpsInterface& ops,
                           bool readOnly,
                           std::shared_ptr<RevisionMap> dbFileRevMap)
    : KVStore(config, readOnly),
      dbname(config.getDBName()),
      dbFileRevMap(dbFileRevMap),
      intransaction(false),
      scanCounter(0),
      logger(config.getLogger()),
      base_ops(ops) {
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

CouchKVStore::CouchKVStore(KVStoreConfig& config, FileOpsInterface& ops)
    : CouchKVStore(config,
                   ops,
                   false /*readonly*/,
                   std::make_shared<RevisionMap>(config.getMaxVBuckets())) {
}

/**
 * Make a read-only CouchKVStore from this object
 */
std::unique_ptr<CouchKVStore> CouchKVStore::makeReadOnlyStore() {
    // Not using make_unique due to the private constructor we're calling
    return std::unique_ptr<CouchKVStore>(
            new CouchKVStore(configuration, dbFileRevMap));
}

CouchKVStore::CouchKVStore(KVStoreConfig& config,
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
        if (errorCode == COUCHSTORE_SUCCESS) {
            auto readStatus = readVBStateAndUpdateCache(db, id).status;
            if (readStatus == ReadVBStateStatus::Success) {
                /* update stat */
                ++st.numLoadedVb;
            } else {
                logger.warn(
                        "CouchKVStore::initialize: readVBState"
                        " readVBState:{}, name:{}/{}.couch.{}",
                        int(readStatus),
                        dbname,
                        id.get(),
                        db.getFileRev());
                cachedVBStates[id.get()] = NULL;
            }
        } else {
            logger.warn(
                    "CouchKVStore::initialize: openDB"
                    " error:{}, name:{}/{}.couch.{}",
                    couchstore_strerror(errorCode),
                    dbname,
                    id.get(),
                    db.getFileRev());
            cachedVBStates[id.get()] = NULL;
        }

        // Setup cachedDocCount
        DbInfo info;
        errorCode = couchstore_db_info(db, &info);
        if (errorCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::initialize: couchstore_db_info"
                    " error:{}, name:{}/{}.couch.{}",
                    couchstore_strerror(errorCode),
                    dbname,
                    id.get(),
                    db.getFileRev());
        }
        cachedDocCount[id.get()] = info.doc_count;

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
        unlinkCouchFile(vbucketId, (*dbFileRevMap)[vbucketId.get()]);
        prepareToCreateImpl(vbucketId);

        setVBucketState(
                vbucketId, *state, VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT);
    } else {
        throw std::invalid_argument(
                "CouchKVStore::reset: No entry in cached "
                "states for " +
                vbucketId.to_string());
    }
}

void CouchKVStore::set(const Item& itm, SetCallback cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::set: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("CouchKVStore::set: intransaction must be "
                        "true to perform a set operation.");
    }

    // each req will be de-allocated after commit
    pendingReqsQ.emplace_back(itm, std::move(cb));
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

GetValue CouchKVStore::getWithHeader(void* dbHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly) {
    Db *db = (Db *)dbHandle;
    auto start = std::chrono::steady_clock::now();
    DocInfo *docInfo = NULL;
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
            db, ids.data(), itms.size(), 0, getMultiCbC, &ctx);
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

void CouchKVStore::del(const Item& itm, DeleteCallback cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::del: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("CouchKVStore::del: intransaction must be "
                        "true to perform a delete operation.");
    }

    pendingReqsQ.emplace_back(itm, std::move(cb));
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
    std::string fname = dbname + "/stats.json";
    cb::io::sanitizePath(fname);
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
    } catch (const nlohmann::json::exception&) {
        logger.warn(
                "CouchKVStore::getPersistedStats:"
                " Failed to parse the session stats json doc!!!");
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

static int edit_docinfo_hook(DocInfo **info, const sized_buf *item) {
    // Examine the metadata of the doc
    auto documentMetaData = MetaDataFactory::createMetaData((*info)->rev_meta);
    // Allocate latest metadata
    std::unique_ptr<MetaData> metadata;
    if (documentMetaData->getVersionInitialisedFrom() == MetaData::Version::V0) {
        // Metadata doesn't have flex_meta_code/datatype. Provision space for
        // these paramenters.

        // If the document is compressed we need to inflate it to
        // determine if it is json or not.
        cb::compression::Buffer inflated;
        cb::const_char_buffer data {item->buf, item->size};
        if (((*info)->content_meta | COUCH_DOC_IS_COMPRESSED) ==
                (*info)->content_meta) {
            if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                          data, inflated)) {
                throw std::runtime_error(
                    "edit_docinfo_hook: failed to inflate document with seqno: " +
                    std::to_string((*info)->db_seq) + " revno: " +
                    std::to_string((*info)->rev_seq));
            }
            data = inflated;
        }

        protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
        if (checkUTF8JSON(reinterpret_cast<const uint8_t*>(data.data()),
                          data.size())) {
            datatype = PROTOCOL_BINARY_DATATYPE_JSON;
        }

        // Now create a blank latest metadata.
        metadata = MetaDataFactory::createMetaData();
        // Copy the metadata this will pull across available V0 fields.
        *metadata = *documentMetaData;

        // Setup flex code and datatype
        metadata->setFlexCode();
        metadata->setDataType(datatype);
    } else {
        // The metadata in the document is V1 and needs no changes.
        return 0;
    }

    // the docInfo pointer includes the DocInfo and the data it points to.
    // this must be a pointer which cb_free() can deallocate
    char* buffer = static_cast<char*>(cb_calloc(1, sizeof(DocInfo) +
                             (*info)->id.size +
                             MetaData::getMetaDataSize(MetaData::Version::V1)));


    DocInfo* docInfo = reinterpret_cast<DocInfo*>(buffer);

    // Deep-copy the incoming DocInfo, then we'll fix the pointers/buffer data
    *docInfo = **info;

    // Correct the id buffer
    docInfo->id.buf = buffer + sizeof(DocInfo);
    std::memcpy(docInfo->id.buf, (*info)->id.buf, docInfo->id.size);

    // Correct the rev_meta pointer and fill it in.
    docInfo->rev_meta.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    docInfo->rev_meta.buf = buffer + sizeof(DocInfo) + docInfo->id.size;
    metadata->copyToBuf(docInfo->rev_meta);

    // Free the orginal
    couchstore_free_docinfo(*info);

    // Return the newly allocated docinfo with corrected metadata
    *info = docInfo;

    return 1;
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

static int time_purge_hook(Db* d, DocInfo* info, sized_buf item, void* ctx_p) {
    compaction_ctx* ctx = static_cast<compaction_ctx*>(ctx_p);

    if (info == nullptr) {
        // Compaction finished
        return couchstore_set_purge_seq(d, ctx->max_purged_seq);
    }

    DbInfo infoDb;
    auto err = couchstore_db_info(d, &infoDb);
    if (err != COUCHSTORE_SUCCESS) {
        EP_LOG_WARN("time_purge_hook: couchstore_db_info() failed: {}",
                    couchstore_strerror(err));
        return err;
    }

    uint64_t max_purge_seq = ctx->max_purged_seq;

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
                ctx->droppedKeyCb(diskKey, int64_t(info->db_seq));
                if (!info->deleted) {
                    ctx->stats.collectionsItemsPurged++;
                } else {
                    ctx->stats.collectionsDeletedItemsPurged++;
                }
                if (metadata->isPrepare()) {
                    ctx->stats.preparesPurged++;
                }
                return COUCHSTORE_COMPACT_DROP_ITEM;
            } else if (info->deleted) {
                ctx->eraserContext->processEndOfCollection(
                        diskKey.getDocKey(), SystemEvent(metadata->getFlags()));
            }
        }

        if (info->deleted) {
            if (info->db_seq != infoDb.last_sequence) {
                if (ctx->compactConfig.drop_deletes) { // all deleted items must
                                                       // be dropped ...
                    if (max_purge_seq < info->db_seq) {
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
                    if (max_purge_seq < info->db_seq) {
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
            ctx->bloomFilterCallback->callback(
                    reinterpret_cast<Vbid&>(ctx->compactConfig.db_file_id),
                    key.getDocKey(),
                    deleted);
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

bool CouchKVStore::compactDB(compaction_ctx *hook_ctx) {
    bool result = false;

    try {
        result = compactDBInternal(hook_ctx, edit_docinfo_hook);
    } catch(std::logic_error& le) {
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

FileInfo CouchKVStore::toFileInfo(const DbInfo& info) {
    return FileInfo{
            info.doc_count, info.deleted_count, info.file_size, info.purge_seq};
}

bool CouchKVStore::compactDBInternal(compaction_ctx* hook_ctx,
                                     couchstore_docinfo_hook docinfo_hook) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::compactDB: Cannot perform "
                        "on a read-only instance.");
    }
    couchstore_compact_hook       hook = time_purge_hook;
    couchstore_docinfo_hook dhook = docinfo_hook;
    FileOpsInterface         *def_iops = statCollectingFileOpsCompaction.get();
    DbHolder compactdb(*this);
    DbHolder targetDb(*this);
    couchstore_error_t         errCode = COUCHSTORE_SUCCESS;
    std::chrono::steady_clock::time_point start =
            std::chrono::steady_clock::now();
    std::string                 dbfile;
    std::string           compact_file;
    std::string               new_file;
    DbInfo                        info;
    Vbid vbid = hook_ctx->compactConfig.db_file_id;
    hook_ctx->config = &configuration;

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

    couchstore_db_info(compactdb, &info);
    hook_ctx->stats.pre = toFileInfo(info);

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

    // Perform COMPACTION of vbucket.couch.rev into vbucket.couch.rev.compact
    errCode = couchstore_compact_db_ex(compactdb,
                                       compact_file.c_str(),
                                       flags,
                                       hook,
                                       dhook,
                                       hook_ctx,
                                       def_iops);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::compactDB:couchstore_compact_db_ex "
                "error:{} [{}], name:{}",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(compactdb, errCode),
                dbfile);
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

    if (hook_ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        if (!hook_ctx->eraserContext->empty()) {
            std::stringstream ss;
            ss << "CouchKVStore::compactDB finalising dropped collections, "
               << "container should be empty" << *hook_ctx->eraserContext
               << std::endl;
            throw std::logic_error(ss.str());
        }
        // Need to ensure the 'dropped' list on disk is now gone
        deleteLocalDoc(*targetDb.getDb(), Collections::droppedCollectionsName);

        errCode = couchstore_commit(targetDb.getDb());
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::compactDB: failed to commit collection "
                    "manifest update errCode:{}",
                    couchstore_strerror(errCode));
        }
    }

    errCode = couchstore_db_info(targetDb.getDb(), &info);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::compactDB: couchstore_db_info errCode:{}",
                    couchstore_strerror(errCode));
    }

    hook_ctx->stats.post = toFileInfo(info);

    cachedFileSize[vbid.get()] = info.file_size;
    cachedSpaceUsed[vbid.get()] = info.space_used;

    // also update cached state with dbinfo
    vbucket_state* state = getVBucketState(vbid);
    if (state) {
        state->highSeqno = info.last_sequence;
        state->purgeSeqno = info.purge_seq;
        cachedDeleteCount[vbid.get()] = info.deleted_count;
        cachedDocCount[vbid.get()] = info.doc_count;
        state->onDiskPrepares -= hook_ctx->stats.preparesPurged;
        // Must sync the modified state back
        saveVBState(targetDb.getDb(), *state);
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

bool CouchKVStore::setVBucketState(Vbid vbucketId,
                                   const vbucket_state& vbstate,
                                   VBStatePersist options) {
    std::map<Vbid, uint64_t>::iterator mapItr;
    couchstore_error_t errorCode;

    if (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
            options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT) {
        DbHolder db(*this);
        errorCode =
                openDB(vbucketId, db, (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            logger.warn(
                    "CouchKVStore::setVBucketState: openDB error:{}, "
                    "{}, fileRev:{}",
                    couchstore_strerror(errorCode),
                    vbucketId,
                    db.getFileRev());
            return false;
        }

        errorCode = saveVBState(db, vbstate);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            logger.warn(
                    "CouchKVStore:setVBucketState: saveVBState error:{}, "
                    "{}, fileRev:{}",
                    couchstore_strerror(errorCode),
                    vbucketId,
                    db.getFileRev());
            return false;
        }

        if (options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT) {
            errorCode = couchstore_commit(db);
            if (errorCode != COUCHSTORE_SUCCESS) {
                ++st.numVbSetFailure;
                logger.warn(
                        "CouchKVStore:setVBucketState: couchstore_commit "
                        "error:{} [{}], {}, rev:{}",
                        couchstore_strerror(errorCode),
                        couchkvstore_strerrno(db, errorCode),
                        vbucketId,
                        db.getFileRev());
                return false;
            }
        }

        DbInfo info;
        errorCode = couchstore_db_info(db, &info);
        if (errorCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::setVBucketState: couchstore_db_info "
                    "error:{}, {}",
                    couchstore_strerror(errorCode),
                    vbucketId);
        } else {
            cachedSpaceUsed[vbucketId.get()] = info.space_used;
            cachedFileSize[vbucketId.get()] = info.file_size;
        }
    } else {
        throw std::invalid_argument(
                "CouchKVStore::setVBucketState: invalid vb state "
                "persist option specified for " +
                vbucketId.to_string());
    }

    return true;
}

bool CouchKVStore::snapshotVBucket(Vbid vbucketId,
                                   const vbucket_state& vbstate,
                                   VBStatePersist options) {
    if (isReadOnly()) {
        logger.warn(
                "CouchKVStore::snapshotVBucket: cannot be performed on a "
                "read-only KVStore instance");
        return false;
    }

    auto start = std::chrono::steady_clock::now();

    if (updateCachedVBState(vbucketId, vbstate) &&
         (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
          options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        vbucket_state* vbs = getVBucketState(vbucketId);
        if (!setVBucketState(vbucketId, *vbs, options)) {
            logger.warn(
                    "CouchKVStore::snapshotVBucket: setVBucketState failed "
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
                         StorageProperties::ConcurrentWriteCompact::No);
    return rv;
}

bool CouchKVStore::commit(Collections::VB::Flush& collectionsFlush) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::commit: Not valid on a read-only "
                        "object.");
    }

    if (intransaction) {
        if (commit2couchstore(collectionsFlush)) {
            intransaction = false;
            transactionCtx.reset();
        }
    }

    return !intransaction;
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

ScanContext* CouchKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    DbHolder db(*this);
    couchstore_error_t errorCode =
            openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errorCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::initScanContext: openDB error:{}, "
                "name:{}/{}.couch.{}",
                couchstore_strerror(errorCode),
                dbname,
                vbid.get(),
                db.getFileRev());
        return NULL;
    }

    DbInfo info;
    errorCode = couchstore_db_info(db, &info);
    if (errorCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::initScanContext: couchstore_db_info error:{}",
                couchstore_strerror(errorCode));
        EP_LOG_WARN(
                "CouchKVStore::initScanContext: Failed to read DB info for "
                "backfill. {} rev:{} error: {}",
                vbid,
                db.getFileRev(),
                couchstore_strerror(errorCode));
        return NULL;
    }

    uint64_t count = 0;
    errorCode = couchstore_changes_count(
            db, startSeqno, std::numeric_limits<uint64_t>::max(), &count);
    if (errorCode != COUCHSTORE_SUCCESS) {
        EP_LOG_WARN(
                "CouchKVStore::initScanContext:Failed to obtain changes "
                "count for {} rev:{} start_seqno:{} error: {}",
                vbid,
                db.getFileRev(),
                startSeqno,
                couchstore_strerror(errorCode));
        return NULL;
    }

    auto readVbStateResult = readVBState(db, vbid);
    if (readVbStateResult.status != ReadVBStateStatus::Success) {
        EP_LOG_WARN(
                "CouchKVStore::initScanContext:Failed to obtain vbState for"
                "the highCompletedSeqno");
        return NULL;
    }

    size_t scanId = scanCounter++;

    auto collectionsManifest = getDroppedCollections(*db);

    {
        LockHolder lh(scanLock);
        scans[scanId] = db.releaseDb();
    }

    ScanContext* sctx = new ScanContext(cb,
                                        cl,
                                        vbid,
                                        scanId,
                                        startSeqno,
                                        info.last_sequence,
                                        info.purge_seq,
                                        options,
                                        valOptions,
                                        count,
                                        readVbStateResult.state,
                                        configuration,
                                        collectionsManifest);
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

scan_error_t CouchKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    TRACE_EVENT_START2("CouchKVStore",
                       "scan",
                       "vbid",
                       ctx->vbid.get(),
                       "startSeqno",
                       ctx->startSeqno);

    Db* db;
    {
        LockHolder lh(scanLock);
        auto itr = scans.find(ctx->scanId);
        if (itr == scans.end()) {
            return scan_failed;
        }

        db = itr->second;
    }

    uint64_t start = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        start = ctx->lastReadSeqno + 1;
    }

    couchstore_error_t errorCode;
    errorCode = couchstore_changes_since(db,
                                         start,
                                         getDocFilter(ctx->docFilter),
                                         recordDbDumpC,
                                         static_cast<void*>(ctx));

    TRACE_EVENT_END1(
            "CouchKVStore", "scan", "lastReadSeqno", ctx->lastReadSeqno);

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

void CouchKVStore::destroyScanContext(ScanContext* ctx) {
    if (!ctx) {
        return;
    }

    LockHolder lh(scanLock);
    auto itr = scans.find(ctx->scanId);
    if (itr != scans.end()) {
        closeDatabaseHandle(itr->second);
        scans.erase(itr);
    }
    delete ctx;
}

DbInfo CouchKVStore::getDbInfo(Vbid vbid) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        DbInfo info;
        errCode = couchstore_db_info(db, &info);
        if (errCode == COUCHSTORE_SUCCESS) {
            return info;
        } else {
            throw std::runtime_error(
                    "CouchKVStore::getDbInfo: failed "
                    "to read database info for " +
                    vbid.to_string() + " revision " +
                    std::to_string(db.getFileRev()) +
                    " - couchstore returned error: " +
                    couchstore_strerror(errCode));
        }
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
    intransaction = false;
}

uint64_t CouchKVStore::checkNewRevNum(std::string &dbFileName, bool newFile) {
    uint64_t newrev = 0;
    std::string nameKey;

    if (!newFile) {
        // extract out the file revision number first
        size_t secondDot = dbFileName.rfind(".");
        nameKey = dbFileName.substr(0, secondDot);
    } else {
        nameKey = dbFileName;
    }
    nameKey.append(".");
    const auto files = cb::io::findFilesWithPrefix(nameKey);
    std::vector<std::string>::const_iterator itor;
    // found file(s) whoes name has the same key name pair with different
    // revision number
    for (itor = files.begin(); itor != files.end(); ++itor) {
        const std::string &filename = *itor;
        if (endWithCompact(filename)) {
            continue;
        }

        size_t secondDot = filename.rfind(".");
        char *ptr = NULL;
        uint64_t revnum = strtoull(filename.substr(secondDot + 1).c_str(), &ptr, 10);
        if (newrev < revnum) {
            newrev = revnum;
            dbFileName = filename;
        }
    }
    return newrev;
}

void CouchKVStore::updateDbFileMap(Vbid vbucketId, uint64_t newFileRev) {
    if (vbucketId.get() >= numDbFiles) {
        logger.warn(
                "CouchKVStore::updateDbFileMap: Cannot update db file map "
                "for an invalid vbucket, {}, rev:{}",
                vbucketId,
                newFileRev);
        return;
    }
    // MB-27963: obtain write access whilst we update the file map openDB also
    // obtains this mutex to ensure the fileRev it obtains doesn't become stale
    // by the time it hits sys_open.
    std::unique_lock<folly::SharedMutex> lg(openDbMutex);

    (*dbFileRevMap)[vbucketId.get()] = newFileRev;
}

couchstore_error_t CouchKVStore::openDB(Vbid vbucketId,
                                        DbHolder& db,
                                        couchstore_open_flags options,
                                        FileOpsInterface* ops) {
    // MB-27963: obtain read access whilst we open the file, updateDbFileMap
    // serialises on this mutex so we can be sure the fileRev we read should
    // still be a valid file once we hit sys_open
    std::shared_lock<folly::SharedMutex> lg(openDbMutex);
    uint64_t fileRev = (*dbFileRevMap)[vbucketId.get()];
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
        char *ptr = NULL;
        uint64_t revNum = strtoull(revNumStr.c_str(), &ptr, 10);

        std::string vbIdStr = nameKey.substr(firstSlash + 1,
                                            (firstDot - firstSlash) - 1);
        if (allDigit(vbIdStr)) {
            int vbId = atoi(vbIdStr.c_str());
            if (vbids) {
                vbids->push_back(static_cast<Vbid>(vbId));
            }
            uint64_t old_rev_num = (*dbFileRevMap)[vbId];
            if (old_rev_num == revNum) {
                continue;
            } else if (old_rev_num < revNum) { // stale revision found
                (*dbFileRevMap)[vbId] = revNum;
            } else { // stale file found (revision id has rolled over)
                old_rev_num = revNum;
            }
            std::stringstream old_file;
            old_file << dbname << "/" << vbId << ".couch." << old_rev_num;
            if (cb::io::isFile(old_file.str())) {
                if (!isReadOnly()) {
                    if (remove(old_file.str().c_str()) == 0) {
                        logger.debug(
                                "CouchKVStore::populateFileNameMap: Removed "
                                "stale file:{}",
                                old_file.str());
                    } else {
                        logger.warn(
                                "CouchKVStore::populateFileNameMap: remove "
                                "error:{}, file:{}",
                                cb_strerror(),
                                old_file.str());
                    }
                } else {
                    logger.warn(
                            "CouchKVStore::populateFileNameMap: A read-only "
                            "instance of the underlying store "
                            "was not allowed to delete a stale file:{}",
                            old_file.str());
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
        auto it = makeItemFromDocInfo(
                vbId, *docinfo, *metadata, {nullptr, docinfo->size});

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
                // This is a super old version of a couchstore file.
                // Try to determine if the document is JSON or raw bytes
                metadata->setDataType(determine_datatype(doc->data));
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

int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx) {

    ScanContext* sctx = static_cast<ScanContext*>(ctx);
    auto* cb = sctx->callback.get();
    auto* cl = sctx->lookup.get();

    Doc *doc = nullptr;
    sized_buf value{nullptr, 0};
    uint64_t byseqno = docinfo->db_seq;
    Vbid vbucketId = sctx->vbid;

    sized_buf key = docinfo->id;
    if (key.size > UINT16_MAX) {
        throw std::invalid_argument("CouchKVStore::recordDbDump: "
                        "docinfo->id.size (which is " + std::to_string(key.size) +
                        ") is greater than " + std::to_string(UINT16_MAX));
    }

    auto diskKey = makeDiskDocKey(docinfo->id);

    // Determine if the key is logically deleted, if it is we skip the key
    // Note that system event keys (like create scope) are never skipped here
    auto docKey = diskKey.getDocKey();
    if (!docKey.getCollectionID().isSystem()) {
        if (sctx->docFilter !=
            DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
            if (sctx->collectionsContext.isLogicallyDeleted(docKey, byseqno)) {
                sctx->lastReadSeqno = byseqno;
                return COUCHSTORE_SUCCESS;
            }
        }

        CacheLookup lookup(diskKey, byseqno, vbucketId);

        cl->callback(lookup);
        if (cl->getStatus() == ENGINE_KEY_EEXISTS) {
            sctx->lastReadSeqno = byseqno;
            return COUCHSTORE_SUCCESS;
        } else if (cl->getStatus() == ENGINE_ENOMEM) {
            return COUCHSTORE_ERROR_CANCEL;
        }
    }

    auto metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);

    if (sctx->valFilter != ValueFilter::KEYS_ONLY) {
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
                    // This is a super old version of a couchstore file.
                    // Try to determine if the document is JSON or raw bytes
                    metadata->setDataType(determine_datatype(doc->data));
                }
            } else {
                // No data, it cannot have a datatype!
                metadata->setDataType(PROTOCOL_BINARY_RAW_BYTES);
            }
        } else if (errCode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            sctx->logger->log(spdlog::level::level_enum::warn,
                              "CouchKVStore::recordDbDump: "
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

    bool onlyKeys = (sctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;
    GetValue rv(std::move(it), ENGINE_SUCCESS, -1, onlyKeys);
    cb->callback(rv);

    couchstore_free_document(doc);

    if (cb->getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    sctx->lastReadSeqno = byseqno;
    return COUCHSTORE_SUCCESS;
}

bool CouchKVStore::commit2couchstore(Collections::VB::Flush& collectionsFlush) {
    bool success = true;

    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return success;
    }

    // Use the vbucket of the first item
    auto vbucket2flush = pendingReqsQ[0].getVBucketId();

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
        if (vbucket2flush != req.getVBucketId()) {
            throw std::logic_error(
                    "CouchKVStore::commit2couchstore: "
                    "mismatch between vbucket2flush (which is " +
                    vbucket2flush.to_string() + ") and pendingReqsQ[" +
                    std::to_string(i) + "] (which is " +
                    req.getVBucketId().to_string() + ")");
        }
    }

    // The docinfo callback needs to know if the CollectionID feature is on
    kvstats_ctx kvctx(collectionsFlush);
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

    // Update keyStats for overall (not per-collection) stats
    if (oldInfo) {
        // Replacing a document
        if (!oldInfo->deleted) {
            auto itr = cbCtx->keyStats.find(newKey);
            if (itr != cbCtx->keyStats.end()) {
                itr->second = true; // mark this key as replaced
            }
        }
    }

    int itemCountDelta = 0;
    if (oldInfo) {
        if (!oldInfo->deleted) {
            if (newInfo->deleted) {
                // New is deleted, so decrement count
                itemCountDelta = -1;
            }
        } else if (!newInfo->deleted) {
            // Adding an item
            itemCountDelta = 1;
        }
    } else if (!newInfo->deleted) {
        // Adding an item
        itemCountDelta = 1;
    }

    auto docKey = newKey.getDocKey();
    switch (itemCountDelta) {
    case -1:
        if (newKey.isCommitted()) {
            cbCtx->collectionsFlush.decrementDiskCount(docKey);
        } else {
            cbCtx->onDiskPrepareDelta--;
        }
        break;
    case 1:
        if (newKey.isCommitted()) {
            cbCtx->collectionsFlush.incrementDiskCount(docKey);
        } else {
            cbCtx->onDiskPrepareDelta++;
        }
        break;
    case 0:
        break;
    default:
        throw std::logic_error(
                "CouchKVStore::saveDocsCallback: invalid delta {}" +
                std::to_string(itemCountDelta));
    }

    // Do not need to update high seqno if we are calling this for a prepare and
    // it will error if we do so return early.
    if (!newKey.isCommitted()) {
        return;
    }

    // Set the highest seqno that we are persisting regardless of if it
    // is a mutation or deletion
    cbCtx->collectionsFlush.setPersistedHighSeqno(
            docKey, newInfo->db_seq, newInfo->deleted);
}

/**
 * Returns the logical size of the data within the given Doc - i.e. the
 * "useful" data ep-engine is writing to disk for this document.
 * Used for Write Amplification calculation.
 */
static size_t calcLogicalDataSize(const DocInfo& info) {
    // key len + revision metadata (expiry, flags, etc) + value len.
    return info.id.size + info.rev_meta.size + info.size;
}

couchstore_error_t CouchKVStore::saveDocs(Vbid vbid,
                                          const std::vector<Doc*>& docs,
                                          std::vector<DocInfo*>& docinfos,
                                          kvstats_ctx& kvctx) {
    couchstore_error_t errCode;
    DbInfo info;
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
        vbucket_state* state = getVBucketState(vbid);
        if (state == nullptr) {
            throw std::logic_error("CouchKVStore::saveDocs: cachedVBStates[" +
                                   vbid.to_string() + "] is NULL");
        }

        uint64_t maxDBSeqno = 0;

        // Count of logical bytes written (key + ep-engine meta + value),
        // used to calculated Write Amplification.
        size_t docsLogicalBytes = 0;

        // Only do a couchstore_save_documents if there are docs
        if (docs.size() > 0) {
            std::vector<sized_buf> ids(docs.size());
            for (size_t idx = 0; idx < docs.size(); idx++) {
                ids[idx] = docinfos[idx]->id;
                maxDBSeqno = std::max(maxDBSeqno, docinfos[idx]->db_seq);
                auto key = makeDiskDocKey(ids[idx]);
                kvctx.keyStats[key] = false;

                // Accumulate the size of the useful data in this docinfo.
                docsLogicalBytes += calcLogicalDataSize(*docinfos[idx]);
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

        kvctx.collectionsFlush.saveCollectionStats(
                std::bind(&CouchKVStore::saveCollectionStats,
                          this,
                          std::ref(*db),
                          std::placeholders::_1,
                          std::placeholders::_2));

        state->onDiskPrepares += kvctx.onDiskPrepareDelta;
        errCode = saveVBState(db, *state);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn("CouchKVStore::saveDocs: saveVBState error:{} [{}]",
                        couchstore_strerror(errCode),
                        couchkvstore_strerrno(db, errCode));
            return errCode;
        }

        if (collectionsMeta.needsCommit) {
            errCode = updateCollectionsMeta(*db, kvctx.collectionsFlush);
            if (errCode) {
                logger.warn(
                        "CouchKVStore::saveDocs: updateCollectionsMeta "
                        "error:{} [{}]",
                        couchstore_strerror(errCode),
                        couchkvstore_strerrno(db, errCode));
            }
        }

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
        errCode = couchstore_db_info(db, &info);
        if (errCode) {
            logger.warn(
                    "CouchKVStore::saveDocs: couchstore_db_info error:{} [{}]",
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode));
            return errCode;
        }
        cachedSpaceUsed[vbid.get()] = info.space_used;
        cachedFileSize[vbid.get()] = info.file_size;
        cachedDeleteCount[vbid.get()] = info.deleted_count;
        cachedDocCount[vbid.get()] = info.doc_count;

        // Check seqno if we wrote documents
        if (docs.size() > 0 && maxDBSeqno != info.last_sequence) {
            logger.warn(
                    "CouchKVStore::saveDocs: Seqno in db header ({})"
                    " is not matched with what was persisted ({})"
                    " for {}",
                    info.last_sequence,
                    maxDBSeqno,
                    vbid);
        }
        state->highSeqno = info.last_sequence;
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
    for (auto& committed : committedReqs) {
        const auto docLogicalSize =
                calcLogicalDataSize(*committed.getDbDocInfo());
        /* update ep stats */
        ++st.io_num_write;
        st.io_document_write_bytes += docLogicalSize;

        if (committed.isDelete()) {
            auto mutationStatus = getMutationStatus(errCode);
            if (mutationStatus != MutationStatus::Failed) {
                const auto& key = committed.getKey();
                if (kvctx.keyStats[key]) {
                    mutationStatus =
                            MutationStatus::Success; // Deletion is for an
                                                     // existing item on
                                                     // DB file.
                } else {
                    mutationStatus =
                            MutationStatus::DocNotFound; // Deletion is for a
                                                         // non-existing item on
                                                         // DB file.
                }
            }
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committed.getDelta());
            }
            committed.getDelCallback()(*transactionCtx, mutationStatus);
        } else {
            auto mutationStatus = getMutationStatus(errCode);
            const auto& key = committed.getKey();
            bool insertion = !kvctx.keyStats[key];
            if (errCode) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(committed.getDelta());
                st.writeSizeHisto.add(docLogicalSize);
            }

            auto setState = MutationSetResultState::Failed;
            if (mutationStatus == MutationStatus::Success) {
                if (insertion) {
                    setState = MutationSetResultState::Insert;
                } else {
                    setState = MutationSetResultState::Update;
                }
            } else if (mutationStatus == MutationStatus::DocNotFound) {
                setState = MutationSetResultState::DocNotFound;
            }
            committed.getSetCallback()(*transactionCtx, setState);
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
    LocalDoc *ldoc = NULL;
    ReadVBStateStatus status = ReadVBStateStatus::Success;
    // High sequence number and purge sequence number are stored automatically
    // by couchstore.
    int64_t highSeqno = 0;
    uint64_t purgeSeqno = 0;
    snapshot_info_t snapshot{0, {0, 0}};
    DbInfo info;
    auto couchStoreStatus = couchstore_db_info(db, &info);
    if (couchStoreStatus == COUCHSTORE_SUCCESS) {
        highSeqno = info.last_sequence;
        purgeSeqno = info.purge_seq;
    } else {
        logger.warn(
                "CouchKVStore::readVBState: couchstore_db_info error:{}"
                ", {}",
                couchstore_strerror(couchStoreStatus),
                vbId);
        return {ReadVBStateStatus::CouchstoreError, {}};
    }

    vbucket_state vbState;

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    couchStoreStatus =
            couchstore_open_local_document(db, (void*)id.buf, id.size, &ldoc);

    if (couchStoreStatus == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        logger.info(
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
        // Cannot use make_unique here as it doesn't support
        // brace-initialization until C++20.
        cachedVBStates[vbid.get()].reset(new vbucket_state(res.state));
    }
    return res;
}

couchstore_error_t CouchKVStore::saveVBState(Db *db,
                                             const vbucket_state &vbState) {
    LocalDoc lDoc;
    lDoc.id.buf = (char *)"_local/vbstate";
    lDoc.id.size = sizeof("_local/vbstate") - 1;
    nlohmann::json j = vbState;

    // Strip out the high_seqno and purge_seqno - they are automatically
    // tracked by couchstore so unnecessary (and potentially confusing) to
    // store in vbstate document.
    j.erase("high_seqno");
    j.erase("purge_seqno");

    std::string state = j.dump();
    lDoc.json.buf = (char *)state.c_str();
    lDoc.json.size = state.size();
    lDoc.deleted = 0;

    couchstore_error_t errCode = couchstore_save_local_document(db, &lDoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::saveVBState couchstore_save_local_document "
                "error:{} [{}]",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode));
    }

    return errCode;
}

void CouchKVStore::saveCollectionStats(Db& db,
                                       CollectionID cid,
                                       Collections::VB::PersistedStats stats) {
    // Write out the stats in BE to a local doc named after the collection
    // Using set-notation cardinality - |cid| which helps keep the keys small
    std::string docName = "|" + cid.to_string() + "|";

    LocalDoc lDoc;
    lDoc.id.buf = const_cast<char*>(docName.c_str());
    lDoc.id.size = docName.size();

    auto encodedStats = stats.getLebEncodedStats();
    lDoc.json.buf = const_cast<char*>(encodedStats.data());
    lDoc.json.size = encodedStats.size();
    lDoc.deleted = 0;

    auto errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::saveCollectionStats cid:{} "
                "couchstore_save_local_document "
                "error:{} [{}]",
                cid.to_string(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }
}

void CouchKVStore::deleteCollectionStats(Db& db, CollectionID cid) {
    deleteLocalDoc(db, "|" + cid.to_string() + "|"); // internally logs
}

Collections::VB::PersistedStats CouchKVStore::getCollectionStats(
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

int CouchKVStore::getMultiCb(Db *db, DocInfo *docinfo, void *ctx) {
    if (docinfo == nullptr) {
        throw std::invalid_argument("CouchKVStore::getMultiCb: docinfo "
                "must be non-NULL");
    }
    if (ctx == nullptr) {
        throw std::invalid_argument("CouchKVStore::getMultiCb: ctx must "
                "be non-NULL");
    }

    GetMultiCbCtx *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    auto key = makeDiskDocKey(docinfo->id);
    KVStoreStats& st = cbCtx->cks.getKVStoreStat();

    vb_bgfetch_queue_t::iterator qitr = cbCtx->fetches.find(key);
    if (qitr == cbCtx->fetches.end()) {
        // this could be a serious race condition in couchstore,
        // log a warning message and continue
        cbCtx->cks.logger.warn(
                "CouchKVStore::getMultiCb: Couchstore returned "
                "invalid docinfo, no pending bgfetch has been "
                "issued for a key in {}, "
                "seqno:{}",
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
        cbCtx->cks.logger.warn(
                "CouchKVStore::getMultiCb called with zero"
                "items in bgfetched_list, {}, seqno:{}",
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
        DbInfo info;
        errCode = couchstore_db_info(db, &info);
        if (errCode == COUCHSTORE_SUCCESS) {
            cachedDeleteCount[vbid.get()] = info.deleted_count;
            return info.deleted_count;
        } else {
            throw std::runtime_error(
                    "CouchKVStore::getNumPersistedDeletes:"
                    "Failed to read database info for " +
                    vbid.to_string() +
                    " rev = " + std::to_string(db.getFileRev()) +
                    " with error:" + couchstore_strerror(errCode));
        }
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
    DbInfo info = getDbInfo(vbid);
    return DBFileInfo{info.file_size, info.space_used};
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
    return getDbInfo(vbid).doc_count;
}

RollbackResult CouchKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::shared_ptr<RollbackCB> cb) {
    DbHolder db(*this);
    DbInfo info;
    couchstore_error_t errCode;

    // Open the vbucket's file and determine the latestSeqno persisted.
    errCode = openDB(vbid, db, (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY);
    std::stringstream dbFileName;
    dbFileName << dbname << "/" << vbid.get() << ".couch." << db.getFileRev();

    if (errCode == COUCHSTORE_SUCCESS) {
        errCode = couchstore_db_info(db, &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::rollback: couchstore_db_info error:{}, "
                    "name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
            return RollbackResult(false);
        }
    } else {
        logger.warn("CouchKVStore::rollback: openDB error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
        return RollbackResult(false);
    }
    uint64_t latestSeqno = info.last_sequence;

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
    DbHolder newdb(*this);
    errCode = openDB(vbid, newdb, 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::rollback: openDB#2 error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
        return RollbackResult(false);
    }

    while (info.last_sequence > rollbackSeqno) {
        errCode = couchstore_rewind_db_header(newdb);
        if (errCode != COUCHSTORE_SUCCESS) {
            // rewind_db_header cleans up (frees DB) on error; so
            // release db in DbHolder to prevent a double-free.
            newdb.releaseDb();
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
        errCode = couchstore_db_info(newdb, &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::rollback: couchstore_db_info error:{}, "
                    "name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
            return RollbackResult(false);
        }
    }

    // Count how many updates we need to discard to rollback to the Rollback
    // Header. If this is too many; then prefer to discard everything (than
    // have to patch up a large amount of in-memory data).
    uint64_t rollbackSeqCount = 0;
    errCode = couchstore_changes_count(
            db, info.last_sequence, latestSeqno, &rollbackSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::rollback: "
                "couchstore_changes_count#2({}, {}) "
                "error:{} [{}], {}, rev:{}",
                info.last_sequence,
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
    cb->setDbHeader(newdb);
    auto cl = std::make_shared<NoLookupCallback>();
    ScanContext* ctx = initScanContext(cb, cl, vbid, info.last_sequence+1,
                                       DocumentFilter::ALL_ITEMS,
                                       ValueFilter::KEYS_ONLY);
    scan_error_t error = scan(ctx);
    destroyScanContext(ctx);

    if (error != scan_success) {
        return RollbackResult(false);
    }

    if (readVBStateAndUpdateCache(newdb, vbid).status !=
        ReadVBStateStatus::Success) {
        return RollbackResult(false);
    }
    cachedDeleteCount[vbid.get()] = info.deleted_count;
    cachedDocCount[vbid.get()] = info.doc_count;

    //Append the rewinded header to the database file
    errCode = couchstore_commit(newdb);

    if (errCode != COUCHSTORE_SUCCESS) {
        return RollbackResult(false);
    }

    vbucket_state* vb_state = getVBucketState(vbid);
    return RollbackResult(true,
                          vb_state->highSeqno,
                          vb_state->lastSnapStart,
                          vb_state->lastSnapEnd);
}

int populateAllKeys(Db *db, DocInfo *docinfo, void *ctx) {
    AllKeysCtx *allKeysCtx = (AllKeysCtx *)ctx;
    auto key = makeDiskDocKey(docinfo->id);
    (allKeysCtx->cb)->callback(key);
    if (--(allKeysCtx->count) <= 0) {
        //Only when count met is less than the actual number of entries
        return COUCHSTORE_ERROR_CANCEL;
    }
    return COUCHSTORE_SUCCESS;
}

ENGINE_ERROR_CODE
CouchKVStore::getAllKeys(Vbid vbid,
                         const DiskDocKey& start_key,
                         uint32_t count,
                         std::shared_ptr<Callback<const DiskDocKey&>> cb) {
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

    std::string fname = dbname + "/" + std::to_string(vbucket.get()) +
                        ".couch." + std::to_string(fRev);
    cb::io::sanitizePath(fname);
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

void CouchKVStore::removeCompactFile(const std::string& dbname, Vbid vbid) {
    std::string dbfile =
            getDBFileName(dbname, vbid, (*dbFileRevMap)[vbid.get()]);
    std::string compact_file = dbfile + ".compact";

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

std::unique_ptr<KVFileHandle, KVFileHandleDeleter> CouchKVStore::makeFileHandle(
        Vbid vbid) {
    std::unique_ptr<CouchKVFileHandle, KVFileHandleDeleter> db(
            new CouchKVFileHandle(*this));
    // openDB logs errors
    openDB(vbid, db->getDbHolder(), COUCHSTORE_OPEN_FLAG_RDONLY);

    return std::move(db);
}

void CouchKVStore::freeFileHandle(KVFileHandle* kvFileHandle) const {
    delete static_cast<CouchKVFileHandle*>(kvFileHandle);
}

void CouchKVStore::prepareToCreateImpl(Vbid vbid) {
    if (!isReadOnly()) {
        (*dbFileRevMap)[vbid.get()]++;
    }
}

uint64_t CouchKVStore::prepareToDeleteImpl(Vbid vbid) {
    // Clear the stats so it looks empty (real deletion of the disk data occurs
    // later)
    cachedDocCount[vbid.get()] = 0;
    cachedDeleteCount[vbid.get()] = 0;
    cachedFileSize[vbid.get()] = 0;
    cachedSpaceUsed[vbid.get()] = 0;
    return (*dbFileRevMap)[vbid.get()];
}

CouchKVStore::LocalDocHolder CouchKVStore::readLocalDoc(
        Db& db, const std::string& name) {
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

couchstore_error_t CouchKVStore::writeLocalDoc(Db& db,
                                               const std::string& name,
                                               cb::const_char_buffer data) {
    LocalDoc lDoc;
    lDoc.id.buf = const_cast<char*>(name.data());
    lDoc.id.size = name.size();

    lDoc.json.buf = const_cast<char*>(data.data());
    lDoc.json.size = data.size();
    lDoc.deleted = 0;

    couchstore_error_t errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::writeLocalDoc (name:{}, size:{}}) "
                "couchstore_save_local_document "
                "error:{} [{}]",
                name,
                data.size(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }

    return errCode;
}

couchstore_error_t CouchKVStore::deleteLocalDoc(Db& db,
                                                const std::string& name) {
    LocalDoc lDoc{};
    lDoc.id.buf = const_cast<char*>(name.data());
    lDoc.id.size = name.size();
    lDoc.deleted = 1;

    couchstore_error_t errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::deleteLocalDoc (name:{}) "
                "couchstore_save_local_document "
                "error:{} [{}]",
                name,
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }
    return errCode;
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

size_t CouchKVStore::getDroppedCollectionCount(Db& db) {
    auto dropped = readLocalDoc(db, Collections::droppedCollectionsName);

    if (!dropped.getLocalDoc()) {
        return {};
    }

    return Collections::KVStore::decodeDroppedCollections(dropped.getBuffer())
            .size();
}

couchstore_error_t CouchKVStore::updateCollectionsMeta(
        Db& db, Collections::VB::Flush& collectionsFlush) {
    auto err = updateManifestUid(db);
    if (err != COUCHSTORE_SUCCESS) {
        return err;
    }

    // If the updateOpenCollections reads the dropped collections, it can pass
    // them via this optional to updateDroppedCollections, thus we only read
    // the dropped list once per update.
    boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
            dropped;

    if (!collectionsMeta.collections.empty() ||
        !collectionsMeta.droppedCollections.empty()) {
        auto rv = updateOpenCollections(db);
        if (rv.first != COUCHSTORE_SUCCESS) {
            return rv.first;
        }
        // move the dropped list read here out so it doesn't need reading again
        dropped = std::move(rv.second);
    }

    if (!collectionsMeta.droppedCollections.empty()) {
        err = updateDroppedCollections(db, dropped);
        if (err != COUCHSTORE_SUCCESS) {
            return err;
        }
        collectionsFlush.setNeedsPurge();
    }

    if (!collectionsMeta.scopes.empty() ||
        !collectionsMeta.droppedScopes.empty()) {
        err = updateScopes(db);
        if (err != COUCHSTORE_SUCCESS) {
            return err;
        }
    }

    collectionsMeta.clear();
    return COUCHSTORE_SUCCESS;
}

couchstore_error_t CouchKVStore::updateManifestUid(Db& db) {
    // write back, no read required
    auto buf = Collections::KVStore::encodeManifestUid(collectionsMeta);
    return writeLocalDoc(
            db,
            Collections::manifestName,
            {reinterpret_cast<const char*>(buf.data()), buf.size()});
}

std::pair<couchstore_error_t,
          std::vector<Collections::KVStore::DroppedCollection>>
CouchKVStore::updateOpenCollections(Db& db) {
    auto droppedCollections = getDroppedCollections(db);
    auto collections = readLocalDoc(db, Collections::openCollectionsName);
    cb::const_byte_buffer empty;
    auto buf = Collections::KVStore::encodeOpenCollections(
            droppedCollections,
            collectionsMeta,
            collections.getLocalDoc() ? collections.getBuffer() : empty);

    return {writeLocalDoc(
                    db,
                    Collections::openCollectionsName,
                    {reinterpret_cast<const char*>(buf.data()), buf.size()}),
            droppedCollections};
}

couchstore_error_t CouchKVStore::updateDroppedCollections(
        Db& db,
        boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
                dropped) {
    for (const auto& drop : collectionsMeta.droppedCollections) {
        // Delete the 'stats' document for the collection
        deleteCollectionStats(db, drop.collectionId);
    }

    // If the input 'dropped' is not initialised we must read the dropped
    // collection data
    if (!dropped.is_initialized()) {
        dropped = getDroppedCollections(db);
    }

    auto buf = Collections::KVStore::encodeDroppedCollections(collectionsMeta,
                                                              dropped);
    return writeLocalDoc(
            db,
            Collections::droppedCollectionsName,
            {reinterpret_cast<const char*>(buf.data()), buf.size()});
}

couchstore_error_t CouchKVStore::updateScopes(Db& db) {
    auto scopes = readLocalDoc(db, Collections::scopesName);
    cb::const_byte_buffer empty;
    auto buf = encodeScopes(collectionsMeta,
                            scopes.getLocalDoc() ? scopes.getBuffer() : empty);
    return writeLocalDoc(
            db,
            Collections::scopesName,
            {reinterpret_cast<const char*>(buf.data()), buf.size()});
}

/* end of couch-kvstore.cc */
