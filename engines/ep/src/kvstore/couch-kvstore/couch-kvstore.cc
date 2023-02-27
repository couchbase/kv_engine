/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "couch-kvstore.h"

#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "couch-kvstore-config.h"
#include "couch-kvstore-db-holder.h"
#include "diskdockey.h"
#include "ep_time.h"
#include "getkeys.h"
#include "item.h"
#include "kvstore/kvstore_config.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore/persistence_callback.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "rollback_result.h"
#include "statistics/cbstat_collector.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <JSON_checker.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>

#include <spdlog/common.h>
#include <charconv>
#include <memory>
#include <shared_mutex>
#include <utility>

static int bySeqnoScanCallback(Db* db, DocInfo* docinfo, void* ctx);
static int byIdScanCallback(Db* db, DocInfo* docinfo, void* ctx);

static int getMultiCallback(Db* db, DocInfo* docinfo, void* ctx);

static bool endWithCompact(const std::string &filename) {
    const std::string suffix{".compact"};
    const auto pos = filename.rfind(suffix);
    return pos != std::string::npos && (pos + suffix.size()) == filename.size();
}

static std::vector<std::string> discoverDbFiles(const std::string& dir) {
    auto files = cb::io::findFilesContaining(dir, ".couch.");
    std::vector<std::string>::iterator ii;
    std::vector<std::string> filenames;
    for (ii = files.begin(); ii != files.end(); ++ii) {
        if (!endWithCompact(*ii)) {
            filenames.push_back(*ii);
        }
    }
    return filenames;
}

static bool allDigit(const std::string& input) {
    size_t numchar = input.length();
    for(size_t i = 0; i < numchar; ++i) {
        if (!isdigit(input[i])) {
            return false;
        }
    }
    return !input.empty();
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

static std::pair<couchstore_error_t, nlohmann::json> getLocalVbState(Db& db) {
    auto [status, doc] =
            cb::couchstore::openLocalDocument(db, LocalDocKey::vbstate);
    if (status == COUCHSTORE_SUCCESS) {
        try {
            auto view = std::string_view{doc->json.buf, doc->json.size};
            return {status, nlohmann::json::parse(view)};
        } catch (const std::exception&) {
            return {COUCHSTORE_ERROR_CORRUPT, {}};
        }
    }
    return {status, {}};
}

/**
 * Helper function to create an Item from couchstore DocInfo & related types.
 * @param vbid The vbucket the Item belongs to
 * @param docinfo The id, db_seq and rev_seq are used in the Item construction
 * @param metadata The metadata to use to create the Item
 * @param value an optional value, this allows "key-only" paths to be
 *        distinguishable from a value of 0 length.
 */
static std::unique_ptr<Item> makeItemFromDocInfo(Vbid vbid,
                                                 const DocInfo& docinfo,
                                                 const MetaData& metadata,
                                                 std::optional<sized_buf> value,
                                                 bool fetchedCompressed) {
    auto datatype = metadata.getDataType();
    // If document was fetched compressed, then set SNAPPY flag for non-zero
    // length documents.
    if (fetchedCompressed && (value.value_or(sized_buf{}).size > 0)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }

    value_t body;
    if (value) {
        body.reset(TaggedPtr<Blob>(Blob::New(value->buf, value->size),
                                   TaggedPtrBase::NoTagValue));
    }
    // Strip off the DurabilityPrepare namespace (if present) from the persisted
    // dockey before we create the in-memory item.
    auto item = std::make_unique<Item>(makeDiskDocKey(docinfo.id).getDocKey(),
                                       metadata.getFlags(),
                                       metadata.getExptime(),
                                       body,
                                       datatype,
                                       metadata.getCas(),
                                       docinfo.db_seq,
                                       vbid,
                                       docinfo.rev_seq);
    if (docinfo.deleted) {
        item->setDeleted(metadata.getDeleteSource());
    }

    KVStore::checkAndFixKVStoreCreatedItem(*item);

    if (metadata.getVersionInitialisedFrom() == MetaData::Version::V3) {
        // Metadata is from a SyncWrite - update the Item appropriately.
        switch (metadata.getDurabilityOp()) {
        case queue_op::mutation:
            // While the current version no longer uses V3 metadata for
            // mutations, prior to MB-48033 we did; so need to handle this case
            // - we can just ignore it as nothing needs to be extracted from the
            // V3 meta.
            break;
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

static std::string getDBFileName(const std::string& dbname,
                                 Vbid vbid,
                                 uint64_t rev) {
    return dbname + "/" + std::to_string(vbid.get()) + ".couch." +
           std::to_string(rev);
}

/// @returns the document ID used to store cid
std::string CouchKVStore::getCollectionStatsLocalDocId(CollectionID cid) {
    return fmt::format("|{:#x}|", uint32_t(cid));
}

/// @returns the cid from a document-ID (see getCollectionStatsLocalDocId)
CollectionID CouchKVStore::getCollectionIdFromStatsDocId(std::string_view id) {
    unsigned int result{0};
    // id is expected to be "|0x<id>|" min size is 5, i.e. "|0x0|"
    if (id.size() >= 5 && id[0] == '|' && id[1] == '0' && id[2] == 'x') {
        id.remove_prefix(3);
        auto [p, ec] = std::from_chars(id.data(), id.data() + id.size(), result, 16);
        (void)p;
        if (ec == std::errc()) {
            return result;
        }
    }
    throw std::logic_error("getCollectionIdFromStatsDocId: cannot convert id:" +
                           std::string(id));
}

struct GetMultiCbCtx {
    GetMultiCbCtx(const CouchKVStore& c, Vbid v, vb_bgfetch_queue_t& f)
        : cks(c), vbId(v), fetches(f) {
    }

    const CouchKVStore& cks;
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

class CouchKVFileHandle : public ::KVFileHandle {
public:
    explicit CouchKVFileHandle(const CouchKVStore& kvstore) : db(kvstore) {
    }

    ~CouchKVFileHandle() override = default;

    DbHolder& getDbHolder() {
        return db;
    }

    Db* getDb() const {
        return db.getDb();
    }

private:
    DbHolder db;
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

    // Prepare namespace operations (prepares and aborts) use the V3 metadata
    // whilst commit namespace operations (mutations and commits) use the V1
    // metadata
    const auto isPrepareNamspace =
            (item->getOperation() == queue_op::pending_sync_write ||
             item->getOperation() == queue_op::abort_sync_write);

    if (isPrepareNamspace) {
        // Durability op is only part of V3 metadata.
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

    if (item->isAbort()) {
        meta.setCompletedProperties(item->getPrepareSeqno());
    }

    dbDocInfo.db_seq = item->getBySeqno();

    // Now allocate space to hold the meta and get it ready for storage
    dbDocInfo.rev_meta.size = MetaData::getMetaDataSize(
            isPrepareNamspace ? MetaData::Version::V3 : MetaData::Version::V1);
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

CouchKVStore::CouchKVStore(const CouchKVStoreConfig& config)
    : CouchKVStore(config, *couchstore_get_default_file_ops()) {
}

CouchKVStore::CouchKVStore(const CouchKVStoreConfig& config,
                           FileOpsInterface& ops,
                           std::shared_ptr<RevisionMap> revMap)
    : configuration(config),
      dbname(config.getDBName()),
      dbFileRevMap(std::move(revMap)),
      logger(config.getLogger()),
      base_ops(ops) {
    statCollectingFileOps = getCouchstoreStatsOps(fsStats, base_ops);
    statCollectingFileOpsCompaction =
            getCouchstoreStatsOps(fsStatsCompaction, base_ops);

    // To save memory only allocate counters for the number of vBuckets that
    // this shard will have to deal with
    auto cacheSize = getCacheSize();
    vbCompactionRunning = std::vector<std::atomic_bool>(cacheSize);
    vbAbortCompaction = std::vector<std::atomic_bool>(cacheSize);
    cachedDeleteCount.assign(cacheSize, 0);
    cachedFileSize.assign(cacheSize, 0);
    cachedOnDiskPrepareSize.assign(cacheSize, 0);
    cachedSpaceUsed.assign(cacheSize, 0);

    cachedVBStates.resize(cacheSize);
    inTransaction = std::vector<std::atomic_bool>(cacheSize);
}

// Helper function to create and resize the 'locked' vector
std::shared_ptr<CouchKVStore::RevisionMap> CouchKVStore::makeRevisionMap(
        const CouchKVStoreConfig& config) {
    auto map = std::make_shared<RevisionMap>();
    (*map->wlock()).resize(config.getCacheSize());
    return map;
}

CouchKVStore::CouchKVStore(const CouchKVStoreConfig& config,
                           FileOpsInterface& ops)
    : CouchKVStore(config, ops, makeRevisionMap(config)) {
    // 1) populate the dbFileRevMap which can remove old revisions, this returns
    //    a map, which the keys (vbid) will be needed for step 3 and 4.
    auto map = populateRevMapAndRemoveStaleFiles();

    // 2) clean up any .compact files
    for (const auto& id : map) {
        maybeRemoveCompactFile(dbname, id.first);
    }

    // 3) continue to intialise the store (reads vbstate etc...)
    initialize(map);
}

size_t CouchKVStore::getMemFootPrint() const {
    return KVStore::getMemFootPrint() + fsStats.getMemFootPrint() +
           fsStatsCompaction.getMemFootPrint();
}

void CouchKVStore::resetStats() {
    KVStore::resetStats();
    fsStats.reset();
    fsStatsCompaction.reset();
}

void CouchKVStore::addStats(const AddStatFn& add_stat, const void* c) const {
    KVStore::addStats(add_stat, c);
    const auto prefix = getStatsPrefix();

    const size_t read = fsStats.totalBytesRead.load() +
                        fsStatsCompaction.totalBytesRead.load();
    add_prefixed_stat(prefix, "io_total_read_bytes", read, add_stat, c);

    const size_t written = fsStats.totalBytesWritten.load() +
                           fsStatsCompaction.totalBytesWritten.load();
    add_prefixed_stat(prefix, "io_total_write_bytes", written, add_stat, c);

    // Flusher Write Amplification - ratio of bytes written to disk by
    // flusher to "useful" user data written - i.e. doesn't include bytes
    // written later by compaction (after initial flush). Used to measure
    // the impact of KVstore on persistTo times.
    const double flusherWriteAmp = double(fsStats.totalBytesWritten.load()) /
                                   st.io_document_write_bytes;
    add_prefixed_stat(prefix,
                      "io_flusher_write_amplification",
                      flusherWriteAmp,
                      add_stat,
                      c);

    // Total Write Amplification - ratio of total bytes written to disk
    // to "useful" user data written over entire disk lifecycle. Includes
    // bytes during initial item flush to disk  and compaction.
    // Used to measure the overall write amplification.
    const double totalWriteAmp = double(written) / st.io_document_write_bytes;
    add_prefixed_stat(
            prefix, "io_total_write_amplification", totalWriteAmp, add_stat, c);

    add_prefixed_stat(prefix,
                      "io_compaction_read_bytes",
                      fsStatsCompaction.totalBytesRead,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "io_compaction_write_bytes",
                      fsStatsCompaction.totalBytesWritten,
                      add_stat,
                      c);
}

void CouchKVStore::addTimingStats(const AddStatFn& add_stat,
                                  const CookieIface* c) const {
    KVStore::addTimingStats(add_stat, c);

    const auto prefix = getStatsPrefix();

    // file ops stats.
    add_prefixed_stat(prefix, "fsReadTime", fsStats.readTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteTime", fsStats.writeTimeHisto, add_stat, c);
    add_prefixed_stat(prefix, "fsSyncTime", fsStats.syncTimeHisto, add_stat, c);
    add_prefixed_stat(prefix, "fsReadSize", fsStats.readSizeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteSize", fsStats.writeSizeHisto, add_stat, c);
    add_prefixed_stat(prefix, "fsReadSeek", fsStats.readSeekHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsReadCount", fsStats.readCountHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteCount", fsStats.writeCountHisto, add_stat, c);
}

void CouchKVStore::initialize(
        const std::unordered_map<Vbid, std::unordered_set<uint64_t>>& map) {
    for (const auto& [vbid, revisions] : map) {
        (void)revisions;
        DbHolder db(*this);
        const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
        const auto errCode = openDB(vbid, db, options);

        if (errCode != COUCHSTORE_SUCCESS) {
            logOpenError(__func__,
                         spdlog::level::level_enum::warn,
                         errCode,
                         vbid,
                         db.getFilename(),
                         options);

            const auto msg = "CouchKVStore::initialize: openDB error:" +
                             std::string(couchstore_strerror(errCode)) +
                             ", file_name:" +
                             getDBFileName(dbname, vbid, db.getFileRev());
            throw std::runtime_error(msg);
        }

        const auto [readRes, state] = readVBStateAndUpdateCache(db, vbid);

        switch (readRes) {
        case ReadVBStateStatus::Success:
            // Success, update stats
            ++st.numLoadedVb;
            readHeaderAndUpdateCache(*db.getDb(), vbid);
            cachedOnDiskPrepareSize[getCacheSlot(vbid)] =
                    state.getOnDiskPrepareBytes();
            break;
        case ReadVBStateStatus::NotFound:
            // No vBucket state found. It's possible for a vBucket header to
            // exist on disk without a state if we crashed or hit some IO error
            // during a flush. Continue as though it does not exist so that we
            // can rebuild it correctly, rather than warmup some odd state.
            logger.warn(
                    "CouchKVStore::initialize found vBucket with no "
                    "state, file_name:{}",
                    getDBFileName(dbname, vbid, db.getFileRev()));
            continue;
        case ReadVBStateStatus::CorruptSnapshot:
            // Note: The  CorruptSnapshot error is a special case. That is
            // generated for replica/pending vbuckets only and we want to
            // continue as if the vbucket does not exist so it gets rebuilt from
            // the active.
            logger.warn(
                    "CouchKVStore::initialize: found corrupt snapshot, "
                    "file_name:{}",
                    getDBFileName(dbname, vbid, db.getFileRev()));
            continue;
        case ReadVBStateStatus::JsonInvalid:
        case ReadVBStateStatus::Error:
            const auto msg = "CouchKVStore::initialize: readVBState error:" +
                             to_string(readRes) + ", file_name:" +
                             getDBFileName(dbname, vbid, db.getFileRev());
            logger.error(msg);
            throw std::runtime_error(msg);
        }
    }
}

CouchKVStore::~CouchKVStore() {
    close();
}

void CouchKVStore::set(TransactionContext& txnCtx, queued_item item) {
    checkIfInTransaction(txnCtx.vbid, "CouchKVStore::set");

    // each req will be de-allocated after commit
    auto& ctx = dynamic_cast<CouchKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqsQ.emplace_back(std::move(item));
}

GetValue CouchKVStore::get(const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openDB(vb, db, options);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;

        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vb,
                     db.getFilename(),
                     options);

        return GetValue(nullptr, couchErr2EngineErr(errCode));
    }

    GetValue gv = getWithHeader(db, key, vb, filter);
    return gv;
}

GetValue CouchKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     ValueFilter filter) const {
    // const_cast away here, the lower level couchstore does not use const
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(
            const_cast<KVFileHandle&>(kvFileHandle));
    return getWithHeader(couchKvHandle.getDbHolder(), key, vb, filter);
}

GetValue CouchKVStore::getWithHeader(DbHolder& db,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     ValueFilter filter) const {
    auto start = std::chrono::steady_clock::now();
    DocInfo *docInfo = nullptr;
    GetValue rv;

    sized_buf id = to_sized_buf(key);

    couchstore_error_t errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                                          id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (filter != ValueFilter::KEYS_ONLY) {
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
        errCode = fetchDoc(db, docInfo, rv, vb, filter);
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

void CouchKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const {
    if (itms.empty()) {
        return;
    }
    int numItems = itms.size();

    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    auto errCode = openDB(vb, db, options);
    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vb,
                     db.getFilename(),
                     options);

        st.numGetFailure += numItems;
        for (auto& item : itms) {
            item.second.value.setStatus(cb::engine_errc::not_my_vbucket);
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
                            ValueFilter filter,
                            const KVStore::GetRangeCb& cb) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    auto errCode = openDB(vb, db, options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vb,
                     db.getFilename(),
                     options);

        throw std::runtime_error("CouchKVStore::getRange: openDB error for " +
                                 vb.to_string() +
                                 " - couchstore returned error: " +
                                 couchstore_strerror(errCode));
    }

    // Trampoine's state. Cannot use lambda capture as couchstore expects a
    // C-style callback.
    struct TrampolineState {
        Vbid vb;
        const ValueFilter filter;
        const DiskDocKey& endKey;
        const KVStore::GetRangeCb& userFunc;
    };
    TrampolineState trampoline_state{vb, filter, endKey, cb};

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

        // Fetch document value if requested.
        const bool fetchCompressed =
                (state.filter == ValueFilter::VALUES_COMPRESSED);
        Doc* doc = nullptr;
        if (state.filter != ValueFilter::KEYS_ONLY) {
            const couchstore_open_options openOptions =
                    fetchCompressed ? 0 : DECOMPRESS_DOC_BODIES;
            auto errCode = couchstore_open_doc_with_docinfo(
                    db, docinfo, &doc, openOptions);
            if (errCode != COUCHSTORE_SUCCESS) {
                // Failed to fetch document - cancel couchstore_docinfos_by_id
                // scan.
                return errCode;
            }
        }

        std::optional<sized_buf> value;
        if (doc) {
            value = doc->data;
        }
        state.userFunc(GetValue{makeItemFromDocInfo(
                state.vb, *docinfo, *metadata, value, fetchCompressed)});
        if (doc) {
            couchstore_free_document(doc);
        }
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

void CouchKVStore::del(TransactionContext& txnCtx, queued_item item) {
    checkIfInTransaction(txnCtx.vbid, "CouchKVStore::del");

    auto& ctx = dynamic_cast<CouchKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqsQ.emplace_back(std::move(item));
}

void CouchKVStore::delVBucket(Vbid vbucket,
                              std::unique_ptr<KVStoreRevision> fileRev) {
    unlinkCouchFile(vbucket, fileRev->getRevision());
}

std::vector<vbucket_state *> CouchKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
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
                               CompactionContext& ctx,
                               time_t currtime) {
    std::optional<sized_buf> data;
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
                        "notify_expired_item: failed to inflate document with "
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

    auto vbid = ctx.getVBucket()->getId();
    auto it = makeItemFromDocInfo(vbid, info, metadata, data, false);

    ctx.expiryCallback->callback(*it, currtime);

    return COUCHSTORE_SUCCESS;
}

static int time_purge_hook(Db& d,
                           DocInfo* info,
                           sized_buf item,
                           CompactionContext& ctx) {
    if (info == nullptr) {
        // Compaction finished
        return couchstore_set_purge_seq(&d, ctx.getRollbackPurgeSeqno());
    }

    if (ctx.isShuttingDown()) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    auto metadata = MetaDataFactory::createMetaData(info->rev_meta);
    uint32_t exptime = metadata->getExptime();

    // We need to check both committed and prepared documents - if the
    // collection has been logically deleted then we need to discard
    // both types of keys.
    // As such use the docKey (i.e. without any DurabilityPrepare
    // namespace) when checking if logically deleted;
    auto diskKey = makeDiskDocKey(info->id);
    const auto& docKey = diskKey.getDocKey();

    // Define a helper function for updating the collection sizes
    auto maybeAccountForPurgedCollectionData = [&ctx, docKey, info] {
        if (!docKey.isInSystemCollection()) {
            auto itr = ctx.stats.collectionSizeUpdates.emplace(
                    docKey.getCollectionID(), 0);
            itr.first->second += info->getTotalSize();
        }
    };

    if (ctx.eraserContext->isLogicallyDeleted(
                docKey, info->deleted, int64_t(info->db_seq))) {
        // Inform vb that the key@seqno is dropped
        try {
            ctx.droppedKeyCb(diskKey,
                             int64_t(info->db_seq),
                             metadata->isAbort(),
                             ctx.highCompletedSeqno);
        } catch (const std::exception& e) {
            EP_LOG_WARN("time_purge_hook: droppedKeyCb exception: {}",
                        e.what());
            return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
        }
        if (metadata->isPrepare()) {
            ctx.stats.preparesPurged++;
            ctx.stats.prepareBytesPurged += info->getTotalSize();

            // Track nothing for the individual collection as the stats doc has
            // already been deleted.
        } else if (!docKey.isInSystemCollection()) {
            // item in a collection was dropped, track how many for item count
            // adjustment.
            if (!info->deleted) {
                ctx.stats.collectionsItemsPurged++;
            } else {
                ctx.stats.collectionsDeletedItemsPurged++;
            }
        }
        // No need to make 'disk-size' changes here as the collection has gone.
        ctx.purgedItemCtx->purgedItem(PurgedItemType::LogicalDelete,
                                      info->db_seq);
        return COUCHSTORE_COMPACT_DROP_ITEM;
    } else if (docKey.isInSystemCollection()) {
        ctx.eraserContext->processSystemEvent(
                docKey, SystemEvent(metadata->getFlags()));
    }

    if (info->deleted) {
        const auto infoDb = cb::couchstore::getHeader(d);
        if (info->db_seq != infoDb.updateSeqNum) {
            bool purgeItem = ctx.compactConfig.drop_deletes;

            /**
             * MB-30015: Found a tombstone whose expiry time is 0. Log this
             * message because tombstones are expected to have a non-zero
             * expiry time
             */
            if (!exptime) {
                EP_LOG_WARN_RAW(
                        "time_purge_hook: tombstone found with an"
                        " expiry time of 0");
            }

            if (exptime < ctx.compactConfig.purge_before_ts &&
                (exptime || !ctx.compactConfig.retain_erroneous_tombstones) &&
                (!ctx.compactConfig.purge_before_seq ||
                 info->db_seq <= ctx.compactConfig.purge_before_seq)) {
                purgeItem = true;
            }

            if (purgeItem) {
                // Update stats and return
                ctx.stats.tombstonesPurged++;
                maybeAccountForPurgedCollectionData();
                ctx.purgedItemCtx->purgedItem(PurgedItemType::Tombstone,
                                              info->db_seq);
                return COUCHSTORE_COMPACT_DROP_ITEM;
            }
        }
    } else {
        // We can remove any prepares that have been completed. This works
        // because we send Mutations instead of Commits when streaming from
        // Disk so we do not need to send a Prepare message to keep things
        // consistent on a replica.
        if (metadata->isPrepare()) {
            if (info->db_seq <= ctx.highCompletedSeqno) {
                ctx.stats.preparesPurged++;
                ctx.stats.prepareBytesPurged += info->getTotalSize();

                // Decrement individual collection disk sizes as we track
                // prepares in the value. We don't do this at collection drop
                // as the stat doc will have already been deleted (for
                // couchstore)
                maybeAccountForPurgedCollectionData();
                ctx.purgedItemCtx->purgedItem(PurgedItemType::Prepare,
                                              info->db_seq);
                return COUCHSTORE_COMPACT_DROP_ITEM;
            }

            // Just keep the item if not complete. We should not attempt to
            // expire a prepare and we do not want to update the bloom
            // filter for a prepare either as that could cause us to run
            // unnecessary BGFetches.
            return COUCHSTORE_COMPACT_KEEP_ITEM;
        }

        time_t timeToExpireFrom;
        if (ctx.timeToExpireFrom) {
            timeToExpireFrom = ctx.timeToExpireFrom.value();
        } else {
            timeToExpireFrom = ep_real_time();
        }

        if (exptime && exptime < timeToExpireFrom && metadata->isCommit()) {
            int ret;
            metadata->setDeleteSource(DeleteSource::TTL);
            try {
                ret = notify_expired_item(
                        *info, *metadata, item, ctx, timeToExpireFrom);
            } catch (const std::bad_alloc&) {
                EP_LOG_WARN_RAW("time_purge_hook: memory allocation failed");
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

    if (ctx.bloomFilterCallback) {
        bool deleted = info->deleted;
        auto key = makeDiskDocKey(info->id);

        try {
            auto vbid = ctx.getVBucket()->getId();
            ctx.bloomFilterCallback->callback(vbid, key.getDocKey(), deleted);
        } catch (std::runtime_error& re) {
            EP_LOG_WARN(
                    "time_purge_hook: exception occurred when invoking the "
                    "bloomfilter callback - Details: {}",
                    re.what());
        }
    }

    return COUCHSTORE_COMPACT_KEEP_ITEM;
}

CompactDBStatus CouchKVStore::compactDB(
        std::unique_lock<std::mutex>& vbLock,
        std::shared_ptr<CompactionContext> hook_ctx) {
    if (!hook_ctx->droppedKeyCb) {
        throw std::invalid_argument(
                "CouchKVStore::compactDB: droppedKeyCb must be set ");
    }

    auto vbid = hook_ctx->getVBucket()->getId();
    // Note that this isn't racy as we'll hold the vbucket lock when we're
    // calling the method for doing the check and when we'll set it to
    // true
    if (vbCompactionRunning[getCacheSlot(vbid)]) {
        EP_LOG_INFO(
                "CouchKVStore::compactDB: compaction already running for {}",
                vbid);
        // The API don't let me tell the caller that a compaction is
        // already running so I just have to fail it..
        return CompactDBStatus::Failed;
    }

    // Open the source VBucket database file
    DbHolder sourceDb(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    auto errCode = openDB(
            vbid, sourceDb, options, statCollectingFileOpsCompaction.get());
    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     sourceDb.getFilename(),
                     options);
        return CompactDBStatus::Failed;
    }
    const auto compact_file =
            getDBFileName(dbname, vbid, sourceDb.getFileRev()) + ".compact";

    vbCompactionRunning[getCacheSlot(vbid)] = true;
    vbAbortCompaction[getCacheSlot(vbid)] = false;

    auto status = CompactDBStatus::Failed;
    try {
        TRACE_EVENT1("CouchKVStore", "compactDB", "vbid", vbid.get());
        const auto start = std::chrono::steady_clock::now();
        status = compactDBInternal(
                sourceDb, compact_file, vbLock, hook_ctx.get());
        if (status == CompactDBStatus::Success) {
            st.compactHisto.add(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start));
        }
    } catch (const std::exception& le) {
        logger.error(
                "CouchKVStore::compactDB: exception while performing "
                "compaction for {}"
                " - Details: {}",
                vbid,
                le.what());
        status = CompactDBStatus::Failed;
    }

    if (status != CompactDBStatus::Success) {
        removeCompactFile(compact_file, vbid);
    }

    // Make sure we own the lock when we'll clear the variables (in the
    // case where an exception was thrown or a return without holding the
    // lock)
    if (!vbLock.owns_lock()) {
        vbLock.lock();
    }
    vbCompactionRunning[getCacheSlot(vbid)] = false;
    vbAbortCompaction[getCacheSlot(vbid)] = false;

    return status;
}

static FileInfo toFileInfo(const cb::couchstore::Header& info) {
    return FileInfo{
            info.docCount, info.deletedCount, info.fileSize, info.purgeSeqNum};
}

couchstore_error_t CouchKVStore::replayPrecommitHook(
        Db& db,
        CompactionReplayPrepareStats& prepareStats,
        const Collections::VB::FlushAccounting& collectionStats,
        uint64_t purge_seqno,
        CompactionContext& hook_ctx) {
    couchstore_set_purge_seq(&db, purge_seqno);
    auto [status, json] = getLocalVbState(db);
    if (status != COUCHSTORE_SUCCESS) {
        return status;
    }

    try {
        json["on_disk_prepares"] = std::to_string(prepareStats.onDiskPrepares);

        auto prepareBytes = json.find("on_disk_prepare_bytes");
        // only update if it's there..
        if (prepareBytes != json.end()) {
            json["on_disk_prepare_bytes"] =
                    std::to_string(prepareStats.onDiskPrepareBytes);
        }

        PendingLocalDocRequestQueue localDocQueue =
                replayPrecommitProcessDroppedCollections(
                        db, hook_ctx, collectionStats);

        try {
            // forEachCollection which had item(s) copied in the replay(s) the
            // stats (diskSize) needs an update.
            // 1) On disk
            // 2) in-memory
            //
            // For 1) we are adding updates to the localDocQueue
            // For 2) update the collectionStats in the hook_ctx which is then
            // read in the compaction complete callback
            collectionStats.forEachCollection(
                    [this, &localDocQueue, &memoryStats = hook_ctx.stats](
                            CollectionID cid,
                            const Collections::VB::PersistedStats& stats) {
                        // Set the value which will be used for in-memory update
                        // when compaction completes
                        memoryStats.collectionSizeUpdates[cid] = stats.diskSize;

                        // Place in local doc queue
                        localDocQueue.emplace_back(
                                getCollectionStatsLocalDocId(cid),
                                stats.getLebEncodedStats());
                    });
        } catch (const std::exception& e) {
            logger.warn(
                    "CouchKVStore::replayPrecommitHook failed "
                    "saveCollectionStats what:{}",
                    e.what());
            return COUCHSTORE_ERROR_CANCEL;
        }

        localDocQueue.emplace_back(std::string{LocalDocKey::vbstate},
                                   json.dump());

        return updateLocalDocuments(db, localDocQueue);
    } catch (const std::exception&) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    return COUCHSTORE_SUCCESS;
}

CouchKVStore::PendingLocalDocRequestQueue
CouchKVStore::replayPrecommitProcessDroppedCollections(
        Db& db,
        const CompactionContext& hook_ctx,
        const Collections::VB::FlushAccounting& collectionStats) {
    PendingLocalDocRequestQueue localDocQueue;

    // Do we need to generate a new dropped collection 'list'? One which loses
    // the collections processed in *this* compaction run, but keeps any new
    // data which may of been flushed whilst compacting.
    if (hook_ctx.eraserContext->needToUpdateCollectionsMetadata()) {
        // This compaction run processed dropped collections

        // 1) Get the current state from disk
        auto [getDroppedStatus, droppedCollections] = getDroppedCollections(db);
        if (getDroppedStatus != COUCHSTORE_SUCCESS) {
            throw std::runtime_error(
                    "CouchKVStore::replayPrecommitProcessDroppedCollections "
                    "failed getDroppedCollections status:" +
                    std::string(couchstore_strerror(getDroppedStatus)));
        }

        // 2) Generate a new flatbuffer document to write back
        auto fbData = Collections::VB::Flush::
                encodeRelativeComplementOfDroppedCollections(
                        droppedCollections,
                        hook_ctx.eraserContext->getDroppedCollections(),
                        hook_ctx.eraserContext->getEndSeqno());

        // 3) If the function returned data, write it, else the document is
        //    deleted.
        if (fbData.data()) {
            hook_ctx.eraserContext->setOnDiskDroppedDataExists(true);
            localDocQueue.emplace_back(
                    std::string{LocalDocKey::droppedCollections}, fbData);
        } else {
            // Need to ensure the 'dropped' list on disk is now gone
            hook_ctx.eraserContext->setOnDiskDroppedDataExists(false);
            localDocQueue.emplace_back(
                    std::string{LocalDocKey::droppedCollections},
                    CouchLocalDocRequest::IsDeleted{});
        }
    }

    // Any collections which were dropped in the replay need to have their stats
    // deleted from the target file - without doing this the compacted file
    // would incorrectly retain stats of the dropped collections.
    collectionStats.forEachDroppedCollection(
            [this, &localDocQueue](CollectionID id) {
                this->deleteCollectionStats(localDocQueue, id);
            });

    return localDocQueue;
}

/**
 * The callback hook called for all documents being copied over from the
 * old database to the new database as part of replay. We need to adjust the
 * on_disk_prepare stat based on the before and after states of the docs we've
 * changed.
 *
 * @param target The destination database
 * @param docInfo The document to check
 * @param prepareStats All the prepare stats changed during compaction[IN/OUT]
 * @return COUCHSTORE_SUCCESS on success, couchstore error otherwise
 */
static couchstore_error_t replayPreCopyHook(
        Db& target,
        const DocInfo* docInfo,
        CompactionReplayPrepareStats& prepareStats,
        Collections::VB::FlushAccounting& collectionStats) {
    if (docInfo) {
        auto metadata = MetaDataFactory::createMetaData(docInfo->rev_meta);

        // Always need to know what we're copying over (if it exists)
        auto [st, di] = cb::couchstore::openDocInfo(
                target, std::string_view{docInfo->id.buf, docInfo->id.size});

        // Reminder! An abort is a deleted (by the DocInfo flag) prepare. A
        // SyncDelete is an alive (not deleted in DocInfo) prepare with a
        // metadata bit set to deleted instead.
        if (metadata->isAbort()) {
            // We have an abort, may have to decrement onDiskPrepares if the
            // doc was previously a prepare (not deleted).
            if (st == COUCHSTORE_SUCCESS && !di->deleted) {
                --prepareStats.onDiskPrepares;
                prepareStats.onDiskPrepareBytes -= di->getTotalSize();
            }
        }

        auto diskDocKey = makeDiskDocKey(docInfo->id);
        if (metadata->isPrepare()) {
            // We have a prepare, may have to increment onDiskPrepares if it
            // didn't exist before or was an abort (deleted)
            // New prepare
            if (st == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
                ++prepareStats.onDiskPrepares;
                prepareStats.onDiskPrepareBytes += docInfo->getTotalSize();
            }

            if (st == COUCHSTORE_SUCCESS) {
                if (di->deleted) {
                    // Abort -> Prepare
                    ++prepareStats.onDiskPrepares;
                    prepareStats.onDiskPrepareBytes += docInfo->getTotalSize();
                } else {
                    // Prepare -> Prepare
                    auto delta = docInfo->getTotalSize() - di->getTotalSize();
                    prepareStats.onDiskPrepareBytes += delta;
                }
            }
        }

        // Finally do the collection stat accounting
        auto isCommitted =
                metadata->isCommit() ? IsCommitted::Yes : IsCommitted::No;
        auto isDeleted = docInfo->deleted ? IsDeleted::Yes : IsDeleted::No;
        if (di) {
            auto oldIsDeleted = di->deleted ? IsDeleted::Yes : IsDeleted::No;
            collectionStats.updateStats(diskDocKey.getDocKey(),
                                        docInfo->db_seq,
                                        isCommitted,
                                        isDeleted,
                                        docInfo->getTotalSize(),
                                        di->db_seq,
                                        oldIsDeleted,
                                        di->getTotalSize(),
                                        IsCompaction::Yes);

        } else {
            collectionStats.updateStats(diskDocKey.getDocKey(),
                                        docInfo->db_seq,
                                        isCommitted,
                                        isDeleted,
                                        docInfo->getTotalSize(),
                                        IsCompaction::Yes);
        }
    }
    return COUCHSTORE_SUCCESS;
}

couchstore_error_t CouchKVStore::replayPreCopyLocalDoc(
        Db& target,
        const DocInfo* localDocInfo,
        Collections::VB::FlushAccounting& collectionStats) {
    std::string_view documentId{localDocInfo->id.buf, localDocInfo->id.size};
    if (documentId.empty()) {
        logger.warn("CouchKVStore::replayPreCopyLocalDoc: empty id");
        return COUCHSTORE_ERROR_CANCEL;
    }
    // Look for collection stat docs
    if (documentId[0] == '|') {
        // This is a collection statistics doc, obtain collection-ID
        auto cid = getCollectionIdFromStatsDocId(documentId);
        // and load the stats
        auto [status, currentStats] =
                getCollectionStats(target, getCollectionStatsLocalDocId(cid));
        if (status == GetCollectionStatsStatus::Failed) {
            logger.warn(
                    "CouchKVStore::replayPreCopyLocalDoc: failed loading stats "
                    "for doc:{} cid:{}",
                    documentId,
                    cid);
            return COUCHSTORE_ERROR_CANCEL;
        }

        // preset the stats from the current value in the target file. This
        // gives a baseline for accounting changes copied over in the replay
        collectionStats.presetStats(cid, currentStats);
    }
    return COUCHSTORE_SUCCESS;
}

CompactDBStatus CouchKVStore::compactDBInternal(
        DbHolder& sourceDb,
        const std::string& compact_file,
        std::unique_lock<std::mutex>& vbLock,
        CompactionContext* hook_ctx) {
    // we may run the first part of the compaction without exclusive access
    vbLock.unlock();

    auto* def_iops = statCollectingFileOpsCompaction.get();
    const auto vbid = hook_ctx->getVBucket()->getId();
    const auto new_rev = sourceDb.getFileRev() + 1;
    const auto dbfile = getDBFileName(dbname, vbid, sourceDb.getFileRev());

    hook_ctx->stats.pre =
            toFileInfo(cb::couchstore::getHeader(*sourceDb.getDb()));

    couchstore_open_flags flags(COUCHSTORE_COMPACT_FLAG_UPGRADE_DB);
    /**
     * This flag disables IO buffering in couchstore which means
     * file operations will trigger syscalls immediately. This has
     * a detrimental impact on performance and is only intended
     * for testing.
     */
    if (!configuration.getBuffered()) {
        flags |= COUCHSTORE_OPEN_FLAG_UNBUFFERED;
    }

    // Should automatic fsync() be configured for compaction?
    const auto periodicSyncBytes = configuration.getPeriodicSyncBytes();
    if (periodicSyncBytes != 0) {
        flags |= couchstore_encode_periodic_sync_flags(periodicSyncBytes);
    }

    // Remove the destination file (in the case there is a leftover somewhere
    // IF one exist compact would start writing into the file, but if the
    // new compaction creates a smaller file people using the file may pick
    // up an old and stale file header at the end of the file!)
    // (couchstore don't remove the target file before starting compaction
    // as callers _may_ use the existence of those for locking purposes)
    removeCompactFile(compact_file, vbid);

    couchstore_error_t errCode;

    // Perform COMPACTION of vbucket.couch.rev into
    // vbucket.couch.rev.compact
    if (configuration.isPitrEnabled()) {
        std::chrono::nanoseconds timestamp =
                std::chrono::system_clock::now().time_since_epoch() -
                configuration.getPitrMaxHistoryAge();
        const auto delta = configuration.getPitrGranularity();
        const auto seconds =
                std::chrono::duration_cast<std::chrono::seconds>(timestamp);
        const auto usec = std::chrono::duration_cast<std::chrono::microseconds>(
                timestamp - seconds);

        EP_LOG_INFO(
                "{}: Full compaction to {}, incremental with granularity of "
                "{} sec",
                vbid.to_string(),
                ISOTime::generatetimestamp(seconds.count(), usec.count()),
                std::chrono::duration_cast<std::chrono::seconds>(
                        configuration.getPitrGranularity())
                        .count());

        // @todo I'm not sure if updating the bloom filter as part of
        //       traversing historical data is what we want to do :S
        hook_ctx->bloomFilterCallback = {};

        CompactionReplayPrepareStats prepareStats;

        Collections::VB::FlushAccounting collectionStats;
        collectionStats.setPiTR(IsPiTR::Yes);
        uint64_t purge_seqno = 0;
        errCode = cb::couchstore::compact(
                *sourceDb,
                compact_file.c_str(),
                flags,
                [hook_ctx](Db& db, DocInfo* docInfo, sized_buf value) -> int {
                    return time_purge_hook(db, docInfo, value, *hook_ctx);
                },
                {},
                def_iops,
                [vbid, hook_ctx, this](Db& source, Db& compacted) {
                    // we don't try to delete the dropped collection document
                    // as it'll come back in the next database header anyway
                    PendingLocalDocRequestQueue localDocQueue;
                    auto ret = maybePatchMetaData(source,
                                                  compacted,
                                                  hook_ctx->stats,
                                                  localDocQueue,
                                                  vbid);
                    if (ret == COUCHSTORE_SUCCESS) {
                        ret = updateLocalDocuments(compacted, localDocQueue);
                    }
                    return ret;
                },
                timestamp.count(),
                delta.count(),
                [this, &vbid, hook_ctx](Db& db) {
                    auto [status, state] = readVBState(&db, vbid);
                    // @TODO MB-51037: We probably shouldn't fail compactions
                    // externally if a vBucket state does not exist in the
                    // header we have picked up....
                    if (status != ReadVBStateStatus::Success &&
                        status != ReadVBStateStatus::NotFound) {
                        return COUCHSTORE_ERROR_CANCEL;
                    }
                    hook_ctx->highCompletedSeqno =
                            state.persistedCompletedSeqno;
                    auto [getDroppedStatus, droppedCollections] =
                            getDroppedCollections(db);
                    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
                        return COUCHSTORE_ERROR_CANCEL;
                    }

                    hook_ctx->eraserContext =
                            std::make_unique<Collections::VB::EraserContext>(
                                    droppedCollections);
                    return COUCHSTORE_SUCCESS;
                },
                [this, &vbid, &prepareStats, &purge_seqno](Db& db) {
                    auto [status, state] = readVBState(&db, vbid);
                    // @TODO MB-51037: We probably shouldn't fail compactions
                    // externally if a vBucket state does not exist in the
                    // header we have picked up....
                    if (status != ReadVBStateStatus::Success &&
                        status != ReadVBStateStatus::NotFound) {
                        return COUCHSTORE_ERROR_CANCEL;
                    }
                    prepareStats.onDiskPrepares = state.onDiskPrepares;
                    purge_seqno = cb::couchstore::getHeader(db).purgeSeqNum;
                    return COUCHSTORE_SUCCESS;
                },
                [&prepareStats, &collectionStats](Db&,
                                                  Db& target,
                                                  const DocInfo* docInfo,
                                                  const DocInfo*) {
                    return replayPreCopyHook(
                            target, docInfo, prepareStats, collectionStats);
                },
                [&prepareStats, &collectionStats, &purge_seqno, hook_ctx, this](
                        Db&, Db& compacted) {
                    return replayPrecommitHook(compacted,
                                               prepareStats,
                                               collectionStats,
                                               purge_seqno,
                                               *hook_ctx);
                });
    } else {
        auto [status, state] = readVBState(sourceDb.getDb(), vbid);
        if (status == ReadVBStateStatus::Success) {
            hook_ctx->highCompletedSeqno = state.persistedCompletedSeqno;
        } else {
            EP_LOG_WARN(
                    "CouchKVStore::compactDBInternal ({}) Failed to obtain "
                    "vbState for the highCompletedSeqno. Won't prune prepares",
                    vbid);
            return CompactDBStatus::Failed;
        }
        auto [getDroppedStatus, droppedCollections] =
                getDroppedCollections(*sourceDb);
        if (getDroppedStatus != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::compactDBInternal {} failed to get "
                    "dropped collections - status:{}",
                    vbid,
                    couchstore_strerror(getDroppedStatus));
            return CompactDBStatus::Failed;
        }
        hook_ctx->stats.collectionsPurged = droppedCollections.size();

        hook_ctx->eraserContext =
                std::make_unique<Collections::VB::EraserContext>(
                        droppedCollections);
        errCode = cb::couchstore::compact(
                *sourceDb,
                compact_file.c_str(),
                flags,
                [hook_ctx](Db& db, DocInfo* docInfo, sized_buf value) -> int {
                    return time_purge_hook(db, docInfo, value, *hook_ctx);
                },
                {},
                def_iops,
                [vbid, hook_ctx, this](Db& source, Db& compacted) {
                    if (mb40415_regression_hook) {
                        return COUCHSTORE_ERROR_CANCEL;
                    }

                    PendingLocalDocRequestQueue localDocQueue;
                    if (hook_ctx->eraserContext
                                ->needToUpdateCollectionsMetadata()) {
                        // Need to ensure the 'dropped' list on disk is now gone
                        localDocQueue.emplace_back(
                                std::string{LocalDocKey::droppedCollections},
                                CouchLocalDocRequest::IsDeleted{});
                        hook_ctx->eraserContext->setOnDiskDroppedDataExists(
                                false);
                    }
                    auto ret = maybePatchMetaData(source,
                                                  compacted,
                                                  hook_ctx->stats,
                                                  localDocQueue,
                                                  vbid);
                    if (ret == COUCHSTORE_SUCCESS) {
                        ret = updateLocalDocuments(compacted, localDocQueue);
                    }

                    return ret;
                });
    }
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_CANCEL) {
            return CompactDBStatus::Aborted;
        }

        logger.warn(
                "CouchKVStore::compactDBInternal: cb::couchstore::compact() "
                "error:{} [{}], name:{}",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(sourceDb, errCode),
                dbfile);
        return CompactDBStatus::Failed;
    }

    // We're done with the "normal" compaction... Unfortunately we're still
    // not "up to speed" so we need to "replay" all of the mutations which
    // happened while we rewrote the entire file.
    // Given that full compaction may take some time let's try a few times
    // without holding the lock to catch up (and allow the flusher to keep
    // on writing), but we don't want to end up in an "endless" loop if
    // the flusher got tons of data for the current vbucket. After 10
    // attempts to do catch up without the lock we run the final catch up
    // with the lock held.

    DbHolder targetDb(*this);
    errCode = openSpecificDBFile(vbid, new_rev, targetDb, 0, compact_file);
    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     compact_file,
                     0);
        return CompactDBStatus::Failed;
    }
    CompactionReplayPrepareStats prepareStats;
    {
        auto [status, json] = getLocalVbState(*targetDb);
        if (status != COUCHSTORE_SUCCESS) {
            logger.warn("Failed to read _local/vbstate for {}: {}",
                        vbid,
                        couchstore_strerror(status));
            return CompactDBStatus::Failed;
        }

        // This field may not be present if first compaction after upgrade from
        // less than 6.5 to 7.0, e.g. 6.0 -> 7.0
        prepareStats.onDiskPrepares =
                std::stoul(json.value("on_disk_prepares", "0"));

        auto prepareBytes = json.find("on_disk_prepare_bytes");
        // only update if it's there..
        if (prepareBytes != json.end()) {
            prepareStats.onDiskPrepareBytes =
                    std::stoul(prepareBytes->get<std::string>());
        }
    }

    concurrentCompactionPreLockHook(compact_file);

    // We'll do catch-up in two phases. First we'll try to catch up with
    // the flusher by releasing the lock every time copy the data,
    // but to avoid doing that "forever" we'll give up after 10 times
    // and then hold the lock (and block the flusher) to make sure we catch
    // up.
    vbLock.lock();
    for (int ii = 0; ii < 10; ++ii) {
        concurrentCompactionPostLockHook(compact_file);
        if (vbAbortCompaction[getCacheSlot(vbid)]) {
            return CompactDBStatus::Aborted;
        }
        if (tryToCatchUpDbFile(*sourceDb,
                               *targetDb,
                               vbLock,
                               true, // copy without lock
                               hook_ctx->getRollbackPurgeSeqno(),
                               prepareStats,
                               vbid,
                               *hook_ctx)) {
            // I'm at the tip
            break;
        }
    }

    concurrentCompactionPostLockHook(compact_file);

    if (vbAbortCompaction[getCacheSlot(vbid)]) {
        return CompactDBStatus::Aborted;
    }

    // Block any writers, do the final catch up and swap the file
    tryToCatchUpDbFile(*sourceDb,
                       *targetDb,
                       vbLock,
                       false, // copy with lock
                       hook_ctx->getRollbackPurgeSeqno(),
                       prepareStats,
                       vbid,
                       *hook_ctx);
    // Close the source Database File once compaction is done
    sourceDb.close();
    // Close the destination file so we may rename it
    targetDb.close();

    // The final part of compaction is done as a sub-function in this try
    // catch block - this is so any exception coming from this final part
    // is caught with vbLock locked. That allows removal of the new_file
    // whilst blocking any advancement of the vbucket's revision. If we
    // don't do this the compacted file is at risk of being used by a future
    // version of the vbucket.
    auto status = CompactDBStatus::Failed;
    const auto new_file = getDBFileName(dbname, vbid, new_rev);
    try {
        // Rename the .compact file to one with the next revision number
        if (rename(compact_file.c_str(), new_file.c_str()) != 0) {
            logger.warn(
                    "CouchKVStore::compactDBInternal: rename error:{}, old:{}, "
                    "new:{}",
                    cb_strerror(),
                    compact_file,
                    new_file);
            return CompactDBStatus::Failed;
        }

        status = compactDBTryAndSwitchToNewFile(
                         vbid, new_rev, hook_ctx, prepareStats)
                         ? CompactDBStatus::Success
                         : CompactDBStatus::Failed;
    } catch (const std::exception& e) {
        logger.error(
                "CouchKVStore::compactDBInternal: exception while performing "
                "finalisation of compaction for {}"
                " - Details: {}",
                vbid,
                e.what());

        // The new file must be removed
        if (remove(new_file.c_str()) != 0) {
            logger.warn(
                    "CouchKVStore::compactDBInternal: remove error:{}, path:{}",
                    cb_strerror(),
                    new_file);
        }
        return CompactDBStatus::Failed;
    }

    if (status == CompactDBStatus::Success) {
        logger.debug(
                "created new couch db file, name:{} rev:{}", new_file, new_rev);
        // Update the global VBucket file map so all operations use the new file
        updateDbFileMap(vbid, new_rev);
        // Unlink the now stale file
        unlinkCouchFile(vbid, sourceDb.getFileRev());
    }

    return status;
}

bool CouchKVStore::compactDBTryAndSwitchToNewFile(
        Vbid vbid,
        uint64_t newRevision,
        CompactionContext* hookCtx,
        const CompactionReplayPrepareStats& prepareStats) {
    // Open the newly compacted VBucket database to update the cached vbstate
    DbHolder targetDb(*this);
    const auto targetFilename = getDBFileName(dbname, vbid, newRevision);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openSpecificDBFile(
            vbid, newRevision, targetDb, options, targetFilename);
    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     targetFilename,
                     options);

        if (remove(targetFilename.c_str()) != 0) {
            logger.warn(
                    "CouchKVStore::compactDBInternal: remove error:{}, path:{}",
                    cb_strerror(),
                    targetFilename);
        }

        return false;
    }

    // Make our completion callback before writing the new file. We should
    // update our in memory state before we finalize on disk state so that we
    // don't have to worry about race conditions with things like the purge
    // seqno.
    if (hookCtx->completionCallback) {
        hookCtx->completionCallback(*hookCtx);
    }

    auto info = readHeaderAndUpdateCache(*targetDb.getDb(), vbid);
    hookCtx->stats.post = toFileInfo(info);

    // also update cached state with dbinfo (the disk entry is already updated)
    auto* state = getCachedVBucketState(vbid);
    if (state) {
        state->highSeqno = info.updateSeqNum;
        state->purgeSeqno = info.purgeSeqNum;
        state->onDiskPrepares = prepareStats.onDiskPrepares;
        state->setOnDiskPrepareBytes(prepareStats.onDiskPrepareBytes);
        cachedOnDiskPrepareSize[getCacheSlot(vbid)] =
                state->getOnDiskPrepareBytes();
    }

    return true;
}

couchstore_error_t CouchKVStore::maybePatchMetaData(
        Db& source,
        Db& target,
        CompactionStats& stats,
        PendingLocalDocRequestQueue& localDocQueue,
        Vbid vbid) {
    // If prepares were purged, update vbstate
    if (stats.preparesPurged) {
        // Must sync the modified state back we need to update the
        // _local/vbstate to update the number of on_disk_prepares to match
        // whatever we purged
        auto [status, json] = getLocalVbState(target);
        if (status != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::maybePatchMetaData: Failed to load "
                    "_local/vbstate: {}",
                    couchstore_strerror(status));
            return status;
        }

        bool updateVbState = false;

        auto prepares = json.find("on_disk_prepares");
        // only update if it's there..
        if (prepares != json.end()) {
            const auto onDiskPrepares =
                    std::stoull(prepares->get<std::string>());

            if (stats.preparesPurged > onDiskPrepares) {
                // Log the message before throwing the exception just in case
                // someone catch the exception but don't log it...
                const std::string msg =
                        "CouchKVStore::maybePatchMetaData(): According to "
                        "_local/vbstate for " +
                        vbid.to_string() + " there should be " +
                        std::to_string(onDiskPrepares) +
                        " prepares, but we just purged " +
                        std::to_string(stats.preparesPurged);
                logger.critical("{}", msg);
                logger.flush();
                throw std::runtime_error(msg);
            }

            *prepares = std::to_string(onDiskPrepares - stats.preparesPurged);
            updateVbState = true;
        }

        auto prepareBytes = json.find("on_disk_prepare_bytes");
        // only update if it's there..
        if (prepareBytes != json.end()) {
            const uint64_t onDiskPrepareBytes =
                    std::stoull(prepareBytes->get<std::string>());

            if (onDiskPrepareBytes > stats.prepareBytesPurged) {
                *prepareBytes = std::to_string(onDiskPrepareBytes -
                                               stats.prepareBytesPurged);
            } else {
                // prepare-bytes has been introduced in 6.6.1. Thus, at
                // compaction we may end up purging prepares that have been
                // persisted by a a pre-6.6.1 node and that are not accounted in
                // prepare-bytes. The counter would go negative, just set to 0.
                *prepareBytes = "0";
            }
            updateVbState = true;
        }

        // only update if at least one of the prepare stats is already there
        if (updateVbState) {
            const auto doc = json.dump();
            localDocQueue.emplace_back(std::string{LocalDocKey::vbstate},
                                       json.dump());
        }
    }

    // Finally see if compaction purged data and disk size stats need updates
    for (auto [cid, purgedBytes] : stats.collectionSizeUpdates) {
        // Need to read the collection stats. We read from the source file so
        // that in the case of a first compaction after upgrade from pre 7.0
        // we get the correct disk (because we use the dbinfo.space_used). The
        // target file could return a value too small in that case (MB-45917)
        auto [status, currentStats] =
                getCollectionStats(source, getCollectionStatsLocalDocId(cid));
        if (status == KVStore::GetCollectionStatsStatus::Failed) {
            logger.warn(
                    "CouchKVStore::maybePatchMetaData: Failed to load "
                    "collection stats for {}, cid:{}, stats could be now "
                    "wrong!",
                    vbid,
                    cid);
            return COUCHSTORE_ERROR_READ;
        }

        // To update them
        currentStats.diskSize -= purgedBytes;

        // Set the size to the total so that we can reset the cached value in
        // the manifest. We don't update using deltas as a concurrent flush
        // during compaction can't update the stats via deltas and we can re-use
        // the code
        stats.collectionSizeUpdates[cid] = currentStats.diskSize;

        // To write them back
        localDocQueue.emplace_back(getCollectionStatsLocalDocId(cid),
                                   currentStats.getLebEncodedStats());
    }

    return COUCHSTORE_SUCCESS;
}

bool CouchKVStore::tryToCatchUpDbFile(Db& source,
                                      Db& destination,
                                      std::unique_lock<std::mutex>& lock,
                                      bool copyWithoutLock,
                                      uint64_t purge_seqno,
                                      CompactionReplayPrepareStats& prepareStats,
                                      Vbid vbid,
                                      CompactionContext& hook_ctx) {
    if (!lock.owns_lock()) {
        throw std::logic_error(
                "CouchKVStore::tryToCatchUpDbFile: lock must be held");
    }

    auto start = cb::couchstore::getHeader(source);

    auto err = cb::couchstore::seek(source, cb::couchstore::Direction::End);
    if (err != COUCHSTORE_SUCCESS) {
        // seek failed - deal with it
        throw std::runtime_error("Failed to seek in the database file: " +
                                 couchkvstore_strerrno(&source, err));
    }

    auto end = cb::couchstore::getHeader(source);
    if (end.headerPosition == start.headerPosition) {
        // Catchup complete
        return true;
    }

    // Get the dropped collections from the source file so the copy hook can
    // account (ignore) any items that belong to dropped collections. This is
    // done after seek end, so we read the most recent state.
    auto [getDroppedStatus, droppedCollections] = getDroppedCollections(source);
    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
        throw std::runtime_error(
                "CouchKVStore::tryToCatchUpDbFile: getDroppedConnections"
                "error: " +
                std::string(couchstore_strerror(getDroppedStatus)));
        return getDroppedStatus;
    }

    if (cb::couchstore::seek(source, start.headerPosition) !=
        COUCHSTORE_SUCCESS) {
        throw std::runtime_error("Failed to move back to the current position");
    }

    if (copyWithoutLock) {
        lock.unlock();
    }

    uint64_t delta = std::numeric_limits<uint64_t>::max();
    if (configuration.isPitrEnabled()) {
        delta = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        configuration.getPitrGranularity())
                        .count();
    }

    EP_LOG_INFO("Try to catch up {}: from {} to {} {} stopping flusher",
                vbid,
                start.updateSeqNum,
                end.updateSeqNum,
                copyWithoutLock ? "without" : "while");

    // Create a FlushAccounting object which can account for items moving from
    // source to destination. This is constructed with the set of dropped
    // collections from the source database (note that the target has been
    // compacted and has no dropped collections remaining). The dropped
    // collections must be known so that the accounting can 'ignore' items of
    // dropped collections (e.g. replace becomes insert etc...)
    Collections::VB::FlushAccounting collectionStats(droppedCollections,
                                                     IsCompaction::Yes);
    if (configuration.isPitrEnabled()) {
        collectionStats.setPiTR(IsPiTR::Yes);
    }

    err = cb::couchstore::replay(
            source,
            destination,
            delta,
            end.headerPosition,
            [this, &prepareStats, &collectionStats](
                    Db&,
                    Db& target,
                    const DocInfo* docInfo,
                    const DocInfo* localDocInfo) {
                if (localDocInfo) {
                    return replayPreCopyLocalDoc(
                            target, localDocInfo, collectionStats);
                }
                return replayPreCopyHook(
                        target, docInfo, prepareStats, collectionStats);
            },
            [&prepareStats, &collectionStats, &purge_seqno, &hook_ctx, this](
                    Db&, Db& compacted) {
                return replayPrecommitHook(compacted,
                                           prepareStats,
                                           collectionStats,
                                           purge_seqno,
                                           hook_ctx);
            });

    if (err != COUCHSTORE_SUCCESS) {
        throw std::runtime_error("Failed to replay data, err:" +
                                 std::string(couchstore_strerror(err)));
    }

    bool ret = true;
    if (copyWithoutLock) {
        lock.lock();
        // the database may have changed.. just return false and let the
        // caller call us again to check..
        ret = false;
    }

    logger.info("Catch up of {} to {} complete{}",
                vbid,
                end.updateSeqNum,
                copyWithoutLock ? ", stopping flusher" : "");
    return ret;
}

vbucket_state* CouchKVStore::getCachedVBucketState(Vbid vbucketId) {
    return cachedVBStates[getCacheSlot(vbucketId)].get();
}

bool CouchKVStore::writeVBucketState(Vbid vbucketId,
                                     const vbucket_state& vbstate) {
    auto handle = openOrCreate(vbucketId);
    if (!handle) {
        ++st.numVbSetFailure;
        logger.warn("CouchKVStore::writeVBucketState: openOrCreate error");
        return false;
    }

    auto& db = *handle;
    auto errorCode = updateLocalDocument(
            *db, LocalDocKey::vbstate, makeJsonVBState(vbstate));
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

    readHeaderAndUpdateCache(*db, vbucketId);

    return true;
}

bool CouchKVStore::snapshotVBucket(Vbid vbucketId, const VB::Commit& meta) {
    if (!needsToBePersisted(vbucketId, meta.proposedVBState)) {
        return true;
    }

    auto start = std::chrono::steady_clock::now();

    if (!writeVBucketState(vbucketId, meta.proposedVBState)) {
        logger.warn(
                "CouchKVStore::snapshotVBucket: writeVBucketState failed "
                "state:{}, {}",
                VBucket::toString(meta.proposedVBState.transition.state),
                vbucketId);
        return false;
    }

    updateCachedVBState(vbucketId, meta.proposedVBState);

    EP_LOG_DEBUG("CouchKVStore::snapshotVBucket: Snapshotted {} state:{}",
                 vbucketId,
                 meta.proposedVBState);

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

StorageProperties CouchKVStore::getStorageProperties() const {
    StorageProperties rv(StorageProperties::ByIdScan::Yes,
                         StorageProperties::AutomaticDeduplication::No,
                         StorageProperties::PrepareCounting::Yes,
                         StorageProperties::CompactionStaleItemCallbacks::No,
                         StorageProperties::HistoryRetentionAvailable::No);
    return rv;
}

bool CouchKVStore::getStat(std::string_view name, size_t& value) const {
    if (name == "failure_get") {
        value = st.numGetFailure.load();
        return true;
    } else if (name == "io_document_write_bytes") {
        value = st.io_document_write_bytes;
        return true;
    } else if (name == "io_flusher_write_bytes") {
        value = fsStats.totalBytesWritten;
        return true;
    } else if (name == "io_total_read_bytes") {
        value = fsStats.totalBytesRead.load() +
                fsStatsCompaction.totalBytesRead.load();
        return true;
    } else if (name == "io_total_write_bytes") {
        value = fsStats.totalBytesWritten.load() +
                fsStatsCompaction.totalBytesWritten.load();
        return true;
    } else if (name == "io_compaction_read_bytes") {
        value = fsStatsCompaction.totalBytesRead;
        return true;
    } else if (name == "io_compaction_write_bytes") {
        value = fsStatsCompaction.totalBytesWritten;
        return true;
    } else if (name == "io_bg_fetch_read_count") {
        value = st.getMultiFsReadCount;
        return true;
    }

    return false;
}

void CouchKVStore::pendingTasks() {
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
                pendingFileDeletions.wlock()->push(filename_str);
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
        SnapshotSource source,
        std::unique_ptr<KVFileHandle> fileHandle) const {
    auto handle = std::move(fileHandle);
    if (!handle) {
        handle = makeFileHandle(vbid);
    }

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

    std::optional<uint64_t> timestamp;
    if (source == SnapshotSource::Historical) {
        auto status = cb::couchstore::seekFirstHeaderContaining(
                *db.getDb(),
                startSeqno,
                configuration.getPitrGranularity().count());
        if (status != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::initBySeqnoScanContext: Failed to locate "
                    "correct database header: {}",
                    couchstore_strerror(status));
            return {};
        }
    }

    const auto header = cb::couchstore::getHeader(*db.getDb());
    if (source == SnapshotSource::Historical) {
        timestamp = header.timestamp;
    }
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
        EP_LOG_WARN_RAW(
                "CouchKVStore::initBySeqnoScanContext:Failed to obtain vbState "
                "for "
                "the highCompletedSeqno");
        return nullptr;
    }

    auto [getDroppedStatus, droppedCollections] = getDroppedCollections(*db);
    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
        EP_LOG_WARN(
                "CouchKVStore::initBySeqnoScanContext: {} Failed to get "
                "dropped collections. Status - {}",
                vbid,
                couchstore_strerror(getDroppedStatus));
        return nullptr;
    }

    auto sctx = std::make_unique<BySeqnoScanContext>(std::move(cb),
                                                     std::move(cl),
                                                     vbid,
                                                     std::move(handle),
                                                     startSeqno,
                                                     header.updateSeqNum,
                                                     header.purgeSeqNum,
                                                     options,
                                                     valOptions,
                                                     count,
                                                     readVbStateResult.state,
                                                     nullptr,
                                                     droppedCollections,
                                                     std::move(timestamp));
    sctx->logger = &logger;
    return sctx;
}

std::unique_ptr<ByIdScanContext> CouchKVStore::initByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        const std::vector<ByIdRange>& ranges,
        DocumentFilter options,
        ValueFilter valOptions) const {
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

    const auto info = cb::couchstore::getHeader(*db.getDb());

    auto [getDroppedStatus, droppedCollections] = getDroppedCollections(*db);
    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
        EP_LOG_WARN(
                "CouchKVStore::initByIdScanContext: {} Failed to get "
                "dropped collections. Status - {}",
                vbid,
                couchstore_strerror(getDroppedStatus));
        return nullptr;
    }

    auto sctx = std::make_unique<ByIdScanContext>(std::move(cb),
                                                  std::move(cl),
                                                  vbid,
                                                  std::move(handle),
                                                  ranges,
                                                  options,
                                                  valOptions,
                                                  droppedCollections,
                                                  info.updateSeqNum);
    sctx->logger = &logger;
    return sctx;
}

static couchstore_docinfos_options getDocFilter(const DocumentFilter& filter) {
    switch (filter) {
    case DocumentFilter::ALL_ITEMS:
    case DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS:
    // Whilst couchstore supports a NO_DELETES option we want to see deletes in
    // CouchKVStore callbacks to increment the lastReadSeqno of the scan or we
    // will potentially restart at the last alive item meaning we scan items
    // unnecessarily.
    case DocumentFilter::NO_DELETES:
        return COUCHSTORE_NO_OPTIONS;
    }

    std::string err("getDocFilter: Illegal document filter!" +
                    std::to_string(static_cast<int>(filter)));
    throw std::runtime_error(err);
}

scan_error_t CouchKVStore::scan(BySeqnoScanContext& ctx) const {
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

scan_error_t CouchKVStore::scanAllVersions(BySeqnoScanContext& sctx) const {
    // StorageProperties reported this feature is not available
    throw std::runtime_error("CouchKVStore::scanAllVersions was invoked");
}

scan_error_t CouchKVStore::scan(ByIdScanContext& ctx) const {
    TRACE_EVENT_START2("CouchKVStore",
                       "scan by id",
                       "vbid",
                       ctx.vbid.get(),
                       "ranges",
                       uint32_t(ctx.ranges.size()));

    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(*ctx.handle);
    auto& db = couchKvHandle.getDbHolder();

    couchstore_error_t errorCode = COUCHSTORE_SUCCESS;
    for (auto& range : ctx.ranges) {
        if (range.rangeScanSuccess) {
            continue;
        }
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
                                              byIdScanCallback,
                                              static_cast<void*>(&ctx));
        if (errorCode != COUCHSTORE_SUCCESS) {
            if (errorCode == COUCHSTORE_ERROR_CANCEL) {
                // Update the startKey so backfill can resume from lastReadKey
                range.startKey = ctx.lastReadKey;
            }
            break;
        } else {
            // This range has been fully scanned and should not be scanned again
            range.rangeScanSuccess = true;
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
            // We don't know how many documents we _should_ have scanned,
            // it could be 1, could be many more; so just count as a single
            // "get" failure to the error is propogated up to ns_server (and
            // can potentially be used to trigger auto-failover).
            st.numGetFailure++;
            return scan_failed;
        }
    }
    return scan_success;
}

cb::couchstore::Header CouchKVStore::getDbInfo(Vbid vbid) {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openDB(vbid, db, options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);

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

    return cb::couchstore::getHeader(*db.getDb());
}

void CouchKVStore::close() {
}

void CouchKVStore::updateDbFileMap(Vbid vbucketId, uint64_t newFileRev) {
    try {
        (*dbFileRevMap->wlock()).at(getCacheSlot(vbucketId)) = newFileRev;
    } catch (std::out_of_range const&) {
        logger.warn(
                "CouchKVStore::updateDbFileMap: Cannot update db file map "
                "for an invalid vbucket, {}, rev:{}",
                vbucketId,
                newFileRev);
    }
}

uint64_t CouchKVStore::getDbRevision(Vbid vbucketId) const {
    return (*dbFileRevMap->rlock())[getCacheSlot(vbucketId)];
}

couchstore_error_t CouchKVStore::openDB(Vbid vbucketId,
                                        DbHolder& db,
                                        couchstore_open_flags options,
                                        FileOpsInterface* ops) const {
    // MB-27963: obtain read access whilst we open the file, updateDbFileMap
    // serialises on this mutex so we can be sure the fileRev we read should
    // still be a valid file once we hit sys_open
    const auto lockedRevMap = dbFileRevMap->rlock();
    uint64_t fileRev = (*lockedRevMap)[getCacheSlot(vbucketId)];
    return openSpecificDBFile(vbucketId,
                              fileRev,
                              db,
                              options,
                              getDBFileName(dbname, vbucketId, fileRev),
                              ops);
}

couchstore_error_t CouchKVStore::openSpecificDBFile(
        Vbid vbucketId,
        uint64_t fileRev,
        DbHolder& db,
        couchstore_open_flags options,
        const std::string& dbFileName,
        FileOpsInterface* ops) const {
    // Save some info for the caller log them
    db.setFileRev(fileRev);
    db.setFilename(dbFileName);

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
                           "openSpecificDBFile",
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
    }

    return errorCode;
}

void CouchKVStore::logExistingVBucketFiles(Vbid vbid) const {
    const std::string filePrefix =
            dbname + "/" + std::to_string(vbid.get()) + ".couch";
    const auto files = cb::io::findFilesWithPrefix(filePrefix);
    logger.info(
            "CouchKVStore: Found {} existing files for {}", files.size(), vbid);
    for (const auto& f : files) {
        logger.info("CouchKVStore: Found {}", f);
    }
}

void CouchKVStore::logOpenError(std::string_view caller,
                                spdlog::level::level_enum level,
                                couchstore_error_t errCode,
                                Vbid vbid,
                                std::string_view filename,
                                couchstore_open_flags options) const {
    logger.log(
            level,
            "CouchKVStore::{}: {} Open error:{} [{}], filename:{}, option:{}",
            caller,
            vbid,
            couchstore_strerror(errCode),
            cb_strerror(),
            filename,
            options);

    if (errCode == COUCHSTORE_ERROR_NO_SUCH_FILE) {
        logExistingVBucketFiles(vbid);
    }
}

std::unordered_map<Vbid, std::unordered_set<uint64_t>>
CouchKVStore::getVbucketRevisions(
        const std::vector<std::string>& filenames) const {
    std::unordered_map<Vbid, std::unordered_set<uint64_t>> vbids;
    for (const auto& filename : filenames) {
        // vbid.couch.rev
        auto basename = cb::io::basename(filename);
        std::string_view couchfile(basename);

        // vbid.couch
        auto name = couchfile.substr(0, couchfile.rfind("."));

        // vbid
        std::string vbid(name.substr(0, name.find(".")));

        // rev
        std::string rev(couchfile.substr(couchfile.rfind('.') + 1));

        // possible to get x..couch..y which is invalid
        bool valid = std::count(couchfile.begin(), couchfile.end(), '.') == 2;

        Vbid id;
        uint64_t revision = 0;
        if (valid && allDigit(vbid) && allDigit(rev)) {
            try {
                id = Vbid(gsl::narrow<uint16_t>(std::stoul(vbid)));
                revision = std::stoull(rev);
            } catch (const std::exception&) {
                valid = false;
            }
        } else if (vbid == "master") {
            // master.couch.x is expected and can be silently ignored
            continue;
        } else {
            valid = false;
        }

        if (valid) {
            if (id.get() % configuration.getMaxShards() !=
                configuration.getShardId()) {
                // Either doesn't belong to this shard or is the last element
                // (case where max vB % shards != 0) which we now need to check
                // for
                if (id.get() != (((configuration.getMaxVBuckets() /
                                   configuration.getMaxShards()) *
                                  configuration.getMaxShards()) +
                                 configuration.getShardId())) {
                    continue;
                }
            }
            // update map or create new element
            if (vbids.count(id)) {
                // id is mapped, add the revision
                vbids[id].emplace(revision);
            } else {
                // nothing mapped, create new vector with revision
                auto inserted =
                        vbids.emplace(Vbid(id), std::unordered_set<uint64_t>{});
                inserted.first->second.emplace(revision);
            }

        } else {
            // Dump all the bits we extracted from the input
            logger.warn(
                    "CouchKVStore::getVbucketRevisions: invalid filename:{}, "
                    "basename:{}, name:{}, vbid:{}, rev:{}",
                    filename,
                    basename,
                    name,
                    vbid,
                    rev);
        }
    }
    return vbids;
}

std::unordered_map<Vbid, std::unordered_set<uint64_t>>
CouchKVStore::populateRevMapAndRemoveStaleFiles() {
    // For each vb, more than 1 file could be found. This occurs if we had an
    // unclean shutdown before deleting a stale file. getVbucketRevisions will
    // return a list of 'revisions' for each vb
    auto map = getVbucketRevisions(discoverDbFiles(dbname));

    for (const auto& [vbid, revisions] : map) {
        for (const auto revision : revisions) {
            uint64_t current = getDbRevision(vbid);
            if (current == revision) {
                continue;
            } else if (current < revision) {
                // current file is stale, update to the new revision
                updateDbFileMap(vbid, revision);
            } else { // stale file found (revision id has rolled over)
                current = revision;
            }

            // stale file left behind to be removed
            const auto staleFile = getDBFileName(dbname, vbid, current);

            if (cb::io::isFile(staleFile)) {
                if (remove(staleFile.c_str()) == 0) {
                    logger.debug(
                            "CouchKVStore::populateRevMapAndRemoveStaleFiles: "
                            "Removed stale file:{}",
                            staleFile);
                } else {
                    logger.warn(
                            "CouchKVStore::populateRevMapAndRemoveStaleFiles: "
                            "remove(\"{}\") returned error:{}",
                            staleFile,
                            cb_strerror());
                }
            }
        }
    }

    return map;
}

couchstore_error_t CouchKVStore::fetchDoc(Db* db,
                                          DocInfo* docinfo,
                                          GetValue& docValue,
                                          Vbid vbId,
                                          ValueFilter filter) const {
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;
    const bool fetchCompressed = (filter == ValueFilter::VALUES_COMPRESSED);
    const couchstore_open_options openOptions =
            fetchCompressed ? 0 : DECOMPRESS_DOC_BODIES;
    std::unique_ptr<MetaData> metadata;
    try {
        metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);
    } catch (std::logic_error&) {
        return COUCHSTORE_ERROR_DB_NO_LONGER_VALID;
    }

    const bool forceValueFetch = isDocumentPotentiallyCorruptedByMB52793(
            docinfo->deleted, metadata->getDataType());
    if (filter == ValueFilter::KEYS_ONLY && !forceValueFetch) {
        // Can skip reading document value.
        auto it = makeItemFromDocInfo(
                vbId, *docinfo, *metadata, std::nullopt, fetchCompressed);

        docValue = GetValue(std::move(it));
        // update ep-engine IO stats
        ++st.io_bg_fetch_docs_read;
        st.io_bgfetch_doc_bytes += (docinfo->id.size + docinfo->rev_meta.size);
        return COUCHSTORE_SUCCESS;
    }

    Doc* doc = nullptr;
    sized_buf value = {nullptr, 0};
    errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, openOptions);
    if (errCode == COUCHSTORE_SUCCESS) {
        if (doc == nullptr) {
            throw std::logic_error("CouchKVStore::fetchDoc: doc is NULL");
        }

        if (doc->id.size > UINT16_MAX) {
            throw std::logic_error(
                    "CouchKVStore::fetchDoc: doc->id.size (which is" +
                    std::to_string(doc->id.size) + ") is greater than " +
                    std::to_string(UINT16_MAX));
        }

        value = doc->data;

    } else if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND && docinfo->deleted) {
        // NOT_FOUND is expected for deleted documents, continue.
    } else {
        return errCode;
    }

    try {
        auto it = makeItemFromDocInfo(
                vbId, *docinfo, *metadata, value, fetchCompressed);
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

    sctx->diskBytesRead += docinfo->id.size + docinfo->rev_meta.size;

    // Whilst couchstore supports a NO_DELETES option which would not even
    // invoke this callback for a deleted item we want to see deletes in
    // this callback to increment the lastReadSeqno of the scan or we will
    // potentially restart at the last alive item meaning we scan items
    // unnecessarily.
    if (docinfo->deleted && sctx->docFilter == DocumentFilter::NO_DELETES) {
        sctx->lastReadSeqno = byseqno;
        return COUCHSTORE_SUCCESS;
    }

    auto diskKey = makeDiskDocKey(docinfo->id);

    // Determine if the key is logically deleted
    auto docKey = diskKey.getDocKey();
    if (sctx->docFilter != DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
        if (sctx->collectionsContext.isLogicallyDeleted(
                    docKey, docinfo->deleted, byseqno)) {
            sctx->lastReadSeqno = byseqno;
            return COUCHSTORE_SUCCESS;
        }
    }

    // Only do cache lookup for non-system events
    if (!docKey.isInSystemCollection()) {
        CacheLookup lookup(diskKey, byseqno, vbucketId);

        cl.callback(lookup);
        if (cb::engine_errc{cl.getStatus()} ==
            cb::engine_errc::key_already_exists) {
            sctx->lastReadSeqno = byseqno;
            return COUCHSTORE_SUCCESS;
        } else if (cb::engine_errc{cl.getStatus()} ==
                   cb::engine_errc::no_memory) {
            return COUCHSTORE_ERROR_CANCEL;
        }
    }

    auto metadata = MetaDataFactory::createMetaData(docinfo->rev_meta);

    /**
     * If no special request is made to retrieve compressed documents
     * as is, then DECOMPRESS the document and update datatype
     */
    bool fetchCompressed = sctx->valFilter == ValueFilter::VALUES_COMPRESSED;

    const bool keysOnly = sctx->valFilter == ValueFilter::KEYS_ONLY;
    const bool isSystemEvent =
            makeDiskDocKey(docinfo->id).getDocKey().isInSystemCollection();
    const bool forceValueFetch = KVStore::isDocumentPotentiallyCorruptedByMB52793(
        docinfo->deleted, metadata->getDataType());
    std::optional<sized_buf> value;
    if (!keysOnly || isSystemEvent || forceValueFetch) {
        const couchstore_open_options openOptions =
                fetchCompressed ? 0 : DECOMPRESS_DOC_BODIES;
        auto errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc,
                                                        openOptions);

        if (errCode == COUCHSTORE_SUCCESS) {
            // Document value read from disk; account for it in diskBytesRead.
            // Note we record the physical_size (i.e. on-disk size, before
            // it is decompressed) to better reflect the cost of data actually
            // read from disk.
            sctx->diskBytesRead += docinfo->physical_size;

            value = doc->data;

            if (doc->data.size == 0) {
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

    auto it = makeItemFromDocInfo(
            vbucketId, *docinfo, *metadata, value, fetchCompressed);
    GetValue rv(std::move(it), cb::engine_errc::success, -1, keysOnly);
    cb.callback(rv);

    couchstore_free_document(doc);

    if (cb.shouldYield()) {
        // Scan is yielding _after_ successfully processing this doc.
        // Resume at next item.
        sctx->lastReadSeqno = byseqno;
        return COUCHSTORE_ERROR_CANCEL;
    } else if (cb::engine_errc{cb.getStatus()} == cb::engine_errc::no_memory) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    sctx->lastReadSeqno = byseqno;
    return COUCHSTORE_SUCCESS;
}

static int byIdScanCallback(Db* db, DocInfo* docinfo, void* ctx) {
    auto status = couchstore_error_t(bySeqnoScanCallback(db, docinfo, ctx));
    if (status == COUCHSTORE_ERROR_CANCEL) {
        auto* sctx = static_cast<ByIdScanContext*>(ctx);
        // save the resume point
        sctx->lastReadKey = makeDiskDocKey(docinfo->id);
    }
    return int(status);
}

bool CouchKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                          VB::Commit& commitData) {
    checkIfInTransaction(txnCtx->vbid, "CouchKVStore::commit");

    auto& ctx = dynamic_cast<CouchKVStoreTransactionContext&>(*txnCtx);

    size_t pendingCommitCnt = ctx.pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return true;
    }

    const auto vbid = txnCtx->vbid;

    TRACE_EVENT2("CouchKVStore",
                 "commit",
                 "vbid",
                 vbid.get(),
                 "pendingCommitCnt",
                 pendingCommitCnt);

    std::vector<Doc*> docs(pendingCommitCnt);
    std::vector<DocInfo*> docinfos(pendingCommitCnt);
    std::vector<void*> kvReqs(pendingCommitCnt);

    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        auto& req = ctx.pendingReqsQ[i];
        docs[i] = req.getDbDoc();
        docinfos[i] = req.getDbDocInfo();
        kvReqs[i] = &req;
    }

    kvstats_ctx kvctx(commitData);

    preFlushHook();

    // flush all
    const auto errCode = saveDocs(ctx, docs, docinfos, kvReqs, kvctx);

    postFlushHook();

    commitCallback(ctx, ctx.pendingReqsQ, kvctx, errCode);

    ctx.pendingReqsQ.clear();

    const auto success = (errCode == COUCHSTORE_SUCCESS);
    if (success) {
        // Updated collections stats and make them readable to the world
        kvctx.commitData.collections.postCommitMakeStatsVisible();

        updateCachedVBState(vbid, commitData.proposedVBState);
    } else {
        logger.warn(
                "CouchKVStore::commit: saveDocs error:{}, "
                "{}",
                couchstore_strerror(errCode),
                vbid);
    }

    return success;
}

// Callback when the btree is updated which we use for tracking create/update
// type statistics and collection stats (item counts, disk changes and seqno)
static void saveDocsCallback(const DocInfo* oldInfo,
                             const DocInfo* newInfo,
                             void* context,
                             void* kvReq) {
    auto* cbCtx = static_cast<kvstats_ctx*>(context);

    if (!newInfo) {
        throw std::logic_error("saveDocsCallback: newInfo should not be null");
    }
    auto newKey = makeDiskDocKey(newInfo->id);
    auto isCommitted =
            newKey.isCommitted() ? IsCommitted::Yes : IsCommitted::No;
    auto isDeleted = newInfo->deleted ? IsDeleted::Yes : IsDeleted::No;
    if (oldInfo) {
        auto oldIsDeleted = oldInfo->deleted ? IsDeleted::Yes : IsDeleted::No;
        cbCtx->commitData.collections.updateStats(newKey.getDocKey(),
                                                  newInfo->db_seq,
                                                  isCommitted,
                                                  isDeleted,
                                                  newInfo->getTotalSize(),
                                                  oldInfo->db_seq,
                                                  oldIsDeleted,
                                                  oldInfo->getTotalSize());

        if (!oldInfo->deleted) {
            // Doc already existed alive on disk, this is an update
            static_cast<CouchRequest*>(kvReq)->setUpdate();
        }
    } else {
        cbCtx->commitData.collections.updateStats(newKey.getDocKey(),
                                                  newInfo->db_seq,
                                                  isCommitted,
                                                  isDeleted,
                                                  newInfo->getTotalSize());
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

    const ssize_t newSize = newInfo->getTotalSize();
    const ssize_t oldSize = oldInfo ? oldInfo->getTotalSize() : 0;

    if (newKey.isPrepared()) {
        switch (onDiskMutationType) {
        case DocMutationType::Delete:
            cbCtx->onDiskPrepareDelta--;
            cbCtx->onDiskPrepareBytesDelta -= oldSize;
            break;
        case DocMutationType::Insert:
            cbCtx->onDiskPrepareDelta++;
            cbCtx->onDiskPrepareBytesDelta += newSize;
            break;
        case DocMutationType::Update:
            if (!newInfo->deleted) {
                // Not an abort, update the stat
                cbCtx->onDiskPrepareBytesDelta += (newSize - oldSize);
            }
            break;
        }
    }
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

couchstore_error_t CouchKVStore::saveDocs(
        CouchKVStoreTransactionContext& txnCtx,
        const std::vector<Doc*>& docs,
        const std::vector<DocInfo*>& docinfos,
        const std::vector<void*>& kvReqs,
        kvstats_ctx& kvctx) {
    auto vbid = txnCtx.vbid;
    auto handle = openOrCreate(vbid);
    if (!handle) {
        logger.warn("CouchKVStore::saveDocs: openOrCreate error");
        return COUCHSTORE_ERROR_OPEN_FILE;
    }

    auto& db = *handle;

    uint64_t maxDBSeqno = 0;

    // Count of logical bytes written (key + ep-engine meta + value),
    // used to calculated Write Amplification.
    size_t docsLogicalBytes = 0;

    // Only do a couchstore_save_documents if there are docs
    if (!docs.empty()) {
        for (size_t idx = 0; idx < docs.size(); idx++) {
            maxDBSeqno = std::max(maxDBSeqno, docinfos[idx]->db_seq);

            // Accumulate the size of the useful data in this docinfo.
            docsLogicalBytes += calcLogicalDataSize(docs[idx], *docinfos[idx]);
        }

        // If dropped collections exists, read the dropped collections metadata
        // so the flusher can do the correct stat calculations. This is
        // conditional as (trying) to read this data slows down saveDocs.
        if (kvctx.commitData.collections.droppedCollectionsExists()) {
            auto [getDroppedStatus, droppedCollections] =
                    getDroppedCollections(*db);
            if (getDroppedStatus != COUCHSTORE_SUCCESS) {
                logger.warn(
                        "CouchKVStore::saveDocs: getDroppedConnections"
                        "error:{}, {}",
                        couchstore_strerror(getDroppedStatus),
                        vbid);
                return getDroppedStatus;
            }

            kvctx.commitData.collections.setDroppedCollectionsForStore(
                    droppedCollections);
        }

        auto cs_begin = std::chrono::steady_clock::now();

        uint64_t flags = COMPRESS_DOC_BODIES | COUCHSTORE_SEQUENCE_AS_IS;
        const auto errCode =
                couchstore_save_documents_and_callback(db,
                                                       docs.data(),
                                                       docinfos.data(),
                                                       kvReqs.data(),
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

    saveDocsPostWriteDocsHook();

    vbucket_state& state = kvctx.commitData.proposedVBState;
    state.onDiskPrepares += kvctx.onDiskPrepareDelta;
    state.updateOnDiskPrepareBytes(kvctx.onDiskPrepareBytesDelta);
    txnCtx.pendingLocalReqsQ.emplace_back(std::string{LocalDocKey::vbstate},
                                          makeJsonVBState(state));

    kvctx.commitData.collections.saveCollectionStats(
            [this, &dbRef = *db, &txnCtx](
                    CollectionID cid,
                    const Collections::VB::PersistedStats& stats) {
                saveCollectionStats(txnCtx, dbRef, cid, stats);
            });

    if (kvctx.commitData.collections.isReadyForCommit()) {
        const auto errCode = updateCollectionsMeta(
                txnCtx, *db, kvctx.commitData.collections);
        if (errCode) {
            logger.warn(
                    "CouchKVStore::saveDocs: {} updateCollections meta "
                    "failed with status {}",
                    vbid,
                    couchstore_strerror(errCode));
            return errCode;
        }
    }

    /// Update the local documents before we commit
    auto errCode = updateLocalDocuments(*db, txnCtx.pendingLocalReqsQ);
    if (errCode) {
        logger.warn(
                "CouchKVStore::saveDocs: updateLocalDocuments size:{} "
                "error:{} [{}]",
                txnCtx.pendingLocalReqsQ.size(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode));
    }
    txnCtx.pendingLocalReqsQ.clear();

    auto cs_begin = std::chrono::steady_clock::now();

    errCode = couchstore_commit(db, kvctx.commitData.sysErrorCallback);
    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - cs_begin));
    if (errCode) {
        logger.warn("CouchKVStore::saveDocs: couchstore_commit error:{} [{}]",
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
    const auto info = readHeaderAndUpdateCache(*db, vbid);
    cachedOnDiskPrepareSize[getCacheSlot(vbid)] = state.getOnDiskPrepareBytes();

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

    // update stat
    st.docsCommitted = docs.size();

    return errCode;
}

void CouchKVStore::commitCallback(TransactionContext& txnCtx,
                                  PendingRequestQueue& committedReqs,
                                  kvstats_ctx& kvctx,
                                  couchstore_error_t errCode) {
    const auto flushSuccess = (errCode == COUCHSTORE_SUCCESS);
    for (auto& committed : committedReqs) {
        const auto docLogicalSize = calcLogicalDataSize(
                committed.getDbDoc(), *committed.getDbDocInfo());
        ++st.io_num_write;
        st.io_document_write_bytes += docLogicalSize;

        if (committed.isDelete()) {
            FlushStateDeletion state;
            if (flushSuccess) {
                if (committed.isUpdate()) {
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

            txnCtx.deleteCallback(committed.getItem(), state);
        } else {
            FlushStateMutation state;
            if (flushSuccess) {
                if (committed.isUpdate()) {
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

            txnCtx.setCallback(committed.getItem(), state);
        }
    }
}

CouchKVStore::ReadVBStateResult CouchKVStore::readVBState(Db* db,
                                                          Vbid vbId) const {
    sized_buf id;
    LocalDoc *ldoc = nullptr;
    // High sequence number and purge sequence number are stored automatically
    // by couchstore.
    int64_t highSeqno = 0;
    uint64_t purgeSeqno = 0;
    const auto info = readHeaderAndUpdateCache(*db, vbId);
    highSeqno = info.updateSeqNum;
    purgeSeqno = info.purgeSeqNum;

    vbucket_state vbState;

    id.buf = const_cast<char*>(LocalDocKey::vbstate.data());
    id.size = LocalDocKey::vbstate.size();
    auto couchStoreStatus =
            couchstore_open_local_document(db, (void*)id.buf, id.size, &ldoc);

    if (couchStoreStatus == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        logger.warn(
                "CouchKVStore::readVBState: '_local/vbstate' not found "
                "for {}",
                vbId);
        return {ReadVBStateStatus::NotFound, {}};
    }

    if (couchStoreStatus != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::readVBState: couchstore_open_local_document"
                " error:{}, {}",
                couchstore_strerror(couchStoreStatus),
                vbId);
        return {ReadVBStateStatus::Error, {}};
    }

    // Proceed to read/parse the vbstate
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

    ReadVBStateStatus status = ReadVBStateStatus::Success;
    bool snapshotValid;
    std::tie(snapshotValid, vbState.lastSnapStart, vbState.lastSnapEnd) =
            processVbstateSnapshot(vbId, vbState);

    if (!snapshotValid) {
        status = ReadVBStateStatus::CorruptSnapshot;
    }

    couchstore_free_local_document(ldoc);

    return {status, vbState};
}

CouchKVStore::ReadVBStateResult CouchKVStore::readVBStateAndUpdateCache(
        Db* db, Vbid vbid) {
    auto res = readVBState(db, vbid);

    // Defensive - ensure that we don't try to put state into the cache for a
    // vBucket that this shard isn't responsible for
    if (vbid.get() % configuration.getMaxShards() !=
        configuration.getShardId()) {
        // Either doesn't belong to this shard or is the last element
        // (case where max vB % shards != 0) which we now need to check for
        if (vbid.get() !=
            (((configuration.getMaxVBuckets() / configuration.getMaxShards()) *
              configuration.getMaxShards()) +
             1)) {
            throw std::runtime_error(
                    "Attempting to access " + vbid.to_string() +
                    " against "
                    "incorrect KVStore:" +
                    std::to_string(configuration.getShardId()) +
                    ". Max shards is:" +
                    std::to_string(configuration.getMaxShards()));
        }
    }

    if (res.status == ReadVBStateStatus::Success) {
        // For the case where a local doc does not exist, readVBState actually
        // returns Success as the read was successful. It also returns a default
        // constructed vbucket_state. This vbucket_state will then be put in
        // cachedVBStates here. As the default constructed vbucket_state
        // defaults the vbucket_state_t to vbucket_state_dead this should behave
        // in the same way as a lack of a vbucket/vbucket_state.
        cachedVBStates[getCacheSlot(vbid)] =
                std::make_unique<vbucket_state>(res.state);
    }
    return res;
}

cb::couchstore::Header CouchKVStore::readHeaderAndUpdateCache(Db& db,
                                                              Vbid vbid) const {
    auto info = cb::couchstore::getHeader(db);
    cachedDeleteCount[getCacheSlot(vbid)] = info.deletedCount;
    cachedFileSize[getCacheSlot(vbid)] = info.fileSize;
    cachedSpaceUsed[getCacheSlot(vbid)] = info.spaceUsed;
    return info;
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

void CouchKVStore::saveCollectionStats(
        CouchKVStoreTransactionContext& txnCtx,
        Db& db,
        CollectionID cid,
        const Collections::VB::PersistedStats& stats) {
    txnCtx.pendingLocalReqsQ.emplace_back(getCollectionStatsLocalDocId(cid),
                                          stats.getLebEncodedStats());
}

void CouchKVStore::deleteCollectionStats(
        PendingLocalDocRequestQueue& pendingLocalReqsQ, CollectionID cid) {
    pendingLocalReqsQ.emplace_back(getCollectionStatsLocalDocId(cid),
                                   CouchLocalDocRequest::IsDeleted{});
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
CouchKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                 CollectionID collection) const {
    const auto& db = static_cast<const CouchKVFileHandle&>(kvFileHandle);
    return getCollectionStats(*db.getDb(),
                              getCollectionStatsLocalDocId(collection));
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
CouchKVStore::getCollectionStats(Vbid vbid, CollectionID collection) const {
    if (!collection.isUserCollection()) {
        logger.warn(
                "CouchKVStore::getCollectionStats stats are not available for "
                "cid:{}",
                collection.to_string());
        return {KVStore::GetCollectionStatsStatus::Failed,
                Collections::VB::PersistedStats{}};
    }
    auto fileHandle = makeFileHandle(vbid);
    if (!fileHandle) {
        logger.warn(
                "CouchKVStore::getCollectionStats unable to get file handle "
                "for "
                "{}",
                vbid);
        return {KVStore::GetCollectionStatsStatus::Failed,
                Collections::VB::PersistedStats{}};
    }
    const auto& db = static_cast<const CouchKVFileHandle&>(*fileHandle);
    return getCollectionStats(*db.getDb(),
                              getCollectionStatsLocalDocId(collection));
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
CouchKVStore::getCollectionStats(Db& db, const std::string& statDocName) const {
    sized_buf id;
    id.buf = const_cast<char*>(statDocName.c_str());
    id.size = statDocName.size();

    LocalDocHolder lDoc;
    auto errCode = couchstore_open_local_document(
            &db, (void*)id.buf, id.size, lDoc.getLocalDocAddress());

    if (errCode != COUCHSTORE_SUCCESS) {
        // Could be a deleted collection, so not found not an issue
        if (errCode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.warn(
                    "CouchKVStore::getCollectionStats doc:{}"
                    "couchstore_open_local_document error:{}",
                    statDocName,
                    couchstore_strerror(errCode));
            return {KVStore::GetCollectionStatsStatus::Failed,
                    Collections::VB::PersistedStats{}};
        }
        // Return Collections::VB::PersistedStats() with everything set to
        // 0 as the collection might have not been persisted to disk yet.
        return {KVStore::GetCollectionStatsStatus::NotFound,
                Collections::VB::PersistedStats{}};
    }

    DbInfo info;
    errCode = couchstore_db_info(&db, &info);
    if (errCode) {
        logger.warn(
                "CouchKVStore::getCollectionStats doc:{}"
                "couchstore_db_info error:{}",
                statDocName,
                couchstore_strerror(errCode));
    }

    return {KVStore::GetCollectionStatsStatus::Success,
            Collections::VB::PersistedStats(lDoc.getLocalDoc()->json.buf,
                                            lDoc.getLocalDoc()->json.size,
                                            info.space_used)};
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
    const KVStoreStats& st = cbCtx->cks.getKVStoreStat();

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

    const auto valueFilter = bg_itm_ctx.getValueFilter();
    couchstore_error_t errCode = cbCtx->cks.fetchDoc(
            db, docinfo, bg_itm_ctx.value, cbCtx->vbId, valueFilter);
    if (errCode != COUCHSTORE_SUCCESS &&
        (valueFilter != ValueFilter::KEYS_ONLY)) {
        st.numGetFailure++;
    }

    bg_itm_ctx.value.setStatus(cbCtx->cks.couchErr2EngineErr(errCode));

    bool return_val_ownership_transferred = false;
    for (auto& fetch : bg_itm_ctx.getRequests()) {
        return_val_ownership_transferred = true;
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

void CouchKVStore::closeDatabaseHandle(Db* db) const {
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

cb::engine_errc CouchKVStore::couchErr2EngineErr(
        couchstore_error_t errCode) const {
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return cb::engine_errc::success;
    case COUCHSTORE_ERROR_ALLOC_FAIL:
        return cb::engine_errc::no_memory;
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        return cb::engine_errc::no_such_key;
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
    case COUCHSTORE_ERROR_NO_HEADER:
    default:
        // same as the general error return code of
        // EPBucket::getInternal
        return cb::engine_errc::temporary_failure;
    }
}

size_t CouchKVStore::getNumPersistedDeletes(Vbid vbid) {
    return cachedDeleteCount[getCacheSlot(vbid)];
}

DBFileInfo CouchKVStore::getDbFileInfo(Vbid vbid) {
    const auto info = getDbInfo(vbid);
    return DBFileInfo{info.fileSize,
                      info.spaceUsed,
                      cachedOnDiskPrepareSize[getCacheSlot(vbid)]};
}

DBFileInfo CouchKVStore::getAggrDbFileInfo() {
    DBFileInfo kvsFileInfo;
    /**
     * Iterate over all the vbuckets to get the total.
     * If the vbucket is dead, then its value would
     * be zero.
     */

    for (uint16_t vbid = 0; vbid < cachedFileSize.size(); vbid++) {
        kvsFileInfo.fileSize += cachedFileSize[vbid].load();
        kvsFileInfo.spaceUsed += cachedSpaceUsed[vbid].load();
        kvsFileInfo.prepareBytes += cachedOnDiskPrepareSize[vbid].load();
    }
    return kvsFileInfo;
}

size_t CouchKVStore::getItemCount(Vbid vbid) {
    return getDbInfo(vbid).docCount;
}

RollbackResult CouchKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::unique_ptr<RollbackCB> cb) {
    // Note that this isn't racy as we'll hold the vbucket lock
    if (vbCompactionRunning[getCacheSlot(vbid)]) {
        // Set the flag so that the compactor aborts the compaction
        vbAbortCompaction[getCacheSlot(vbid)] = true;
    }

    // Open the vbucket's file and determine the latestSeqno persisted.
    DbHolder db(*this);
    auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    auto errCode = openDB(vbid, db, options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__ + std::to_string(__LINE__),
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);

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
    options = 0;
    errCode = openDB(vbid, newdb->getDbHolder(), options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__ + std::to_string(__LINE__),
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);
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

    if (configuration.isMidpointRollbackOptimisationEnabled() &&
        (totSeqCount / 2) <= rollbackSeqCount) {
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

    auto ctx = initBySeqnoScanContext(
            std::move(cb),
            std::move(cl),
            vbid,
            info.updateSeqNum + 1,
            DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS,
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
    cachedDeleteCount[getCacheSlot(vbid)] = info.deletedCount;

    // Append the rewinded header to the database file
    errCode = couchstore_commit(handle->getDbHolder());

    if (errCode != COUCHSTORE_SUCCESS) {
        return RollbackResult(false);
    }

    vbucket_state* vb_state = getCachedVBucketState(vbid);
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

cb::engine_errc CouchKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& start_key,
        uint32_t count,
        std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    couchstore_error_t errCode = openDB(vbid, db, options);
    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);
        return cb::engine_errc::failed;
    }

    sized_buf ref = to_sized_buf(start_key);

    AllKeysCtx ctx(cb, count);

    auto start = std::chrono::steady_clock::now();
    errCode = couchstore_all_docs(db,
                                  &ref,
                                  COUCHSTORE_NO_DELETES,
                                  populateAllKeys,
                                  static_cast<void*>(&ctx));
    if (!(errCode == COUCHSTORE_SUCCESS ||
          errCode == COUCHSTORE_ERROR_CANCEL)) {
        logger.warn(
                "CouchKVStore::getAllKeys: couchstore_all_docs "
                "error:{} [{}] {}, rev:{}",
                couchstore_strerror(errCode),
                cb_strerror(),
                vbid,
                db.getFileRev());
        return cb::engine_errc::failed;
    }

    st.getAllKeysHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start));
    return cb::engine_errc::success;
}

void CouchKVStore::unlinkCouchFile(Vbid vbucket, uint64_t fRev) {
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
            pendingFileDeletions.wlock()->push(file_str);
        }
    }
}

void CouchKVStore::maybeRemoveCompactFile(const std::string& filename,
                                          Vbid vbid) {
    const auto compact_file =
            getDBFileName(filename, vbid, getDbRevision(vbid)) +
            ".compact";

    removeCompactFile(compact_file, vbid);
}

void CouchKVStore::removeCompactFile(const std::string& filename, Vbid vbid) {
    if (cb::io::isFile(filename)) {
        if (remove(filename.c_str()) == 0) {
            logger.info(
                    "CouchKVStore::removeCompactFile: {} Removed compact "
                    "filename:{}",
                    vbid,
                    filename);
        }
        else {
            logger.warn(
                    "CouchKVStore::removeCompactFile: {} remove error:{}, "
                    "filename:{}",
                    vbid,
                    cb_strerror(),
                    filename);

            if (errno != ENOENT) {
                pendingFileDeletions.wlock()->push(filename);
            }
        }
    }
}

std::unique_ptr<KVFileHandle> CouchKVStore::makeFileHandle(Vbid vbid) const {
    auto db = std::make_unique<CouchKVFileHandle>(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openDB(vbid, db->getDbHolder(), options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db->getDbHolder().getFilename(),
                     options);

        return {};
    }

    return std::move(db);
}

void CouchKVStore::prepareToCreateImpl(Vbid vbid) {
    (*dbFileRevMap->wlock())[getCacheSlot(vbid)]++;
}

std::unique_ptr<KVStoreRevision> CouchKVStore::prepareToDeleteImpl(Vbid vbid) {
    // Clear the stats so it looks empty (real deletion of the disk data occurs
    // later)
    cachedDeleteCount[getCacheSlot(vbid)] = 0;
    cachedFileSize[getCacheSlot(vbid)] = 0;
    cachedSpaceUsed[getCacheSlot(vbid)] = 0;
    cachedOnDiskPrepareSize[getCacheSlot(vbid)] = 0;
    return std::make_unique<KVStoreRevision>(getDbRevision(vbid));
}

CouchKVStore::ReadLocalDocResult CouchKVStore::readLocalDoc(
        Db& db, std::string_view name) const {
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

        return {errCode, LocalDocHolder()};
    }

    return {COUCHSTORE_SUCCESS, std::move(lDoc)};
}

std::optional<Collections::ManifestUid> CouchKVStore::getCollectionsManifestUid(
        KVFileHandle& kvFileHandle) const {
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(kvFileHandle);
    auto db = couchKvHandle.getDb();
    auto manifestRes = readLocalDoc(*db, LocalDocKey::manifest);
    if (manifestRes.status != COUCHSTORE_SUCCESS) {
        if (manifestRes.status == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            return Collections::ManifestUid{0};
        } else {
            logger.warn(
                    "CouchKVStore::getCollectionsManifestUid(): "
                    "error:{}",
                    couchstore_strerror(manifestRes.status));
            return std::nullopt;
        }
    }
    return Collections::KVStore::decodeManifestUid(manifestRes.doc.getBuffer());
}

std::pair<bool, Collections::KVStore::Manifest>
CouchKVStore::getCollectionsManifest(Vbid vbid) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openDB(vbid, db, options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);
        return {false,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    auto [status, manifest] = getCollectionsManifest(*db.getDb());
    bool ret = status == COUCHSTORE_SUCCESS ? true : false;
    return {ret, manifest};
}

std::pair<couchstore_error_t, Collections::KVStore::Manifest>
CouchKVStore::getCollectionsManifest(Db& db) const {
    auto manifestRes = readLocalDoc(db, LocalDocKey::manifest);
    if (manifestRes.status != COUCHSTORE_SUCCESS &&
        manifestRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return {manifestRes.status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    auto collectionsRes = readLocalDoc(db, LocalDocKey::openCollections);
    if (collectionsRes.status != COUCHSTORE_SUCCESS &&
        collectionsRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return {collectionsRes.status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    auto scopesRes = readLocalDoc(db, LocalDocKey::openScopes);
    if (scopesRes.status != COUCHSTORE_SUCCESS &&
        scopesRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return {scopesRes.status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    auto droppedRes = readLocalDoc(db, LocalDocKey::droppedCollections);
    if (droppedRes.status != COUCHSTORE_SUCCESS &&
        droppedRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return {droppedRes.status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    cb::const_byte_buffer empty;
    return {COUCHSTORE_SUCCESS,
            Collections::KVStore::decodeManifest(
                    manifestRes.doc.getLocalDoc() ? manifestRes.doc.getBuffer()
                                                  : empty,
                    collectionsRes.doc.getLocalDoc()
                            ? collectionsRes.doc.getBuffer()
                            : empty,
                    scopesRes.doc.getLocalDoc() ? scopesRes.doc.getBuffer()
                                                : empty,
                    droppedRes.doc.getLocalDoc() ? droppedRes.doc.getBuffer()
                                                 : empty)};
}

std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
CouchKVStore::getDroppedCollections(Vbid vbid) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errCode = openDB(vbid, db, options);

    if (errCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errCode,
                     vbid,
                     db.getFilename(),
                     options);

        return {false, {}};
    }

    auto [getDroppedStatus, droppedCollections] =
            getDroppedCollections(*db.getDb());
    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
        return {false, {}};
    }

    return {true, droppedCollections};
}

std::pair<couchstore_error_t,
          std::vector<Collections::KVStore::DroppedCollection>>
CouchKVStore::getDroppedCollections(Db& db) const {
    auto droppedRes = readLocalDoc(db, LocalDocKey::droppedCollections);

    if (droppedRes.status == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        // Doc not found case, remap to success as that means there are no
        // dropped collections
        return {COUCHSTORE_SUCCESS, {}};
    }

    if (droppedRes.status != COUCHSTORE_SUCCESS) {
        // Error case, return status up
        return {droppedRes.status, {}};
    }

    // Decode will throw if the passed in data is invalid
    return {COUCHSTORE_SUCCESS,
            Collections::KVStore::decodeDroppedCollections(
                    droppedRes.doc.getBuffer())};
}

couchstore_error_t CouchKVStore::updateCollectionsMeta(
        CouchKVStoreTransactionContext& txnCtx,
        Db& db,
        Collections::VB::Flush& collectionsFlush) {
    updateManifestUid(txnCtx, collectionsFlush);

    if (collectionsFlush.isOpenCollectionsChanged()) {
        auto status = updateOpenCollections(txnCtx, db, collectionsFlush);
        if (status != COUCHSTORE_SUCCESS) {
            return status;
        }
    }

    if (collectionsFlush.isDroppedCollectionsChanged()) {
        auto status = updateDroppedCollections(txnCtx, db, collectionsFlush);
        if (status != COUCHSTORE_SUCCESS) {
            return status;
        }
    }

    if (collectionsFlush.isScopesChanged()) {
        auto status = updateScopes(txnCtx, db, collectionsFlush);
        if (status != COUCHSTORE_SUCCESS) {
            return status;
        }
    }

    return COUCHSTORE_SUCCESS;
}

void CouchKVStore::updateManifestUid(CouchKVStoreTransactionContext& txnCtx,
                                     Collections::VB::Flush& collectionsFlush) {
    // write back, no read required
    txnCtx.pendingLocalReqsQ.emplace_back(std::string{LocalDocKey::manifest},
                                          collectionsFlush.encodeManifestUid());
}

couchstore_error_t CouchKVStore::updateOpenCollections(
        CouchKVStoreTransactionContext& txnCtx,
        Db& db,
        Collections::VB::Flush& collectionsFlush) {
    auto collectionsRes = readLocalDoc(db, LocalDocKey::openCollections);

    if (collectionsRes.status != COUCHSTORE_SUCCESS &&
        collectionsRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return collectionsRes.status;
    }

    cb::const_byte_buffer empty;

    txnCtx.pendingLocalReqsQ.emplace_back(
            std::string{LocalDocKey::openCollections},
            collectionsFlush.encodeOpenCollections(
                    collectionsRes.doc.getLocalDoc()
                            ? collectionsRes.doc.getBuffer()
                            : empty));
    return COUCHSTORE_SUCCESS;
}

couchstore_error_t CouchKVStore::updateDroppedCollections(
        CouchKVStoreTransactionContext& txnCtx,
        Db& db,
        Collections::VB::Flush& collectionsFlush) {
    // Delete the stats doc for dropped collections
    collectionsFlush.forEachDroppedCollection([this, &txnCtx](CollectionID id) {
        this->deleteCollectionStats(txnCtx.pendingLocalReqsQ, id);
    });

    auto [getDroppedStatus, dropped] = getDroppedCollections(db);
    if (getDroppedStatus != COUCHSTORE_SUCCESS) {
        return getDroppedStatus;
    }

    auto encodedDroppedCollections =
            collectionsFlush.encodeDroppedCollections(dropped);
    if (encodedDroppedCollections.data()) {
        txnCtx.pendingLocalReqsQ.emplace_back(
                std::string{LocalDocKey::droppedCollections},
                encodedDroppedCollections);
    }

    return COUCHSTORE_SUCCESS;
}

couchstore_error_t CouchKVStore::updateScopes(
        CouchKVStoreTransactionContext& txnCtx,
        Db& db,
        Collections::VB::Flush& collectionsFlush) {
    auto scopesRes = readLocalDoc(db, LocalDocKey::openScopes);

    if (scopesRes.status != COUCHSTORE_SUCCESS &&
        scopesRes.status != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        return scopesRes.status;
    }

    cb::const_byte_buffer empty;
    txnCtx.pendingLocalReqsQ.emplace_back(
            std::string{LocalDocKey::openScopes},
            collectionsFlush.encodeOpenScopes(
                    scopesRes.doc.getLocalDoc() ? scopesRes.doc.getBuffer()
                                                : empty));

    return COUCHSTORE_SUCCESS;
}

const KVStoreConfig& CouchKVStore::getConfig() const {
    return configuration;
}

KVStoreIface::ReadVBStateResult CouchKVStore::getPersistedVBucketState(
        Vbid vbid) const {
    DbHolder db(*this);
    const auto options = COUCHSTORE_OPEN_FLAG_RDONLY;
    const auto errorCode = openDB(vbid, db, options);
    if (errorCode != COUCHSTORE_SUCCESS) {
        logOpenError(__func__,
                     spdlog::level::level_enum::warn,
                     errorCode,
                     vbid,
                     db.getFilename(),
                     options);

        throw std::logic_error(
                "CouchKVStore::getPersistedVBucketState: openDB error:" +
                std::string(couchstore_strerror(errorCode)) +
                ", file:" + getDBFileName(dbname, vbid, db.getFileRev()));
    }

    const auto res = readVBState(db, vbid);
    // @TODO MB-51413 Expand the status returned by this. NotFound is valid
    // in certain circumstances and we should neither throw nor return the
    // default constructed vbucket_state.
    if (res.status != ReadVBStateStatus::Success &&
        res.status != ReadVBStateStatus::NotFound) {
        throw std::logic_error(
                "CouchKVStore::getPersistedVBucketState: readVBState error:" +
                to_string(res.status) +
                ", file:" + getDBFileName(dbname, vbid, db.getFileRev()));
    }

    return res;
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
void CouchKVStore::abortCompactionIfRunning(
        std::unique_lock<std::mutex>& vbLock, Vbid vbid) {
    if (!vbLock.owns_lock()) {
        throw std::logic_error(
                "CouchKVStore::abortCompactionIfRunning: lock should be held");
    }
    // Note that this isn't racy as we'll hold the vbucket lock
    if (vbCompactionRunning[getCacheSlot(vbid)]) {
        // Set the flag so that the compactor aborts the compaction
        vbAbortCompaction[getCacheSlot(vbid)] = true;
    }
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

std::optional<DbHolder> CouchKVStore::openOrCreate(Vbid vbid) noexcept {
    // Do we already have a well-formed file for this vbid?
    {
        DbHolder db(*this);
        const auto options = 0;
        const auto errCode = openDB(vbid, db, options);
        if (errCode == COUCHSTORE_SUCCESS) {
            // We have a well-formed file with (rev > 0), return the DbHolder
            return std::move(db);
        }

        if (errCode != COUCHSTORE_ERROR_NO_SUCH_FILE) {
            // We have only two legal states, a legal file or no file. Anything
            // else is an error condition.
            logOpenError(__func__,
                         spdlog::level::level_enum::warn,
                         errCode,
                         vbid,
                         db.getFilename(),
                         options);
            return {};
        }
    }

    // No file for vbid, start bootstrap procedure for creating vbid.couch.rev

    // Remove temp file if any left around by a previous failed bootstrap
    const auto tempFile =
            dbname + "/" + std::to_string(vbid.get()) + ".couch.boot";
    const auto removeFileIfExists = [this](const std::string& file) -> void {
        if (cb::io::isFile(file)) {
            if (remove(file.c_str()) != 0) {
                logger.warn(
                        "CouchKVStore::openOrCreate: Failed to remove file:{}, "
                        "error:{}",
                        file,
                        cb_strerror());
            } else {
                logger.info("CouchKVStore::openOrCreate: Removed file:{}",
                            file);
            }
        }
    };
    removeFileIfExists(tempFile);

    // The following open operation will:
    // 1) Create the vbid.couch.boot temp file
    // 2) Write the first header (filepos 0) to the OS buffer-cache
    {
        DbHolder db(*this);
        const auto options =
                COUCHSTORE_OPEN_FLAG_CREATE | COUCHSTORE_OPEN_FLAG_EXCL;
        auto errCode =
                openSpecificDBFile(vbid, 0 /*fileRev*/, db, options, tempFile);
        if (errCode != COUCHSTORE_SUCCESS) {
            // Any failure may leave an empty temp-file around
            removeFileIfExists(tempFile);

            logOpenError(__func__,
                         spdlog::level::level_enum::warn,
                         errCode,
                         vbid,
                         tempFile,
                         options);
            return {};
        }

        // Note: At this point the temp file is created, so any failure requires
        // to remove it in case.

        // Sync temp-file to disk
        errCode = couchstore_commit(db);
        if (errCode != COUCHSTORE_SUCCESS) {
            removeFileIfExists(tempFile);
            logger.warn(
                    "CouchKVStore::openOrCreate: couchstore_commit error:{}, "
                    "file:{}",
                    std::string(couchstore_strerror(errCode)),
                    tempFile);
            return {};
        }
    }

    // Rename vbid.couch.boot into vbid.couch.rev
    {
        const auto lockedRevMap = dbFileRevMap->rlock();
        const auto revision = (*lockedRevMap)[getCacheSlot(vbid)];
        const auto newFile = getDBFileName(dbname, vbid, revision);
        if (rename(tempFile.c_str(), newFile.c_str()) != 0) {
            removeFileIfExists(tempFile);
            // Note: On the FileSystems that we support (so not NFS for example)
            // a failure at rename() should guarantee that the new file is not
            // created, but better to be resilient to OS bugs / misbehaviour.
            removeFileIfExists(newFile);
            logger.warn(
                    "CouchKVStore::openOrCreate: rename error:{}, tempFile:{}, "
                    "newFile:{}",
                    cb_strerror(), // errno
                    tempFile,
                    newFile);
            return {};
        }
    }

    DbHolder db(*this);
    const auto res = openDB(vbid, db, 0 /*flags*/);
    if (res != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::openOrCreate: Post-create openDB error:{}, "
                "file:{}",
                std::string(couchstore_strerror(res)),
                getDBFileName(dbname, vbid, db.getFileRev()));
        return {};
    }

    return std::move(db);
}

GetValue CouchKVStore::getBySeqno(KVFileHandle& handle,
                                  Vbid vbid,
                                  uint64_t seq,
                                  ValueFilter filter) const {
    auto& couchKvHandle = static_cast<CouchKVFileHandle&>(handle);
    auto& db = couchKvHandle.getDbHolder();

    DocInfo* docInfo = nullptr;
    auto status = couchstore_docinfo_by_sequence(db, seq, &docInfo);

    if (status != COUCHSTORE_SUCCESS || docInfo == nullptr) {
        return GetValue(nullptr, couchErr2EngineErr(status));
    }

    GetValue rv;
    status = fetchDoc(db, docInfo, rv, vbid, filter);

    if (status != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
    }
    couchstore_free_docinfo(docInfo);
    rv.setStatus(couchErr2EngineErr(status));
    return rv;
}

std::unique_ptr<TransactionContext> CouchKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    if (!startTransaction(vbid)) {
        return {};
    }

    return std::make_unique<CouchKVStoreTransactionContext>(
            *this, vbid, std::move(pcb));
}
