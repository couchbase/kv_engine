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

#include "config.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <gsl/gsl>
#include <list>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "bucket_logger.h"
#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
#include "ep_types.h"
#include "kvstore_config.h"
#include "statwriter.h"
#include "vbucket.h"
#include "vbucket_bgfetch_item.h"

#include <JSON_checker.h>
#include <kvstore.h>
#include <platform/compress.h>

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

struct kvstats_ctx {
    kvstats_ctx(bool persistDocNamespace,
                Collections::VB::Flush& collectionsFlush)
        : persistDocNamespace(persistDocNamespace),
          collectionsFlush(collectionsFlush) {
    }
    /// A map of key to bool. If true, the key exists in the VB datafile
    std::unordered_map<StoredDocKey, bool> keyStats;
    /// Collections: When enabled this means persisted keys have namespaces
    bool persistDocNamespace;

    /// Collection flusher data for managing manifest changes and item counts
    Collections::VB::Flush& collectionsFlush;
};

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

static int getMutationStatus(couchstore_error_t errCode) {
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return MUTATION_SUCCESS;
    case COUCHSTORE_ERROR_NO_HEADER:
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        // this return causes ep engine to drop the failed flush
        // of an item since it does not know about the itme any longer
        return DOC_NOT_FOUND;
    default:
        // this return causes ep engine to keep requeuing the failed
        // flush of an item
        return MUTATION_FAILED;
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
    return (err == COUCHSTORE_ERROR_OPEN_FILE ||
            err == COUCHSTORE_ERROR_READ ||
            err == COUCHSTORE_ERROR_WRITE ||
            err == COUCHSTORE_ERROR_FILE_CLOSE) ? getStrError(db) : "none";
}

static DocKey makeDocKey(const sized_buf buf, bool restoreNamespace) {
    return DocKey(reinterpret_cast<const uint8_t*>(buf.buf),
                  buf.size,
                  restoreNamespace ? DocKeyEncodesCollectionId::Yes
                                   : DocKeyEncodesCollectionId::No);
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
    AllKeysCtx(std::shared_ptr<Callback<const DocKey&>> callback,
               uint32_t cnt,
               bool persistDocNamespace)
        : cb(callback), count(cnt), persistDocNamespace(persistDocNamespace) {
    }

    std::shared_ptr<Callback<const DocKey&>> cb;
    uint32_t count;
    bool persistDocNamespace{false};
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

CouchRequest::CouchRequest(const Item& it,
                           uint64_t rev,
                           MutationRequestCallback& cb,
                           bool del,
                           bool persistDocNamespace)
    : IORequest(it.getVBucketId(), cb, del, it.getKey()),
      value(it.getValue()),
      fileRevNum(rev) {
    // Collections: TODO: Temporary switch to ensure upgrades don't break.
    if (!persistDocNamespace) {
        auto noprefix = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
                {key.data(), key.size()});
        dbDoc.id = {const_cast<char*>(
                            reinterpret_cast<const char*>(noprefix.data())),
                    noprefix.size()};
    } else {
        dbDoc.id = {
                const_cast<char*>(reinterpret_cast<const char*>(key.data())),
                key.size()};
    }

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

    dbDocInfo.db_seq = it.getBySeqno();

    // Now allocate space to hold the meta and get it ready for storage
    dbDocInfo.rev_meta.size = MetaData::getMetaDataSize(MetaData::Version::V1);
    dbDocInfo.rev_meta.buf = meta.prepareAndGetForPersistence();

    dbDocInfo.rev_seq = it.getRevSeqno();
    dbDocInfo.size = dbDoc.data.size;

    if (del) {
        dbDocInfo.deleted =  1;
        meta.setDeleteSource(it.deletionSource());
    } else {
        dbDocInfo.deleted = 0;
    }
    dbDocInfo.id = dbDoc.id;
    dbDocInfo.content_meta = getContentMeta(it);
}

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
    cachedDocCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(0));
    cachedDeleteCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(-1));
    cachedFileSize.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(0));
    cachedSpaceUsed.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(0));
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
            auto engineError = readVBState(db, id);
            // We return ENGINE_EINVAL if something went wrong with the JSON
            // parsing, all other error codes are acceptable at this point in
            // the code
            if (engineError != ENGINE_EINVAL) {
                /* update stat */
                ++st.numLoadedVb;
            } else {
                logger.warn(
                        "CouchKVStore::initialize: readVBState"
                        " error:{}, name:{}/{}.couch.{}",
                        cb::to_string(cb::to_engine_errc(engineError)),
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
        incrementRevision(vbucketId);

        setVBucketState(
                vbucketId, *state, VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT);
    } else {
        throw std::invalid_argument(
                "CouchKVStore::reset: No entry in cached "
                "states for " +
                vbucketId.to_string());
    }
}

void CouchKVStore::set(const Item& itm,
                       Callback<TransactionContext, mutation_result>& cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::set: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("CouchKVStore::set: intransaction must be "
                        "true to perform a set operation.");
    }

    bool deleteItem = false;
    MutationRequestCallback requestcb;
    uint64_t fileRev = (*dbFileRevMap)[itm.getVBucketId().get()];

    // each req will be de-allocated after commit
    requestcb.setCb = &cb;
    CouchRequest* req =
            new CouchRequest(itm,
                             fileRev,
                             requestcb,
                             deleteItem,
                             configuration.shouldPersistDocNamespace());
    pendingReqsQ.push_back(req);
}

GetValue CouchKVStore::get(const StoredDocKey& key, Vbid vb, bool fetchDelete) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vb, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        logger.warn("CouchKVStore::get: openDB error:{}, {}",
                    couchstore_strerror(errCode),
                    vb);
        return GetValue(nullptr, couchErr2EngineErr(errCode));
    }

    GetValue gv = getWithHeader(db, key, vb, GetMetaOnly::No, fetchDelete);
    return gv;
}

GetValue CouchKVStore::getWithHeader(void* dbHandle,
                                     const StoredDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly,
                                     bool fetchDelete) {
    Db *db = (Db *)dbHandle;
    auto start = std::chrono::steady_clock::now();
    DocInfo *docInfo = NULL;
    sized_buf id;
    GetValue rv;

    if (!configuration.shouldPersistDocNamespace()) {
        auto noprefix = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
                {key.data(), key.size()});
        id = {const_cast<char*>(reinterpret_cast<const char*>(noprefix.data())),
              noprefix.size()};
    } else {
        id = {const_cast<char*>(reinterpret_cast<const char*>(key.data())),
              key.size()};
    }

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
        if (!configuration.shouldPersistDocNamespace()) {
            auto noprefix = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
                    {item.first.data(), item.first.size()});
            ids[idx] = {const_cast<char*>(
                                reinterpret_cast<const char*>(noprefix.data())),
                        noprefix.size()};
        } else {
            ids[idx] = {const_cast<char*>(reinterpret_cast<const char*>(
                                item.first.data())),
                        item.first.size()};
        }

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

void CouchKVStore::del(const Item& itm, Callback<TransactionContext, int>& cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::del: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("CouchKVStore::del: intransaction must be "
                        "true to perform a delete operation.");
    }

    uint64_t fileRev = (*dbFileRevMap)[itm.getVBucketId().get()];
    MutationRequestCallback requestcb;
    requestcb.delCb = &cb;
    CouchRequest* req =
            new CouchRequest(itm,
                             fileRev,
                             requestcb,
                             true,
                             configuration.shouldPersistDocNamespace());
    pendingReqsQ.push_back(req);
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

    if (!json.is_array()) {
        logger.warn(
                "CouchKVStore::getPersistedStats:"
                " Parsed json is not an array!!!");
        return;
    }

    for (const auto& elem : json) {
        cb_assert(elem.size() == 1);
        auto it = elem.begin();
        stats[it.key()] = it.value();
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
    cb::char_buffer data;
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
            data = inflated;
        }
    }

    // Collections: TODO: Restore to stored namespace
    Item it(makeDocKey(info.id, ctx.config->shouldPersistDocNamespace()),
            metadata.getFlags(),
            metadata.getExptime(),
            data.buf,
            data.len,
            metadata.getDataType(),
            metadata.getCas(),
            info.db_seq,
            ctx.compactConfig.db_file_id,
            info.rev_seq);

    it.setRevSeqno(info.rev_seq);
    it.setDeleted(DeleteSource::TTL);
    ctx.expiryCallback->callback(it, currtime);

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

        // Is the collections eraser installed?
        if (ctx->collectionsEraser &&
            ctx->collectionsEraser(
                    makeDocKey(info->id,
                               ctx->config->shouldPersistDocNamespace()),
                    int64_t(info->db_seq),
                    info->deleted,
                    *ctx->eraserContext)) {
            ctx->stats.collectionsItemsPurged++;
            return COUCHSTORE_COMPACT_DROP_ITEM;
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
            time_t currtime = ep_real_time();
            if (exptime && exptime < currtime) {
                int ret;
                try {
                    ret = notify_expired_item(*info, *metadata, item,
                                             *ctx, currtime);
                } catch (const std::bad_alloc&) {
                    EP_LOG_WARN("time_purge_hook: memory allocation failed");
                    return COUCHSTORE_ERROR_ALLOC_FAIL;
                }

                if (ret != COUCHSTORE_SUCCESS) {
                    return ret;
                }
            }
        }
    }

    if (ctx->bloomFilterCallback) {
        bool deleted = info->deleted;
        // Collections: TODO: Permanently restore to stored namespace
        DocKey key = makeDocKey(
                info->id, ctx->config->shouldPersistDocNamespace());

        try {
            ctx->bloomFilterCallback->callback(
                    reinterpret_cast<Vbid&>(ctx->compactConfig.db_file_id),
                    key,
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
            readCollectionsManifest(*compactdb));

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

    couchstore_open_flags openFlags =
            hook_ctx->eraserContext->needToUpdateCollectionsManifest()
                    ? 0
                    : COUCHSTORE_OPEN_FLAG_RDONLY;

    // Open the newly compacted VBucket database file ...
    errCode = openSpecificDB(vbid, new_rev, targetDb, (uint64_t)openFlags);
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

    if (hook_ctx->eraserContext->needToUpdateCollectionsManifest()) {
        // Finalise any collections metadata which may have changed during the
        // compaction due to collection Deletion
        hook_ctx->eraserContext->finaliseCollectionsManifest(
                std::bind(&CouchKVStore::saveCollectionsManifest,
                          this,
                          std::ref(*targetDb.getDb()),
                          std::placeholders::_1));

        errCode = couchstore_commit(targetDb.getDb());
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::compactDB: failed to commit collection "
                    "manifest update errCode:{}",
                    couchstore_strerror(errCode));
        }
    }

    // Update the global VBucket file map so all operations use the new file
    updateDbFileMap(vbid, new_rev);

    logger.debug("INFO: created new couch db file, name:{} rev:{}",
                 new_file,
                 new_rev);

    couchstore_db_info(targetDb.getDb(), &info);
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
    }

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
                    VBucket::toString(vbstate.state),
                    vbucketId);
            return false;
        }
    }

    EP_LOG_DEBUG("CouchKVStore::snapshotVBucket: Snapshotted {} state:{}",
                 vbucketId,
                 vbstate.toJSON());

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

    if (!pendingFileDeletions.empty()) {
        std::queue<std::string> queue;
        pendingFileDeletions.getAll(queue);

        while (!queue.empty()) {
            std::string filename_str = queue.front();
            if (remove(filename_str.c_str()) == -1) {
                logger.warn(
                        "CouchKVStore::pendingTasks: "
                        "remove error:{}, file{}",
                        errno,
                        filename_str);
                if (errno != ENOENT) {
                    pendingFileDeletions.push(filename_str);
                }
            }
            queue.pop();
        }
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

    size_t scanId = scanCounter++;

    auto collectionsManifest = readCollectionsManifest(*db);

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
                                        configuration,
                                        collectionsManifest);
    sctx->logger = &logger;
    return sctx;
}

static couchstore_docinfos_options getDocFilter(const DocumentFilter& filter) {
    switch (filter) {
    case DocumentFilter::ALL_ITEMS:
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
    std::lock_guard<cb::WriterLock> lg(openDbMutex);

    (*dbFileRevMap)[vbucketId.get()] = newFileRev;
}

couchstore_error_t CouchKVStore::openDB(Vbid vbucketId,
                                        DbHolder& db,
                                        couchstore_open_flags options,
                                        FileOpsInterface* ops) {
    // MB-27963: obtain read access whilst we open the file, updateDbFileMap
    // serialises on this mutex so we can be sure the fileRev we read should
    // still be a valid file once we hit sys_open
    std::lock_guard<cb::ReaderLock> lg(openDbMutex);
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
        size_t firstSlash = nameKey.rfind(DIRECTORY_SEPARATOR_CHARACTER);

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
            if (access(old_file.str().c_str(), F_OK) == 0) {
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
        // Collections: TODO: Permanently restore to stored namespace
        auto it = std::make_unique<Item>(
                makeDocKey(docinfo->id,
                           configuration.shouldPersistDocNamespace()),
                metadata->getFlags(),
                metadata->getExptime(),
                nullptr,
                docinfo->size,
                metadata->getDataType(),
                metadata->getCas(),
                docinfo->db_seq,
                vbId);

        it->setRevSeqno(docinfo->rev_seq);

        if (docinfo->deleted) {
            it->setDeleted(metadata->getDeleteSource());
        }
        docValue = GetValue(std::move(it));
        // update ep-engine IO stats
        ++st.io_bg_fetch_docs_read;
        st.io_bgfetch_doc_bytes += (docinfo->id.size + docinfo->rev_meta.size);
    } else {
        Doc *doc = nullptr;
        size_t valuelen = 0;
        void* valuePtr = nullptr;
        protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
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

            valuelen = doc->data.size;
            valuePtr = doc->data.buf;

            if (metadata->getVersionInitialisedFrom() == MetaData::Version::V0) {
                // This is a super old version of a couchstore file.
                // Try to determine if the document is JSON or raw bytes
                datatype = determine_datatype(doc->data);
            } else {
                datatype = metadata->getDataType();
            }
        } else if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND && docinfo->deleted) {
            datatype = metadata->getDataType();
        } else {
            return errCode;
        }

        try {
            // Collections: TODO: Restore to stored namespace
            auto it = std::make_unique<Item>(
                    makeDocKey(docinfo->id,
                               configuration.shouldPersistDocNamespace()),
                    metadata->getFlags(),
                    metadata->getExptime(),
                    valuePtr,
                    valuelen,
                    datatype,
                    metadata->getCas(),
                    docinfo->db_seq,
                    vbId,
                    docinfo->rev_seq);

             if (docinfo->deleted) {
                 it->setDeleted(metadata->getDeleteSource());
             }
             docValue = GetValue(std::move(it));
        } catch (std::bad_alloc&) {
            couchstore_free_document(doc);
            return COUCHSTORE_ERROR_ALLOC_FAIL;
        }

        // update ep-engine IO stats
        ++st.io_bg_fetch_docs_read;
        st.io_bgfetch_doc_bytes +=
                (docinfo->id.size + docinfo->rev_meta.size + valuelen);

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

    // Collections: TODO: Permanently restore to stored namespace
    DocKey docKey = makeDocKey(
            docinfo->id, sctx->config.shouldPersistDocNamespace());

    auto collectionsRHandle = sctx->collectionsContext.lockCollections(
            docKey, true /*system allowed*/);
    CacheLookup lookup(docKey, byseqno, vbucketId, collectionsRHandle);

    cl->callback(lookup);
    if (cl->getStatus() == ENGINE_KEY_EEXISTS) {
        sctx->lastReadSeqno = byseqno;
        return COUCHSTORE_SUCCESS;
    } else if (cl->getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
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

    // Collections: TODO: Permanently restore to stored namespace
    auto it = std::make_unique<Item>(
            DocKey(makeDocKey(key, sctx->config.shouldPersistDocNamespace())),
            metadata->getFlags(),
            metadata->getExptime(),
            value.buf,
            value.size,
            metadata->getDataType(),
            metadata->getCas(),
            docinfo->db_seq, // return seq number being persisted on disk
            vbucketId,
            docinfo->rev_seq);

    if (docinfo->deleted) {
        it->setDeleted(metadata->getDeleteSource());
    }

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
    if (pendingCommitCnt == 0 &&
        !collectionsFlush.getCollectionsManifestItem()) {
        return success;
    }

    // Use the vbucket of the first item or the manifest item
    auto vbucket2flush =
            pendingCommitCnt ? pendingReqsQ[0]->getVBucketId()
                             : collectionsFlush.getCollectionsManifestItem()
                                       ->getVBucketId();

    TRACE_EVENT2("CouchKVStore",
                 "commit2couchstore",
                 "vbid",
                 vbucket2flush.get(),
                 "pendingCommitCnt",
                 pendingCommitCnt);

    // When an item and a manifest are present, vbucket2flush is read from the
    // item. Check it matches the manifest
    if (pendingCommitCnt && collectionsFlush.getCollectionsManifestItem() &&
        vbucket2flush !=
                collectionsFlush.getCollectionsManifestItem()->getVBucketId()) {
        throw std::logic_error(
                "CouchKVStore::commit2couchstore: manifest/item vbucket "
                "mismatch vbucket2flush:" +
                std::to_string(vbucket2flush.get()) + " manifest " +
                collectionsFlush.getCollectionsManifestItem()
                        ->getVBucketId()
                        .to_string());
    }

    std::vector<Doc*> docs(pendingCommitCnt);
    std::vector<DocInfo*> docinfos(pendingCommitCnt);

    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        CouchRequest *req = pendingReqsQ[i];
        docs[i] = (Doc *)req->getDbDoc();
        docinfos[i] = req->getDbDocInfo();
        if (vbucket2flush != req->getVBucketId()) {
            throw std::logic_error(
                    "CouchKVStore::commit2couchstore: "
                    "mismatch between vbucket2flush (which is " +
                    vbucket2flush.to_string() + ") and pendingReqsQ[" +
                    std::to_string(i) + "] (which is " +
                    req->getVBucketId().to_string() + ")");
        }
    }

    // The docinfo callback needs to know if the CollectionID feature is on
    kvstats_ctx kvctx(configuration.shouldPersistDocNamespace(),
                      collectionsFlush);
    // flush all
    couchstore_error_t errCode =
            saveDocs(vbucket2flush, docs, docinfos, kvctx, collectionsFlush);

    if (errCode) {
        success = false;
        logger.warn(
                "CouchKVStore::commit2couchstore: saveDocs error:{}, "
                "{}",
                couchstore_strerror(errCode),
                vbucket2flush);
    }

    commitCallback(pendingReqsQ, kvctx, errCode);

    // clean up
    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        delete pendingReqsQ[i];
    }
    pendingReqsQ.clear();
    return success;
}

// Callback when the btree is updated which we use for tracking create/update
// type statistics.
static void saveDocsCallback(const DocInfo* oldInfo,
                             const DocInfo* newInfo,
                             void* context) {
    kvstats_ctx* cbCtx = static_cast<kvstats_ctx*>(context);
    // Replacing a document
    if (oldInfo && newInfo) {
        // Old is not deleted
        if (!oldInfo->deleted) {
            auto itr = cbCtx->keyStats.find(
                    makeDocKey(newInfo->id, cbCtx->persistDocNamespace));
            if (itr != cbCtx->keyStats.end()) {
                itr->second = true; // mark this key as replaced
            }

            // New is deleted, so decrement count
            if (newInfo->deleted && cbCtx->persistDocNamespace) {
                cbCtx->collectionsFlush.decrementDiskCount(
                        makeDocKey(newInfo->id, cbCtx->persistDocNamespace));
            }
        } else if (!newInfo->deleted && cbCtx->persistDocNamespace) {
            // Adding an item
            cbCtx->collectionsFlush.incrementDiskCount(
                    makeDocKey(newInfo->id, cbCtx->persistDocNamespace));
        }

    } else if (newInfo && !newInfo->deleted && cbCtx->persistDocNamespace) {
        // Adding an item
        cbCtx->collectionsFlush.incrementDiskCount(
                makeDocKey(newInfo->id, cbCtx->persistDocNamespace));
    }
}

couchstore_error_t CouchKVStore::saveDocs(
        Vbid vbid,
        const std::vector<Doc*>& docs,
        std::vector<DocInfo*>& docinfos,
        kvstats_ctx& kvctx,
        Collections::VB::Flush& collectionsFlush) {
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

        // Only do a couchstore_save_documents if there are docs
        if (docs.size() > 0) {
            std::vector<sized_buf> ids(docs.size());
            for (size_t idx = 0; idx < docs.size(); idx++) {
                ids[idx] = docinfos[idx]->id;
                maxDBSeqno = std::max(maxDBSeqno, docinfos[idx]->db_seq);
                DocKey key = makeDocKey(
                        ids[idx], configuration.shouldPersistDocNamespace());
                kvctx.keyStats[key] = false;
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

        // Only saving collection stats if collections enabled
        if (configuration.shouldPersistDocNamespace()) {
            collectionsFlush.saveItemCounts(
                    std::bind(&CouchKVStore::saveItemCount,
                              this,
                              std::ref(*db),
                              std::placeholders::_1,
                              std::placeholders::_2));
        }

        errCode = saveVBState(db, *state);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn("CouchKVStore::saveDocs: saveVBState error:{} [{}]",
                        couchstore_strerror(errCode),
                        couchkvstore_strerrno(db, errCode));
            return errCode;
        }

        if (collectionsFlush.getCollectionsManifestItem()) {
            auto data = collectionsFlush.getManifestData();
            saveCollectionsManifest(*db, {data.data(), data.size()});
            // Process any collection deletes (removing the item count docs)
            collectionsFlush.saveDeletes(
                    std::bind(&CouchKVStore::deleteItemCount,
                              this,
                              std::ref(*db),
                              std::placeholders::_1));
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

void CouchKVStore::commitCallback(std::vector<CouchRequest *> &committedReqs,
                                  kvstats_ctx &kvctx,
                                  couchstore_error_t errCode) {
    size_t commitSize = committedReqs.size();

    for (size_t index = 0; index < commitSize; index++) {
        size_t dataSize = committedReqs[index]->getNBytes();
        size_t keySize = committedReqs[index]->getKeySize();
        /* update ep stats */
        ++st.io_num_write;
        st.io_write_bytes += (keySize + dataSize);

        if (committedReqs[index]->isDelete()) {
            int rv = getMutationStatus(errCode);
            if (rv != -1) {
                const auto& key = committedReqs[index]->getKey();
                if (kvctx.keyStats[key]) {
                    rv = 1; // Deletion is for an existing item on DB file.
                } else {
                    rv = 0; // Deletion is for a non-existing item on DB file.
                }
            }
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta());
            }
            committedReqs[index]->getDelCallback()->callback(*transactionCtx,
                                                             rv);
        } else {
            int rv = getMutationStatus(errCode);
            const auto& key = committedReqs[index]->getKey();
            bool insertion = !kvctx.keyStats[key];
            if (errCode) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(committedReqs[index]->getDelta());
                st.writeSizeHisto.add(dataSize + keySize);
            }
            mutation_result p(rv, insertion);
            committedReqs[index]->getSetCallback()->callback(*transactionCtx,
                                                             p);
        }
    }
}

ENGINE_ERROR_CODE CouchKVStore::readVBState(Db* db, Vbid vbId) {
    sized_buf id;
    LocalDoc *ldoc = NULL;
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    int64_t highSeqno = 0;
    std::string failovers;
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    bool mightContainXattrs = false;
    bool supportsCollections = false;

    DbInfo info;
    errCode = couchstore_db_info(db, &info);
    if (errCode == COUCHSTORE_SUCCESS) {
        highSeqno = info.last_sequence;
        purgeSeqno = info.purge_seq;
    } else {
        logger.warn(
                "CouchKVStore::readVBState: couchstore_db_info error:{}"
                ", {}",
                couchstore_strerror(errCode),
                vbId);
        return couchErr2EngineErr(errCode);
    }

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    errCode = couchstore_open_local_document(db, (void *)id.buf,
                                             id.size, &ldoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.info(
                    "CouchKVStore::readVBState: '_local/vbstate' not found "
                    "for {}",
                    vbId);
        } else {
            logger.warn(
                    "CouchKVStore::readVBState: couchstore_open_local_document"
                    " error:{}, {}",
                    couchstore_strerror(errCode),
                    vbId);
        }
    } else {
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
            return ENGINE_EINVAL;
        }

        auto vb_state = json.value("state", "");
        auto checkpoint_id = json.value("checkpoint_id", "");
        auto max_deleted_seqno = json.value("max_deleted_seqno", "");
        auto snapStart = json.find("snap_start");
        auto snapEnd = json.find("snap_end");
        auto maxCasValue = json.find("max_cas");
        auto hlcCasEpoch = json.find("hlc_epoch");
        mightContainXattrs = json.value("might_contain_xattrs", false);
        supportsCollections = json.value("collections_supported", false);

        auto failover_json = json.find("failover_table");
        if (vb_state.empty() || checkpoint_id.empty() ||
            max_deleted_seqno.empty()) {
            logger.warn(
                    "CouchKVStore::readVBState: State"
                    " JSON doc for {} is in the wrong format:{}, "
                    "vb state:{}, checkpoint id:{} and max deleted seqno:{}",
                    vbId,
                    statjson,
                    vb_state,
                    checkpoint_id,
                    max_deleted_seqno);
        } else {
            state = VBucket::fromString(vb_state.c_str());
            if (!parseUint64(max_deleted_seqno, &maxDeletedSeqno)) {
                logger.warn(
                        "CouchKVStore::readVBState: Failed to call "
                        "parseUint64 on max_deleted_seqno, which has a "
                        "value of: {} for {}",
                        max_deleted_seqno,
                        vbId);
                return ENGINE_EINVAL;
            }
            if (!parseUint64(checkpoint_id, &checkpointId)) {
                logger.warn(
                        "CouchKVStore::readVBState: Failed to call "
                        "parseUint64 on checkpoint_id, which has a value "
                        "of: {} for {}",
                        checkpoint_id,
                        vbId);
                return ENGINE_EINVAL;
            }

            if (snapStart == json.end()) {
                lastSnapStart = gsl::narrow<uint64_t>(highSeqno);
            } else {
                if (!parseUint64(snapStart->get<std::string>(),
                                 &lastSnapStart)) {
                    logger.warn(
                            "CouchKVStore::readVBState: Failed to call "
                            "parseUint64 on snapStart, which has a value "
                            "of: {} for {}",
                            (snapStart->get<std::string>()),
                            vbId);
                    return ENGINE_EINVAL;
                }
            }

            if (snapEnd == json.end()) {
                lastSnapEnd = gsl::narrow<uint64_t>(highSeqno);
            } else {
                if (!parseUint64(snapEnd->get<std::string>(), &lastSnapEnd)) {
                    logger.warn(
                            "CouchKVStore::readVBState: Failed to call "
                            "parseUint64 on snapEnd, which has a value of: "
                            "{} for {}",
                            (snapEnd->get<std::string>()),
                            vbId);
                    return ENGINE_EINVAL;
                }
            }

            if (maxCasValue != json.end()) {
                if (!parseUint64(maxCasValue->get<std::string>(), &maxCas)) {
                    logger.warn(
                            "CouchKVStore::readVBState: Failed to call "
                            "parseUint64 on maxCasValue, which has a value "
                            "of: {} for {}",
                            (maxCasValue->get<std::string>()),
                            vbId);
                    return ENGINE_EINVAL;
                }

                // MB-17517: If the maxCas on disk was invalid then don't use it -
                // instead rebuild from the items we load from disk (i.e. as per
                // an upgrade from an earlier version).
                if (maxCas == static_cast<uint64_t>(-1)) {
                    logger.warn(
                            "CouchKVStore::readVBState: Invalid max_cas "
                            "({:#x}) read from '{}' for {}. Resetting "
                            "max_cas to zero.",
                            maxCas,
                            id.buf,
                            vbId);
                    maxCas = 0;
                }
            }

            if (hlcCasEpoch != json.end()) {
                if (!parseInt64(hlcCasEpoch->get<std::string>(),
                                &hlcCasEpochSeqno)) {
                    logger.warn(
                            "CouchKVStore::readVBState: Failed to call "
                            "parseInt64 on hlcCasEpoch, which has a value "
                            "of: {} for {}",
                            (hlcCasEpoch->get<std::string>()),
                            vbId);
                    return ENGINE_EINVAL;
                }
            }

            if (failover_json != json.end()) {
                failovers = failover_json->dump();
            }
        }
        couchstore_free_local_document(ldoc);
    }

    cachedVBStates[vbId.get()] =
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
                                            supportsCollections);

    return couchErr2EngineErr(errCode);
}

couchstore_error_t CouchKVStore::saveVBState(Db *db,
                                             const vbucket_state &vbState) {
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state) << "\""
              << ",\"checkpoint_id\": \"" << vbState.checkpointId << "\""
              << ",\"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno << "\"";
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

    // Only mark the VB as supporting collections if enabled in the config
    if (vbState.supportsCollections &&
        configuration.shouldPersistDocNamespace()) {
        jsonState << ",\"collections_supported\": true";
    }

    jsonState << "}";

    LocalDoc lDoc;
    lDoc.id.buf = (char *)"_local/vbstate";
    lDoc.id.size = sizeof("_local/vbstate") - 1;
    std::string state = jsonState.str();
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

couchstore_error_t CouchKVStore::saveCollectionsManifest(
        Db& db, cb::const_byte_buffer serialisedManifest) {
    LocalDoc lDoc;
    lDoc.id.buf = const_cast<char*>(Collections::CouchstoreManifest);
    lDoc.id.size = Collections::CouchstoreManifestLen;

    lDoc.json.buf = reinterpret_cast<char*>(
            const_cast<uint8_t*>(serialisedManifest.data()));
    lDoc.json.size = serialisedManifest.size();
    lDoc.deleted = 0;
    couchstore_error_t errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::saveCollectionsManifest "
                "couchstore_save_local_document "
                "error:{} [{}]",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }

    return errCode;
}

Collections::VB::PersistedManifest CouchKVStore::readCollectionsManifest(
        Db& db) {
    sized_buf id;
    id.buf = const_cast<char*>(Collections::CouchstoreManifest);
    id.size = sizeof(Collections::CouchstoreManifest) - 1;

    LocalDocHolder lDoc;
    auto errCode = couchstore_open_local_document(
            &db, (void*)id.buf, id.size, lDoc.getLocalDocAddress());
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.info("CouchKVStore::readCollectionsManifest: doc not found");
        } else {
            logger.warn(
                    "CouchKVStore::readCollectionsManifest: "
                    "couchstore_open_local_document error:{}",
                    couchstore_strerror(errCode));
        }

        return {};
    }

    return {lDoc.getLocalDoc()->json.buf,
            lDoc.getLocalDoc()->json.buf + lDoc.getLocalDoc()->json.size};
}

void CouchKVStore::saveItemCount(Db& db, CollectionID cid, uint64_t count) {
    // Write out the count in BE to a local doc named after the collection
    // Using set-notation cardinality - |cid| which helps keep the keys small
    std::string docName = "|" + cid.to_string() + "|";
    cb::mcbp::unsigned_leb128<uint64_t> leb128(count);
    LocalDoc lDoc;
    lDoc.id.buf = const_cast<char*>(docName.c_str());
    lDoc.id.size = docName.size();

    lDoc.json.buf =
            const_cast<char*>(reinterpret_cast<const char*>(leb128.data()));
    lDoc.json.size = leb128.size();
    lDoc.deleted = 0;

    auto errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::saveCollectionCount cid:{} count:{} "
                "couchstore_save_local_document "
                "error:{} [{}]",
                cid.to_string(),
                count,
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }
}

void CouchKVStore::deleteItemCount(Db& db, CollectionID cid) {
    std::string docName = "|" + cid.to_string() + "|";
    LocalDoc lDoc;
    lDoc.id.buf = (char*)docName.c_str();
    lDoc.id.size = docName.size();
    lDoc.deleted = 1;

    auto errCode = couchstore_save_local_document(&db, &lDoc);

    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn(
                "CouchKVStore::deleteCollectionCount cid:{}"
                "couchstore_save_local_document "
                "error:{} [{}]",
                cid.to_string(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(&db, errCode));
    }
}

uint64_t CouchKVStore::getCollectionItemCount(const KVFileHandle& kvFileHandle,
                                              CollectionID collection) {
    const auto& db = static_cast<const CouchKVFileHandle&>(kvFileHandle);
    sized_buf id;
    std::string docName = "|" + collection.to_string() + "|";
    id.buf = const_cast<char*>(docName.c_str());
    id.size = docName.size();

    LocalDocHolder lDoc;
    auto errCode = couchstore_open_local_document(
            db.getDb(), (void*)id.buf, id.size, lDoc.getLocalDocAddress());
    if (errCode != COUCHSTORE_SUCCESS) {
        // Could be a deleted collection, so not found not an issue
        if (errCode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            logger.warn(
                    "CouchKVStore::getCollectionCount cid:{}"
                    "couchstore_open_local_document error:{}",
                    collection.to_string(),
                    couchstore_strerror(errCode));
        }

        return 0;
    }

    return cb::mcbp::decode_unsigned_leb128<uint64_t>(
                   {reinterpret_cast<const uint8_t*>(
                            lDoc.getLocalDoc()->json.buf),
                    lDoc.getLocalDoc()->json.size})
            .first;
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
    // Collections: TODO: Permanently restore to stored namespace
    DocKey key = makeDocKey(docinfo->id,
                            cbCtx->cks.getConfig().shouldPersistDocNamespace());
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
            return RollbackResult(false, 0, 0, 0);
        }
    } else {
        logger.warn("CouchKVStore::rollback: openDB error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
        return RollbackResult(false, 0, 0, 0);
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
        return RollbackResult(false, 0, 0, 0);
    }

    // Open the vBucket file again; and search for a header which is
    // before the requested rollback point - the Rollback Header.
    DbHolder newdb(*this);
    errCode = openDB(vbid, newdb, 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        logger.warn("CouchKVStore::rollback: openDB#2 error:{}, name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
        return RollbackResult(false, 0, 0, 0);
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
            return RollbackResult(false, 0, 0, 0);
        }
        errCode = couchstore_db_info(newdb, &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            logger.warn(
                    "CouchKVStore::rollback: couchstore_db_info error:{}, "
                    "name:{}",
                    couchstore_strerror(errCode),
                    dbFileName.str());
            return RollbackResult(false, 0, 0, 0);
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
        return RollbackResult(false, 0, 0, 0);
    }
    if ((totSeqCount / 2) <= rollbackSeqCount) {
        //doresetVbucket flag set or rollback is greater than 50%,
        //reset the vbucket and send the entire snapshot
        return RollbackResult(false, 0, 0, 0);
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
        return RollbackResult(false, 0, 0, 0);
    }

    if (readVBState(newdb, vbid) != ENGINE_SUCCESS) {
        return RollbackResult(false, 0, 0, 0);
    }
    cachedDeleteCount[vbid.get()] = info.deleted_count;
    cachedDocCount[vbid.get()] = info.doc_count;

    //Append the rewinded header to the database file
    errCode = couchstore_commit(newdb);

    if (errCode != COUCHSTORE_SUCCESS) {
        return RollbackResult(false, 0, 0, 0);
    }

    vbucket_state* vb_state = getVBucketState(vbid);
    return RollbackResult(true, vb_state->highSeqno,
                          vb_state->lastSnapStart, vb_state->lastSnapEnd);
}

int populateAllKeys(Db *db, DocInfo *docinfo, void *ctx) {
    AllKeysCtx *allKeysCtx = (AllKeysCtx *)ctx;
    DocKey key = makeDocKey(docinfo->id, allKeysCtx->persistDocNamespace);
    (allKeysCtx->cb)->callback(key);
    if (--(allKeysCtx->count) <= 0) {
        //Only when count met is less than the actual number of entries
        return COUCHSTORE_ERROR_CANCEL;
    }
    return COUCHSTORE_SUCCESS;
}

ENGINE_ERROR_CODE
CouchKVStore::getAllKeys(Vbid vbid,
                         const DocKey start_key,
                         uint32_t count,
                         std::shared_ptr<Callback<const DocKey&>> cb) {
    DbHolder db(*this);
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if(errCode == COUCHSTORE_SUCCESS) {
        sized_buf ref = {NULL, 0};

        if (!configuration.shouldPersistDocNamespace()) {
            auto noprefix = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
                    {start_key.data(), start_key.size()});
            ref.buf = (char*)noprefix.data();
            ref.size = noprefix.size();
        } else {
            ref.buf = (char*)start_key.data();
            ref.size = start_key.size();
        }

        AllKeysCtx ctx(cb, count, configuration.shouldPersistDocNamespace());
        errCode = couchstore_all_docs(db,
                                      &ref,
                                      COUCHSTORE_NO_DELETES,
                                      populateAllKeys,
                                      static_cast<void*>(&ctx));
        if (errCode == COUCHSTORE_SUCCESS ||
                errCode == COUCHSTORE_ERROR_CANCEL)  {
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
            pendingFileDeletions.push(file_str);
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

    if (access(filename.c_str(), F_OK) == 0) {
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
                pendingFileDeletions.push(const_cast<std::string &>(filename));
            }
        }
    }
}

Collections::VB::PersistedManifest CouchKVStore::getCollectionsManifest(
        Vbid vbid) {
    DbHolder db(*this);

    // openDB logs error details
    couchstore_error_t errCode = openDB(vbid, db, COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        return {};
    }

    return readCollectionsManifest(*db);
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

void CouchKVStore::incrementRevision(Vbid vbid) {
    (*dbFileRevMap)[vbid.get()]++;
}

uint64_t CouchKVStore::prepareToDelete(Vbid vbid) {
    // Clear the stats so it looks empty (real deletion of the disk data occurs
    // later)
    cachedDocCount[vbid.get()] = 0;
    cachedDeleteCount[vbid.get()] = 0;
    cachedFileSize[vbid.get()] = 0;
    cachedSpaceUsed[vbid.get()] = 0;
    return (*dbFileRevMap)[vbid.get()];
}

/* end of couch-kvstore.cc */
