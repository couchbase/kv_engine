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

#ifdef _MSC_VER
#define PATH_MAX MAX_PATH
#endif

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <string>
#include <utility>
#include <vector>
#include <cJSON.h>
#include <platform/dirutils.h>

#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
#define STATWRITER_NAMESPACE couchstore_engine
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "vbucket.h"

#include <JSON_checker.h>
#include <snappy-c.h>

using namespace CouchbaseDirectoryUtilities;

static const int MAX_OPEN_DB_RETRY = 10;

/*
 * MetaData warning
 * Sherlock began storing an extra byte of data (taking meta_len to 19 bytes).
 * Watson (4.6) removes this byte as it is never utilised.
 *
 * WARNING: any *new* meta-data we wish to store may cause upgrade trouble if it
 * takes the length to 19 bytes.
 */
static const uint32_t DEFAULT_META_LEN = 16;
static const uint32_t V1_META_LEN = 18;
#define UNUSED_V2_LEN 19

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

static uint8_t determine_datatype(const unsigned char* value,
                                  size_t length) {
    if (checkUTF8JSON(value, length)) {
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
    std::vector<std::string> files = findFilesContaining(dir, ".couch");
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
            err == COUCHSTORE_ERROR_WRITE) ? getStrError(db) : "none";
}

struct GetMultiCbCtx {
    GetMultiCbCtx(CouchKVStore &c, uint16_t v, vb_bgfetch_queue_t &f) :
        cks(c), vbId(v), fetches(f) {}

    CouchKVStore &cks;
    uint16_t vbId;
    vb_bgfetch_queue_t &fetches;
};

struct StatResponseCtx {
public:
    StatResponseCtx(std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &sm,
                    uint16_t vb) : statMap(sm), vbId(vb) {
        /* EMPTY */
    }

    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &statMap;
    uint16_t vbId;
};

struct AllKeysCtx {
    AllKeysCtx(std::shared_ptr<Callback<uint16_t&, char*&> > callback, uint32_t cnt)
        : cb(callback), count(cnt) { }

    std::shared_ptr<Callback<uint16_t&, char*&> > cb;
    uint32_t count;
};

CouchRequest::CouchRequest(const Item &it, uint64_t rev,
                           MutationRequestCallback &cb, bool del)
    : IORequest(it.getVBucketId(), cb, del, it.getKey()), value(it.getValue()),
      fileRevNum(rev)
{
    uint64_t cas = htonll(it.getCas());
    uint32_t flags = it.getFlags();
    uint32_t vlen = it.getNBytes();
    uint32_t exptime = it.getExptime();

    // Datatype used to determine whether document requires compression or not
    uint8_t datatype;

    // Save time of deletion in expiry time field of deleted item's metadata.
    if (del) {
        exptime = ep_real_time();
    }
    exptime = htonl(exptime);

    dbDoc.id.buf = const_cast<char *>(key.c_str());
    dbDoc.id.size = it.getNKey();
    if (vlen) {
        dbDoc.data.buf = const_cast<char *>(value->getData());
        dbDoc.data.size = vlen;
        datatype = it.getDataType();
    } else {
        dbDoc.data.buf = NULL;
        dbDoc.data.size = 0;
        datatype = 0x00;
    }

    memset(meta, 0, sizeof(meta));
    memcpy(meta, &cas, 8);
    memcpy(meta + 8, &exptime, 4);
    memcpy(meta + 12, &flags, 4);
    *(meta + DEFAULT_META_LEN) = FLEX_META_CODE;

    //For a deleted item, there is no extended meta data available
    //as part of the item object, hence by default populate the
    //data type to PROTOCOL_BINARY_RAW_BYTES
    if (del) {
        uint8_t del_datatype = PROTOCOL_BINARY_RAW_BYTES;
        memcpy(meta + DEFAULT_META_LEN + FLEX_DATA_OFFSET,
               &del_datatype, sizeof(uint8_t));
    } else {
        memcpy(meta + DEFAULT_META_LEN + FLEX_DATA_OFFSET, it.getExtMeta(),
               it.getExtMetaLen());
    }

    dbDocInfo.db_seq = it.getBySeqno();
    dbDocInfo.rev_meta.buf = reinterpret_cast<char *>(meta);
    dbDocInfo.rev_meta.size = COUCHSTORE_METADATA_SIZE;
    dbDocInfo.rev_seq = it.getRevSeqno();
    dbDocInfo.size = dbDoc.data.size;
    if (del) {
        dbDocInfo.deleted =  1;
    } else {
        dbDocInfo.deleted = 0;
    }
    dbDocInfo.id = dbDoc.id;
    dbDocInfo.content_meta = (datatype == PROTOCOL_BINARY_DATATYPE_JSON) ?
                                    COUCH_DOC_IS_JSON : COUCH_DOC_NON_JSON_MODE;

    //Compress only those documents that aren't already compressed.
    if (dbDoc.data.size > 0 && !deleteItem) {
        if (datatype == PROTOCOL_BINARY_RAW_BYTES ||
                datatype == PROTOCOL_BINARY_DATATYPE_JSON) {
            dbDocInfo.content_meta |= COUCH_DOC_IS_COMPRESSED;
        }
    }
}

CouchKVStore::CouchKVStore(KVStoreConfig &config, bool read_only) :
    KVStore(config, read_only), dbname(config.getDBName()),
    intransaction(false), backfillCounter(0)
{
    createDataDir(dbname);
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);
    statCollectingFileOpsCompaction = getCouchstoreStatsOps(&st.fsStatsCompaction);

    // init db file map with default revision number, 1
    numDbFiles = configuration.getMaxVBuckets();
    cachedVBStates.reserve(numDbFiles);

    // pre-allocate lookup maps (vectors) given we have a relatively
    // small, fixed number of vBuckets.
    dbFileRevMap.assign(numDbFiles, Couchbase::RelaxedAtomic<uint64_t>(1));
    cachedDocCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(-1));
    cachedDeleteCount.assign(numDbFiles, Couchbase::RelaxedAtomic<size_t>(-1));
    cachedVBStates.assign(numDbFiles, nullptr);

    initialize();
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
    KVStore(copyFrom), dbname(copyFrom.dbname),
    dbFileRevMap(copyFrom.dbFileRevMap), numDbFiles(copyFrom.numDbFiles),
    intransaction(false)
{
    createDataDir(dbname);
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);
}

void CouchKVStore::initialize() {
    std::vector<uint16_t> vbids;
    std::vector<std::string> files;
    discoverDbFiles(dbname, files);
    populateFileNameMap(files, &vbids);

    Db *db = NULL;
    couchstore_error_t errorCode;

    std::vector<uint16_t>::iterator itr = vbids.begin();
    for (; itr != vbids.end(); ++itr) {
        uint16_t id = *itr;
        uint64_t rev = dbFileRevMap[id];

        errorCode = openDB(id, rev, &db, COUCHSTORE_OPEN_FLAG_RDONLY);
        if (errorCode == COUCHSTORE_SUCCESS) {
            readVBState(db, id);
            /* update stat */
            ++st.numLoadedVb;
            closeDatabaseHandle(db);
        } else {
            LOG(EXTENSION_LOG_WARNING, "Failed to open database file "
                "%s/%" PRIu16 ".couch.%" PRIu64, dbname.c_str(), id, rev);
            remVBucketFromDbFileMap(id);
            cachedVBStates[id] = NULL;
        }

        db = NULL;
        if (!isReadOnly()) {
            removeCompactFile(dbname, id, rev);
        }
    }
}

CouchKVStore::~CouchKVStore() {
    close();

    for (std::vector<vbucket_state *>::iterator it = cachedVBStates.begin();
         it != cachedVBStates.end(); it++) {
        vbucket_state *vbstate = *it;
        if (vbstate) {
            delete vbstate;
            *it = NULL;
        }
    }
}
void CouchKVStore::reset(uint16_t vbucketId) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::reset: Not valid on a read-only "
                        "object.");
    }

    vbucket_state *state = cachedVBStates[vbucketId];
    if (state) {
        state->reset();

        cachedDocCount[vbucketId] = 0;
        cachedDeleteCount[vbucketId] = 0;

        //Unlink the couchstore file upon reset
        unlinkCouchFile(vbucketId, dbFileRevMap[vbucketId]);
        setVBucketState(vbucketId, *state, NULL, true);
        updateDbFileMap(vbucketId, 1);
    } else {
        throw std::invalid_argument("CouchKVStore::reset: No entry in cached "
                        "states for vbucket " + std::to_string(vbucketId));
    }
}

void CouchKVStore::set(const Item &itm, Callback<mutation_result> &cb) {
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
    uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];

    // each req will be de-allocated after commit
    requestcb.setCb = &cb;
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, deleteItem);
    pendingReqsQ.push_back(req);
}

void CouchKVStore::get(const std::string &key, uint16_t vb,
                       Callback<GetValue> &cb, bool fetchDelete) {
    Db *db = NULL;
    GetValue rv;
    uint64_t fileRev = dbFileRevMap[vb];

    couchstore_error_t errCode = openDB(vb, fileRev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        LOG(EXTENSION_LOG_WARNING,
            "Failed to open database to retrieve data "
            "from vBucketId = %d, key = %s\n",
            vb, key.c_str());
        rv.setStatus(couchErr2EngineErr(errCode));
        cb.callback(rv);
        return;
    }

    getWithHeader(db, key, vb, cb, fetchDelete);
    closeDatabaseHandle(db);
}

void CouchKVStore::getWithHeader(void *dbHandle, const std::string &key,
                                 uint16_t vb, Callback<GetValue> &cb,
                                 bool fetchDelete) {

    Db *db = (Db *)dbHandle;
    hrtime_t start = gethrtime();
    RememberingCallback<GetValue> *rc = dynamic_cast<RememberingCallback<GetValue> *>(&cb);
    bool getMetaOnly = rc && rc->val.isPartial();
    DocInfo *docInfo = NULL;
    sized_buf id;
    GetValue rv;

    id.size = key.size();
    id.buf = const_cast<char *>(key.c_str());

    couchstore_error_t errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                                          id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (!getMetaOnly) {
            // log error only if this is non-xdcr case
            LOG(EXTENSION_LOG_WARNING,
                "Failed to retrieve doc info from "
                "database, vbucketId=%d, key=%s error=%s [%s]\n",
                vb, id.buf, couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
        }
    } else {
        if (docInfo == nullptr) {
            throw std::logic_error("CouchKVStore::getWithHeader: "
                    "couchstore_docinfo_by_id returned success but docInfo "
                    "is NULL");
        }
        errCode = fetchDoc(db, docInfo, rv, vb, getMetaOnly, fetchDelete);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to retrieve key value from "
                "database, vbucketId=%d key=%s error=%s [%s] "
                "deleted=%s", vb, id.buf,
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str(),
                docInfo->deleted ? "yes" : "no");
        }

        // record stats
        st.readTimeHisto.add((gethrtime() - start) / 1000);
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(key.length() + rv.getValue()->getNBytes());
        }
    }

    if(errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
    }

    couchstore_free_docinfo(docInfo);
    rv.setStatus(couchErr2EngineErr(errCode));
    cb.callback(rv);
}

void CouchKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) {
    int numItems = itms.size();
    uint64_t fileRev = dbFileRevMap[vb];

    Db *db = NULL;
    couchstore_error_t errCode = openDB(vb, fileRev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to open database for data fetch, "
            "vBucketId = %" PRIu16 ", numDocs = %d\n",
            vb, numItems);
        st.numGetFailure.fetch_add(numItems);
        vb_bgfetch_queue_t::iterator itr = itms.begin();
        for (; itr != itms.end(); ++itr) {
            vb_bgfetch_item_ctx_t &bg_itm_ctx = (*itr).second;
            std::list<VBucketBGFetchItem *> &fetches = bg_itm_ctx.bgfetched_list;
            std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
            for (; fitr != fetches.end(); ++fitr) {
                (*fitr)->value.setStatus(ENGINE_NOT_MY_VBUCKET);
            }
        }
        return;
    }

    size_t idx = 0;
    sized_buf *ids = new sized_buf[itms.size()];
    vb_bgfetch_queue_t::iterator itr = itms.begin();
    for (; itr != itms.end(); ++itr) {
        ids[idx].size = itr->first.size();
        ids[idx].buf = const_cast<char *>(itr->first.c_str());
        ++idx;
    }

    GetMultiCbCtx ctx(*this, vb, itms);

    errCode = couchstore_docinfos_by_id(db, ids, itms.size(),
                                        0, getMultiCbC, &ctx);
    if (errCode != COUCHSTORE_SUCCESS) {
        st.numGetFailure.fetch_add(numItems);
        for (itr = itms.begin(); itr != itms.end(); ++itr) {
            LOG(EXTENSION_LOG_WARNING, "Failed to read database by"
                " vBucketId = %" PRIu16 " key = %s error = %s [%s]\n",
                vb, (*itr).first.c_str(),
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
            vb_bgfetch_item_ctx_t &bg_itm_ctx = (*itr).second;
            std::list<VBucketBGFetchItem *> &fetches = bg_itm_ctx.bgfetched_list;
            std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
            for (; fitr != fetches.end(); ++fitr) {
                (*fitr)->value.setStatus(couchErr2EngineErr(errCode));
            }
        }
    }
    closeDatabaseHandle(db);
    delete []ids;
}

void CouchKVStore::del(const Item &itm,
                       Callback<int> &cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::del: Not valid on a read-only "
                        "object.");
    }
    if (!intransaction) {
        throw std::invalid_argument("CouchKVStore::del: intransaction must be "
                        "true to perform a delete operation.");
    }

    uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];
    MutationRequestCallback requestcb;
    requestcb.delCb = &cb;
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, true);
    pendingReqsQ.push_back(req);
}

void CouchKVStore::delVBucket(uint16_t vbucket) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::delVBucket: Not valid on a "
                        "read-only object.");
    }

    unlinkCouchFile(vbucket, dbFileRevMap[vbucket]);

    if (cachedVBStates[vbucket]) {
        delete cachedVBStates[vbucket];
    }

    std::string failovers("[{\"id\":0, \"seq\":0}]");
    cachedVBStates[vbucket] = new vbucket_state(vbucket_state_dead, 0, 0, 0, 0,
                                                0, 0, 0, failovers);
    updateDbFileMap(vbucket, 1);
}

std::vector<vbucket_state *> CouchKVStore::listPersistedVbuckets() {
    return cachedVBStates;
}

void CouchKVStore::getPersistedStats(std::map<std::string,
                                     std::string> &stats) {
    char *buffer = NULL;
    std::string fname = dbname + "/stats.json";
    if (access(fname.c_str(), R_OK) == -1) {
        return ;
    }

    std::ifstream session_stats;
    session_stats.exceptions (session_stats.failbit | session_stats.badbit);
    try {
        session_stats.open(fname.c_str(), std::ios::binary);
        session_stats.seekg(0, std::ios::end);
        int flen = session_stats.tellg();
        if (flen < 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Error in session stats ifstream!!!");
            session_stats.close();
            return;
        }
        session_stats.seekg(0, std::ios::beg);
        buffer = new char[flen + 1];
        session_stats.read(buffer, flen);
        session_stats.close();
        buffer[flen] = '\0';

        cJSON *json_obj = cJSON_Parse(buffer);
        if (!json_obj) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to parse the session stats json doc!!!");
            delete[] buffer;
            return;
        }

        int json_arr_size = cJSON_GetArraySize(json_obj);
        for (int i = 0; i < json_arr_size; ++i) {
            cJSON *obj = cJSON_GetArrayItem(json_obj, i);
            if (obj) {
                stats[obj->string] = obj->valuestring ? obj->valuestring : "";
            }
        }
        cJSON_Delete(json_obj);

    } catch (const std::ifstream::failure &e) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to load the engine session stats "
            "due to IO exception \"%s\"", e.what());
    } catch (...) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to load the engine session stats "
            "due to IO exception");
    }

    delete[] buffer;
}

static std::string getDBFileName(const std::string &dbname,
                                 uint16_t vbid,
                                 uint64_t rev) {
    std::stringstream ss;
    ss << dbname << "/" << vbid << ".couch." << rev;
    return ss.str();
}

static int edit_docinfo_hook(DocInfo **info, const sized_buf *item) {
    if ((*info)->rev_meta.size == DEFAULT_META_LEN) {
        // Metadata doesn't have flex_meta_code and datatype so provision space
        // for these paramenters.
        const unsigned char* data;
        bool ret;
        if (((*info)->content_meta | COUCH_DOC_IS_COMPRESSED) ==
                (*info)->content_meta) {
            size_t uncompr_len;
            snappy_uncompressed_length(item->buf, item->size, &uncompr_len);
            char *dbuf = (char *) cb_malloc(uncompr_len);
            snappy_uncompress(item->buf, item->size, dbuf, &uncompr_len);
            data = (const unsigned char*)dbuf;
            ret = checkUTF8JSON(data, uncompr_len);
            cb_free(dbuf);
        } else {
            data = (const unsigned char*)item->buf;
            ret = checkUTF8JSON(data, item->size);
        }
        uint8_t flex_code = FLEX_META_CODE;
        uint8_t datatype;
        if (ret) {
            datatype = PROTOCOL_BINARY_DATATYPE_JSON;
        } else {
            datatype = PROTOCOL_BINARY_RAW_BYTES;
        }

        DocInfo *docinfo = (DocInfo *) cb_calloc(1,
                                               sizeof(DocInfo) +
                                               (*info)->id.size +
                                               (*info)->rev_meta.size +
                                               FLEX_DATA_OFFSET + EXT_META_LEN +
                                               sizeof(uint8_t));
        if (!docinfo) {
            LOG(EXTENSION_LOG_WARNING, "Failed to allocate docInfo, "
                    "while editing docinfo in the compaction's docinfo_hook");
            return 0;
        }

        char *extra = (char *)docinfo + sizeof(DocInfo);
        memcpy(extra, (*info)->id.buf, (*info)->id.size);
        docinfo->id.buf = extra;
        docinfo->id.size = (*info)->id.size;

        extra += (*info)->id.size;
        memcpy(extra, (*info)->rev_meta.buf, (*info)->rev_meta.size);
        memcpy(extra + (*info)->rev_meta.size,
               &flex_code, FLEX_DATA_OFFSET);
        memcpy(extra + (*info)->rev_meta.size + FLEX_DATA_OFFSET,
               &datatype, sizeof(uint8_t));
        docinfo->rev_meta.buf = extra;
        docinfo->rev_meta.size = (*info)->rev_meta.size +
                                 FLEX_DATA_OFFSET + EXT_META_LEN +
                                 sizeof(uint8_t);

        docinfo->db_seq = (*info)->db_seq;
        docinfo->rev_seq = (*info)->rev_seq;
        docinfo->deleted = (*info)->deleted;
        docinfo->content_meta = (*info)->content_meta;
        docinfo->bp = (*info)->bp;
        docinfo->size = (*info)->size;

        couchstore_free_docinfo(*info);
        *info = docinfo;
        return 1;
    }
    return 0;
}

static int time_purge_hook(Db* d, DocInfo* info, void* ctx_p) {
    compaction_ctx* ctx = (compaction_ctx*) ctx_p;
    DbInfo infoDb;

    couchstore_db_info(d, &infoDb);
    //Compaction finished
    if (info == NULL) {
        return couchstore_set_purge_seq(d, ctx->max_purged_seq);
    }

    if (info->rev_meta.size >= DEFAULT_META_LEN) {
        uint32_t exptime;
        memcpy(&exptime, info->rev_meta.buf + 8, 4);
        exptime = ntohl(exptime);
        if (info->deleted) {
            if (info->db_seq != infoDb.last_sequence) {
                if (ctx->drop_deletes) { // all deleted items must be dropped ...
                    if (ctx->max_purged_seq < info->db_seq) {
                        ctx->max_purged_seq = info->db_seq; // track max_purged_seq
                    }
                    return COUCHSTORE_COMPACT_DROP_ITEM;      // ...unconditionally
                }
                if (exptime < ctx->purge_before_ts &&
                        (!ctx->purge_before_seq ||
                         info->db_seq <= ctx->purge_before_seq)) {
                    if (ctx->max_purged_seq < info->db_seq) {
                        ctx->max_purged_seq = info->db_seq;
                    }
                    return COUCHSTORE_COMPACT_DROP_ITEM;
                }
            }
        } else if (exptime && exptime < ctx->curr_time) {
            std::string key(info->id.buf, info->id.size);
            ctx->expiryCallback->callback(key, info->rev_seq);
        }
    }

    if (ctx->bloomFilterCallback) {
        bool deleted = info->deleted;
        std::string key((const char *)info->id.buf, info->id.size);
        ctx->bloomFilterCallback->callback(key, deleted);
    }

    return COUCHSTORE_COMPACT_KEEP_ITEM;
}

bool CouchKVStore::compactDB(compaction_ctx *hook_ctx,
                             Callback<kvstats_ctx> &kvcb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::compactDB: Cannot perform "
                        "on a read-only instance.");
    }

    couchstore_compact_hook       hook = time_purge_hook;
    couchstore_docinfo_hook      dhook = edit_docinfo_hook;
    const couch_file_ops     *def_iops = &statCollectingFileOpsCompaction;
    Db                      *compactdb = NULL;
    Db                       *targetDb = NULL;
    couchstore_error_t         errCode = COUCHSTORE_SUCCESS;
    hrtime_t                     start = gethrtime();
    std::string                 dbfile;
    std::string           compact_file;
    std::string               new_file;
    kvstats_ctx                  kvctx;
    DbInfo                        info;
    uint16_t                      vbid = hook_ctx->db_file_id;
    uint64_t                   fileRev = dbFileRevMap[vbid];
    uint64_t                   new_rev = fileRev + 1;

    // Open the source VBucket database file ...
    errCode = openDB(vbid, fileRev, &compactdb,
                     (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY, nullptr, false,
                     def_iops);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, vbucketId = %d "
                "fileRev = %" PRIu64, vbid, fileRev);
        return false;
    }

    // Build the temporary vbucket.compact file name
    dbfile       = getDBFileName(dbname, vbid, fileRev);
    compact_file = dbfile + ".compact";

    // Perform COMPACTION of vbucket.couch.rev into vbucket.couch.rev.compact
    errCode = couchstore_compact_db_ex(compactdb, compact_file.c_str(),
                                       COUCHSTORE_COMPACT_FLAG_UPGRADE_DB,
                                       hook, dhook, hook_ctx, def_iops);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to compact database with name=%s "
            "error=%s errno=%s",
            dbfile.c_str(),
            couchstore_strerror(errCode),
            couchkvstore_strerrno(compactdb, errCode).c_str());
        closeDatabaseHandle(compactdb);
        return false;
    }

    // Close the source Database File once compaction is done
    closeDatabaseHandle(compactdb);

    // Rename the .compact file to one with the next revision number
    new_file = getDBFileName(dbname, vbid, new_rev);
    if (rename(compact_file.c_str(), new_file.c_str()) != 0) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to rename '%s' to '%s': %s",
            compact_file.c_str(), new_file.c_str(),
            cb_strerror().c_str());

        removeCompactFile(compact_file);
        return false;
    }

    // Open the newly compacted VBucket database file ...
    errCode = openDB(vbid, new_rev, &targetDb,
                     (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open compacted database file %s "
                "fileRev = %" PRIu64, new_file.c_str(), new_rev);
        if (remove(new_file.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: Failed to remove '%s': %s",
                new_file.c_str(), cb_strerror().c_str());
        }
        return false;
    }

    // Update the global VBucket file map so all operations use the new file
    updateDbFileMap(vbid, new_rev);

    LOG(EXTENSION_LOG_INFO,
            "INFO: created new couch db file, name=%s rev=%" PRIu64,
            new_file.c_str(), new_rev);

    // Update stats to caller
    kvctx.vbucket = vbid;
    couchstore_db_info(targetDb, &info);
    kvctx.fileSpaceUsed = info.space_used;
    kvctx.fileSize = info.file_size;
    kvcb.callback(kvctx);

    // also update cached state with dbinfo
    vbucket_state *state = cachedVBStates[vbid];
    if (state) {
        state->highSeqno = info.last_sequence;
        state->purgeSeqno = info.purge_seq;
        cachedDeleteCount[vbid] = info.deleted_count;
        cachedDocCount[vbid] = info.doc_count;
    }

    closeDatabaseHandle(targetDb);

    // Removing the stale couch file
    unlinkCouchFile(vbid, fileRev);

    st.compactHisto.add((gethrtime() - start) / 1000);

    return true;
}

vbucket_state * CouchKVStore::getVBucketState(uint16_t vbucketId) {
    return cachedVBStates[vbucketId];
}

bool CouchKVStore::setVBucketState(uint16_t vbucketId,
                                   const vbucket_state &vbstate,
                                   Callback<kvstats_ctx> *kvcb, bool reset) {
    Db *db = NULL;
    uint64_t fileRev, newFileRev;
    std::stringstream id, rev;
    std::string dbFileName;
    std::map<uint16_t, uint64_t>::iterator mapItr;
    kvstats_ctx kvctx;
    kvctx.vbucket = vbucketId;

    id << vbucketId;
    fileRev = dbFileRevMap[vbucketId];
    rev << fileRev;
    dbFileName = dbname + "/" + id.str() + ".couch." + rev.str();

    couchstore_error_t errorCode;
    errorCode = openDB(vbucketId, fileRev, &db,
            (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE, &newFileRev, reset);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, name=%s",
                dbFileName.c_str());
        return false;
    }

    fileRev = newFileRev;
    rev << fileRev;
    dbFileName = dbname + "/" + id.str() + ".couch." + rev.str();

    errorCode = saveVBState(db, vbstate);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Failed to save local doc, name=%s",
                dbFileName.c_str());
        closeDatabaseHandle(db);
        return false;
    }

    errorCode = couchstore_commit(db);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Commit failed, vbid=%u rev=%" PRIu64 " error=%s [%s]",
                vbucketId, fileRev, couchstore_strerror(errorCode),
                couchkvstore_strerrno(db, errorCode).c_str());
        closeDatabaseHandle(db);
        return false;
    }
    if (kvcb) {
        DbInfo info;
        couchstore_db_info(db, &info);
        kvctx.fileSpaceUsed = info.space_used;
        kvctx.fileSize = info.file_size;
        kvcb->callback(kvctx);
    }
    closeDatabaseHandle(db);

    return true;
}

bool CouchKVStore::snapshotVBucket(uint16_t vbucketId,
                                   const vbucket_state &vbstate,
                                   Callback<kvstats_ctx> *cb, bool persist) {
    if (isReadOnly()) {
        LOG(EXTENSION_LOG_WARNING,
            "Snapshotting a vbucket cannot be performed on a read-only "
            "KVStore instance");
        return false;
    }

    hrtime_t start = gethrtime();

    if (updateCachedVBState(vbucketId, vbstate) && persist) {
        vbucket_state *vbs = cachedVBStates[vbucketId];
        if (!setVBucketState(vbucketId, *vbs, cb)) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to persist new state, %s, for vbucket %d\n",
                VBucket::toString(vbstate.state), vbucketId);
           return false;
        }
    }

    LOG(EXTENSION_LOG_DEBUG,
        "CouchKVStore::snapshotVBucket: Snapshotted vbucket:%" PRIu16 " state:%s",
        vbucketId,
        vbstate.toJSON().c_str());

    st.snapshotHisto.add((gethrtime() - start) / 1000);

    return true;
}

StorageProperties CouchKVStore::getStorageProperties() {
    StorageProperties rv(true, true, true, true);
    return rv;
}

bool CouchKVStore::commit(Callback<kvstats_ctx> *cb) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::commit: Not valid on a read-only "
                        "object.");
    }

    if (intransaction) {
        if (commit2couchstore(cb)) {
            intransaction = false;
        }
    }

    return !intransaction;
}

void CouchKVStore::addStats(const std::string &prefix,
                            ADD_STAT add_stat,
                            const void *c) {
    const char *prefix_str = prefix.c_str();

    /* stats for both read-only and read-write threads */
    addStat(prefix_str, "backend_type",   "couchstore",       add_stat, c);
    addStat(prefix_str, "open",           st.numOpen,         add_stat, c);
    addStat(prefix_str, "close",          st.numClose,        add_stat, c);
    addStat(prefix_str, "readTime",       st.readTimeHisto,   add_stat, c);
    addStat(prefix_str, "readSize",       st.readSizeHisto,   add_stat, c);
    addStat(prefix_str, "numLoadedVb",    st.numLoadedVb,     add_stat, c);

    // failure stats
    addStat(prefix_str, "failure_open",   st.numOpenFailure, add_stat, c);
    addStat(prefix_str, "failure_get",    st.numGetFailure,  add_stat, c);

    if (!isReadOnly()) {
        addStat(prefix_str, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix_str, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix_str, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix_str, "lastCommDocs",  st.docsCommitted,   add_stat, c);
    }

    addStat(prefix_str, "io_num_read", st.io_num_read, add_stat, c);
    addStat(prefix_str, "io_num_write", st.io_num_write, add_stat, c);
    addStat(prefix_str, "io_read_bytes", st.io_read_bytes, add_stat, c);
    addStat(prefix_str, "io_write_bytes", st.io_write_bytes, add_stat, c);

    const size_t read = st.fsStats.totalBytesRead.load() +
                        st.fsStatsCompaction.totalBytesRead.load();
    addStat(prefix_str, "io_total_read_bytes", read, add_stat, c);

    const size_t written = st.fsStats.totalBytesWritten.load() +
                           st.fsStatsCompaction.totalBytesWritten.load();
    addStat(prefix_str, "io_total_write_bytes", written, add_stat, c);

    addStat(prefix_str, "io_compaction_read_bytes",
            st.fsStatsCompaction.totalBytesRead, add_stat, c);
    addStat(prefix_str, "io_compaction_write_bytes",
            st.fsStatsCompaction.totalBytesWritten, add_stat, c);
}

void CouchKVStore::addTimingStats(const std::string &prefix,
                                  ADD_STAT add_stat, const void *c) {
    if (isReadOnly()) {
        return;
    }
    const char *prefix_str = prefix.c_str();
    addStat(prefix_str, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix_str, "compact",     st.compactHisto,     add_stat, c);
    addStat(prefix_str, "snapshot",    st.snapshotHisto,    add_stat, c);
    addStat(prefix_str, "delete",      st.delTimeHisto,     add_stat, c);
    addStat(prefix_str, "save_documents", st.saveDocsHisto, add_stat, c);
    addStat(prefix_str, "writeTime",   st.writeTimeHisto,   add_stat, c);
    addStat(prefix_str, "writeSize",   st.writeSizeHisto,   add_stat, c);
    addStat(prefix_str, "bulkSize",    st.batchSize,        add_stat, c);

    // Couchstore file ops stats
    addStat(prefix_str, "fsReadTime",  st.fsStats.readTimeHisto,  add_stat, c);
    addStat(prefix_str, "fsWriteTime", st.fsStats.writeTimeHisto, add_stat, c);
    addStat(prefix_str, "fsSyncTime",  st.fsStats.syncTimeHisto,  add_stat, c);
    addStat(prefix_str, "fsReadSize",  st.fsStats.readSizeHisto,  add_stat, c);
    addStat(prefix_str, "fsWriteSize", st.fsStats.writeSizeHisto, add_stat, c);
    addStat(prefix_str, "fsReadSeek",  st.fsStats.readSeekHisto,  add_stat, c);
}

bool CouchKVStore::getStat(const char* name, size_t& value)  {
    if (strcmp("io_total_read_bytes", name) == 0) {
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
    }

    return false;
}

template <typename T>
void CouchKVStore::addStat(const std::string &prefix, const char *stat, T &val,
                           ADD_STAT add_stat, const void *c) {
    std::stringstream fullstat;
    fullstat << prefix << ":" << stat;
    add_casted_stat(fullstat.str().c_str(), val, add_stat, c);
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
                LOG(EXTENSION_LOG_WARNING, "Failed to remove file '%s' "
                    "with error code: %d", filename_str.c_str(), errno);
                if (errno != ENOENT) {
                    pendingFileDeletions.push(filename_str);
                }
            }
            queue.pop();
        }
    }
}

ScanContext* CouchKVStore::initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                           std::shared_ptr<Callback<CacheLookup> > cl,
                                           uint16_t vbid, uint64_t startSeqno,
                                           DocumentFilter options,
                                           ValueFilter valOptions) {
    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errorCode = openDB(vbid, rev, &db,
                                          COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errorCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Failed to open database, "
            "name=%s/%" PRIu16 ".couch.%" PRIu64, dbname.c_str(), vbid, rev);
        remVBucketFromDbFileMap(vbid);
        return NULL;
    }

    DbInfo info;
    errorCode = couchstore_db_info(db, &info);
    if (errorCode != COUCHSTORE_SUCCESS) {
        closeDatabaseHandle(db);
        throw std::runtime_error("Failed to read DB info for backfill. vb:" +
                                 std::to_string(vbid) + " rev:" +
                                 std::to_string(rev));
    }

    uint64_t count = 0;
    errorCode = couchstore_changes_count(db,
                                         startSeqno,
                                         std::numeric_limits<uint64_t>::max(),
                                         &count);
    if (errorCode != COUCHSTORE_SUCCESS) {
        std::string err("CouchKVStore::initScanContext:Failed to obtain changes "
                        "count with error: " +
                        std::string(couchstore_strerror(errorCode)));
        closeDatabaseHandle(db);
        throw std::runtime_error(err);
    }

    size_t backfillId = backfillCounter++;

    LockHolder lh(backfillLock);
    backfills[backfillId] = db;

    return new ScanContext(cb, cl, vbid, backfillId, startSeqno,
                           info.last_sequence, options,
                           valOptions, count);
}

scan_error_t CouchKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    LockHolder lh(backfillLock);
    std::map<size_t, Db*>::iterator itr = backfills.find(ctx->scanId);
    if (itr == backfills.end()) {
        return scan_failed;
    }

    Db* db = itr->second;
    lh.unlock();

    couchstore_docinfos_options options;
    switch (ctx->docFilter) {
        case DocumentFilter::NO_DELETES:
            options = COUCHSTORE_NO_DELETES;
            break;
        case DocumentFilter::ONLY_DELETES:
            options = COUCHSTORE_DELETES_ONLY;
            break;
        case DocumentFilter::ALL_ITEMS:
            options = COUCHSTORE_NO_OPTIONS;
            break;
        default:
            std::string err("CouchKVStore::scan:Illegal document filter!" +
                            std::to_string(static_cast<int>(ctx->docFilter)));
            throw std::runtime_error(err);
    }

    uint64_t start = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        start = ctx->lastReadSeqno + 1;
    }

    couchstore_error_t errorCode;
    errorCode = couchstore_changes_since(db, start, options, recordDbDumpC,
                                         static_cast<void*>(ctx));
    if (errorCode != COUCHSTORE_SUCCESS) {
        if (errorCode == COUCHSTORE_ERROR_CANCEL) {
            return scan_again;
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "couchstore_changes_since failed, error=%s [%s]",
                couchstore_strerror(errorCode),
                couchkvstore_strerrno(db, errorCode).c_str());
            remVBucketFromDbFileMap(ctx->vbid);
            return scan_failed;
        }
    }
    return scan_success;
}

void CouchKVStore::destroyScanContext(ScanContext* ctx) {
    if (!ctx) {
        return;
    }

    LockHolder lh(backfillLock);
    std::map<size_t, Db*>::iterator itr = backfills.find(ctx->scanId);
    if (itr != backfills.end()) {
        closeDatabaseHandle(itr->second);
        backfills.erase(itr);
    }
    delete ctx;
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
    const std::vector<std::string> files = findFilesWithPrefix(nameKey);
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

void CouchKVStore::updateDbFileMap(uint16_t vbucketId, uint64_t newFileRev) {
    if (vbucketId >= numDbFiles) {
        LOG(EXTENSION_LOG_WARNING,
            "Cannot update db file map for an invalid vbucket, "
            "vbucket id = %d, rev = %" PRIu64, vbucketId, newFileRev);
        return;
    }

    dbFileRevMap[vbucketId] = newFileRev;
}

couchstore_error_t CouchKVStore::openDB(uint16_t vbucketId,
                                        uint64_t fileRev,
                                        Db **db,
                                        uint64_t options,
                                        uint64_t *newFileRev,
                                        bool reset,
                                        const couch_file_ops* ops) {
    std::string dbFileName = getDBFileName(dbname, vbucketId, fileRev);

    if(ops == nullptr) {
        ops = &statCollectingFileOps;
    }

    uint64_t newRevNum = fileRev;
    couchstore_error_t errorCode = COUCHSTORE_SUCCESS;

    if (reset) {
        errorCode = couchstore_open_db_ex(dbFileName.c_str(), options,
                                          ops, db);
        if (errorCode == COUCHSTORE_SUCCESS) {
            newRevNum = 1;
            updateDbFileMap(vbucketId, fileRev);
            LOG(EXTENSION_LOG_INFO,
                "reset: created new couchstore file, name=%s rev=%" PRIu64,
                dbFileName.c_str(), fileRev);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "reset: creating a new couchstore file,"
                "name=%s rev=%" PRIu64 " failed with error=%s", dbFileName.c_str(),
                fileRev, couchstore_strerror(errorCode));
        }
    } else {
        if (options == COUCHSTORE_OPEN_FLAG_CREATE) {
            // first try to open the requested file without the
            // create option in case it does already exist
            errorCode = couchstore_open_db_ex(dbFileName.c_str(), 0, ops, db);
            if (errorCode != COUCHSTORE_SUCCESS) {
                // open_db failed but still check if the file exists
                newRevNum = checkNewRevNum(dbFileName);
                bool fileExists = (newRevNum) ? true : false;
                if (fileExists) {
                    errorCode = openDB_retry(dbFileName, 0, ops, db,
                                             &newRevNum);
                } else {
                    // requested file doesn't seem to exist, just create one
                    errorCode = couchstore_open_db_ex(dbFileName.c_str(),
                                                      options, ops, db);
                    if (errorCode == COUCHSTORE_SUCCESS) {
                        newRevNum = 1;
                        updateDbFileMap(vbucketId, fileRev);
                        LOG(EXTENSION_LOG_INFO,
                            "INFO: created new couch db file, name=%s rev=%" PRIu64,
                            dbFileName.c_str(), fileRev);
                    }
                }
            }
        } else {
            errorCode = openDB_retry(dbFileName, options, ops, db,
                                     &newRevNum);
        }
    }

    /* update command statistics */
    st.numOpen++;
    if (errorCode) {
        st.numOpenFailure++;
        LOG(EXTENSION_LOG_WARNING, "couchstore_open_db failed, name=%s"
            " option=%" PRIX64 " rev=%" PRIu64 " error=%s [%s]",
            dbFileName.c_str(), options,
            ((newRevNum > fileRev) ? newRevNum : fileRev),
            couchstore_strerror(errorCode),
            cb_strerror().c_str());
    } else {
        if (newRevNum > fileRev) {
            // new revision number found, update it
            updateDbFileMap(vbucketId, newRevNum);
        }
    }

    if (newFileRev != NULL) {
        *newFileRev = (newRevNum > fileRev) ? newRevNum : fileRev;
    }
    return errorCode;
}

couchstore_error_t CouchKVStore::openDB_retry(std::string &dbfile,
                                              uint64_t options,
                                              const couch_file_ops *ops,
                                              Db** db, uint64_t *newFileRev) {
    int retry = 0;
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;

    while (retry < MAX_OPEN_DB_RETRY) {
        errCode = couchstore_open_db_ex(dbfile.c_str(), options, ops, db);
        if (errCode == COUCHSTORE_SUCCESS) {
            return errCode;
        }
        LOG(EXTENSION_LOG_NOTICE, "INFO: couchstore_open_db failed, name=%s "
            "options=%" PRIX64 " error=%s [%s], try it again!",
            dbfile.c_str(), options, couchstore_strerror(errCode),
            cb_strerror().c_str());
        *newFileRev = checkNewRevNum(dbfile);
        ++retry;
        if (retry == MAX_OPEN_DB_RETRY - 1 && options == 0 &&
            errCode == COUCHSTORE_ERROR_NO_SUCH_FILE) {
            options = COUCHSTORE_OPEN_FLAG_CREATE;
        }
    }
    return errCode;
}

void CouchKVStore::populateFileNameMap(std::vector<std::string> &filenames,
                                       std::vector<uint16_t> *vbids) {
    std::vector<std::string>::iterator fileItr;

    for (fileItr = filenames.begin(); fileItr != filenames.end(); ++fileItr) {
        const std::string &filename = *fileItr;
        size_t secondDot = filename.rfind(".");
        std::string nameKey = filename.substr(0, secondDot);
        size_t firstDot = nameKey.rfind(".");
#ifdef _MSC_VER
        size_t firstSlash = nameKey.rfind("\\");
#else
        size_t firstSlash = nameKey.rfind("/");
#endif

        std::string revNumStr = filename.substr(secondDot + 1);
        char *ptr = NULL;
        uint64_t revNum = strtoull(revNumStr.c_str(), &ptr, 10);

        std::string vbIdStr = nameKey.substr(firstSlash + 1,
                                            (firstDot - firstSlash) - 1);
        if (allDigit(vbIdStr)) {
            int vbId = atoi(vbIdStr.c_str());
            if (vbids) {
                vbids->push_back(static_cast<uint16_t>(vbId));
            }
            uint64_t old_rev_num = dbFileRevMap[vbId];
            if (old_rev_num == revNum) {
                continue;
            } else if (old_rev_num < revNum) { // stale revision found
                dbFileRevMap[vbId] = revNum;
            } else { // stale file found (revision id has rolled over)
                old_rev_num = revNum;
            }
            std::stringstream old_file;
            old_file << dbname << "/" << vbId << ".couch." << old_rev_num;
            if (access(old_file.str().c_str(), F_OK) == 0) {
                if (!isReadOnly()) {
                    if (remove(old_file.str().c_str()) == 0) {
                        LOG(EXTENSION_LOG_INFO, "Removed stale file '%s'",
                            old_file.str().c_str());
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "Warning: Failed to remove the stale file '%s': %s",
                            old_file.str().c_str(), cb_strerror().c_str());
                    }
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "A read-only instance of the underlying store was not "
                        "allowed to delete a stale file: %s!",
                        old_file.str().c_str());
                }
            }
        } else {
            // skip non-vbucket database file, master.couch etc
            LOG(EXTENSION_LOG_DEBUG,
                "Non-vbucket database file, %s, skip adding "
                "to CouchKVStore dbFileMap\n", filename.c_str());
        }
    }
}

couchstore_error_t CouchKVStore::fetchDoc(Db *db, DocInfo *docinfo,
                                          GetValue &docValue, uint16_t vbId,
                                          bool metaOnly, bool fetchDelete) {
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;
    sized_buf metadata = docinfo->rev_meta;
    uint32_t itemFlags = 0;
    uint64_t cas = 0;
    time_t exptime = 0;
    uint8_t ext_meta[EXT_META_LEN];
    uint8_t ext_len = 0;

    if (metadata.size < DEFAULT_META_LEN) {
        throw std::invalid_argument("CouchKVStore::fetchDoc: "
                        "docValue->rev_meta.size (which is " +
                        std::to_string(metadata.size) +
                        ") is less than DEFAULT_META_LEN (which is " +
                        std::to_string(DEFAULT_META_LEN) + ")");
    }

    if (metadata.size >= DEFAULT_META_LEN) {
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemFlags, (metadata.buf) + 12, 4);
        ext_len = 0;
    }

    if (metadata.size >= V1_META_LEN) {
        memcpy(ext_meta, (metadata.buf) + DEFAULT_META_LEN + FLEX_DATA_OFFSET,
               EXT_META_LEN);
        ext_len = EXT_META_LEN;
    }

    cas = ntohll(cas);
    exptime = ntohl(exptime);

    if (metaOnly || (fetchDelete && docinfo->deleted)) {
        Item *it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                            itemFlags, (time_t)exptime, NULL, docinfo->size,
                            ext_meta, ext_len, cas, docinfo->db_seq, vbId);
        if (docinfo->deleted) {
            it->setDeleted();
        }

        it->setRevSeqno(docinfo->rev_seq);
        docValue = GetValue(it);
        // update ep-engine IO stats
        ++st.io_num_read;
        st.io_read_bytes.fetch_add(docinfo->id.size);
    } else {
        Doc *doc = NULL;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc,
                                                   DECOMPRESS_DOC_BODIES);
        if (errCode == COUCHSTORE_SUCCESS) {
            if (docinfo->deleted) {
                // do not read a doc that is marked deleted, just return the
                // error code as not found but still release the document body.
                errCode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
            } else {
                if (doc == nullptr) {
                    throw std::logic_error("CouchKVStore::fetchDoc: doc is NULL");
                }
                if (doc->id.size > UINT16_MAX) {
                    throw std::logic_error("CouchKVStore::fetchDoc: "
                            "doc->id.size (which is" +
                            std::to_string(doc->id.size) + ") is greater than "
                            + std::to_string(UINT16_MAX));
                }

                size_t valuelen = doc->data.size;
                void *valuePtr = doc->data.buf;

                /**
                 * Set Datatype correctly if data is being
                 * read from couch files where datatype is
                 * not supported.
                 */
                if (metadata.size == DEFAULT_META_LEN) {
                    ext_len = EXT_META_LEN;
                    ext_meta[0] = determine_datatype((const unsigned char*)valuePtr,
                                                     valuelen);
                }

                Item *it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                                    itemFlags, (time_t)exptime, valuePtr, valuelen,
                                    ext_meta, ext_len, cas, docinfo->db_seq, vbId,
                                    docinfo->rev_seq);

                docValue = GetValue(it);

                // update ep-engine IO stats
                ++st.io_num_read;
                st.io_read_bytes.fetch_add(docinfo->id.size + valuelen);
            }
            couchstore_free_document(doc);
        }
    }
    return errCode;
}

int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx) {

    ScanContext* sctx = static_cast<ScanContext*>(ctx);
    std::shared_ptr<Callback<GetValue> > cb = sctx->callback;
    std::shared_ptr<Callback<CacheLookup> > cl = sctx->lookup;

    Doc *doc = NULL;
    void *valuePtr = NULL;
    size_t valuelen = 0;
    uint64_t byseqno = docinfo->db_seq;
    sized_buf  metadata = docinfo->rev_meta;
    uint16_t vbucketId = sctx->vbid;
    sized_buf key = docinfo->id;
    uint32_t itemflags = 0;
    uint64_t cas = 0;
    uint32_t exptime = 0;
    uint8_t ext_meta[EXT_META_LEN] = {0};
    uint8_t ext_len = 0;

    if (key.size > UINT16_MAX) {
        throw std::invalid_argument("CouchKVStore::recordDbDump: "
                        "docinfo->id.size (which is " + std::to_string(key.size) +
                        ") is greater than " + std::to_string(UINT16_MAX));
    }
    if (metadata.size < DEFAULT_META_LEN) {
        throw std::invalid_argument("CouchKVStore::recordDbDump: "
                        "docinfo->rev_meta.size (which is " + std::to_string(key.size) +
                        ") is less than " + std::to_string(DEFAULT_META_LEN));
    }

    std::string docKey(docinfo->id.buf, docinfo->id.size);
    CacheLookup lookup(docKey, byseqno, vbucketId);
    cl->callback(lookup);
    if (cl->getStatus() == ENGINE_KEY_EEXISTS) {
        sctx->lastReadSeqno = byseqno;
        return COUCHSTORE_SUCCESS;
    } else if (cl->getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    if (metadata.size >= DEFAULT_META_LEN) {
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemflags, (metadata.buf) + 12, 4);
        ext_len = 0;
    }

    if (metadata.size >= V1_META_LEN) {
        memcpy(ext_meta, (metadata.buf) + DEFAULT_META_LEN + FLEX_DATA_OFFSET,
               EXT_META_LEN);
        ext_len = EXT_META_LEN;
    }

    exptime = ntohl(exptime);
    cas = ntohll(cas);

    if (sctx->valFilter != ValueFilter::KEYS_ONLY && !docinfo->deleted) {
        couchstore_error_t errCode;
        bool expectCompressed = false;
        /**
         * If couch files do not support datatype or no special
         * request is made to retrieve compressed documents as is,
         * then DECOMPRESS the document.
         */
        couchstore_open_options openOptions = 0;
        if (metadata.size == DEFAULT_META_LEN ||
            sctx->valFilter == ValueFilter::VALUES_DECOMPRESSED) {
            openOptions = DECOMPRESS_DOC_BODIES;
        } else {
            // => sctx->valFilter == ValueFilter::VALUES_COMPRESSED
            expectCompressed = true;
        }
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, openOptions);

        if (errCode == COUCHSTORE_SUCCESS) {
            if (doc->data.size) {
                valuelen = doc->data.size;
                valuePtr = doc->data.buf;

                /**
                 * Set Datatype correctly if data is being
                 * read from couch files where datatype is
                 * not supported.
                 */
                if (metadata.size == DEFAULT_META_LEN) {
                    ext_len = EXT_META_LEN;
                    ext_meta[0] = determine_datatype((const unsigned char*)valuePtr,
                                                     valuelen);
                }

                if (expectCompressed) {
                    /**
                     * If a compressed document was retrieved as is,
                     * update the datatype of the document.
                     */
                    uint8_t datatype = ext_meta[0];
                    if (datatype == PROTOCOL_BINARY_DATATYPE_JSON) {
                        ext_meta[0] = PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON;
                    } else if (datatype == PROTOCOL_BINARY_RAW_BYTES) {
                        ext_meta[0] = PROTOCOL_BINARY_DATATYPE_COMPRESSED;
                    }
                }
            }
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to retrieve key value from database "
                "database, vBucket=%d key=%s error=%s [%s]\n",
                vbucketId, key.buf, couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
            return COUCHSTORE_SUCCESS;
        }
    }

    Item *it = new Item((void *)key.buf,
                        key.size,
                        itemflags,
                        (time_t)exptime,
                        valuePtr, valuelen,
                        ext_meta, ext_len,
                        cas,
                        docinfo->db_seq, // return seq number being persisted on disk
                        vbucketId,
                        docinfo->rev_seq);
    if (docinfo->deleted) {
        it->setDeleted();
    }

    bool onlyKeys = (sctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;
    GetValue rv(it, ENGINE_SUCCESS, -1, onlyKeys);
    cb->callback(rv);

    couchstore_free_document(doc);

    if (cb->getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
    }

    sctx->lastReadSeqno = byseqno;
    return COUCHSTORE_SUCCESS;
}

bool CouchKVStore::commit2couchstore(Callback<kvstats_ctx> *cb) {
    bool success = true;

    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return success;
    }

    Doc **docs = new Doc *[pendingCommitCnt];
    DocInfo **docinfos = new DocInfo *[pendingCommitCnt];

    if (pendingReqsQ[0] == nullptr) {
        throw std::logic_error("CouchKVStore::commit2couchstore: "
                        "pendingReqsQ[0] is NULL");
    }
    uint16_t vbucket2flush = pendingReqsQ[0]->getVBucketId();
    uint64_t fileRev = pendingReqsQ[0]->getRevNum();
    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        CouchRequest *req = pendingReqsQ[i];
        if (req == nullptr) {
            throw std::logic_error("CouchKVStore::commit2couchstore: "
                                       "pendingReqsQ["
                                       + std::to_string(i) + "] is NULL");
        }
        docs[i] = (Doc *)req->getDbDoc();
        docinfos[i] = req->getDbDocInfo();
        if (vbucket2flush != req->getVBucketId()) {
            throw std::logic_error(
                    "CouchKVStore::commit2couchstore: "
                    "mismatch between vbucket2flush (which is "
                    + std::to_string(vbucket2flush) + ") and pendingReqsQ["
                    + std::to_string(i) + "] (which is "
                    + std::to_string(req->getVBucketId()) + ")");
        }
    }

    kvstats_ctx kvctx;
    kvctx.vbucket = vbucket2flush;
    // flush all
    couchstore_error_t errCode = saveDocs(vbucket2flush, fileRev, docs,
                                          docinfos, pendingCommitCnt,
                                          kvctx);
    if (errCode) {
        success = false;
        LOG(EXTENSION_LOG_WARNING,
            "Commit failed, cannot save CouchDB docs "
            "for vbucket = %d rev = %" PRIu64, vbucket2flush, fileRev);
    }
    if (cb) {
        cb->callback(kvctx);
    }
    commitCallback(pendingReqsQ, kvctx, errCode);

    // clean up
    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        delete pendingReqsQ[i];
    }
    pendingReqsQ.clear();
    delete [] docs;
    delete [] docinfos;
    return success;
}

static int readDocInfos(Db *db, DocInfo *docinfo, void *ctx) {
    if (ctx == nullptr) {
        throw std::invalid_argument("readDocInfos: ctx must be non-NULL");
    }
    kvstats_ctx *cbCtx = static_cast<kvstats_ctx *>(ctx);
    if(docinfo) {
        // An item exists in the VB DB file.
        if (!docinfo->deleted) {
            std::string key(docinfo->id.buf, docinfo->id.size);
            std::unordered_map<std::string, kstat_entry_t>::iterator itr =
                cbCtx->keyStats.find(key);
            if (itr != cbCtx->keyStats.end()) {
                itr->second.first = true;
            }
        }
    }
    return 0;
}

couchstore_error_t CouchKVStore::saveDocs(uint16_t vbid, uint64_t rev,
                                          Doc **docs, DocInfo **docinfos,
                                          size_t docCount, kvstats_ctx &kvctx) {
    couchstore_error_t errCode;
    uint64_t fileRev = rev;
    DbInfo info;
    if (rev == 0) {
        throw std::invalid_argument("CouchKVStore::saveDocs: rev must be non-zero");
    }

    Db *db = NULL;
    uint64_t newFileRev;
    errCode = openDB(vbid, fileRev, &db, 0, &newFileRev);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, vbucketId = %d "
                "fileRev = %" PRIu64 " numDocs = %" PRIu64, vbid, fileRev,
                uint64_t(docCount));
        return errCode;
    } else {
        vbucket_state *state = cachedVBStates[vbid];
        if (state == nullptr) {
            throw std::logic_error(
                    "CouchKVStore::saveDocs: cachedVBStates[" +
                    std::to_string(vbid) + "] is NULL");
        }

        uint64_t maxDBSeqno = 0;
        sized_buf *ids = new sized_buf[docCount];
        for (size_t idx = 0; idx < docCount; idx++) {
            ids[idx] = docinfos[idx]->id;
            maxDBSeqno = std::max(maxDBSeqno, docinfos[idx]->db_seq);
            std::string key(ids[idx].buf, ids[idx].size);
            kvctx.keyStats[key] = std::make_pair(false,
                    !docinfos[idx]->deleted);
        }
        couchstore_docinfos_by_id(db, ids, (unsigned) docCount, 0,
                readDocInfos, &kvctx);
        delete[] ids;

        hrtime_t cs_begin = gethrtime();
        uint64_t flags = COMPRESS_DOC_BODIES | COUCHSTORE_SEQUENCE_AS_IS;
        errCode = couchstore_save_documents(db, docs, docinfos,
                (unsigned) docCount, flags);
        st.saveDocsHisto.add((gethrtime() - cs_begin) / 1000);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                    "Failed to save docs to database, "
                    "numDocs = %" PRIu64 " error=%s [%s]\n",
                    uint64_t(docCount), couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode).c_str());
            closeDatabaseHandle(db);
            return errCode;
        }

        errCode = saveVBState(db, *state);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "Failed to save local docs to "
                "database, error=%s [%s]", couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
                closeDatabaseHandle(db);
                return errCode;
        }

        cs_begin = gethrtime();
        errCode = couchstore_commit(db);
        st.commitHisto.add((gethrtime() - cs_begin) / 1000);
        if (errCode) {
            LOG(EXTENSION_LOG_WARNING,
                    "couchstore_commit failed, error=%s [%s]",
                    couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode).c_str());
            closeDatabaseHandle(db);
            return errCode;
        }

        st.batchSize.add(docCount);

        // retrieve storage system stats for file fragmentation computation
        couchstore_db_info(db, &info);
        kvctx.fileSpaceUsed = info.space_used;
        kvctx.fileSize = info.file_size;
        cachedDeleteCount[vbid] = info.deleted_count;
        cachedDocCount[vbid] = info.doc_count;

        if (maxDBSeqno != info.last_sequence) {
            LOG(EXTENSION_LOG_WARNING, "Seqno in db header (%" PRIu64 ")"
                " is not matched with what was persisted (%" PRIu64 ")"
                " for vbucket %d",
                info.last_sequence, maxDBSeqno, vbid);
        }
        state->highSeqno = info.last_sequence;

        closeDatabaseHandle(db);
    }

    /* update stat */
    if(errCode == COUCHSTORE_SUCCESS) {
        st.docsCommitted = docCount;
    }

    return errCode;
}

void CouchKVStore::remVBucketFromDbFileMap(uint16_t vbucketId) {
    if (vbucketId >= numDbFiles) {
        LOG(EXTENSION_LOG_WARNING,
            "Cannot remove db file map entry for an invalid vbucket, "
            "vbucket id = %d\n", vbucketId);
        return;
    }

    // just reset revision number of the requested vbucket
    dbFileRevMap[vbucketId] = 1;
}

void CouchKVStore::commitCallback(std::vector<CouchRequest *> &committedReqs,
                                  kvstats_ctx &kvctx,
                                  couchstore_error_t errCode) {
    size_t commitSize = committedReqs.size();

    for (size_t index = 0; index < commitSize; index++) {
        size_t dataSize = committedReqs[index]->getNBytes();
        size_t keySize = committedReqs[index]->getKey().length();
        /* update ep stats */
        ++st.io_num_write;
        st.io_write_bytes.fetch_add(keySize + dataSize);

        if (committedReqs[index]->isDelete()) {
            int rv = getMutationStatus(errCode);
            if (rv != -1) {
                const std::string &key = committedReqs[index]->getKey();
                if (kvctx.keyStats[key].first) {
                    rv = 1; // Deletion is for an existing item on DB file.
                } else {
                    rv = 0; // Deletion is for a non-existing item on DB file.
                }
            }
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta() / 1000);
            }
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            int rv = getMutationStatus(errCode);
            const std::string &key = committedReqs[index]->getKey();
            bool insertion = !kvctx.keyStats[key].first;
            if (errCode) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(committedReqs[index]->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + keySize);
            }
            mutation_result p(rv, insertion);
            committedReqs[index]->getSetCallback()->callback(p);
        }
    }
}

ENGINE_ERROR_CODE CouchKVStore::readVBState(Db *db, uint16_t vbId) {
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

    DbInfo info;
    errCode = couchstore_db_info(db, &info);
    if (errCode == COUCHSTORE_SUCCESS) {
        highSeqno = info.last_sequence;
        purgeSeqno = info.purge_seq;
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "CouchKVStore::readVBState:Failed to read database info "
            "for vbucket: %d with error: %s", vbId,
            couchstore_strerror(errCode));
        return couchErr2EngineErr(errCode);
    }

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    errCode = couchstore_open_local_document(db, (void *)id.buf,
                                             id.size, &ldoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_DOC_NOT_FOUND) {
            LOG(EXTENSION_LOG_NOTICE,
                "CouchKVStore::readVBState: '_local/vbstate' not found "
                "for vBucket: %d", vbId);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "CouchKVStore::readVBState: Failed to "
                "retrieve stat info for vBucket: %d with error: %s",
                vbId, couchstore_strerror(errCode));
        }
    } else {
        const std::string statjson(ldoc->json.buf, ldoc->json.size);
        cJSON *jsonObj = cJSON_Parse(statjson.c_str());
        if (!jsonObj) {
            couchstore_free_local_document(ldoc);
            LOG(EXTENSION_LOG_WARNING, "CouchKVStore::readVBState: Failed to "
                "parse the vbstat json doc for vbucket %d: %s",
                vbId , statjson.c_str());
            return couchErr2EngineErr(errCode);
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
        cJSON *failover_json = cJSON_GetObjectItem(jsonObj, "failover_table");
        if (vb_state.compare("") == 0 || checkpoint_id.compare("") == 0
                || max_deleted_seqno.compare("") == 0) {
            LOG(EXTENSION_LOG_WARNING, "CouchKVStore::readVBState: State JSON doc "
                "for vbucket: %d is in the wrong format: %s, vb state: %s,"
                "checkpoint id: %s and max deleted seqno: %s",
                vbId, statjson.c_str(), vb_state.c_str(),
                checkpoint_id.c_str(), max_deleted_seqno.c_str());
        } else {
            state = VBucket::fromString(vb_state.c_str());
            parseUint64(max_deleted_seqno.c_str(), &maxDeletedSeqno);
            parseUint64(checkpoint_id.c_str(), &checkpointId);

            if (snapStart.compare("") == 0) {
                lastSnapStart = highSeqno;
            } else {
                parseUint64(snapStart.c_str(), &lastSnapStart);
            }

            if (snapEnd.compare("") == 0) {
                lastSnapEnd = highSeqno;
            } else {
                parseUint64(snapEnd.c_str(), &lastSnapEnd);
            }

            if (maxCasValue.compare("") != 0) {
                parseUint64(maxCasValue.c_str(), &maxCas);

                // MB-17517: If the maxCas on disk was invalid then don't use it -
                // instead rebuild from the items we load from disk (i.e. as per
                // an upgrade from an earlier version).
                if (maxCas == static_cast<uint64_t>(-1)) {
                    LOG(EXTENSION_LOG_WARNING,
                        "Invalid max_cas (0x%" PRIx64 ") read from '%s' for "
                        "vbucket %" PRIu16 ". Resetting max_cas to zero.",
                        maxCas, id.buf, vbId);
                    maxCas = 0;
                }
            }

            if (failover_json) {
                char* json = cJSON_PrintUnformatted(failover_json);
                failovers.assign(json);
                cJSON_Free(json);
            }
        }
        cJSON_Delete(jsonObj);
        couchstore_free_local_document(ldoc);
    }

    delete cachedVBStates[vbId];
    cachedVBStates[vbId] = new vbucket_state(state, checkpointId,
                                             maxDeletedSeqno, highSeqno,
                                             purgeSeqno, lastSnapStart,
                                             lastSnapEnd, maxCas, failovers);

    return couchErr2EngineErr(errCode);
}

couchstore_error_t CouchKVStore::saveVBState(Db *db,
                                             const vbucket_state &vbState) {
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state) << "\""
              << ",\"checkpoint_id\": \"" << vbState.checkpointId << "\""
              << ",\"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno << "\""
              << ",\"failover_table\": " << vbState.failovers
              << ",\"snap_start\": \"" << vbState.lastSnapStart << "\""
              << ",\"snap_end\": \"" << vbState.lastSnapEnd << "\""
              << ",\"max_cas\": \"" << vbState.maxCas << "\""
              << "}";

    LocalDoc lDoc;
    lDoc.id.buf = (char *)"_local/vbstate";
    lDoc.id.size = sizeof("_local/vbstate") - 1;
    std::string state = jsonState.str();
    lDoc.json.buf = (char *)state.c_str();
    lDoc.json.size = state.size();
    lDoc.deleted = 0;

    couchstore_error_t errCode = couchstore_save_local_document(db, &lDoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "couchstore_save_local_document failed "
            "error=%s [%s]\n", couchstore_strerror(errCode),
            couchkvstore_strerrno(db, errCode).c_str());
    }
    return errCode;
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

    std::string keyStr(docinfo->id.buf, docinfo->id.size);
    GetMultiCbCtx *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    CouchKVStoreStats &st = cbCtx->cks.getCKVStoreStat();

    vb_bgfetch_queue_t::iterator qitr = cbCtx->fetches.find(keyStr);
    if (qitr == cbCtx->fetches.end()) {
        // this could be a serious race condition in couchstore,
        // log a warning message and continue
        LOG(EXTENSION_LOG_WARNING,
            "Couchstore returned invalid docinfo, "
            "no pending bgfetch has been issued for key = %s\n",
            keyStr.c_str());
        return 0;
    }

    vb_bgfetch_item_ctx_t& bg_itm_ctx = (*qitr).second;
    bool meta_only = bg_itm_ctx.isMetaOnly;

    GetValue returnVal;

    couchstore_error_t errCode = cbCtx->cks.fetchDoc(db, docinfo, returnVal,
                                                     cbCtx->vbId, meta_only);
    if (errCode != COUCHSTORE_SUCCESS && !meta_only) {
        LOG(EXTENSION_LOG_WARNING, "Failed to fetch data from database, "
            "vBucket=%d key=%s error=%s [%s]", cbCtx->vbId,
            keyStr.c_str(), couchstore_strerror(errCode),
            couchkvstore_strerrno(db, errCode).c_str());
        st.numGetFailure++;
    }

    returnVal.setStatus(cbCtx->cks.couchErr2EngineErr(errCode));

    std::list<VBucketBGFetchItem *> &fetches = bg_itm_ctx.bgfetched_list;
    std::list<VBucketBGFetchItem *>::iterator itr = fetches.begin();

    bool return_val_ownership_transferred = false;
    for (itr = fetches.begin(); itr != fetches.end(); ++itr) {
        return_val_ownership_transferred = true;
        // populate return value for remaining fetch items with the
        // same seqid
        (*itr)->value = returnVal;
        st.readTimeHisto.add((gethrtime() - (*itr)->initTime) / 1000);
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(returnVal.getValue()->getNKey() +
                                 returnVal.getValue()->getNBytes());
        }
    }
    if (!return_val_ownership_transferred) {
        LOG(EXTENSION_LOG_WARNING, "CouchKVStore::getMultiCb called with zero"
            "items in bgfetched_list, vBucket=%d key=%s",
            cbCtx->vbId, keyStr.c_str());
        delete returnVal.getValue();
    }

    return 0;
}


void CouchKVStore::closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "couchstore_close_db failed, error=%s [%s]",
            couchstore_strerror(ret), couchkvstore_strerrno(NULL, ret).c_str());
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
        // EventuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
}

size_t CouchKVStore::getNumPersistedDeletes(uint16_t vbid) {
    size_t delCount = cachedDeleteCount[vbid];
    if (delCount != (size_t) -1) {
        return delCount;
    }

    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        DbInfo info;
        errCode = couchstore_db_info(db, &info);
        if (errCode == COUCHSTORE_SUCCESS) {
            cachedDeleteCount[vbid] = info.deleted_count;
            closeDatabaseHandle(db);
            return info.deleted_count;
        } else {
            throw std::runtime_error("CouchKVStore::getNumPersistedDeletes:"
                "Failed to read database info for vBucket = " +
                std::to_string(vbid) + " rev = " + std::to_string(rev) +
                " with error:" + couchstore_strerror(errCode));
        }
        closeDatabaseHandle(db);
    } else {
        // open failed - map couchstore error code to exception.
        std::errc ec;
        switch (errCode) {
            case COUCHSTORE_ERROR_OPEN_FILE:
                ec = std::errc::no_such_file_or_directory; break;
            default:
                ec = std::errc::io_error; break;
        }
        throw std::system_error(std::make_error_code(ec),
                                "CouchKVStore::getNumPersistedDeletes:"
            "Failed to open database file for vBucket = " +
            std::to_string(vbid) + " rev = " + std::to_string(rev) +
            " with error:" + couchstore_strerror(errCode));
    }
    return 0;
}

DBFileInfo CouchKVStore::getDbFileInfo(uint16_t vbid) {
    Db *db = nullptr;
    uint64_t rev = dbFileRevMap[vbid];

    DBFileInfo vbinfo;

    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        DbInfo info;
        errCode = couchstore_db_info(db, &info);
        closeDatabaseHandle(db);
        if (errCode == COUCHSTORE_SUCCESS) {
            cachedDocCount[vbid] = info.doc_count;
            vbinfo.itemCount = info.doc_count;
            vbinfo.fileSize = info.file_size;
            vbinfo.spaceUsed = info.space_used;
        } else {
            throw std::runtime_error("CouchKVStore::getDbFileInfo: Failed "
                "to read database info for vBucket = " + std::to_string(vbid) +
                " rev = " + std::to_string(rev) +
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
        throw std::system_error(std::make_error_code(ec),
                                "CouchKVStore::getDbInfo: failed to open database file for "
                                "vBucket = " + std::to_string(vbid) +
                                " rev = " + std::to_string(rev) +
                                " with error:" + couchstore_strerror(errCode));
    }
    return vbinfo;
}

size_t CouchKVStore::getNumItems(uint16_t vbid, uint64_t min_seq,
                                 uint64_t max_seq) {
    Db *db = NULL;
    uint64_t count = 0;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        errCode = couchstore_changes_count(db, min_seq, max_seq, &count);
        if (errCode != COUCHSTORE_SUCCESS) {
            throw std::runtime_error("CouchKVStore::getNumItems: Failed to "
                "get changes count for vBucket = " + std::to_string(vbid) +
                " rev = " + std::to_string(rev) +
                " with error:" + couchstore_strerror(errCode));
        }
        closeDatabaseHandle(db);
    } else {
        throw std::invalid_argument("CouchKVStore::getNumItems: Failed to "
            "open database file for vBucket = " + std::to_string(vbid) +
            " rev = " + std::to_string(rev) +
            " with error:" + couchstore_strerror(errCode));
    }
    return count;
}

RollbackResult CouchKVStore::rollback(uint16_t vbid, uint64_t rollbackSeqno,
                                      std::shared_ptr<RollbackCB> cb) {
    DbHolder db(this);
    DbInfo info;
    uint64_t fileRev = dbFileRevMap[vbid];
    std::stringstream dbFileName;
    dbFileName << dbname << "/" << vbid << ".couch." << fileRev;
    couchstore_error_t errCode;

    errCode = openDB(vbid, fileRev, db.getDbAddress(),
                     (uint64_t) COUCHSTORE_OPEN_FLAG_RDONLY);

    if (errCode == COUCHSTORE_SUCCESS) {
        errCode = couchstore_db_info(db.getDb(), &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to read DB info, name=%s",
                dbFileName.str().c_str());
            return RollbackResult(false, 0, 0, 0);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, name=%s",
                dbFileName.str().c_str());
        return RollbackResult(false, 0, 0, 0);
    }

    uint64_t latestSeqno = info.last_sequence;

    //Count from latest seq no to 0
    uint64_t totSeqCount = 0;
    errCode = couchstore_changes_count(db.getDb(), 0, latestSeqno, &totSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Failed to get changes count for "
            "rollback vBucket = %d, rev = %" PRIu64 ", error=%s [%s]",
            vbid, fileRev,  couchstore_strerror(errCode),
            cb_strerror().c_str());
        return RollbackResult(false, 0, 0, 0);
    }

    DbHolder newdb(this);
    errCode = openDB(vbid, fileRev, newdb.getDbAddress(), 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, name=%s",
                dbFileName.str().c_str());
        return RollbackResult(false, 0, 0, 0);
    }

    while (info.last_sequence > rollbackSeqno) {
        errCode = couchstore_rewind_db_header(newdb.getDb());
        if (errCode != COUCHSTORE_SUCCESS) {
            // rewind_db_header cleans up (frees DB) on error; so
            // release db in DbHolder to prevent a double-free.
            newdb.releaseDb();
            LOG(EXTENSION_LOG_WARNING,
                    "Failed to rewind Db pointer "
                    "for couch file with vbid: %u, whose "
                    "lastSeqno: %" PRIu64 ", while trying to roll back "
                    "to seqNo: %" PRIu64 ", error=%s [%s]",
                    vbid, latestSeqno, rollbackSeqno,
                    couchstore_strerror(errCode), cb_strerror().c_str());
            //Reset the vbucket and send the entire snapshot,
            //as a previous header wasn't found.
            return RollbackResult(false, 0, 0, 0);
        }
        errCode = couchstore_db_info(newdb.getDb(), &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to read DB info, name=%s",
                dbFileName.str().c_str());
            return RollbackResult(false, 0, 0, 0);
        }
    }

    //Count from latest seq no to rollback seq no
    uint64_t rollbackSeqCount = 0;
    errCode = couchstore_changes_count(db.getDb(), info.last_sequence, latestSeqno,
                                       &rollbackSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Failed to get changes count for "
            "rollback vBucket = %d, rev = %" PRIu64 ", error=%s [%s]",
            vbid, fileRev, couchstore_strerror(errCode), cb_strerror().c_str());
        return RollbackResult(false, 0, 0, 0);
    }

    if ((totSeqCount / 2) <= rollbackSeqCount) {
        //doresetVbucket flag set or rollback is greater than 50%,
        //reset the vbucket and send the entire snapshot
        return RollbackResult(false, 0, 0, 0);
    }

    cb->setDbHeader(newdb.getDb());
    std::shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback());
    ScanContext* ctx = initScanContext(cb, cl, vbid, info.last_sequence+1,
                                       DocumentFilter::ALL_ITEMS,
                                       ValueFilter::KEYS_ONLY);
    scan_error_t error = scan(ctx);
    destroyScanContext(ctx);

    if (error != scan_success) {
        return RollbackResult(false, 0, 0, 0);
    }

    readVBState(newdb.getDb(), vbid);
    cachedDeleteCount[vbid] = info.deleted_count;
    cachedDocCount[vbid] = info.doc_count;

    //Append the rewinded header to the database file
    errCode = couchstore_commit(newdb.getDb());

    if (errCode != COUCHSTORE_SUCCESS) {
        return RollbackResult(false, 0, 0, 0);
    }

    vbucket_state *vb_state = cachedVBStates[vbid];
    return RollbackResult(true, vb_state->highSeqno,
                          vb_state->lastSnapStart, vb_state->lastSnapEnd);
}

int populateAllKeys(Db *db, DocInfo *docinfo, void *ctx) {
    AllKeysCtx *allKeysCtx = (AllKeysCtx *)ctx;
    uint16_t keylen = docinfo->id.size;
    char *key = docinfo->id.buf;
    (allKeysCtx->cb)->callback(keylen, key);
    if (--(allKeysCtx->count) <= 0) {
        //Only when count met is less than the actual number of entries
        return COUCHSTORE_ERROR_CANCEL;
    }
    return COUCHSTORE_SUCCESS;
}

ENGINE_ERROR_CODE
CouchKVStore::getAllKeys(uint16_t vbid, std::string &start_key, uint32_t count,
                         std::shared_ptr<Callback<uint16_t&, char*&> > cb) {
    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if(errCode == COUCHSTORE_SUCCESS) {
        sized_buf ref = {NULL, 0};
        ref.buf = (char*) start_key.c_str();
        ref.size = start_key.size();
        AllKeysCtx ctx(cb, count);
        errCode = couchstore_all_docs(db, &ref, COUCHSTORE_NO_DELETES,
                                      populateAllKeys,
                                      static_cast<void *>(&ctx));
        closeDatabaseHandle(db);
        if (errCode == COUCHSTORE_SUCCESS ||
                errCode == COUCHSTORE_ERROR_CANCEL)  {
            return ENGINE_SUCCESS;
        } else {
            LOG(EXTENSION_LOG_WARNING, "couchstore_all_docs failed for "
                "database file of vbucket = %d rev = %" PRIu64
                ", error=%s [%s]", vbid, rev, couchstore_strerror(errCode),
                cb_strerror().c_str());
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "Failed to open database file for "
                "vbucket = %d rev = %" PRIu64 ", errCode = %u", vbid, rev, errCode);

    }
    return ENGINE_FAILED;
}

void CouchKVStore::unlinkCouchFile(uint16_t vbucket,
                                   uint64_t fRev) {

    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::unlinkCouchFile: Not valid on a "
                "read-only object.");
    }
    char fname[PATH_MAX];
    try {
        checked_snprintf(fname, sizeof(fname), "%s/%d.couch.%" PRIu64,
                         dbname.c_str(), vbucket, fRev);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "CouchKVStore::unlinkCouchFile: Failed to build filename: %s",
            fname);
        return;
    }

    if (remove(fname) == -1) {
        LOG(EXTENSION_LOG_WARNING, "Failed to remove database file for "
            "vbucket = %d rev = %" PRIu64 ", errCode = %u", vbucket, fRev,
            errno);

        if (errno != ENOENT) {
            std::string file_str = fname;
            pendingFileDeletions.push(file_str);
        }
    }
}

void CouchKVStore::removeCompactFile(const std::string &dbname,
                                     uint16_t vbid,
                                     uint64_t fileRev) {

    std::string dbfile = getDBFileName(dbname, vbid, fileRev);
    std::string compact_file = dbfile + ".compact";

    if (!isReadOnly()) {
        removeCompactFile(compact_file);
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "A read-only instance of the underlying store was not allowed "
            "to delete a temporary file: %s", compact_file.c_str());
    }
}

void CouchKVStore::removeCompactFile(const std::string &filename) {
    if (isReadOnly()) {
        throw std::logic_error("CouchKVStore::removeCompactFile: Not valid on "
                "a read-only object.");
    }

    if (access(filename.c_str(), F_OK) == 0) {
        if (remove(filename.c_str()) == 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Removed compact file '%s'", filename.c_str());
        }
        else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: Failed to remove compact file '%s': %s",
                filename.c_str(), cb_strerror().c_str());

            if (errno != ENOENT) {
                pendingFileDeletions.push(const_cast<std::string &>(filename));
            }
        }
    }
}

/* end of couch-kvstore.cc */
