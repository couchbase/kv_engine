/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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
#include <direct.h>
#define mkdir(a, b) _mkdir(a)
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
#include <string>
#include <utility>
#include <vector>
#include <platform/dirutils.h>
#include <cJSON.h>

#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
#define STATWRITER_NAMESPACE couchstore_engine
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

#include <JSON_checker.h>
#include <snappy-c.h>

using namespace CouchbaseDirectoryUtilities;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

static const int MAX_OPEN_DB_RETRY = 10;

static const uint32_t DEFAULT_META_LEN = 16;

class NoLookupCallback : public Callback<CacheLookup> {
public:
    NoLookupCallback() {}
    ~NoLookupCallback() {}
    void callback(CacheLookup&) {}
};

class NoRangeCallback : public Callback<SeqnoRange> {
public:
    NoRangeCallback() {}
    ~NoRangeCallback() {}
    void callback(SeqnoRange&) {}
};

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

static std::string getSystemStrerror(void) {
    std::stringstream ss;
#ifdef WIN32
    char* win_msg = NULL;
    DWORD err = GetLastError();
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                   FORMAT_MESSAGE_FROM_SYSTEM |
                   FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, err,
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                   (LPTSTR) &win_msg,
                   0, NULL);
    ss << "errno = " << err << ": '" << win_msg << "'";
    LocalFree(win_msg);
#else
    ss << "errno = " << errno << ": '" << strerror(errno) << "'";
#endif

    return ss.str();
}

static uint8_t determine_datatype(const unsigned char* value,
                                  size_t length) {
    if (checkUTF8JSON(value, length)) {
        return PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        return PROTOCOL_BINARY_RAW_BYTES;
    }
}

static const std::string getJSONObjString(const cJSON *i) {
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        abort();
    }
    return i->valuestring;
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

static uint64_t computeMaxDeletedSeqNum(DocInfo **docinfos,
                                        const int numdocs) {
    uint64_t max = 0;
    for (int idx = 0; idx < numdocs; idx++) {
        if (docinfos[idx]->deleted) {
            // check seq number only from a deleted file
            uint64_t seqNum = docinfos[idx]->rev_seq;
            max = std::max(seqNum, max);
        }
    }
    return max;
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

struct LoadResponseCtx {
    shared_ptr<Callback<GetValue> > callback;
    shared_ptr<Callback<CacheLookup> > lookup;
    uint16_t vbucketId;
    bool keysonly;
    EPStats *stats;
};

struct AllKeysCtx {
    AllKeysCtx(AllKeysCB *callback, uint32_t cnt) :
        cb(callback), count(cnt) { }

    AllKeysCB *cb;
    uint32_t count;
};

CouchRequest::CouchRequest(const Item &it, uint64_t rev,
                           CouchRequestCallback &cb, bool del) :
    value(it.getValue()), vbucketId(it.getVBucketId()), fileRevNum(rev),
    key(it.getKey()), deleteItem(del)
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
    memcpy(meta + DEFAULT_META_LEN + FLEX_DATA_OFFSET, it.getExtMeta(),
           it.getExtMetaLen());

    dbDocInfo.db_seq = it.getBySeqno();
    dbDocInfo.rev_meta.buf = reinterpret_cast<char *>(meta);
    dbDocInfo.rev_meta.size = COUCHSTORE_METADATA_SIZE;
    dbDocInfo.rev_seq = it.getRevSeqno();
    dbDocInfo.size = dbDoc.data.size;
    if (del) {
        dbDocInfo.deleted =  1;
        callback.delCb = cb.delCb;
    } else {
        dbDocInfo.deleted = 0;
        callback.setCb = cb.setCb;
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
    start = gethrtime();
}

CouchKVStore::CouchKVStore(EPStats &stats, Configuration &config, bool read_only) :
    KVStore(read_only), epStats(stats), configuration(config),
    dbname(configuration.getDbname()), intransaction(false)
{
    open();
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);

    // init db file map with default revision number, 1
    numDbFiles = static_cast<uint16_t>(configuration.getMaxVbuckets());
    cachedVBStates.reserve(numDbFiles);
    for (uint16_t i = 0; i < numDbFiles; i++) {
        // pre-allocate to avoid rehashing for safe read-only operations
        dbFileRevMap.push_back(1);
        cachedDocCount[i] = (size_t)-1;
        cachedDeleteCount[i] = (size_t)-1;
        cachedVBStates.push_back((vbucket_state *)NULL);
    }

    initialize();
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
    KVStore(copyFrom), epStats(copyFrom.epStats),
    configuration(copyFrom.configuration),
    dbname(copyFrom.dbname), dbFileRevMap(copyFrom.dbFileRevMap),
    numDbFiles(copyFrom.numDbFiles), intransaction(false)
{
    open();
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
                "%s/%d.couch.%d", dbname.c_str(), id, rev);
            remVBucketFromDbFileMap(id);
            cachedVBStates[id] = NULL;
        }

        db = NULL;
        removeCompactFile(dbname, id, rev);
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
    cb_assert(!isReadOnly());

    vbucket_state *state = cachedVBStates[vbucketId];
    if (state) {
        state->checkpointId = 0;
        state->maxDeletedSeqno = 0;
        state->highSeqno = 0;
        state->lastSnapStart = 0;
        state->lastSnapEnd = 0;

        // Unlink the couchstore file upon reset
        unlinkCouchFile(vbucketId, dbFileRevMap[vbucketId]);

        resetVBucket(vbucketId, *state);
        updateDbFileMap(vbucketId, 1);
    } else {
        LOG(EXTENSION_LOG_WARNING, "No entry in cached states "
                "for vbucket %u", vbucketId);
        cb_assert(false);
    }
}

void CouchKVStore::set(const Item &itm, Callback<mutation_result> &cb) {
    cb_assert(!isReadOnly());
    cb_assert(intransaction);
    bool deleteItem = false;
    CouchRequestCallback requestcb;
    std::string dbFile;
    uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];

    // each req will be de-allocated after commit
    requestcb.setCb = &cb;
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, deleteItem);
    pendingReqsQ.push_back(req);
}

void CouchKVStore::get(const std::string &key, uint64_t, uint16_t vb,
                       Callback<GetValue> &cb, bool fetchDelete) {
    Db *db = NULL;
    std::string dbFile;
    GetValue rv;
    uint64_t fileRev = dbFileRevMap[vb];

    couchstore_error_t errCode = openDB(vb, fileRev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to open database to retrieve data "
            "from vBucketId = %d, key = %s, file = %s\n",
            vb, key.c_str(), dbFile.c_str());
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
    std::string dbFile;

    id.size = key.size();
    id.buf = const_cast<char *>(key.c_str());
    couchstore_error_t errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                                          id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (!getMetaOnly) {
            // log error only if this is non-xdcr case
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve doc info from "
                "database, name=%s key=%s error=%s [%s]\n",
                dbFile.c_str(), id.buf, couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
        }
    } else {
        cb_assert(docInfo);
        errCode = fetchDoc(db, docInfo, rv, vb, getMetaOnly, fetchDelete);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve key value from "
                "database, name=%s key=%s error=%s [%s] "
                "deleted=%s", dbFile.c_str(), id.buf,
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
    std::string dbFile;
    int numItems = itms.size();
    uint64_t fileRev = dbFileRevMap[vb];

    Db *db = NULL;
    couchstore_error_t errCode = openDB(vb, fileRev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to open database for data fetch, "
            "vBucketId = %d file = %s numDocs = %d\n",
            vb, dbFile.c_str(), numItems);
        st.numGetFailure.fetch_add(numItems);
        vb_bgfetch_queue_t::iterator itr = itms.begin();
        for (; itr != itms.end(); ++itr) {
            std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
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
            LOG(EXTENSION_LOG_WARNING, "Warning: failed to read database by"
                " vBucketId = %d key = %s file = %s error = %s [%s]\n",
                vb, (*itr).first.c_str(),
                dbFile.c_str(), couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
            std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
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
    cb_assert(!isReadOnly());
    cb_assert(intransaction);
    uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];
    CouchRequestCallback requestcb;
    requestcb.delCb = &cb;
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, true);
    pendingReqsQ.push_back(req);
}

void CouchKVStore::delVBucket(uint16_t vbucket) {
    cb_assert(!isReadOnly());

    unlinkCouchFile(vbucket, dbFileRevMap[vbucket]);

    if (cachedVBStates[vbucket]) {
        delete cachedVBStates[vbucket];
    }

    std::string failovers("[{\"id\":0, \"seq\":0}]");
    cachedVBStates[vbucket] = new vbucket_state(vbucket_state_dead, 0, 0, 0, 0,
                                                0, 0, failovers);
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
                "Warning: error in session stats ifstream!!!");
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
                "Warning: failed to parse the session stats json doc!!!");
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
            "Warning: failed to load the engine session stats "
            " due to IO exception \"%s\"", e.what());
    } catch (...) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to load the engine session stats "
            " due to IO exception");
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
        const unsigned char* data;
        bool ret;
        if (((*info)->content_meta | COUCH_DOC_IS_COMPRESSED) ==
                (*info)->content_meta) {
            size_t uncompr_len;
            snappy_uncompressed_length(item->buf, item->size, &uncompr_len);
            char *dbuf = (char *) malloc(uncompr_len);
            snappy_uncompress(item->buf, item->size, dbuf, &uncompr_len);
            data = (const unsigned char*)dbuf;
            ret = checkUTF8JSON(data, uncompr_len);
            free(dbuf);
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

        DocInfo *docinfo = (DocInfo *) calloc (sizeof(DocInfo) +
                                               (*info)->id.size +
                                               (*info)->rev_meta.size +
                                               FLEX_DATA_OFFSET + EXT_META_LEN,
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
                                 FLEX_DATA_OFFSET + EXT_META_LEN;

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
            std::string keyStr(info->id.buf, info->id.size);
            expiredItemCtx expItem = { info->rev_seq, keyStr };
            ctx->expiredItems.push_back(expItem);
        }
    }

    return COUCHSTORE_COMPACT_KEEP_ITEM;
}

void CouchKVStore::compactVBucket(const uint16_t vbid,
                                  compaction_ctx *hook_ctx,
                                  Callback<compaction_ctx> &cb,
                                  Callback<kvstats_ctx> &kvcb) {
    couchstore_compact_hook       hook = time_purge_hook;
    couchstore_docinfo_hook      dhook = edit_docinfo_hook;
    const couch_file_ops     *def_iops = couchstore_get_default_file_ops();
    Db                      *compactdb = NULL;
    Db                       *targetDb = NULL;
    uint64_t                   fileRev = dbFileRevMap[vbid];
    uint64_t                   new_rev = fileRev + 1;
    couchstore_error_t         errCode = COUCHSTORE_SUCCESS;
    hrtime_t                     start = gethrtime();
    uint64_t              newHeaderPos = 0;
    std::string                 dbfile;
    std::string           compact_file;
    std::string               new_file;
    kvstats_ctx                  kvctx;
    DbInfo                        info;

    // Open the source VBucket database file ...
    errCode = openDB(vbid, fileRev, &compactdb,
                     (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, vbucketId = %d "
                "fileRev = %llu", vbid, fileRev);
        return;
    }

    // Build the temporary vbucket.compact file name
    dbfile       = getDBFileName(dbname, vbid, fileRev);
    compact_file = dbfile + ".compact";

    // Perform COMPACTION of vbucket.couch.rev into vbucket.couch.rev.compact
    errCode = couchstore_compact_db_ex(compactdb, compact_file.c_str(), 0,
                                       hook, dhook, hook_ctx, def_iops);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to compact database with name=%s "
            "error=%s errno=%s",
            dbfile.c_str(),
            couchstore_strerror(errCode),
            couchkvstore_strerrno(compactdb, errCode).c_str());
        closeDatabaseHandle(compactdb);
        return;
    }

    // Close the source Database File once compaction is done
    closeDatabaseHandle(compactdb);

    // Rename the .compact file to one with the next revision number
    new_file = getDBFileName(dbname, vbid, new_rev);
    if (rename(compact_file.c_str(), new_file.c_str()) != 0) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to rename '%s' to '%s': %s",
            compact_file.c_str(), new_file.c_str(),
            getSystemStrerror().c_str());

        removeCompactFile(compact_file);
        return;
    }

    // Open the newly compacted VBucket database file ...
    errCode = openDB(vbid, new_rev, &targetDb,
                     (uint64_t)COUCHSTORE_OPEN_FLAG_RDONLY, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open compacted database file %s "
                "fileRev = %llu", new_file.c_str(), new_rev);
        if (remove(new_file.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING, NULL,
                "Warning: Failed to remove '%s': %s",
                new_file.c_str(), getSystemStrerror().c_str());
        }
        return;
    }

    // Update the global VBucket file map so all operations use the new file
    updateDbFileMap(vbid, new_rev);

    LOG(EXTENSION_LOG_INFO,
            "INFO: created new couch db file, name=%s rev=%llu",
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

    // Notify MCCouch that compaction is Done...
    newHeaderPos = couchstore_get_header_position(targetDb);
    closeDatabaseHandle(targetDb);

    if (hook_ctx->expiredItems.size()) {
        cb.callback(*hook_ctx);
    }

    // Removing the stale couch file
    unlinkCouchFile(vbid, fileRev);

    st.compactHisto.add((gethrtime() - start) / 1000);
}

bool CouchKVStore::snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                                   Callback<kvstats_ctx> *cb) {
    cb_assert(!isReadOnly());

    vbucket_state *state = cachedVBStates[vbucketId];
    if (state) {
        if (state->state == vbstate.state &&
            state->checkpointId == vbstate.checkpointId &&
            state->failovers.compare(vbstate.failovers) == 0) {
            return true; // no changes
        }
        state->state = vbstate.state;
        state->checkpointId = vbstate.checkpointId;
        state->failovers = vbstate.failovers;
        // Note that max deleted seq number is maintained within CouchKVStore
        vbstate.maxDeletedSeqno = state->maxDeletedSeqno;
    } else {
        cachedVBStates[vbucketId] = new vbucket_state(vbstate);
    }

    if (!setVBucketState(vbucketId, vbstate, cb)) {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to set new state, %s, for vbucket %d\n",
                VBucket::toString(vbstate.state), vbucketId);
        return false;
    }
    return true;
}

bool CouchKVStore::snapshotStats(const std::map<std::string,
                                 std::string> &stats) {
    cb_assert(!isReadOnly());
    size_t count = 0;
    size_t size = stats.size();
    std::stringstream stats_buf;
    stats_buf << "{";
    std::map<std::string, std::string>::const_iterator it = stats.begin();
    for (; it != stats.end(); ++it) {
        stats_buf << "\"" << it->first << "\": \"" << it->second << "\"";
        ++count;
        if (count < size) {
            stats_buf << ", ";
        }
    }
    stats_buf << "}";

    // TODO: This stats json should be written into the master database. However,
    // we don't support the write synchronization between CouchKVStore in C++ and
    // compaction manager in the erlang side for the master database yet. At this time,
    // we simply log the engine stats into a separate json file. As part of futhre work,
    // we need to get rid of the tight coupling between those two components.
    bool rv = true;
    std::string next_fname = dbname + "/stats.json.new";
    std::ofstream new_stats;
    new_stats.exceptions (new_stats.failbit | new_stats.badbit);
    try {
        new_stats.open(next_fname.c_str());
        new_stats << stats_buf.str().c_str() << std::endl;
        new_stats.flush();
        new_stats.close();
    } catch (const std::ofstream::failure& e) {
        LOG(EXTENSION_LOG_WARNING, "Warning: failed to log the engine stats to "
            "file \"%s\" due to IO exception \"%s\"; Not critical because new "
            "stats will be dumped later, please ignore.",
            next_fname.c_str(), e.what());
        rv = false;
    }

    if (rv) {
        std::string old_fname = dbname + "/stats.json.old";
        std::string stats_fname = dbname + "/stats.json";
        if (access(old_fname.c_str(), F_OK) == 0 && remove(old_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING, "FATAL: Failed to remove '%s': %s",
                old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (access(stats_fname.c_str(), F_OK) == 0 &&
                   rename(stats_fname.c_str(), old_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to rename '%s' to '%s': %s",
                stats_fname.c_str(), old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (rename(next_fname.c_str(), stats_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to rename '%s' to '%s': %s",
                next_fname.c_str(), stats_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        }
    }

    return rv;
}

bool CouchKVStore::setVBucketState(uint16_t vbucketId, vbucket_state &vbstate,
                                   Callback<kvstats_ctx> *kvcb) {
    Db *db = NULL;
    uint64_t fileRev, newFileRev;
    std::stringstream id;
    std::string dbFileName;
    std::map<uint16_t, uint64_t>::iterator mapItr;
    kvstats_ctx kvctx;
    kvctx.vbucket = vbucketId;

    id << vbucketId;
    dbFileName = dbname + "/" + id.str() + ".couch." + id.str();
    fileRev = dbFileRevMap[vbucketId];

    couchstore_error_t errorCode;
    errorCode = openDB(vbucketId, fileRev, &db,
            (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE, &newFileRev);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, name=%s",
                dbFileName.c_str());
        return false;
    }

    fileRev = newFileRev;

    vbucket_state *state = cachedVBStates[vbucketId];
    vbstate.highSeqno = state->highSeqno;
    vbstate.lastSnapStart = state->lastSnapStart;
    vbstate.lastSnapEnd = state->lastSnapEnd;
    vbstate.maxDeletedSeqno = state->maxDeletedSeqno;

    errorCode = saveVBState(db, vbstate);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to save local doc, name=%s",
                dbFileName.c_str());
        closeDatabaseHandle(db);
        return false;
    }

    errorCode = couchstore_commit(db);
    if (errorCode != COUCHSTORE_SUCCESS) {
        ++st.numVbSetFailure;
        LOG(EXTENSION_LOG_WARNING,
                "Warning: commit failed, vbid=%u rev=%llu error=%s [%s]",
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

void CouchKVStore::dump(std::vector<uint16_t> &vbids,
                        shared_ptr<Callback<GetValue> > cb,
                        shared_ptr<Callback<CacheLookup> > cl) {

    shared_ptr<Callback<SeqnoRange> > sr(new NoRangeCallback());
    std::vector<uint16_t>::iterator itr = vbids.begin();
    for (; itr != vbids.end(); ++itr) {
        loadDB(cb, cl, sr, false, *itr, 0, COUCHSTORE_NO_DELETES);
    }
}

void CouchKVStore::dump(uint16_t vb, uint64_t stSeqno,
                        shared_ptr<Callback<GetValue> > cb,
                        shared_ptr<Callback<CacheLookup> > cl,
                        shared_ptr<Callback<SeqnoRange> > sr) {

    loadDB(cb, cl, sr, false, vb, stSeqno);
}

void CouchKVStore::dumpKeys(std::vector<uint16_t> &vbids,
                            shared_ptr<Callback<GetValue> > cb) {

    shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback());
    shared_ptr<Callback<SeqnoRange> > sr(new NoRangeCallback());
    std::vector<uint16_t>::iterator itr = vbids.begin();
    for (; itr != vbids.end(); ++itr) {
        loadDB(cb, cl, sr, true, *itr, 0, COUCHSTORE_NO_DELETES);
    }
}

void CouchKVStore::dumpDeleted(uint16_t vb, uint64_t stSeqno, uint64_t enSeqno,
                               shared_ptr<Callback<GetValue> > cb) {

    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback());
    shared_ptr<Callback<SeqnoRange> > sr(new NoRangeCallback());
    loadDB(cb, cl, sr, true, vb, stSeqno, COUCHSTORE_DELETES_ONLY);
}

StorageProperties CouchKVStore::getStorageProperties() {
    StorageProperties rv(true, true, true, true);
    return rv;
}

bool CouchKVStore::commit(Callback<kvstats_ctx> *cb, uint64_t snapStartSeqno,
                          uint64_t snapEndSeqno) {
    cb_assert(!isReadOnly());
    if (intransaction) {
        if (commit2couchstore(cb, snapStartSeqno, snapEndSeqno)) {
            intransaction = false;
        }
    }

    return !intransaction;
}

uint64_t CouchKVStore::getLastPersistedSeqno(uint16_t vbid) {
    vbucket_state *state = cachedVBStates[vbid];
    if (state) {
        return state->highSeqno;
    }
    return 0;
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
}

void CouchKVStore::addTimingStats(const std::string &prefix,
                                  ADD_STAT add_stat, const void *c) {
    if (isReadOnly()) {
        return;
    }
    const char *prefix_str = prefix.c_str();
    addStat(prefix_str, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix_str, "compact",     st.compactHisto,     add_stat, c);
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

template <typename T>
void CouchKVStore::addStat(const std::string &prefix, const char *stat, T &val,
                           ADD_STAT add_stat, const void *c) {
    std::stringstream fullstat;
    fullstat << prefix << ":" << stat;
    add_casted_stat(fullstat.str().c_str(), val, add_stat, c);
}

void CouchKVStore::optimizeWrites(std::vector<queued_item> &items) {
    cb_assert(!isReadOnly());
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsBySeqnoAndKey cq;
    std::sort(items.begin(), items.end(), cq);
}

void CouchKVStore::pendingTasks() {

    if (!pendingFileDeletions.empty()) {
        std::queue<std::string> queue;
        pendingFileDeletions.getAll(queue);

        while (!queue.empty()) {
            std::string filename_str = queue.front();
            int errCode;
#ifdef _MSC_VER
            errCode = _unlink(filename_str.c_str());
#else
            errCode = unlink(filename_str.c_str());
#endif
            if (errCode == -1) {
                LOG(EXTENSION_LOG_WARNING, "Failed to unlink file '%s' with error "
                    "code: %d", filename_str.c_str(), errno);
                if (errno != ENOENT) {
                    pendingFileDeletions.push(filename_str);
                }
            }
            queue.pop();
        }
    }
}

void CouchKVStore::loadDB(shared_ptr<Callback<GetValue> > cb,
                          shared_ptr<Callback<CacheLookup> > cl,
                          shared_ptr<Callback<SeqnoRange> > sr,
                          bool keysOnly, uint16_t vbid,
                          uint64_t startSeqno,
                          couchstore_docinfos_options options) {
    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errorCode = openDB(vbid, rev, &db,
                                          COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errorCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to open database, name=%s/%d.couch.%lu",
            dbname.c_str(), vbid, rev);
        remVBucketFromDbFileMap(vbid);
    } else {
        DbInfo info;
        errorCode = couchstore_db_info(db, &info);
        if (errorCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "Failed to read DB info for backfill");
            closeDatabaseHandle(db);
            abort();
        }
        SeqnoRange range(startSeqno, info.last_sequence);
        sr->callback(range);

        LoadResponseCtx ctx;
        ctx.vbucketId = vbid;
        ctx.keysonly = keysOnly;
        ctx.callback = cb;
        ctx.lookup = cl;
        ctx.stats = &epStats;
        errorCode = couchstore_changes_since(db, startSeqno, options,
                                             recordDbDumpC,
                                             static_cast<void *>(&ctx));
        if (errorCode != COUCHSTORE_SUCCESS) {
            if (errorCode == COUCHSTORE_ERROR_CANCEL) {
                LOG(EXTENSION_LOG_WARNING,
                    "Canceling loading database, warmup has completed\n");
            } else {
                LOG(EXTENSION_LOG_WARNING,
                    "couchstore_changes_since failed, error=%s [%s]",
                    couchstore_strerror(errorCode),
                    couchkvstore_strerrno(db, errorCode).c_str());
                remVBucketFromDbFileMap(vbid);
            }
        }
        closeDatabaseHandle(db);
    }
}

void CouchKVStore::open() {
    // TODO intransaction, is it needed?
    intransaction = false;

    struct stat dbstat;
    bool havedir = false;

    if (stat(dbname.c_str(), &dbstat) == 0 &&
                            (dbstat.st_mode & S_IFDIR) == S_IFDIR) {
        havedir = true;
    }

    if (!havedir) {
        if (mkdir(dbname.c_str(), S_IRWXU) == -1) {
            std::stringstream ss;
            ss << "Warning: Failed to create data directory ["
               << dbname << "]: " << strerror(errno);
            throw std::runtime_error(ss.str());
        }
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
        LOG(EXTENSION_LOG_WARNING, NULL,
            "Warning: cannot update db file map for an invalid vbucket, "
            "vbucket id = %d, rev = %lld\n", vbucketId, newFileRev);
        return;
    }

    dbFileRevMap[vbucketId] = newFileRev;
}

couchstore_error_t CouchKVStore::openDB(uint16_t vbucketId,
                                        uint64_t fileRev,
                                        Db **db,
                                        uint64_t options,
                                        uint64_t *newFileRev) {
    std::string dbFileName = getDBFileName(dbname, vbucketId, fileRev);
    couch_file_ops* ops = &statCollectingFileOps;

    uint64_t newRevNum = fileRev;
    couchstore_error_t errorCode = COUCHSTORE_SUCCESS;

    if (options == COUCHSTORE_OPEN_FLAG_CREATE) {
        // first try to open the requested file without the create option
        // in case it does already exist
        errorCode = couchstore_open_db_ex(dbFileName.c_str(), 0, ops, db);
        if (errorCode != COUCHSTORE_SUCCESS) {
            // open_db failed but still check if the file exists
            newRevNum = checkNewRevNum(dbFileName);
            bool fileExists = (newRevNum) ? true : false;
            if (fileExists) {
                errorCode = openDB_retry(dbFileName, 0, ops, db, &newRevNum);
            } else {
                // requested file doesn't seem to exist, just create one
                errorCode = couchstore_open_db_ex(dbFileName.c_str(), options,
                                                  ops, db);
                if (errorCode == COUCHSTORE_SUCCESS) {
                    newRevNum = 1;
                    updateDbFileMap(vbucketId, fileRev);
                    LOG(EXTENSION_LOG_INFO,
                        "INFO: created new couch db file, name=%s rev=%llu",
                        dbFileName.c_str(), fileRev);
                }
            }
        }
    } else {
        errorCode = openDB_retry(dbFileName, options, ops, db, &newRevNum);
    }

    /* update command statistics */
    st.numOpen++;
    if (errorCode) {
        st.numOpenFailure++;
        LOG(EXTENSION_LOG_WARNING, "Warning: couchstore_open_db failed, name=%s"
            " option=%X rev=%llu error=%s [%s]\n", dbFileName.c_str(), options,
            ((newRevNum > fileRev) ? newRevNum : fileRev),
            couchstore_strerror(errorCode),
            getSystemStrerror().c_str());
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
        LOG(EXTENSION_LOG_INFO, "INFO: couchstore_open_db failed, name=%s "
            "options=%X error=%s [%s], try it again!",
            dbfile.c_str(), options, couchstore_strerror(errCode),
            getSystemStrerror().c_str());
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
                if (remove(old_file.str().c_str()) != 0) {
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: Failed to remove the stale file '%s': %s",
                        old_file.str().c_str(), getSystemStrerror().c_str());
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: Removed stale file '%s'",
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
    uint32_t itemFlags;
    uint64_t cas;
    time_t exptime;
    uint8_t ext_meta[EXT_META_LEN];
    uint8_t ext_len;

    cb_assert(metadata.size >= DEFAULT_META_LEN);
    if (metadata.size == DEFAULT_META_LEN) {
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemFlags, (metadata.buf) + 12, 4);
        ext_len = 0;
    } else {
        //metadata.size => 18, FLEX_META_CODE at offset 16
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemFlags, (metadata.buf) + 12, 4);
        ext_len = metadata.size - DEFAULT_META_LEN - FLEX_DATA_OFFSET;
        memcpy(ext_meta, (metadata.buf) + DEFAULT_META_LEN + FLEX_DATA_OFFSET,
               ext_len);
    }
    cas = ntohll(cas);
    exptime = ntohl(exptime);

    if (metaOnly || (fetchDelete && docinfo->deleted)) {
        Item *it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                            docinfo->size, itemFlags, (time_t)exptime,
                            ext_meta, ext_len, cas, docinfo->db_seq, vbId);
        if (docinfo->deleted) {
            it->setDeleted();
        }
        it->setRevSeqno(docinfo->rev_seq);
        docValue = GetValue(it);
        // update ep-engine IO stats
        ++epStats.io_num_read;
        epStats.io_read_bytes.fetch_add(docinfo->id.size);
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
                cb_assert(doc && (doc->id.size <= UINT16_MAX));
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
                ++epStats.io_num_read;
                epStats.io_read_bytes.fetch_add(docinfo->id.size + valuelen);
            }
            couchstore_free_document(doc);
        }
    }
    return errCode;
}

int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx) {

    LoadResponseCtx *loadCtx = (LoadResponseCtx *)ctx;
    shared_ptr<Callback<GetValue> > cb = loadCtx->callback;
    shared_ptr<Callback<CacheLookup> > cl = loadCtx->lookup;

    Doc *doc = NULL;
    void *valuePtr = NULL;
    size_t valuelen = 0;
    uint64_t byseqno = docinfo->db_seq;
    sized_buf  metadata = docinfo->rev_meta;
    uint16_t vbucketId = loadCtx->vbucketId;
    sized_buf key = docinfo->id;
    uint32_t itemflags;
    uint64_t cas;
    uint32_t exptime;
    uint8_t ext_meta[EXT_META_LEN];
    uint8_t ext_len;

    cb_assert(key.size <= UINT16_MAX);
    cb_assert(metadata.size >= DEFAULT_META_LEN);

    std::string docKey(docinfo->id.buf, docinfo->id.size);
    CacheLookup lookup(docKey, byseqno, vbucketId);
    cl->callback(lookup);
    if (cl->getStatus() == ENGINE_KEY_EEXISTS) {
        return COUCHSTORE_SUCCESS;
    }

    if (metadata.size == DEFAULT_META_LEN) {
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemflags, (metadata.buf) + 12, 4);
        ext_len = 0;
    } else {
        //metadata.size > 16, FLEX_META_CODE at offset 16
        memcpy(&cas, (metadata.buf), 8);
        memcpy(&exptime, (metadata.buf) + 8, 4);
        memcpy(&itemflags, (metadata.buf) + 12, 4);
        ext_len = metadata.size - DEFAULT_META_LEN - FLEX_DATA_OFFSET;
        memcpy(ext_meta, (metadata.buf) + DEFAULT_META_LEN + FLEX_DATA_OFFSET,
               ext_len);
    }
    exptime = ntohl(exptime);
    cas = ntohll(cas);

    if (!loadCtx->keysonly && !docinfo->deleted) {
        couchstore_error_t errCode ;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc,
                                                   DECOMPRESS_DOC_BODIES);

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
            }
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve key value from database "
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


    GetValue rv(it, ENGINE_SUCCESS, -1, loadCtx->keysonly);
    cb->callback(rv);

    couchstore_free_document(doc);

    if (cb->getStatus() == ENGINE_ENOMEM) {
        return COUCHSTORE_ERROR_CANCEL;
    }
    return COUCHSTORE_SUCCESS;
}

bool CouchKVStore::commit2couchstore(Callback<kvstats_ctx> *cb,
                                     uint64_t snapStartSeqno,
                                     uint64_t snapEndSeqno) {
    bool success = true;

    size_t pendingCommitCnt = pendingReqsQ.size();
    if (pendingCommitCnt == 0) {
        return success;
    }

    Doc **docs = new Doc *[pendingCommitCnt];
    DocInfo **docinfos = new DocInfo *[pendingCommitCnt];

    cb_assert(pendingReqsQ[0]);
    uint16_t vbucket2flush = pendingReqsQ[0]->getVBucketId();
    uint64_t fileRev = pendingReqsQ[0]->getRevNum();
    for (size_t i = 0; i < pendingCommitCnt; ++i) {
        CouchRequest *req = pendingReqsQ[i];
        cb_assert(req);
        docs[i] = req->getDbDoc();
        docinfos[i] = req->getDbDocInfo();
        cb_assert(vbucket2flush == req->getVBucketId());
    }

    kvstats_ctx kvctx;
    kvctx.vbucket = vbucket2flush;
    // flush all
    couchstore_error_t errCode = saveDocs(vbucket2flush, fileRev, docs,
                                          docinfos, pendingCommitCnt, kvctx,
                                          snapStartSeqno, snapEndSeqno);
    if (errCode) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: commit failed, cannot save CouchDB docs "
            "for vbucket = %d rev = %llu\n", vbucket2flush, fileRev);
        ++epStats.commitFailed;
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
    cb_assert(ctx);
    kvstats_ctx *cbCtx = static_cast<kvstats_ctx *>(ctx);
    if(docinfo) {
        // An item exists in the VB DB file.
        if (!docinfo->deleted) {
            std::string key(docinfo->id.buf, docinfo->id.size);
            unordered_map<std::string, kstat_entry_t>::iterator itr =
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
                                          size_t docCount, kvstats_ctx &kvctx,
                                          uint64_t snapStartSeqno,
                                          uint64_t snapEndSeqno) {
    couchstore_error_t errCode;
    uint64_t fileRev = rev;
    DbInfo info;
    cb_assert(fileRev);

    Db *db = NULL;
    uint64_t newFileRev;
    errCode = openDB(vbid, fileRev, &db, 0, &newFileRev);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, vbucketId = %d "
                "fileRev = %llu numDocs = %d", vbid, fileRev, docCount);
        return errCode;
    } else {
        uint64_t max = computeMaxDeletedSeqNum(docinfos, docCount);

        vbucket_state *state = cachedVBStates[vbid];
        cb_assert(state);

        // update max_deleted_seq in the local doc (vbstate)
        // before save docs for the given vBucket
        if (max > 0 && state->maxDeletedSeqno < max) {
            state->maxDeletedSeqno = max;
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
                    "Warning: failed to save docs to database, numDocs = %d "
                    "error=%s [%s]\n", docCount, couchstore_strerror(errCode),
                    couchkvstore_strerrno(db, errCode).c_str());
            closeDatabaseHandle(db);
            return errCode;
        }

        state->lastSnapStart = snapStartSeqno;
        state->lastSnapEnd = snapEndSeqno;
        errCode = saveVBState(db, *state);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "Warning: failed to save local docs to "
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
                    "Warning: couchstore_commit failed, error=%s [%s]",
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
            LOG(EXTENSION_LOG_WARNING, "Seqno in db header (%llu) is not matched with "
                "what was persisted (%llu) for vbucket %d", info.last_sequence,
                maxDBSeqno, vbid);
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
        LOG(EXTENSION_LOG_WARNING, NULL,
            "Warning: cannot remove db file map entry for an invalid vbucket, "
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
        ++epStats.io_num_write;
        epStats.io_write_bytes.fetch_add(keySize + dataSize);

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

void CouchKVStore::readVBState(Db *db, uint16_t vbId) {
    sized_buf id;
    LocalDoc *ldoc = NULL;
    couchstore_error_t errCode;

    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    int64_t highSeqno = 0;
    std::string failovers("[{\"id\":0,\"seq\":0}]");
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;

    DbInfo info;
    errCode = couchstore_db_info(db, &info);
    if (errCode == COUCHSTORE_SUCCESS) {
        highSeqno = info.last_sequence;
        purgeSeqno = info.purge_seq;
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to read database info for vBucket = %d", vbId);
        abort();
    }

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    errCode = couchstore_open_local_document(db, (void *)id.buf,
                                             id.size, &ldoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_DEBUG,
            "Warning: failed to retrieve stat info for vBucket=%d error=%s [%s]",
            vbId, couchstore_strerror(errCode),
            couchkvstore_strerrno(db, errCode).c_str());
    } else {
        const std::string statjson(ldoc->json.buf, ldoc->json.size);
        cJSON *jsonObj = cJSON_Parse(statjson.c_str());
        if (!jsonObj) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to parse the vbstat json doc for vbucket %d: %s",
                vbId, statjson.c_str());
            couchstore_free_local_document(ldoc);
            abort();
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
        cJSON *failover_json = cJSON_GetObjectItem(jsonObj, "failover_table");
        if (vb_state.compare("") == 0 || checkpoint_id.compare("") == 0
                || max_deleted_seqno.compare("") == 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: state JSON doc for vbucket %d is in the wrong format: %s",
                vbId, statjson.c_str());
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

            if (failover_json) {
                char* json = cJSON_PrintUnformatted(failover_json);
                failovers.assign(json);
                free(json);
            }
        }
        cJSON_Delete(jsonObj);
        couchstore_free_local_document(ldoc);
    }

    delete cachedVBStates[vbId];
    cachedVBStates[vbId] = new vbucket_state(state, checkpointId,
                                               maxDeletedSeqno, highSeqno,
                                               purgeSeqno, lastSnapStart,
                                               lastSnapEnd, failovers);
}

couchstore_error_t CouchKVStore::saveVBState(Db *db, vbucket_state &vbState) {
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state) << "\""
              << ",\"checkpoint_id\": \"" << vbState.checkpointId << "\""
              << ",\"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno << "\""
              << ",\"failover_table\": " << vbState.failovers
              << ",\"snap_start\": \"" << vbState.lastSnapStart << "\""
              << ",\"snap_end\": \"" << vbState.lastSnapEnd << "\""
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
            "Warning: couchstore_save_local_document failed "
            "error=%s [%s]\n", couchstore_strerror(errCode),
            couchkvstore_strerrno(db, errCode).c_str());
    }
    return errCode;
}

int CouchKVStore::getMultiCb(Db *db, DocInfo *docinfo, void *ctx) {
    cb_assert(docinfo);
    std::string keyStr(docinfo->id.buf, docinfo->id.size);
    cb_assert(ctx);
    GetMultiCbCtx *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    CouchKVStoreStats &st = cbCtx->cks.getCKVStoreStat();


    vb_bgfetch_queue_t::iterator qitr = cbCtx->fetches.find(keyStr);
    if (qitr == cbCtx->fetches.end()) {
        // this could be a serious race condition in couchstore,
        // log a warning message and continue
        LOG(EXTENSION_LOG_WARNING,
            "Warning: couchstore returned invalid docinfo, "
            "no pending bgfetch has been issued for key = %s\n",
            keyStr.c_str());
        return 0;
    }

    bool meta_only = true;
    std::list<VBucketBGFetchItem *> &fetches = (*qitr).second;
    std::list<VBucketBGFetchItem *>::iterator itr = fetches.begin();
    for (; itr != fetches.end(); ++itr) {
        if (!((*itr)->metaDataOnly)) {
            meta_only = false;
            break;
        }
    }

    GetValue returnVal;
    couchstore_error_t errCode = cbCtx->cks.fetchDoc(db, docinfo, returnVal,
                                                     cbCtx->vbId, meta_only);
    if (errCode != COUCHSTORE_SUCCESS && !meta_only) {
        LOG(EXTENSION_LOG_WARNING, "Warning: failed to fetch data from database, "
            "vBucket=%d key=%s error=%s [%s]", cbCtx->vbId,
            keyStr.c_str(), couchstore_strerror(errCode),
            couchkvstore_strerrno(db, errCode).c_str());
        st.numGetFailure++;
    }

    returnVal.setStatus(cbCtx->cks.couchErr2EngineErr(errCode));
    for (itr = fetches.begin(); itr != fetches.end(); ++itr) {
        // populate return value for remaining fetch items with the
        // same seqid
        (*itr)->value = returnVal;
        st.readTimeHisto.add((gethrtime() - (*itr)->initTime) / 1000);
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(returnVal.getValue()->getNKey() +
                                 returnVal.getValue()->getNBytes());
        }
    }
    return 0;
}


void CouchKVStore::closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: couchstore_close_db failed, error=%s [%s]",
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
        // EvetuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
}

size_t CouchKVStore::getEstimatedItemCount(std::vector<uint16_t> &vbs) {
    size_t items = 0;
    std::vector<uint16_t>::iterator it;
    for (it = vbs.begin(); it != vbs.end(); ++it) {
        items += getNumItems(*it);
    }
    return items;
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
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to read database info for "
                "vBucket = %d rev = %llu\n", vbid, rev);
        }
        closeDatabaseHandle(db);
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to open database file for "
            "vBucket = %d rev = %llu\n", vbid, rev);
    }
    return 0;

}

size_t CouchKVStore::getNumItems(uint16_t vbid) {
    size_t docCount = cachedDocCount[vbid];
    if ( docCount != (size_t) -1) {
        return docCount;
    }

    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode == COUCHSTORE_SUCCESS) {
        DbInfo info;
        errCode = couchstore_db_info(db, &info);
        if (errCode == COUCHSTORE_SUCCESS) {
            cachedDocCount[vbid] = info.doc_count;
            closeDatabaseHandle(db);
            return info.doc_count;
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to read database info for "
                "vBucket = %d rev = %llu\n", vbid, rev);
        }
        closeDatabaseHandle(db);
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to open database file for "
            "vBucket = %d rev = %llu\n", vbid, rev);
    }
    return 0;
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
            LOG(EXTENSION_LOG_WARNING, "Failed to get changes count for "
                "vBucket = %d rev = %llu", vbid, rev);
        }
        closeDatabaseHandle(db);
    } else {
        LOG(EXTENSION_LOG_WARNING, "Failed to open database file for vBucket"
            " = %d rev = %llu", vbid, rev);
    }
    return count;
}

RollbackResult CouchKVStore::rollback(uint16_t vbid, uint64_t rollbackSeqno,
                                      shared_ptr<RollbackCB> cb) {

    Db *db = NULL;
    DbInfo info;
    uint64_t fileRev = dbFileRevMap[vbid];
    std::stringstream dbFileName;
    dbFileName << dbname << "/" << vbid << ".couch." << fileRev;
    couchstore_error_t errCode;

    errCode = openDB(vbid, fileRev, &db,
                     (uint64_t) COUCHSTORE_OPEN_FLAG_RDONLY);

    if (errCode == COUCHSTORE_SUCCESS) {
        errCode = couchstore_db_info(db, &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to read DB info, name=%s",
                dbFileName.str().c_str());
            closeDatabaseHandle(db);
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
    errCode = couchstore_changes_count(db, 0, latestSeqno, &totSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Failed to get changes count for "
            "rollback vBucket = %d, rev = %llu", vbid, fileRev);
        closeDatabaseHandle(db);
        return RollbackResult(false, 0, 0, 0);
    }

    Db *newdb = NULL;
    errCode = openDB(vbid, fileRev, &newdb, 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
                "Failed to open database, name=%s",
                dbFileName.str().c_str());
        closeDatabaseHandle(db);
        return RollbackResult(false, 0, 0, 0);
    }

    while (info.last_sequence > rollbackSeqno) {
        errCode = couchstore_rewind_db_header(newdb);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                    "Failed to rewind Db pointer "
                    "for couch file with vbid: %u, whose "
                    "lastSeqno: %llu, while trying to roll back "
                    "to seqNo: %llu", vbid, latestSeqno, rollbackSeqno);
            //Reset the vbucket and send the entire snapshot,
            //as a previous header wasn't found.
            closeDatabaseHandle(db);
            return RollbackResult(false, 0, 0, 0);
        }
        errCode = couchstore_db_info(newdb, &info);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Failed to read DB info, name=%s",
                dbFileName.str().c_str());
            closeDatabaseHandle(db);
            closeDatabaseHandle(newdb);
            return RollbackResult(false, 0, 0, 0);
        }
    }

    //Count from latest seq no to rollback seq no
    uint64_t rollbackSeqCount = 0;
    errCode = couchstore_changes_count(db, info.last_sequence, latestSeqno,
                                       &rollbackSeqCount);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Failed to get changes count for "
            "rollback vBucket = %d, rev = %llu", vbid, fileRev);
        closeDatabaseHandle(db);
        closeDatabaseHandle(newdb);
        return RollbackResult(false, 0, 0, 0);
    }

    if ((totSeqCount / 2) <= rollbackSeqCount) {
        //doresetVbucket flag set or rollback is greater than 50%,
        //reset the vbucket and send the entire snapshot
        closeDatabaseHandle(db);
        closeDatabaseHandle(newdb);
        return RollbackResult(false, 0, 0, 0);
    }

    cb->setDbHeader(newdb);
    shared_ptr<Callback<CacheLookup> > cl(new NoLookupCallback());
    LoadResponseCtx ctx;
    ctx.vbucketId = vbid;
    ctx.keysonly = true;
    ctx.lookup = cl;
    ctx.callback = cb;
    ctx.stats = &epStats;
    errCode = couchstore_changes_since(db, info.last_sequence + 1,
                                       COUCHSTORE_NO_OPTIONS,
                                       recordDbDumpC,
                                       static_cast<void *>(&ctx));
    if (errCode != COUCHSTORE_SUCCESS) {
        if (errCode == COUCHSTORE_ERROR_CANCEL) {
            LOG(EXTENSION_LOG_WARNING,
                "Canceling loading database\n");
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Couchstore_changes_since failed, error=%s [%s]",
                couchstore_strerror(errCode),
                couchkvstore_strerrno(db, errCode).c_str());
        }
        closeDatabaseHandle(db);
        closeDatabaseHandle(newdb);
        return RollbackResult(false, 0, 0, 0);
    }

    readVBState(newdb, vbid);
    cachedDeleteCount[vbid] = info.deleted_count;
    cachedDocCount[vbid] = info.doc_count;

    closeDatabaseHandle(db);
    //Append the rewinded header to the database file, before closing handle
    errCode = couchstore_commit(newdb);
    closeDatabaseHandle(newdb);

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
    (allKeysCtx->cb)->addtoAllKeys(keylen, key);
    if (--(allKeysCtx->count) <= 0) {
        //Only when count met is less than the actual number of entries
        return COUCHSTORE_ERROR_CANCEL;
    }
    return COUCHSTORE_SUCCESS;
}

ENGINE_ERROR_CODE CouchKVStore::getAllKeys(uint16_t vbid,
                                           std::string &start_key,
                                           uint32_t count,
                                           AllKeysCB *cb) {
    Db *db = NULL;
    uint64_t rev = dbFileRevMap[vbid];
    couchstore_error_t errCode = openDB(vbid, rev, &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if(errCode == COUCHSTORE_SUCCESS) {
        sized_buf ref = {NULL, 0};
        ref.buf = (char*) start_key.c_str();
        ref.size = start_key.size();
        AllKeysCtx ctx(cb, count);
        errCode = couchstore_all_docs(db, &ref, COUCHSTORE_NO_OPTIONS,
                                      populateAllKeys,
                                      static_cast<void *>(&ctx));
        closeDatabaseHandle(db);
        if (errCode == COUCHSTORE_SUCCESS ||
                errCode == COUCHSTORE_ERROR_CANCEL)  {
            return ENGINE_SUCCESS;
        } else {
            LOG(EXTENSION_LOG_WARNING, "couchstore_all_docs failed for "
                    "database file of vbucket = %d rev = %llu, errCode = %u\n",
                    vbid, rev, errCode);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "Failed to open database file for "
                "vbucket = %d rev = %llu, errCode = %u\n", vbid, rev, errCode);

    }
    return ENGINE_FAILED;
}

void CouchKVStore::unlinkCouchFile(uint16_t vbucket,
                                   uint64_t fRev) {

    int errCode;
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/%d.couch.%llu",
             dbname.c_str(), vbucket, fRev);
#ifdef _MSC_VER
    errCode = _unlink(fname);
#else
    errCode = unlink(fname);
#endif
    if (errCode == -1) {
        LOG(EXTENSION_LOG_WARNING, "Failed to unlink database file for "
            "vbucket = %d rev = %llu, errCode = %u\n", vbucket, fRev, errno);

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
    removeCompactFile(compact_file);
}

void CouchKVStore::removeCompactFile(const std::string &filename) {

    if (access(filename.c_str(), F_OK) == 0) {
        if (remove(filename.c_str()) == 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: Removed compact file '%s'", filename.c_str());
        }
        else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: Failed to remove compact file '%s': %s",
                filename.c_str(), getSystemStrerror().c_str());

            if (errno != ENOENT) {
                pendingFileDeletions.push(const_cast<std::string &>(filename));
            }
        }
    }
}

/* end of couch-kvstore.cc */
