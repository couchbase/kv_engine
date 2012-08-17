/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>

#include "common.hh"
#include "ep_engine.h"
#include "couch-kvstore/couch-kvstore.hh"
#include "couch-kvstore/dirutils.hh"
#include "tools/cJSON.h"

#define STATWRITER_NAMESPACE couchstore_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE

using namespace CouchKVStoreDirectoryUtilities;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

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

#ifdef WIN32
static std::string getStrError() {
    DWORD err = GetLastError();
    char* msg = NULL;
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                   FORMAT_MESSAGE_FROM_SYSTEM |
                   FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, err,
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                   (LPTSTR) &msg,
                   0, NULL);
    std::string err_msg(msg);
    LocalFree(msg);
    return err_msg;
}
#else
static std::string getStrError() {
    std::string err_msg(strerror(errno));
    return err_msg;
}
#endif

static bool isJSON(const value_t &value)
{
    bool isJSON = false;
    const char *ptr = value->getData();
    size_t len = value->length();
    size_t ii = 0;
    while (ii < len && isspace(*ptr)) {
        ++ptr;
        ++ii;
    }

    std::string data(value->getData(), value->length());
    cJSON *json = cJSON_Parse(data.c_str());
    if (json != 0) {
        isJSON = true;
        cJSON_Delete(json);
    }
    return isJSON;
}

static const std::string getJSONObjString(const cJSON *i)
{
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        abort();
    }
    return i->valuestring;
}

static bool endWithCompact(const std::string &filename)
{
    size_t pos = filename.find(".compact");
    if (pos == std::string::npos ||
            (filename.size()  - sizeof(".compact")) != pos) {
        return false;
    }
    return true;
}

static uint64_t dbFileRev(const std::string &dbname)
{
    char *ptr = NULL;
    size_t secondDot = dbname.rfind(".");
    return strtoull(dbname.substr(secondDot + 1).c_str(), &ptr, 10);
}

static void discoverDbFiles(const std::string &dir, std::vector<std::string> &v)
{
    std::vector<std::string> files = findFilesContaining(dir, ".couch");
    std::vector<std::string>::iterator ii;
    for (ii = files.begin(); ii != files.end(); ++ii) {
        if (!endWithCompact(*ii)) {
            v.push_back(*ii);
        }
    }
}

static uint32_t computeMaxDeletedSeqNum(DocInfo **docinfos, const int numdocs)
{
    uint32_t max = 0;
    uint32_t seqNum;
    for (int idx = 0; idx < numdocs; idx++) {
        if (docinfos[idx]->deleted) {
            // check seq number only from a deleted file
            seqNum = (uint32_t)docinfos[idx]->rev_seq;
            max = std::max(seqNum, max);
        }
    }
    return max;
}

static int getMutationStatus(couchstore_error_t errCode)
{
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return MUTATION_SUCCESS;
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

static bool allDigit(std::string &input)
{
    size_t numchar = input.length();
    for(size_t i = 0; i < numchar; ++i) {
        if (!isdigit(input[i])) {
            return false;
        }
    }
    return true;
}

static std::string couchkvstore_strerrno(couchstore_error_t err) {
    return (err == COUCHSTORE_ERROR_OPEN_FILE ||
            err == COUCHSTORE_ERROR_READ ||
            err == COUCHSTORE_ERROR_WRITE) ? getStrError() : "none";
}

struct WarmupCookie {
    WarmupCookie(KVStore *s, Callback<GetValue>&c) :
        store(s), cb(c), engine(s->getEngine()), loaded(0), skipped(0), error(0)
    { /* EMPTY */ }
    KVStore *store;
    Callback<GetValue> &cb;
    EventuallyPersistentEngine *engine;
    size_t loaded;
    size_t skipped;
    size_t error;
};

static void batchWarmupCallback(uint16_t vbId,
                                std::vector<std::pair<std::string, uint64_t> > &fetches,
                                void *arg)
{
    WarmupCookie *c = static_cast<WarmupCookie *>(arg);
    EventuallyPersistentEngine *engine = c->engine;

    if (engine->stillWarmingUp()) {
        vb_bgfetch_queue_t items2fetch;
        std::vector<std::pair<std::string, uint64_t> >::iterator itm = fetches.begin();
        for (; itm != fetches.end(); itm++) {
            // ignore duplicate Doc seq_id, if any in access log
            if (items2fetch.find((*itm).second) == items2fetch.end()) {
                continue;
            }
            items2fetch[(*itm).second].push_back(
                new VBucketBGFetchItem((*itm).first, (*itm).second, NULL));
        }

        c->store->getMulti(vbId, items2fetch);

        vb_bgfetch_queue_t::iterator items = items2fetch.begin();
        for (; items != items2fetch.end(); items++) {
           VBucketBGFetchItem * fetchedItem = (*items).second.back();
           GetValue &val = fetchedItem->value;
           if (val.getStatus() == ENGINE_SUCCESS) {
               c->loaded++;
               c->cb.callback(val);
           } else {
               getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                "Warning: warmup failed to load data for "
                                "vBucket = %d key = %s error = %X\n", vbId,
                                fetchedItem->key.c_str(), val.getStatus());
               c->error++;

          }
          delete fetchedItem;
        }
    } else {
        c->skipped++;
    }
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
    uint16_t vbucketId;
    bool keysonly;
    EventuallyPersistentEngine *engine;
};

CouchRequest::CouchRequest(const Item &it, uint64_t rev, CouchRequestCallback &cb, bool del) :
    value(it.getValue()), valuelen(it.getNBytes()),
    vbucketId(it.getVBucketId()), fileRevNum(rev),
    key(it.getKey()), deleteItem(del)
{
    bool isjson = false;
    uint64_t cas = htonll(it.getCas());
    uint32_t flags = htonl(it.getFlags());
    uint32_t exptime = htonl(it.getExptime());

    itemId = (it.getId() <= 0) ? 1 : 0;
    dbDoc.id.buf = const_cast<char *>(key.c_str());
    dbDoc.id.size = it.getNKey();
    if (valuelen) {
        isjson = isJSON(value);
        dbDoc.data.buf = const_cast<char *>(value->getData());
        dbDoc.data.size = valuelen;
    } else {
        dbDoc.data.buf = NULL;
        dbDoc.data.size = 0;
    }

    memcpy(meta, &cas, 8);
    memcpy(meta + 8, &exptime, 4);
    memcpy(meta + 12, &flags, 4);
    dbDocInfo.rev_meta.buf = reinterpret_cast<char *>(meta);
    dbDocInfo.rev_meta.size = COUCHSTORE_METADATA_SIZE;
    dbDocInfo.rev_seq = it.getSeqno();
    dbDocInfo.size = dbDoc.data.size;
    if (del) {
        dbDocInfo.deleted =  1;
        callback.delCb = cb.delCb;
    } else {
        dbDocInfo.deleted = 0;
        callback.setCb = cb.setCb;
    }
    dbDocInfo.id = dbDoc.id;
    dbDocInfo.content_meta = isjson ? COUCH_DOC_IS_JSON : COUCH_DOC_NON_JSON_MODE;
    start = gethrtime();
}

CouchKVStore::CouchKVStore(EventuallyPersistentEngine &theEngine,
                           bool read_only) :
    KVStore(read_only), engine(theEngine),
    epStats(theEngine.getEpStats()),
    configuration(theEngine.getConfiguration()),
    dbname(configuration.getDbname()),
    couchNotifier(NULL), pendingCommitCnt(0),
    intransaction(false)
{
    open();
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
    KVStore(copyFrom), engine(copyFrom.engine),
    epStats(copyFrom.epStats),
    configuration(copyFrom.configuration),
    dbname(copyFrom.dbname),
    couchNotifier(NULL),
    pendingCommitCnt(0), intransaction(false)
{
    open();
    dbFileMap = copyFrom.dbFileMap;
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);
}

void CouchKVStore::reset()
{
    assert(!isReadOnly());
    // TODO CouchKVStore::flush() when couchstore api ready
    RememberingCallback<bool> cb;

    couchNotifier->flush(cb);
    cb.waitForValue();

    vbucket_map_t::iterator itor = cachedVBStates.begin();
    for (; itor != cachedVBStates.end(); ++itor) {
        uint16_t vbucket = itor->first;
        itor->second.checkpointId = 0;
        itor->second.maxDeletedSeqno = 0;
        updateDbFileMap(vbucket, 1, true);
    }
}

void CouchKVStore::set(const Item &itm, Callback<mutation_result> &cb)
{
    assert(!isReadOnly());
    assert(intransaction);
    bool deleteItem = false;
    CouchRequestCallback requestcb;
    std::string dbFile;
    uint64_t fileRev = 1;

    requestcb.setCb = &cb;
    if ((getDbFile(itm.getVBucketId(), dbFile))) {
        fileRev = dbFileRev(dbFile);
    } else {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Warning: cannot locate database file %s\n",
                         dbFile.c_str());
    }

    // each req will be de-allocated after commit
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, deleteItem);
    queueItem(req);
}

void CouchKVStore::get(const std::string &key, uint64_t, uint16_t vb,
                       Callback<GetValue> &cb)
{
    hrtime_t start = gethrtime();
    Db *db = NULL;
    std::string dbFile;
    GetValue rv;

    if (!(getDbFile(vb, dbFile))) {
        ++st.numGetFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to retrieve data from vBucketId = %d "
                         "[key=%s], cannot locate database file %s\n",
                         vb, key.c_str(), dbFile.c_str());
        rv.setStatus(ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return;
    }
    couchstore_error_t errCode = openDB(vb, dbFileRev(dbFile), &db, 0, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to open database, name=%s\n",
                         dbFile.c_str());
        rv.setStatus(couchErr2EngineErr(errCode));
        cb.callback(rv);
        return;
    }

    RememberingCallback<GetValue> *rc = dynamic_cast<RememberingCallback<GetValue> *>(&cb);
    bool getMetaOnly = rc && rc->val.isPartial();
    DocInfo *docInfo = NULL;
    sized_buf id;

    id.size = key.size();
    id.buf = const_cast<char *>(key.c_str());
    errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf, id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (!getMetaOnly) {
            // log error only if this is non-xdcr case
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to retrieve doc info from "
                             "database, name=%s key=%s error=%s errno=%s\n",
                             dbFile.c_str(), id.buf,
                             couchstore_strerror(errCode),
                             couchkvstore_strerrno(errCode).c_str());
        }
    } else {
        errCode = fetchDoc(db, docInfo, rv, vb, getMetaOnly);
        if (errCode != COUCHSTORE_SUCCESS) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to retrieve key value from "
                             "database, name=%s key=%s error=%s errno=%s "
                             "deleted=%s\n", dbFile.c_str(), id.buf,
                             couchstore_strerror(errCode),
                             couchkvstore_strerrno(errCode).c_str(),
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
    closeDatabaseHandle(db);
    rv.setStatus(couchErr2EngineErr(errCode));
    cb.callback(rv);
}

void CouchKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms)
{
    Db *db = NULL;
    GetValue returnVal;
    std::string dbFile;
    int numItems = itms.size();
    couchstore_error_t errCode;

    if (!(getDbFile(vb, dbFile))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: cannot locate database file for data fetch, "
                         "vBucketId = %d file = %s numDocs = %d\n",
                          vb, dbFile.c_str(), numItems);
        st.numGetFailure += numItems;

        // populate error return value for each requested item
        vb_bgfetch_queue_t::iterator itr = itms.begin();
        for (; itr != itms.end(); itr++) {
            std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
            std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
            for (; fitr != fetches.end(); fitr++) {
                (*fitr)->value.setStatus(ENGINE_NOT_MY_VBUCKET);
            }
        }
        return;
    }

    errCode = openDB(vb, dbFileRev(dbFile), &db, 0);
    if (errCode != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to open database for data fetch, "
                         "vBucketId = %d file = %s numDocs = %d\n",
                         vb, dbFile.c_str(), numItems);
        st.numGetFailure += numItems;
        vb_bgfetch_queue_t::iterator itr = itms.begin();
        for (; itr != itms.end(); itr++) {
            std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
            std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
            for (; fitr != fetches.end(); fitr++) {
                (*fitr)->value.setStatus(ENGINE_NOT_MY_VBUCKET);
            }
        }
        return;
    }

    std::vector<uint64_t> seqIds;
    VBucketBGFetchItem *item2fetch;
    vb_bgfetch_queue_t::iterator itr = itms.begin();
    for (; itr != itms.end(); itr++) {
        item2fetch = (*itr).second.front();
        seqIds.push_back(item2fetch->value.getId());
    }

    GetMultiCbCtx ctx(*this, vb, itms);
    errCode = couchstore_docinfos_by_sequence(db, &seqIds[0], seqIds.size(),
                                              0, getMultiCbC, &ctx);
    if (errCode != COUCHSTORE_SUCCESS) {
        st.numGetFailure += numItems;
        for (itr = itms.begin(); itr != itms.end(); itr++) {
            std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
            std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
            for (; fitr != fetches.end(); fitr++) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: failed to read database by "
                                 "sequence id = %lld, vBucketId = %d "
                                 "key = %s file = %s error = %s errono=%s\n",
                                 (*fitr)->value.getId(), vb,
                                 (*fitr)->key.c_str(), dbFile.c_str(),
                                 couchstore_strerror(errCode),
                                 couchkvstore_strerrno(errCode).c_str());
                (*fitr)->value.setStatus(couchErr2EngineErr(errCode));
            }
        }
    }
    closeDatabaseHandle(db);
}

void CouchKVStore::del(const Item &itm,
                       uint64_t,
                       Callback<int> &cb)
{
    assert(!isReadOnly());
    assert(intransaction);
    std::string dbFile;

    if (getDbFile(itm.getVBucketId(), dbFile)) {
        CouchRequestCallback requestcb;
        requestcb.delCb = &cb;
        // each req will be de-allocated after commit
        CouchRequest *req = new CouchRequest(itm, dbFileRev(dbFile),
                                             requestcb, true);
        queueItem(req);
    } else {
        ++st.numDelFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to delete data, cannot locate "
                         "database file %s\n",
                         dbFile.c_str());
        int value = DOC_NOT_FOUND;
        cb.callback(value);
    }
}

bool CouchKVStore::delVBucket(uint16_t vbucket)
{
    assert(!isReadOnly());
    assert(couchNotifier);
    RememberingCallback<bool> cb;

    couchNotifier->delVBucket(vbucket, cb);
    cb.waitForValue();

    cachedVBStates.erase(vbucket);
    updateDbFileMap(vbucket, 1, false);
    return cb.val;
}

vbucket_map_t CouchKVStore::listPersistedVbuckets()
{
    std::vector<std::string> files;

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        discoverDbFiles(dbname, files);
        populateFileNameMap(files);
    }

    if (!cachedVBStates.empty()) {
        cachedVBStates.clear();
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    std::map<uint16_t, uint64_t>::iterator itr = dbFileMap.begin();
    for (; itr != dbFileMap.end(); itr++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string fileName = dbname + "/" + vbid.str() + ".couch." +
                rev.str();
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database file, name=%s\n",
                             fileName.c_str());
            remVBucketFromDbFileMap(itr->first);
        } else {
            vbucket_state vb_state;
            uint16_t vbID = itr->first;

            /* read state of VBucket from db file */
            readVBState(db, vbID, vb_state);
            /* insert populated state to the array to return to the caller */
            cachedVBStates[vbID] = vb_state;
            /* update stat */
            ++st.numLoadedVb;
            closeDatabaseHandle(db);
        }
        db = NULL;
    }
    return cachedVBStates;
}

void CouchKVStore::getPersistedStats(std::map<std::string, std::string> &stats)
{
    char *buffer = NULL;
    std::string fname = dbname + "/stats.json";
    std::ifstream session_stats;
    session_stats.exceptions (session_stats.failbit | session_stats.badbit);
    try {
        session_stats.open(fname.c_str(), ios::binary);
        session_stats.seekg (0, ios::end);
        int flen = session_stats.tellg();
        if (flen < 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: error in session stats ifstream!!!\n");
            session_stats.close();
            return;
        }
        session_stats.seekg (0, ios::beg);
        buffer = new char[flen + 1];
        session_stats.read(buffer, flen);
        session_stats.close();
        buffer[flen] = '\0';

        cJSON *json_obj = cJSON_Parse(buffer);
        if (!json_obj) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to parse the session stats json doc!!!\n");
            delete[] buffer;
            return;
        }

        int json_arr_size = cJSON_GetArraySize(json_obj);
        for (int i = 0; i < json_arr_size; ++i) {
            cJSON *obj = cJSON_GetArrayItem(json_obj, i);
            if (obj) {
                stats[obj->string] = obj->valuestring;
            }
        }
        cJSON_Delete(json_obj);

    } catch (const std::ifstream::failure& e) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to load the engine session stats "
                         " due to IO exception \"%s\"", e.what());
    }

    delete[] buffer;
}

bool CouchKVStore::snapshotVBuckets(const vbucket_map_t &m)
{
    assert(!isReadOnly());
    vbucket_map_t::const_reverse_iterator iter;
    bool success = true;

    for (iter = m.rbegin(); iter != m.rend(); ++iter) {
        uint16_t vbucketId = iter->first;
        vbucket_state vbstate = iter->second;
        vbucket_map_t::iterator it =
            cachedVBStates.find(vbucketId);
        bool state_changed = true;
        if (it != cachedVBStates.end()) {
            if (it->second.state == vbstate.state &&
                it->second.checkpointId == vbstate.checkpointId) {
                continue; // no changes
            } else {
                if (it->second.state == vbstate.state) {
                    state_changed = false;
                }
                it->second.state = vbstate.state;
                it->second.checkpointId = vbstate.checkpointId;
                // Note that the max deleted seq number is maintained within CouchKVStore
                vbstate.maxDeletedSeqno = it->second.maxDeletedSeqno;
            }
        } else {
            cachedVBStates[vbucketId] = vbstate;
        }

        success = setVBucketState(vbucketId, vbstate, state_changed, false);
        if (!success) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to set new state, %s, for vbucket %d\n",
                             VBucket::toString(vbstate.state), vbucketId);
            break;
        }
    }
    return success;
}

bool CouchKVStore::snapshotStats(const std::map<std::string, std::string> &stats)
{
    assert(!isReadOnly());
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
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to log the engine stats due to IO exception "
                         "\"%s\"", e.what());
        rv = false;
    }

    if (rv) {
        std::string old_fname = dbname + "/stats.json.old";
        std::string stats_fname = dbname + "/stats.json";
        if (access(old_fname.c_str(), F_OK) == 0 && remove(old_fname.c_str()) != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "FATAL: Failed to remove '%s': %s",
                             old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (access(stats_fname.c_str(), F_OK) == 0 &&
                   rename(stats_fname.c_str(), old_fname.c_str()) != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to rename '%s' to '%s': %s",
                             stats_fname.c_str(), old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (rename(next_fname.c_str(), stats_fname.c_str()) != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to rename '%s' to '%s': %s",
                             next_fname.c_str(), stats_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        }
    }

    return rv;
}

bool CouchKVStore::setVBucketState(uint16_t vbucketId, vbucket_state vbstate,
                                   bool stateChanged, bool newfile)
{
    Db *db = NULL;
    uint64_t fileRev, newFileRev;
    std::stringstream id;
    std::string dbFileName;
    std::map<uint16_t, uint64_t>::iterator mapItr;

    id << vbucketId;
    dbFileName = dbname + "/" + id.str() + ".couch";

    if (newfile) {
        fileRev = 1; // create a db file with rev = 1
    } else {
        mapItr = dbFileMap.find(vbucketId);
        if (mapItr == dbFileMap.end()) {
            uint64_t rev = checkNewRevNum(dbFileName, true);
            if (rev == 0) {
                fileRev = 1;
                newfile = true;
            } else {
                fileRev = rev;
            }
        } else {
            fileRev = mapItr->second;
        }
    }

    couchstore_error_t errorCode;
    bool retry = true;
    while (retry) {
        retry = false;
        errorCode = openDB(vbucketId, fileRev, &db,
                           (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE, &newFileRev);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream filename;
            filename << dbname << "/" << vbucketId << ".couch." << fileRev;
            ++st.numVbSetFailure;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database, name=%s\n",
                             filename.str().c_str());
            return false;
        }
        fileRev = newFileRev;

        errorCode = saveVBState(db, vbstate);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "Warning: failed to save local doc, name=%s\n",
                            dbFileName.c_str());
            closeDatabaseHandle(db);
            return false;
        }

        errorCode = couchstore_commit(db);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: commit failed, vbid=%u rev=%llu "
                             "error=%s errno=%s",
                             vbucketId, fileRev, couchstore_strerror(errorCode),
                             couchkvstore_strerrno(errorCode).c_str());
            closeDatabaseHandle(db);
            return false;
        } else {
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            RememberingCallback<uint16_t> lcb;

            couchNotifier->notify_update(vbucketId, fileRev, newHeaderPos,
                                         stateChanged, vbstate.state,
                                         vbstate.checkpointId, lcb);
            if (lcb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (lcb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Retry notify CouchDB of update, "
                                     "vbid=%u rev=%llu\n", vbucketId, fileRev);
                    retry = true;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warning: failed to notify CouchDB of update, "
                                     "vbid=%u rev=%llu error=0x%x\n",
                                     vbucketId, fileRev, lcb.val);
                    abort();
                }
            }
        }
        closeDatabaseHandle(db);
    }

    return true;
}


void CouchKVStore::dump(shared_ptr<Callback<GetValue> > cb)
{
    loadDB(cb, false, NULL, COUCHSTORE_NO_DELETES);
}

void CouchKVStore::dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb)
{
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    loadDB(cb, false, &vbids);
}

void CouchKVStore::dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb)
{
    (void)vbids;
    loadDB(cb, true, NULL, COUCHSTORE_NO_DELETES);
}

void CouchKVStore::dumpDeleted(uint16_t vb,  shared_ptr<Callback<GetValue> > cb)
{
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    loadDB(cb, true, &vbids, COUCHSTORE_DELETES_ONLY);
}

StorageProperties CouchKVStore::getStorageProperties()
{
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true,
                         true, true);
    return rv;
}

bool CouchKVStore::commit(void)
{
    assert(!isReadOnly());
    // TODO get rid of bogus intransaction business
    assert(intransaction);
    intransaction = commit2couchstore() ? false : true;
    return !intransaction;

}

void CouchKVStore::addStats(const std::string &prefix,
                            ADD_STAT add_stat,
                            const void *c)
{
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

    if (prefix.compare("rw") == 0) {
        addStat(prefix_str, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix_str, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix_str, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix_str, "lastCommDocs",  st.docsCommitted,   add_stat, c);
        addStat(prefix_str, "numCommitRetry", st.numCommitRetry, add_stat, c);
    }
}

void CouchKVStore::addTimingStats(const std::string &prefix,
                                  ADD_STAT add_stat, const void *c) {
    if (prefix.compare("rw") != 0) {
        return;
    }
    const char *prefix_str = prefix.c_str();
    addStat(prefix_str, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix_str, "commitRetry", st.commitRetryHisto, add_stat, c);
    addStat(prefix_str, "delete",      st.delTimeHisto,     add_stat, c);
    addStat(prefix_str, "save_documents", st.saveDocsHisto, add_stat, c);
    addStat(prefix_str, "writeTime",   st.writeTimeHisto,   add_stat, c);
    addStat(prefix_str, "writeSize",   st.writeSizeHisto,   add_stat, c);

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
                           ADD_STAT add_stat, const void *c)
{
    std::stringstream fullstat;
    fullstat << prefix << ":" << stat;
    add_casted_stat(fullstat.str().c_str(), val, add_stat, c);
}

void CouchKVStore::optimizeWrites(std::vector<queued_item> &items)
{
    assert(!isReadOnly());
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsByVBAndKey cq;
    std::sort(items.begin(), items.end(), cq);
}

void CouchKVStore::loadDB(shared_ptr<Callback<GetValue> > cb, bool keysOnly,
                          std::vector<uint16_t> *vbids,
                          couchstore_docinfos_options options)
{
    std::vector<std::string> files;
    std::map<uint16_t, uint64_t> &filemap = dbFileMap;
    std::map<uint16_t, uint64_t> vbmap;
    std::vector< std::pair<uint16_t, uint64_t> > vbuckets;
    std::vector< std::pair<uint16_t, uint64_t> > replicaVbuckets;
    bool loadingData = !vbids && !keysOnly;

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        discoverDbFiles(dbname, files);
        populateFileNameMap(files);
    }

    if (vbids) {
        // get entries for given vbucket(s) from dbFileMap
        std::string dirname = dbname;
        getFileNameMap(vbids, dirname, vbmap);
        filemap = vbmap;
    }

    // order vbuckets data loading by using vbucket states
    if (loadingData && cachedVBStates.empty()) {
        listPersistedVbuckets();
    }

    std::map<uint16_t, uint64_t>::iterator fitr = filemap.begin();
    for (; fitr != filemap.end(); fitr++) {
        if (loadingData) {
            vbucket_map_t::const_iterator vsit = cachedVBStates.find(fitr->first);
            if (vsit != cachedVBStates.end()) {
                vbucket_state vbs = vsit->second;
                // ignore loading dead vbuckets during warmup
                if (vbs.state == vbucket_state_active) {
                    vbuckets.push_back(make_pair(fitr->first, fitr->second));
                } else if (vbs.state == vbucket_state_replica) {
                    replicaVbuckets.push_back(make_pair(fitr->first, fitr->second));
                }
            }
        } else {
            vbuckets.push_back(make_pair(fitr->first, fitr->second));
        }
    }

    if (loadingData) {
        std::vector< std::pair<uint16_t, uint64_t> >::iterator it = replicaVbuckets.begin();
        for (; it != replicaVbuckets.end(); it++) {
            vbuckets.push_back(*it);
        }
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    int keyNum = 0;
    std::vector< std::pair<uint16_t, uint64_t> >::iterator itr = vbuckets.begin();
    for (; itr != vbuckets.end(); itr++, keyNum++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string dbName = dbname + "/" + vbid.str() + ".couch." +
                                 rev.str();
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database, name=%s\n",
                             dbName.c_str());
            remVBucketFromDbFileMap(itr->first);
        } else {
            LoadResponseCtx ctx;
            ctx.vbucketId = itr->first;
            ctx.keysonly = keysOnly;
            ctx.callback = cb;
            ctx.engine = &engine;
            errorCode = couchstore_changes_since(db, 0, options, recordDbDumpC,
                                                 static_cast<void *>(&ctx));
            if (errorCode != COUCHSTORE_SUCCESS) {
                if (errorCode == COUCHSTORE_ERROR_CANCEL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "Canceling loading database, warmup has completed\n");
                    closeDatabaseHandle(db);
                    break;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warning: couchstore_changes_since failed, "
                                     "error=%s errno=%s\n",
                                     couchstore_strerror(errorCode),
                                     couchkvstore_strerrno(errorCode).c_str());
                    remVBucketFromDbFileMap(itr->first);
                }
            }
            closeDatabaseHandle(db);
        }
        db = NULL;
    }
}

void CouchKVStore::open()
{
    // TODO intransaction, is it needed?
    intransaction = false;
    if (!isReadOnly()) {
        delete couchNotifier;
        couchNotifier = new CouchNotifier(&engine, configuration);
    }

    struct stat dbstat;
    bool havedir = false;

    if (stat(dbname.c_str(), &dbstat) == 0 && (dbstat.st_mode & S_IFDIR) == S_IFDIR) {
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

void CouchKVStore::close()
{
    intransaction = false;
    if (!isReadOnly()) {
        delete couchNotifier;
    }
    couchNotifier = NULL;
}

bool CouchKVStore::getDbFile(uint16_t vbucketId, std::string &dbFileName)
{
    std::stringstream fileName;
    fileName << dbname << "/" << vbucketId << ".couch";
    std::map<uint16_t, uint64_t>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr == dbFileMap.end()) {
        dbFileName = fileName.str();
        uint64_t rev = checkNewRevNum(dbFileName, true);
        if (rev > 0) {
            updateDbFileMap(vbucketId, rev, true);
            fileName << "." << rev;
        } else {
            return false;
        }
    } else {
        fileName  <<  "." << itr->second;
    }

    dbFileName = fileName.str();
    return true;
}

uint64_t CouchKVStore::checkNewRevNum(std::string &dbFileName, bool newFile)
{
    using namespace CouchKVStoreDirectoryUtilities;
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

void CouchKVStore::updateDbFileMap(uint16_t vbucketId, uint64_t newFileRev,
                                   bool insertImmediately)
{
    if (insertImmediately) {
        dbFileMap.insert(std::pair<uint16_t, uint64_t>(vbucketId, newFileRev));
        return;
    }

    std::map<uint16_t, uint64_t>::iterator itr;
    itr = dbFileMap.find(vbucketId);
    if (itr != dbFileMap.end()) {
        itr->second = newFileRev;
    } else {
        dbFileMap.insert(std::pair<uint16_t, uint64_t>(vbucketId, newFileRev));
    }
}

static std::string getDBFileName(const std::string &dbname, uint16_t vbid)
{
    std::stringstream ss;
    ss << dbname << "/" << vbid << ".couch";
    return ss.str();
}

static std::string getDBFileName(const std::string &dbname,
                                 uint16_t vbid,
                                 uint64_t rev)
{
    std::stringstream ss;
    ss << dbname << "/" << vbid << ".couch." << rev;
    return ss.str();
}

couchstore_error_t CouchKVStore::openDB(uint16_t vbucketId,
                                        uint64_t fileRev,
                                        Db **db,
                                        uint64_t options,
                                        uint64_t *newFileRev)
{
    std::string dbFileName = getDBFileName(dbname, vbucketId, fileRev);
    couch_file_ops* ops = &statCollectingFileOps;

    uint64_t newRevNum = fileRev;
    // first try to open database without options, we don't want to create
    // a duplicate db that has the same name with different revision number
    couchstore_error_t errorCode = couchstore_open_db_ex(dbFileName.c_str(), 0, ops, db);
    if (errorCode != COUCHSTORE_SUCCESS) {
        newRevNum = checkNewRevNum(dbFileName);
        if (newRevNum > 0) {
            errorCode = couchstore_open_db_ex(dbFileName.c_str(), 0, ops, db);
            if (errorCode == COUCHSTORE_SUCCESS) {
                updateDbFileMap(vbucketId, newRevNum);
            }
        } else {
            if (options) {
                newRevNum = fileRev;
                errorCode = couchstore_open_db_ex(dbFileName.c_str(), options, ops, db);
                if (errorCode == COUCHSTORE_SUCCESS) {
                    updateDbFileMap(vbucketId, newRevNum, true);
                }
            }
        }
    }

    if (newFileRev != NULL) {
        *newFileRev = newRevNum;
    }

    if (errorCode) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Warning: couchstore_open_db failed, name=%s error=%s errno=%s\n",
                dbFileName.c_str(), couchstore_strerror(errorCode),
                         couchkvstore_strerrno(errorCode).c_str());
    }

    /* update command statistics */
    st.numOpen++;
    if (errorCode) {
        st.numOpenFailure++;
    }

    return errorCode;
}

void CouchKVStore::getFileNameMap(std::vector<uint16_t> *vbids,
                                  std::string &dirname,
                                  std::map<uint16_t, uint64_t> &filemap)
{
    std::vector<uint16_t>::iterator vbidItr;

    for (vbidItr = vbids->begin(); vbidItr != vbids->end(); vbidItr++) {
        std::string dbFileName = getDBFileName(dirname, *vbidItr);
        std::map<uint16_t, uint64_t>::iterator dbFileItr = dbFileMap.find(*vbidItr);
        if (dbFileItr == dbFileMap.end()) {
            uint64_t rev = checkNewRevNum(dbFileName, true);
            if (rev > 0) {
                filemap.insert(std::pair<uint16_t, uint64_t>(*vbidItr, rev));
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: database file does not exist, %s\n",
                                 dbFileName.c_str());
            }
        } else {
            filemap.insert(std::pair<uint16_t, uint64_t>(dbFileItr->first,
                                                         dbFileItr->second));
        }
    }
}

void CouchKVStore::populateFileNameMap(std::vector<std::string> &filenames)
{
    std::vector<std::string>::iterator fileItr;

    for (fileItr = filenames.begin(); fileItr != filenames.end(); fileItr++) {
        const std::string &filename = *fileItr;
        size_t secondDot = filename.rfind(".");
        std::string nameKey = filename.substr(0, secondDot);
        size_t firstDot = nameKey.rfind(".");
        size_t firstSlash = nameKey.rfind("/");

        std::string revNumStr = filename.substr(secondDot + 1);
        char *ptr = NULL;
        uint64_t revNum = strtoull(revNumStr.c_str(), &ptr, 10);

        std::string vbIdStr = nameKey.substr(firstSlash + 1, (firstDot - firstSlash) - 1);
        if (allDigit(vbIdStr)) {
            int vbId = atoi(vbIdStr.c_str());
            std::map<uint16_t, uint64_t>::iterator mapItr = dbFileMap.find(vbId);
            if (mapItr == dbFileMap.end()) {
                dbFileMap.insert(std::pair<uint16_t, uint64_t>(vbId, revNum));
            } else {
                // duplicate key
                if (mapItr->second < revNum) {
                    mapItr->second = revNum;
                }
            }
        } else {
            // skip non-vbucket database file, master.couch etc
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Non-vbucket database file, %s, skip adding "
                             "to CouchKVStore dbFileMap\n",
                             filename.c_str());
        }
    }
}

couchstore_error_t CouchKVStore::fetchDoc(Db *db, DocInfo *docinfo,
                                          GetValue &docValue, uint16_t vbId,
                                          bool metaOnly)
{
    assert(docinfo && db);
    uint32_t itemFlags;
    void *valuePtr = NULL;
    size_t valuelen = 0;
    uint64_t cas;
    time_t exptime;
    sized_buf metadata;

    Item *it;
    Doc *doc = NULL;
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;

    metadata = docinfo->rev_meta;
    assert(metadata.size == 16);
    memcpy(&cas, (metadata.buf), 8);
    cas = ntohll(cas);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    exptime = ntohl(exptime);
    memcpy(&itemFlags, (metadata.buf) + 12, 4);
    itemFlags = ntohl(itemFlags);

    if (metaOnly) {
        // we should allow a metadata disk fetch for deleted items only
        assert(docinfo->deleted);
        it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                      docinfo->size, itemFlags, (time_t)exptime, cas);
        it->setSeqno(docinfo->rev_seq);
        docValue = GetValue(it);

        // update ep-engine IO stats
        ++epStats.io_num_read;
        epStats.io_read_bytes += docinfo->id.size;
    } else {
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, 0);
        if (errCode == COUCHSTORE_SUCCESS) {
            if (docinfo->deleted) {
                // cannot read a doc that is marked deleted, just return
                // document not found error code
                errCode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
            } else {
                assert(doc && (doc->id.size <= UINT16_MAX));
                valuelen = doc->data.size;
                valuePtr = doc->data.buf;
                it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                        itemFlags, (time_t)exptime, valuePtr, valuelen,
                        cas, -1, vbId);
                docValue = GetValue(it);
                couchstore_free_document(doc);

                // update ep-engine IO stats
                ++epStats.io_num_read;
                epStats.io_read_bytes += docinfo->id.size + valuelen;
            }
        }
    }
    return errCode;
}
int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx)
{
    Item *it = NULL;
    Doc *doc = NULL;
    LoadResponseCtx *loadCtx = (LoadResponseCtx *)ctx;
    shared_ptr<Callback<GetValue> > cb = loadCtx->callback;
    EventuallyPersistentEngine *engine= loadCtx->engine;
    bool warmup = engine->stillWarmingUp();

    // TODO enable below when couchstore is ready
    // bool compressed = (docinfo->content_meta & 0x80);
    // uint8_t metaflags = (docinfo->content_meta & ~0x80);

    void *valuePtr;
    size_t valuelen;
    sized_buf  metadata = docinfo->rev_meta;
    uint32_t itemflags;
    uint16_t vbucketId = loadCtx->vbucketId;
    sized_buf key = docinfo->id;
    uint64_t cas;
    uint32_t exptime;

    assert(key.size <= UINT16_MAX);
    assert(metadata.size == 16);

    memcpy(&cas, metadata.buf, 8);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    memcpy(&itemflags, (metadata.buf) + 12, 4);
    itemflags = ntohl(itemflags);
    exptime = ntohl(exptime);
    cas = ntohll(cas);

    valuePtr = NULL;
    valuelen = 0;

    if (!loadCtx->keysonly && !docinfo->deleted) {
        couchstore_error_t errCode ;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, 0);

        if (errCode == COUCHSTORE_SUCCESS) {
            if (doc->data.size) {
                valuelen = doc->data.size;
                valuePtr = doc->data.buf;
            }
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to retrieve key value from database "
                             "database, vBucket=%d key=%s error=%s errno=%s\n",
                             vbucketId, key.buf, couchstore_strerror(errCode),
                             couchkvstore_strerrno(errCode).c_str());
            return 0;
        }
    }

    it = new Item((void *)key.buf,
                  key.size,
                  itemflags,
                  (time_t)exptime,
                  valuePtr, valuelen,
                  cas,
                  docinfo->db_seq, // return seq number being persisted on disk
                  vbucketId,
                  docinfo->rev_seq);

    GetValue rv(it, ENGINE_SUCCESS, -1, loadCtx->keysonly);
    cb->callback(rv);

    couchstore_free_document(doc);

    int returnCode = COUCHSTORE_SUCCESS;
    if (warmup) {
        if (!engine->stillWarmingUp()) {
            // warmup has completed, return COUCHSTORE_ERROR_CANCEL to
            // cancel remaining data dumps from couchstore
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Engine warmup is complete, request to stop "
                             "loading remaining database\n");
            returnCode = COUCHSTORE_ERROR_CANCEL;
        }
    }
    return returnCode;
}

bool CouchKVStore::commit2couchstore(void)
{
    bool success = true;

    if (pendingCommitCnt == 0) {
        return success;
    }

    CouchRequest **committedReqs = new CouchRequest *[pendingCommitCnt];
    Doc **docs = new Doc *[pendingCommitCnt];
    DocInfo **docinfos = new DocInfo *[pendingCommitCnt];

    assert(pendingReqsQ[0]);
    uint16_t vbucket2flush = pendingReqsQ[0]->getVBucketId();
    uint64_t fileRev = pendingReqsQ[0]->getRevNum();
    int reqIndex = 0;
    for (; pendingCommitCnt > 0; ++reqIndex, --pendingCommitCnt) {
        CouchRequest *req = pendingReqsQ[reqIndex];
        assert(req);
        committedReqs[reqIndex] = req;
        docs[reqIndex] = req->getDbDoc();
        docinfos[reqIndex] = req->getDbDocInfo();
        assert(vbucket2flush == req->getVBucketId());
    }

    // flush all
    couchstore_error_t errCode = saveDocs(vbucket2flush, fileRev, docs, docinfos, reqIndex);
    if (errCode) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Warning: commit failed, cannot save CouchDB docs "
                "for vbucket = %d rev = %llu\n", vbucket2flush, fileRev);
        ++epStats.commitFailed;
    }
    commitCallback(committedReqs, reqIndex, errCode);

    // clean up
    pendingReqsQ.clear();
    while (reqIndex--) {
        delete committedReqs[reqIndex];
    }
    delete [] committedReqs;
    delete [] docs;
    delete [] docinfos;
    return success;
}

couchstore_error_t CouchKVStore::saveDocs(uint16_t vbid, uint64_t rev, Doc **docs,
                                          DocInfo **docinfos, int docCount)
{
    couchstore_error_t errCode;
    bool retry_save_docs = false;
    bool retried = false;
    hrtime_t retry_begin = 0;
    uint64_t fileRev = rev;
    assert(fileRev);

    do {
        Db *db = NULL;
        uint64_t newFileRev;
        retry_save_docs = false;
        errCode = openDB(vbid, fileRev, &db,
                         (uint64_t)COUCHSTORE_OPEN_FLAG_CREATE, &newFileRev);
        if (errCode != COUCHSTORE_SUCCESS) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database, vbucketId = %d "
                             "fileRev = %llu\n", vbid, fileRev);
            return errCode;
        } else {
            uint32_t max = computeMaxDeletedSeqNum(docinfos, docCount);

            // update max_deleted_seq in the local doc (vbstate)
            // before save docs for the given vBucket
            if (max > 0) {
                vbucket_map_t::iterator it =
                    cachedVBStates.find(vbid);
                if (it != cachedVBStates.end() && it->second.maxDeletedSeqno < max) {
                    it->second.maxDeletedSeqno = max;
                    errCode = saveVBState(db, it->second);
                    if (errCode != COUCHSTORE_SUCCESS) {
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "Warning: failed to save local doc for, "
                                         "vBucket = %d\n", vbid);
                        closeDatabaseHandle(db);
                        return errCode;
                    }
                }
            }

            hrtime_t cs_begin = gethrtime();
            errCode = couchstore_save_documents(db, docs, docinfos, docCount,
                                                0 /* no options */);
            st.saveDocsHisto.add((gethrtime() - cs_begin) / 1000);
            if (errCode != COUCHSTORE_SUCCESS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Warning: failed to save docs to database, error=%s errno=%s\n",
                    couchstore_strerror(errCode), couchkvstore_strerrno(errCode).c_str());
                closeDatabaseHandle(db);
                return errCode;
            }

            cs_begin = gethrtime();
            errCode = couchstore_commit(db);
            st.commitHisto.add((gethrtime() - cs_begin) / 1000);
            if (errCode) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Warning: couchstore_commit failed, error=%s errno=%s\n",
                    couchstore_strerror(errCode), couchkvstore_strerrno(errCode).c_str());
                closeDatabaseHandle(db);
                return errCode;
            }

            RememberingCallback<uint16_t> cb;
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            couchNotifier->notify_headerpos_update(vbid, newFileRev, newHeaderPos, cb);
            if (cb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (cb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                   "Retry notify CouchDB of update, vbucket=%d rev=%llu\n",
                                     vbid, newFileRev);
                    fileRev = newFileRev;
                    retry_save_docs = true;
                    ++st.numCommitRetry;
                    if (!retried) {
                        retry_begin = gethrtime();
                        retried = true;
                    }
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warning: failed to notify CouchDB of "
                                     "update for vbucket=%d, error=0x%x\n",
                                     vbid, cb.val);
                    abort();
                }
            }
            closeDatabaseHandle(db);
        }
    } while (retry_save_docs);

    /* update stat */
    if(errCode == COUCHSTORE_SUCCESS) {
        st.docsCommitted = docCount;
    }
    if (retried) {
        st.commitRetryHisto.add((gethrtime() - retry_begin) / 1000);
    }

    return errCode;
}

void CouchKVStore::queueItem(CouchRequest *req)
{
    if (pendingCommitCnt &&
        pendingReqsQ.front()->getVBucketId() != req->getVBucketId()) {
        // got new request for a different vb, commit pending
        // pending requests of the current vb firt
        commit2couchstore();
    }
    pendingReqsQ.push_back(req);
    pendingCommitCnt++;
}

void CouchKVStore::remVBucketFromDbFileMap(uint16_t vbucketId)
{
    std::map<uint16_t, uint64_t>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr != dbFileMap.end()) {
        dbFileMap.erase(itr);
    }
}

void CouchKVStore::commitCallback(CouchRequest **committedReqs, int numReqs,
                                  couchstore_error_t errCode)
{
    for (int index = 0; index < numReqs; index++) {
        size_t dataSize = committedReqs[index]->getNBytes();
        size_t keySize = committedReqs[index]->getKey().length();
        /* update ep stats */
        ++epStats.io_num_write;
        epStats.io_write_bytes += keySize + dataSize;

        if (committedReqs[index]->isDelete()) {
            int rv = getMutationStatus(errCode);
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta() / 1000);
            }
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            int rv = getMutationStatus(errCode);
            int64_t newItemId = committedReqs[index]->getDbDocInfo()->db_seq;
            if (errCode) {
                ++st.numSetFailure;
                newItemId = 0;
            } else {
                st.writeTimeHisto.add(committedReqs[index]->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + keySize);
            }
            mutation_result p(rv, newItemId);
            committedReqs[index]->getSetCallback()->callback(p);
        }
    }
}

void CouchKVStore::readVBState(Db *db, uint16_t vbId, vbucket_state &vbState)
{
    sized_buf id;
    LocalDoc *ldoc = NULL;
    couchstore_error_t errCode;

    vbState.state = vbucket_state_dead;
    vbState.checkpointId = 0;
    vbState.maxDeletedSeqno = 0;

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    errCode = couchstore_open_local_document(db, (void *)id.buf,
                                             id.size, &ldoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Warning: failed to retrieve stat info for vBucket=%d "
                         "error=%s errno=%s\n",
                         vbId, couchstore_strerror(errCode),
                         couchkvstore_strerrno(errCode).c_str());
    } else {
        const std::string statjson(ldoc->json.buf, ldoc->json.size);
        cJSON *jsonObj = cJSON_Parse(statjson.c_str());
        const std::string state = getJSONObjString(cJSON_GetObjectItem(jsonObj, "state"));
        const std::string checkpoint_id = getJSONObjString(cJSON_GetObjectItem(jsonObj,
                                                                               "checkpoint_id"));
        const std::string max_deleted_seqno = getJSONObjString(cJSON_GetObjectItem(jsonObj,
                                                                                   "max_deleted_seqno"));
        if (state.compare("") == 0 || checkpoint_id.compare("") == 0
                || max_deleted_seqno.compare("") == 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: state JSON doc for vbucket %d is in the wrong format: %s\n",
                             vbId, statjson.c_str());
        } else {
            vbState.state = VBucket::fromString(state.c_str());
            parseUint32(max_deleted_seqno.c_str(), &vbState.maxDeletedSeqno);
            parseUint64(checkpoint_id.c_str(), &vbState.checkpointId);
        }
        cJSON_Delete(jsonObj);
        couchstore_free_local_document(ldoc);
    }
}

couchstore_error_t CouchKVStore::saveVBState(Db *db, vbucket_state &vbState)
{
    LocalDoc lDoc;
    std::stringstream jsonState;
    std::string state;
    couchstore_error_t errCode;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state)
              << "\", \"checkpoint_id\": \"" << vbState.checkpointId
              << "\", \"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno
              << "\"}";

    lDoc.id.buf = (char *)"_local/vbstate";
    lDoc.id.size = sizeof("_local/vbstate") - 1;
    state = jsonState.str();
    lDoc.json.buf = (char *)state.c_str();
    lDoc.json.size = state.size();
    lDoc.deleted = 0;

    errCode = couchstore_save_local_document(db, &lDoc);
    if (errCode != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: couchstore_save_local_document failed "
                         "error=%s errno=%s\n", couchstore_strerror(errCode),
                         couchkvstore_strerrno(errCode).c_str());
    }
    return errCode;
}

int CouchKVStore::getMultiCb(Db *db, DocInfo *docinfo, void *ctx)
{
    couchstore_error_t errCode;
    std::string keyStr(docinfo->id.buf, docinfo->id.size);
    assert(ctx);
    GetMultiCbCtx *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    CouchKVStoreStats &st = cbCtx->cks.getCKVStoreStat();


    vb_bgfetch_queue_t::iterator qitr;
    qitr = cbCtx->fetches.find(docinfo->db_seq);
    assert(qitr != cbCtx->fetches.end());
    std::list<VBucketBGFetchItem *> &fetches = (*qitr).second;
    GetValue returnVal;

    errCode = cbCtx->cks.fetchDoc(db, docinfo, returnVal, cbCtx->vbId, false);
    if (errCode != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Warning: failed to fetch data from database, "
                "vBucket=%d key=%s error=%s errno=%s\n", cbCtx->vbId,
                keyStr.c_str(), couchstore_strerror(errCode),
                         couchkvstore_strerrno(errCode).c_str());
        st.numGetFailure++;
    }

    returnVal.setStatus(cbCtx->cks.couchErr2EngineErr(errCode));
    std::list<VBucketBGFetchItem *>::iterator itr = fetches.begin();
    for (; itr != fetches.end(); itr++) {
        // populate return value for remaining fetch items with the
        // same seqid
        (*itr)->value = returnVal;
        st.readTimeHisto.add((gethrtime() - (*itr)->initTime) / 1000);
        if (errCode == COUCHSTORE_SUCCESS) {
            st.readSizeHisto.add(returnVal.getValue()->getKey().length() +
                                 returnVal.getValue()->getNBytes());
        }
    }
    return 0;
}


void CouchKVStore::closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: couchstore_close_db failed, error=%s errno=%s",
                         couchstore_strerror(ret), couchkvstore_strerrno(ret).c_str());
    }
    st.numClose++;
}

ENGINE_ERROR_CODE CouchKVStore::couchErr2EngineErr(couchstore_error_t errCode)
{
    switch (errCode) {
    case COUCHSTORE_SUCCESS:
        return ENGINE_SUCCESS;
    case COUCHSTORE_ERROR_ALLOC_FAIL:
        return ENGINE_ENOMEM;
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        return ENGINE_KEY_ENOENT;
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
        return ENGINE_NOT_MY_VBUCKET;
    default:
        // same as the general error return code of
        // EvetuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
}

size_t CouchKVStore::warmup(MutationLog &lf,
                            const std::map<uint16_t, vbucket_state> &vbmap,
                            Callback<GetValue> &cb,
                            Callback<size_t> &estimate)
{
    assert(engine.getEpStore()->multiBGFetchEnabled());
    MutationLogHarvester harvester(lf, &engine);
    std::map<uint16_t, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVBucket(it->first);
    }

    hrtime_t start = gethrtime();
    if (!harvester.load()) {
        return -1;
    }
    hrtime_t end = gethrtime();

    size_t total = harvester.total();
    estimate.callback(total);
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
            "Completed log read in %s with %ld entries\n",
            hrtime2text(end - start).c_str(), total);

    start = gethrtime();
    WarmupCookie cookie(this, cb);
    harvester.apply(&cookie, &batchWarmupCallback);
    end = gethrtime();
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Populated log in %s with (l: %ld, s: %ld, e: %ld)",
                      hrtime2text(end - start).c_str(),
                      cookie.loaded, cookie.skipped, cookie.error);
    return cookie.loaded;
}

bool CouchKVStore::getEstimatedItemCount(size_t &items)
{
    items = 0;

    if (dbFileMap.empty()) {
        std::vector<std::string> files;
        // first scan all db files from data directory
        discoverDbFiles(dbname, files);
        populateFileNameMap(files);
    }

    std::map<uint16_t, uint64_t>::iterator fitr = dbFileMap.begin();
    for (; fitr != dbFileMap.end(); ++fitr) {
        Db *db = NULL;
        couchstore_error_t errCode = openDB(fitr->first, fitr->second, &db, 0);
        if (errCode == COUCHSTORE_SUCCESS) {
            DbInfo info;
            errCode = couchstore_db_info(db, &info);
            if (errCode == COUCHSTORE_SUCCESS) {
                items += info.doc_count;
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: failed to read database info for "
                                 "vBucket = %d rev = %llu\n",
                                 fitr->first, fitr->second);
            }
            closeDatabaseHandle(db);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database file for "
                             "vBucket = %d rev = %llu\n",
                             fitr->first, fitr->second);
        }
    }
    return true;
}

/* end of couch-kvstore.cc */
