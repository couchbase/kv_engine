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
#include "couch-kvstore/couch-kvstore.hh"
#include "couch-kvstore/dirutils.hh"
#include "warmup.hh"
#include "tools/cJSON.h"
#include "tools/JSON_checker.h"

#define STATWRITER_NAMESPACE couchstore_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE

using namespace CouchKVStoreDirectoryUtilities;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

static const int MAX_OPEN_DB_RETRY = 10;

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

extern "C" {
    static std::string getStrError() {
        const size_t max_msg_len = 256;
        char msg[max_msg_len];
        couchstore_last_os_error(msg, max_msg_len);
        std::string errorStr(msg);
        return errorStr;
    }
}

static bool isJSON(const value_t &value)
{
    const int len = value->length();
    const unsigned char *data = (unsigned char*) value->getData();
    return checkUTF8JSON(data, len);
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
    if (pos == std::string::npos || (filename.size() - sizeof(".compact")) != pos) {
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

static uint64_t computeMaxDeletedSeqNum(DocInfo **docinfos, const int numdocs)
{
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

static int getMutationStatus(couchstore_error_t errCode)
{
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
    EPStats *stats;
};

CouchRequest::CouchRequest(const Item &it, uint64_t rev, CouchRequestCallback &cb, bool del) :
    value(it.getValue()), valuelen(it.getNBytes()),
    vbucketId(it.getVBucketId()), fileRevNum(rev),
    key(it.getKey()), deleteItem(del)
{
    bool isjson = false;
    uint64_t cas = htonll(it.getCas());
    uint32_t flags = it.getFlags();
    uint32_t exptime = it.getExptime();
    // Save time of deletion in expiry time field of deleted item's metadata.
    if (del) {
        exptime = ep_real_time();
    }
    exptime = htonl(exptime);

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
    //Compress everything. Snappy is fast. Don't attempt to compress empty bodies.
    if(dbDoc.data.size > 0 && !deleteItem) {
        dbDocInfo.content_meta |= COUCH_DOC_IS_COMPRESSED;
    }
    start = gethrtime();
}

CouchKVStore::CouchKVStore(EPStats &stats, Configuration &config, bool read_only) :
    KVStore(read_only), epStats(stats), configuration(config),
    dbname(configuration.getDbname()), couchNotifier(NULL), pendingCommitCnt(0),
    intransaction(false)
{
    open();
    statCollectingFileOps = getCouchstoreStatsOps(&st.fsStats);
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
    KVStore(copyFrom), epStats(copyFrom.epStats),
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
        resetVBucket(vbucket, itor->second);
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
        LOG(EXTENSION_LOG_DEBUG, "Warning: cannot locate database file %s",
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
        // just log error, openDB will attemp to open the database
        // file again and handle the final error
        LOG(EXTENSION_LOG_DEBUG, "Info: cannot locate database file %s",
            dbFile.c_str());
    }

    couchstore_error_t errCode = openDB(vb, dbFileRev(dbFile), &db,
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
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve doc info from "
                "database, name=%s key=%s error=%s [%s]\n",
                dbFile.c_str(), id.buf, couchstore_strerror(errCode),
                couchkvstore_strerrno(errCode).c_str());
        }
    } else {
        assert(docInfo);
        errCode = fetchDoc(db, docInfo, rv, vb, getMetaOnly);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve key value from "
                "database, name=%s key=%s error=%s [%s] "
                "deleted=%s", dbFile.c_str(), id.buf,
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
    std::string dbFile;
    int numItems = itms.size();

    if (!(getDbFile(vb, dbFile))) {
        // just log error, openDB will attempt to open the database
        // file again and handle the final error
        LOG(EXTENSION_LOG_DEBUG, "Info: cannot locate database file %s",
            dbFile.c_str());
    }

    Db *db = NULL;
    couchstore_error_t errCode = openDB(vb, dbFileRev(dbFile), &db,
                                        COUCHSTORE_OPEN_FLAG_RDONLY);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING,
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
                LOG(EXTENSION_LOG_WARNING, "Warning: failed to read database by"
                    " sequence id = %lld, vBucketId = %d "
                    "key = %s file = %s error = %s [%s]\n",
                    (*fitr)->value.getId(), vb, (*fitr)->key.c_str(),
                    dbFile.c_str(), couchstore_strerror(errCode),
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
    CouchRequestCallback requestcb;
    uint64_t fileRev = 1;

    requestcb.delCb = &cb;
    if (getDbFile(itm.getVBucketId(), dbFile)) {
        fileRev = dbFileRev(dbFile);
     } else {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: cannot locate database file %s for delete\n",
            dbFile.c_str());
    }

    // each req will be de-allocated after commit
    CouchRequest *req = new CouchRequest(itm, fileRev, requestcb, true);
    queueItem(req);
}

bool CouchKVStore::delVBucket(uint16_t vbucket, bool recreate)
{
    assert(!isReadOnly());
    assert(couchNotifier);
    RememberingCallback<bool> cb;

    couchNotifier->delVBucket(vbucket, cb);
    cb.waitForValue();

    if (recreate) {
        vbucket_state vbstate(vbucket_state_dead, 0, 0);
        vbucket_map_t::iterator it = cachedVBStates.find(vbucket);
        if (it != cachedVBStates.end()) {
            vbstate.state = it->second.state;
        }
        cachedVBStates[vbucket] = vbstate;
        resetVBucket(vbucket, vbstate);
    } else {
        cachedVBStates.erase(vbucket);
    }
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
        errorCode = openDB(itr->first, itr->second, &db,
                           COUCHSTORE_OPEN_FLAG_RDONLY);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string fileName = dbname + "/" + vbid.str() + ".couch." + rev.str();
            LOG(EXTENSION_LOG_WARNING,
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
    if (access(fname.c_str(), R_OK) == -1) {
        return ;
    }

    std::ifstream session_stats;
    session_stats.exceptions (session_stats.failbit | session_stats.badbit);
    try {
        session_stats.open(fname.c_str(), ios::binary);
        session_stats.seekg (0, ios::end);
        int flen = session_stats.tellg();
        if (flen < 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: error in session stats ifstream!!!");
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

bool CouchKVStore::snapshotVBuckets(const vbucket_map_t &vbstates)
{
    assert(!isReadOnly());
    bool success = true;

    vbucket_map_t::const_reverse_iterator iter = vbstates.rbegin();
    for (; iter != vbstates.rend(); ++iter) {
        uint16_t vbucketId = iter->first;
        vbucket_state vbstate = iter->second;
        vbucket_map_t::iterator it = cachedVBStates.find(vbucketId);
        uint32_t vb_change_type = VB_NO_CHANGE;
        if (it != cachedVBStates.end()) {
            if (it->second.state != vbstate.state) {
                vb_change_type |= VB_STATE_CHANGED;
            }
            if (it->second.checkpointId != vbstate.checkpointId) {
                vb_change_type |= VB_CHECKPOINT_CHANGED;
            }
            if (vb_change_type == VB_NO_CHANGE) {
                continue; // no changes
            }
            it->second.state = vbstate.state;
            it->second.checkpointId = vbstate.checkpointId;
            // Note that the max deleted seq number is maintained within CouchKVStore
            vbstate.maxDeletedSeqno = it->second.maxDeletedSeqno;
        } else {
            vb_change_type = VB_STATE_CHANGED;
            cachedVBStates[vbucketId] = vbstate;
        }

        success = setVBucketState(vbucketId, vbstate, vb_change_type, false);
        if (!success) {
            LOG(EXTENSION_LOG_WARNING,
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
        LOG(EXTENSION_LOG_WARNING, "Warning: failed to log the engine stats due"
            " to IO exception \"%s\"", e.what());
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
                                   uint32_t vb_change_type, bool newfile,
                                   bool notify)
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
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, name=%s",
                filename.str().c_str());
            return false;
        }
        fileRev = newFileRev;

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
                couchkvstore_strerrno(errorCode).c_str());
            closeDatabaseHandle(db);
            return false;
        } else {
            if (notify) {
                uint64_t newHeaderPos = couchstore_get_header_position(db);
                RememberingCallback<uint16_t> lcb;

                VBStateNotification vbs(vbstate.checkpointId, vbstate.state,
                        vb_change_type, vbucketId);

                couchNotifier->notify_update(vbs, fileRev, newHeaderPos, lcb);
                if (lcb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                    if (lcb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                        LOG(EXTENSION_LOG_WARNING,
                            "Retry notify CouchDB of update, "
                            "vbid=%u rev=%llu\n", vbucketId, fileRev);
                        retry = true;
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "Warning: failed to notify CouchDB of update, "
                            "vbid=%u rev=%llu error=0x%x\n",
                            vbucketId, fileRev, lcb.val);
                        if (!epStats.shutdown.isShutdown) {
                            closeDatabaseHandle(db);
                            return false;
                        }
                    }
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
    StorageProperties rv(true, true, true, true);
    return rv;
}

bool CouchKVStore::commit(void)
{
    assert(!isReadOnly());
    if (intransaction) {
        intransaction = commit2couchstore() ? false : true;
    }
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

    if (!isReadOnly()) {
        addStat(prefix_str, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix_str, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix_str, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix_str, "lastCommDocs",  st.docsCommitted,   add_stat, c);
        addStat(prefix_str, "numCommitRetry", st.numCommitRetry, add_stat, c);

        // stats for CouchNotifier
        couchNotifier->addStats(prefix, add_stat, c);
    }
}

void CouchKVStore::addTimingStats(const std::string &prefix,
                                  ADD_STAT add_stat, const void *c) {
    if (isReadOnly()) {
        return;
    }
    const char *prefix_str = prefix.c_str();
    addStat(prefix_str, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix_str, "commitRetry", st.commitRetryHisto, add_stat, c);
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
        errorCode = openDB(itr->first, itr->second, &db,
                           COUCHSTORE_OPEN_FLAG_RDONLY);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string dbName = dbname + "/" + vbid.str() + ".couch." + rev.str();
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, name=%s\n", dbName.c_str());
            remVBucketFromDbFileMap(itr->first);
        } else {
            LoadResponseCtx ctx;
            ctx.vbucketId = itr->first;
            ctx.keysonly = keysOnly;
            ctx.callback = cb;
            ctx.stats = &epStats;
            errorCode = couchstore_changes_since(db, 0, options, recordDbDumpC,
                                                 static_cast<void *>(&ctx));
            if (errorCode != COUCHSTORE_SUCCESS) {
                if (errorCode == COUCHSTORE_ERROR_CANCEL) {
                    LOG(EXTENSION_LOG_WARNING,
                        "Canceling loading database, warmup has completed\n");
                    closeDatabaseHandle(db);
                    break;
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Warning: couchstore_changes_since failed, error=%s [%s]",
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
        couchNotifier = CouchNotifier::create(epStats, configuration);
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
        CouchNotifier::deleteNotifier();
    }
    couchNotifier = NULL;
}

bool CouchKVStore::getDbFile(uint16_t vbucketId, std::string &dbFileName)
{
    bool success = true;
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
            // assign file rev to initial value
            fileName << "." << 1;
            success = false;
        }
    } else {
        fileName  <<  "." << itr->second;
    }

    dbFileName = fileName.str();
    return success;
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
                    updateDbFileMap(vbucketId, fileRev, true);
                    LOG(EXTENSION_LOG_INFO,
                        "INFO: created new couch db file, name=%s rev=%d",
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
            " option=%X rev=%d error=%s [%s]\n", dbFileName.c_str(), options,
            ((newRevNum > fileRev) ? newRevNum : fileRev),
            couchstore_strerror(errorCode),
            couchkvstore_strerrno(errorCode).c_str());
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
                                              Db** db, uint64_t *newFileRev)
{
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
            couchkvstore_strerrno(errCode).c_str());
       *newFileRev = checkNewRevNum(dbfile);
       ++retry;
       if (retry == MAX_OPEN_DB_RETRY - 1 && options == 0 &&
           errCode == COUCHSTORE_ERROR_NO_SUCH_FILE) {
           options = COUCHSTORE_OPEN_FLAG_CREATE;
       }
    }
    return errCode;
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
                LOG(EXTENSION_LOG_WARNING,
                    "Warning: database file does not exist, %s",
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
            LOG(EXTENSION_LOG_DEBUG,
                "Non-vbucket database file, %s, skip adding "
                "to CouchKVStore dbFileMap\n", filename.c_str());
        }
    }
}

couchstore_error_t CouchKVStore::fetchDoc(Db *db, DocInfo *docinfo,
                                          GetValue &docValue, uint16_t vbId,
                                          bool metaOnly)
{
    couchstore_error_t errCode = COUCHSTORE_SUCCESS;
    sized_buf metadata = docinfo->rev_meta;
    uint32_t itemFlags;
    uint64_t cas;
    time_t exptime;

    assert(metadata.size == 16);
    memcpy(&cas, (metadata.buf), 8);
    cas = ntohll(cas);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    exptime = ntohl(exptime);
    memcpy(&itemFlags, (metadata.buf) + 12, 4);
    itemFlags = itemFlags;

    if (metaOnly) {
        Item *it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                            docinfo->size, itemFlags, (time_t)exptime, cas);
        it->setSeqno(docinfo->rev_seq);
        docValue = GetValue(it);

        // update ep-engine IO stats
        ++epStats.io_num_read;
        epStats.io_read_bytes += docinfo->id.size;
    } else {
        Doc *doc = NULL;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, DECOMPRESS_DOC_BODIES);
        if (errCode == COUCHSTORE_SUCCESS) {
            if (docinfo->deleted) {
                // do not read a doc that is marked deleted, just return the
                // error code as not found but still release the document body.
                errCode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
            } else {
                assert(doc && (doc->id.size <= UINT16_MAX));
                size_t valuelen = doc->data.size;
                void *valuePtr = doc->data.buf;
                Item *it = new Item(docinfo->id.buf, (size_t)docinfo->id.size,
                                    itemFlags, (time_t)exptime, valuePtr, valuelen,
                                    cas, -1, vbId);
                docValue = GetValue(it);

                // update ep-engine IO stats
                ++epStats.io_num_read;
                epStats.io_read_bytes += docinfo->id.size + valuelen;
            }
            couchstore_free_document(doc);
        }
    }
    return errCode;
}

int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx)
{
    LoadResponseCtx *loadCtx = (LoadResponseCtx *)ctx;
    shared_ptr<Callback<GetValue> > cb = loadCtx->callback;

    EPStats *stats= loadCtx->stats;
    volatile bool warmup = !stats->warmupComplete.get();

    Doc *doc = NULL;
    void *valuePtr = NULL;
    size_t valuelen = 0;
    sized_buf  metadata = docinfo->rev_meta;
    uint16_t vbucketId = loadCtx->vbucketId;
    sized_buf key = docinfo->id;
    uint32_t itemflags;
    uint64_t cas;
    uint32_t exptime;

    assert(key.size <= UINT16_MAX);
    assert(metadata.size == 16);

    if (warmup) {
        // skip items already loaded during earlier warmup stage
        LoadStorageKVPairCallback *lscb = static_cast<LoadStorageKVPairCallback *>(cb.get());

        if (lscb->isLoaded(docinfo->id.buf, docinfo->id.size, vbucketId)) {
            return 0;
        }
    }

    memcpy(&cas, metadata.buf, 8);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    memcpy(&itemflags, (metadata.buf) + 12, 4);
    itemflags = itemflags;
    exptime = ntohl(exptime);
    cas = ntohll(cas);

    if (!loadCtx->keysonly && !docinfo->deleted) {
        couchstore_error_t errCode ;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, DECOMPRESS_DOC_BODIES);

        if (errCode == COUCHSTORE_SUCCESS) {
            if (doc->data.size) {
                valuelen = doc->data.size;
                valuePtr = doc->data.buf;
            }
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to retrieve key value from database "
                "database, vBucket=%d key=%s error=%s [%s]\n",
                vbucketId, key.buf, couchstore_strerror(errCode),
                couchkvstore_strerrno(errCode).c_str());
            return 0;
        }
    }

    Item *it = new Item((void *)key.buf,
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
        if (stats->warmupComplete.get()) {
            // warmup has completed, return COUCHSTORE_ERROR_CANCEL to
            // cancel remaining data dumps from couchstore
            LOG(EXTENSION_LOG_WARNING,
                "Engine warmup is complete, request to stop "
                "loading remaining database");
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
        LOG(EXTENSION_LOG_WARNING,
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
        errCode = openDB(vbid, fileRev, &db, 0, &newFileRev);
        if (errCode != COUCHSTORE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database, vbucketId = %d "
                "fileRev = %llu numDocs = %d", vbid, fileRev, docCount);
            return errCode;
        } else {
            uint64_t max = computeMaxDeletedSeqNum(docinfos, docCount);

            // update max_deleted_seq in the local doc (vbstate)
            // before save docs for the given vBucket
            if (max > 0) {
                vbucket_map_t::iterator it =
                    cachedVBStates.find(vbid);
                if (it != cachedVBStates.end() && it->second.maxDeletedSeqno < max) {
                    it->second.maxDeletedSeqno = max;
                    errCode = saveVBState(db, it->second);
                    if (errCode != COUCHSTORE_SUCCESS) {
                        LOG(EXTENSION_LOG_WARNING,
                            "Warning: failed to save local doc for, "
                            "vBucket = %d numDocs = %d\n", vbid, docCount);
                        closeDatabaseHandle(db);
                        return errCode;
                    }
                }
            }

            hrtime_t cs_begin = gethrtime();
            errCode = couchstore_save_documents(db, docs, docinfos, docCount,
                                                COMPRESS_DOC_BODIES);
            st.saveDocsHisto.add((gethrtime() - cs_begin) / 1000);
            if (errCode != COUCHSTORE_SUCCESS) {
                LOG(EXTENSION_LOG_WARNING,
                    "Warning: failed to save docs to database, numDocs = %d "
                    "error=%s [%s]\n", docCount, couchstore_strerror(errCode),
                    couchkvstore_strerrno(errCode).c_str());
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
                    couchkvstore_strerrno(errCode).c_str());
                closeDatabaseHandle(db);
                return errCode;
            }

            RememberingCallback<uint16_t> cb;
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            couchNotifier->notify_headerpos_update(vbid, newFileRev, newHeaderPos, cb);
            if (cb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (cb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    LOG(EXTENSION_LOG_WARNING,
                        "Retry notify CouchDB of update, vbucket=%d rev=%llu\n",
                        vbid, newFileRev);
                    std::string dbFileName = getDBFileName(dbname, vbid, newFileRev);
                    fileRev = checkNewRevNum(dbFileName);
                    retry_save_docs = true;
                    ++st.numCommitRetry;
                    if (!retried) {
                        retry_begin = gethrtime();
                        retried = true;
                    }
                } else {
                    LOG(EXTENSION_LOG_WARNING, "Warning: failed to notify "
                        "CouchDB of update for vbucket=%d, error=0x%x\n",
                        vbid, cb.val);
                }
            }
            st.batchSize.add(docCount);

            if (db && !retry_save_docs) {
                DbInfo info;
                couchstore_db_info(db, &info);
                cachedDeleteCount[vbid] = info.deleted_count;
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
        LOG(EXTENSION_LOG_DEBUG,
            "Warning: failed to retrieve stat info for vBucket=%d error=%s [%s]",
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
            LOG(EXTENSION_LOG_WARNING,
                "Warning: state JSON doc for vbucket %d is in the wrong format: %s",
                vbId, statjson.c_str());
        } else {
            vbState.state = VBucket::fromString(state.c_str());
            parseUint64(max_deleted_seqno.c_str(), &vbState.maxDeletedSeqno);
            parseUint64(checkpoint_id.c_str(), &vbState.checkpointId);
        }
        cJSON_Delete(jsonObj);
        couchstore_free_local_document(ldoc);
    }
}

couchstore_error_t CouchKVStore::saveVBState(Db *db, vbucket_state &vbState)
{
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state)
              << "\", \"checkpoint_id\": \"" << vbState.checkpointId
              << "\", \"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno
              << "\"}";

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
            couchkvstore_strerrno(errCode).c_str());
    }
    return errCode;
}

int CouchKVStore::getMultiCb(Db *db, DocInfo *docinfo, void *ctx)
{
    assert(docinfo);
    std::string keyStr(docinfo->id.buf, docinfo->id.size);
    assert(ctx);
    GetMultiCbCtx *cbCtx = static_cast<GetMultiCbCtx *>(ctx);
    CouchKVStoreStats &st = cbCtx->cks.getCKVStoreStat();


    vb_bgfetch_queue_t::iterator qitr = cbCtx->fetches.find(docinfo->db_seq);
    if (qitr == cbCtx->fetches.end()) {
        // this could be a serious race condition in couchstore,
        // log a warning message and continue
        LOG(EXTENSION_LOG_WARNING,
            "Warning: couchstore returned invalid docinfo, "
            "no pending bgfetch has been issued for db_seq=%lld "
            "key = %s\n", docinfo->db_seq, keyStr.c_str());
        return 0;
    }

    std::list<VBucketBGFetchItem *> &fetches = (*qitr).second;
    GetValue returnVal;
    couchstore_error_t errCode = cbCtx->cks.fetchDoc(db, docinfo, returnVal,
                                                     cbCtx->vbId, false);
    if (errCode != COUCHSTORE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "Warning: failed to fetch data from database, "
            "vBucket=%d key=%s error=%s [%s]", cbCtx->vbId,
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
        LOG(EXTENSION_LOG_WARNING,
            "Warning: couchstore_close_db failed, error=%s [%s]",
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
    case COUCHSTORE_ERROR_NO_SUCH_FILE:
    case COUCHSTORE_ERROR_NO_HEADER:
        return ENGINE_KEY_ENOENT;
    default:
        // same as the general error return code of
        // EvetuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
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
        couchstore_error_t errCode = openDB(fitr->first, fitr->second, &db,
                                            COUCHSTORE_OPEN_FLAG_RDONLY);
        if (errCode == COUCHSTORE_SUCCESS) {
            DbInfo info;
            errCode = couchstore_db_info(db, &info);
            if (errCode == COUCHSTORE_SUCCESS) {
                items += info.doc_count;
            } else {
                LOG(EXTENSION_LOG_WARNING,
                    "Warning: failed to read database info for "
                    "vBucket = %d rev = %llu\n", fitr->first, fitr->second);
            }
            closeDatabaseHandle(db);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to open database file for "
                "vBucket = %d rev = %llu\n", fitr->first, fitr->second);
        }
    }
    return true;
}

size_t CouchKVStore::getNumPersistedDeletes(uint16_t vbid) {
    std::map<uint16_t, size_t>::iterator itr = cachedDeleteCount.find(vbid);
    if (itr != cachedDeleteCount.end()) {
        return itr->second;
    }
    return 0;
}

/* end of couch-kvstore.cc */
