/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include <stdio.h>
#include <dirent.h>
#include <glob.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

#include "couch-kvstore/couch-kvstore.hh"
#include "ep_engine.h"
#include "tools/cJSON.h"
#include "common.hh"

#define STATWRITER_NAMESPACE couchstore_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE

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

    if (ii < len && *ptr == '{') {
        // This may be JSON. Unfortunately we don't know if it's zero
        // terminated
        std::string data(value->getData(), value->length());
        cJSON *json = cJSON_Parse(data.c_str());
        if (json != 0) {
            isJSON = true;
            cJSON_Delete(json);
        }
    }
    return isJSON;
}

extern "C" {
    static int recordDbDumpC(Db *db, DocInfo *docinfo, void *ctx)
    {
        return CouchKVStore::recordDbDump(db, docinfo, ctx);
    }
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

static int dbFileRev(const std::string &dbname)
{
    size_t secondDot = dbname.rfind(".");
    return atoi(dbname.substr(secondDot + 1).c_str());
}

static bool discoverDbFiles(const std::string &dir,
                            std::vector<std::string> &v)
{
    DIR *dhdl;

    if ((dhdl = opendir(dir.c_str())) == NULL) {
        return false;
    }

    struct dirent *direntry;
    while ((direntry = readdir(dhdl))) {
        if (strlen(direntry->d_name) < (sizeof(".couch") - 1)) {
            continue;
        }
        std::stringstream filename;
        filename << dir << std::string("/") << std::string(direntry->d_name);
        if (!endWithCompact(filename.str())) {
            v.push_back(std::string(filename.str()));
        }
    }
    closedir(dhdl);

    return true;
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

static bool compareItemsByKey(const queued_item &i1, const queued_item &i2)
{
    return i1->getKey() == i2->getKey();
}

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
    shared_ptr<LoadCallback> callback;
    uint16_t vbucketId;
    bool keysonly;
};

CouchRequest::CouchRequest(const Item &it, int rev, CouchRequestCallback &cb, bool del) :
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

CouchKVStore::CouchKVStore(EventuallyPersistentEngine &theEngine) :
    KVStore(), engine(theEngine),
    epStats(theEngine.getEpStats()),
    configuration(theEngine.getConfiguration()),
    dbname(configuration.getDbname()),
    mc(NULL), pendingCommitCnt(0),
    intransaction(false)
{
    open();
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
    KVStore(copyFrom), engine(copyFrom.engine),
    epStats(copyFrom.epStats),
    configuration(copyFrom.configuration),
    dbname(copyFrom.dbname),
    mc(NULL),
    pendingCommitCnt(0), intransaction(false)
{
    open();
    dbFileMap = copyFrom.dbFileMap;
}

void CouchKVStore::reset()
{
    // TODO CouchKVStore::flush() when couchstore api ready
    RememberingCallback<bool> cb;
    mc->flush(cb);
    cb.waitForValue();
}

void CouchKVStore::set(const Item &itm, uint16_t, Callback<mutation_result> &cb)
{
    assert(intransaction);
    bool deleteItem = false;
    CouchRequestCallback requestcb;
    std::string dbFile;

    requestcb.setCb = &cb;
    if (!(getDbFile(itm.getVBucketId(), dbFile))) {
        ++st.numSetFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to set data, cannot locate database file %s\n",
                         dbFile.c_str());
        mutation_result p(0, -1);
        cb.callback(p);
        return;
    }

    // each req will be de-allocated after commit
    CouchRequest *req = new CouchRequest(itm, dbFileRev(dbFile), requestcb,
                                         deleteItem);
    this->queue(*req);
}

void CouchKVStore::get(const std::string &key, uint64_t, uint16_t vb, uint16_t,
                       Callback<GetValue> &cb)
{
    hrtime_t start = gethrtime();
    Db *db = NULL;
    DocInfo *docInfo = NULL;
    Doc *doc = NULL;
    Item *it = NULL;
    std::string dbFile;
    GetValue rv;
    sized_buf id;

    if (!(getDbFile(vb, dbFile))) {
        ++st.numGetFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to retrieve data from vBucketId = %d "
                         "[key=%s], cannot locate database file %s\n",
                         vb, key.c_str(), dbFile.c_str());
        cb.callback(rv);
        return;
    }

    couchstore_error_t errCode = openDB(vb, dbFileRev(dbFile), &db, 0, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to open database, name=%s error=%s\n",
                         dbFile.c_str(),
                         couchstore_strerror(errCode));
        cb.callback(rv);
        return;
    }

    RememberingCallback<GetValue> *rc =
        dynamic_cast<RememberingCallback<GetValue> *>(&cb);
    bool getMetaOnly = rc && rc->val.isPartial();

    id.size = key.size();
    id.buf = const_cast<char *>(key.c_str());
    errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                       id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        if (!getMetaOnly) {
            // log error only if this is non-xdcr case
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to retrieve doc info from "
                             "database, name=%s key=%s error=%s\n",
                             dbFile.c_str(), id.buf,
                             couchstore_strerror(errCode));
        }
    } else {
        assert(docInfo);
        uint32_t itemFlags;
        void *valuePtr = NULL;
        size_t valuelen = 0;
        uint64_t cas;
        sized_buf metadata;

        metadata = docInfo->rev_meta;
        assert(metadata.size == 16);
        memcpy(&cas, (metadata.buf), 8);
        cas = ntohll(cas);
        memcpy(&itemFlags, (metadata.buf) + 12, 4);
        itemFlags = ntohl(itemFlags);

        if (getMetaOnly) {
            it = new Item(key.c_str(), (size_t)key.length(), docInfo->size,
                          itemFlags, (time_t)0, cas);
            rv = GetValue(it);
            /* update ep-engine IO stats */
            ++epStats.io_num_read;
            epStats.io_read_bytes += key.length();
        } else {
            errCode = couchstore_open_doc_with_docinfo(db, docInfo, &doc, 0);
            if (errCode != COUCHSTORE_SUCCESS || docInfo->deleted) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: failed to retrieve key value from "
                                 "database, name=%s key=%s error=%s deleted=%s\n",
                                 dbFile.c_str(), id.buf,
                                 couchstore_strerror(errCode),
                                 docInfo->deleted ? "yes" : "no");
            } else {
                assert(doc && (doc->id.size <= UINT16_MAX));
                assert(strncmp(doc->id.buf, key.c_str(), doc->id.size) == 0);

                valuelen = doc->data.size;
                valuePtr = doc->data.buf;
                it = new Item(key, itemFlags, 0, valuePtr, valuelen, cas, -1, vb);
                rv = GetValue(it);
                /* update ep-engine IO stats */
                ++epStats.io_num_read;
                epStats.io_read_bytes += key.length() + valuelen;
            }
        }

        // record stats
        st.readTimeHisto.add((gethrtime() - start) / 1000);
        st.readSizeHisto.add(key.length() + valuelen);
    }

    couchstore_free_docinfo(docInfo);
    couchstore_free_document(doc);
    closeDatabaseHandle(db);
    cb.callback(rv);

    if(errCode != COUCHSTORE_SUCCESS) {
        ++st.numGetFailure;
    }
}

void CouchKVStore::del(const Item &itm,
                       uint64_t,
                       uint16_t,
                       Callback<int> &cb)
{
    assert(intransaction);
    std::string dbFile;

    if (getDbFile(itm.getVBucketId(), dbFile)) {
        CouchRequestCallback requestcb;
        requestcb.delCb = &cb;
        // each req will be de-allocated after commit
        CouchRequest *req = new CouchRequest(itm, dbFileRev(dbFile),
                                             requestcb, true);
        this->queue(*req);
    } else {
        int value = -1;
        ++st.numDelFailure;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: failed to delete data, cannot locate "
                         "database file %s\n",
                         dbFile.c_str());
        cb.callback(value);
    }
}

bool CouchKVStore::delVBucket(uint16_t,
                              uint16_t,
                              std::pair<int64_t, int64_t>)
{
    // noop, it is required for abstract base class
    return true;
}

bool CouchKVStore::delVBucket(uint16_t vbucket, uint16_t)
{
    assert(mc);
    RememberingCallback<bool> cb;
    mc->delVBucket(vbucket, cb);
    cb.waitForValue();
    remVBucketFromDbFileMap(vbucket);
    return cb.val;
}

vbucket_map_t CouchKVStore::listPersistedVbuckets()
{
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    std::vector<std::string> files = std::vector<std::string>();

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        if (discoverDbFiles(dbname, files)) {
            populateFileNameMap(files);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: data directory does not exist, %s\n",
                             dbname.c_str());
            return rv;
        }
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    std::map<uint16_t, int>::iterator itr = dbFileMap.begin();
    for (; itr != dbFileMap.end(); itr++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string fileName = dbname + "/" + vbid.str() + ".couch." +
                rev.str();
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database file, name=%s error=%s\n",
                             fileName.c_str(), couchstore_strerror(errorCode));
            remVBucketFromDbFileMap(itr->first);
        } else {
            vbucket_state vb_state;
            uint16_t      vbID = itr->first;
            std::pair<uint16_t, uint16_t> vb(vbID, -1);

            /* read state of VBucket from db file */
            readVBState(db, vbID, vb_state);
            /* insert populated state to the array to return to the caller */
            rv[vb] = vb_state;
            /* update stat */
            ++st.numLoadedVb;
            closeDatabaseHandle(db);
        }
        db = NULL;
    }
    return rv;
}

void CouchKVStore::vbStateChanged(uint16_t vbucket, vbucket_state_t newState)
{
    if (!(setVBucketState(vbucket, newState, 0))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: Failed to set new state, %s, for vbucket %d\n",
                         VBucket::toString(newState), vbucket);
    }
}

bool CouchKVStore::snapshotVBuckets(const vbucket_map_t &m)
{

    vbucket_map_t::const_iterator iter;
    uint16_t vbucketId;
    vbucket_state_t state;
    uint64_t checkpointId;
    bool success = m.empty() ? true : false;
    hrtime_t start = gethrtime();

    for (iter = m.begin(); iter != m.end(); ++iter) {
        const vbucket_state vbstate = iter->second;
        vbucketId = iter->first.first;
        state = vbstate.state;
        checkpointId = vbstate.checkpointId;

        success = setVBucketState(vbucketId, state, checkpointId);
        if (!success) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to set new state, %s, for vbucket %d\n",
                             VBucket::toString(state), vbucketId);
            break;
        }
    }

    st.snapshotVbHisto.add((gethrtime() - start) / 1000);
    return success;
}

bool CouchKVStore::snapshotStats(const std::map<std::string, std::string> &)
{
    // noop, virtual function implementation for abstract base class
    return true;
}

bool CouchKVStore::setVBucketState(uint16_t vbucketId, vbucket_state_t state,
                                   uint64_t checkpointId)
{
    Db *db = NULL;
    couchstore_error_t errorCode;
    uint16_t fileRev, newFileRev;
    std::stringstream id;
    std::string dbFileName;
    std::map<uint16_t, int>::iterator mapItr;
    int rev;

    id << vbucketId;
    dbFileName = dbname + "/" + id.str() + ".couch";
    mapItr = dbFileMap.find(vbucketId);
    if (mapItr == dbFileMap.end()) {
        rev = checkNewRevNum(dbFileName, true);
        if (rev == 0) {
            fileRev = 1; // file does not exist, create the db file with rev = 1
        } else {
            fileRev = rev;
        }
    } else {
        fileRev = mapItr->second;
    }

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
                             "Warning: failed to open database, name=%s error=%s\n",
                             filename.str().c_str(),
                             couchstore_strerror(errorCode));
            return false;
        }
        fileRev = newFileRev;

        // first get current max_deleted_seq before updating
        // local doc (vbstate)
        vbucket_state vbState;
        readVBState(db, vbucketId, vbState);

        // update local doc with new state & checkpoint_id
        vbState.state = state;
        vbState.checkpointId = checkpointId;
        errorCode = saveVBState(db, vbState);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "Warning: failed to save local doc, name=%s error=%s\n",
                            dbFileName.c_str(),
                            couchstore_strerror(errorCode));
            closeDatabaseHandle(db);
            return false;
        }

        errorCode = couchstore_commit(db);
        if (errorCode != COUCHSTORE_SUCCESS) {
            ++st.numVbSetFailure;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: commit failed, vbid=%u rev=%u error=%s",
                             vbucketId, fileRev, couchstore_strerror(errorCode));
            closeDatabaseHandle(db);
            return false;
        } else {
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            RememberingCallback<uint16_t> lcb;

            mc->notify_update(vbucketId, fileRev, newHeaderPos,
                              true, state, checkpointId, lcb);
            if (lcb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (lcb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Retry notify CouchDB of update, "
                                     "vbid=%u rev=%u\n", vbucketId, fileRev);
                    retry = true;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warning: failed to notify CouchDB of update, "
                                     "vbid=%u rev=%u error=0x%x\n",
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
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<LoadCallback> callback(new LoadCallback(cb, wait));
    loadDB(callback, false, NULL);
}

void CouchKVStore::dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb)
{
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<LoadCallback> callback(new LoadCallback(cb, wait));
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    loadDB(callback, false, &vbids);
}

void CouchKVStore::dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb)
{
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<LoadCallback> callback(new LoadCallback(cb, wait));
    (void)vbids;
    loadDB(callback, true, NULL);
}


StorageProperties CouchKVStore::getStorageProperties()
{
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}

bool CouchKVStore::commit(void)
{
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

    // stats for read-write thread only
    if( prefix.compare("rw") == 0 ) {
        addStat(prefix_str, "delete",        st.delTimeHisto,    add_stat, c);
        addStat(prefix_str, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix_str, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix_str, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix_str, "writeTime",     st.writeTimeHisto,  add_stat, c);
        addStat(prefix_str, "writeSize",     st.writeSizeHisto,  add_stat, c);
        addStat(prefix_str, "lastCommDocs",  st.docsCommitted,   add_stat, c);
        addStat(prefix_str, "snapshotVbTime",st.snapshotVbHisto, add_stat, c);
    }
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
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsByVBAndKey cq;
    std::sort(items.begin(), items.end(), cq);
    std::vector<queued_item>::iterator itr;
    itr = std::unique(items.begin(), items.end(), compareItemsByKey);
    items.resize(itr - items.begin());
}

void CouchKVStore::loadDB(shared_ptr<LoadCallback> cb, bool keysOnly,
                          std::vector<uint16_t> *vbids)
{
    std::vector<std::string> files = std::vector<std::string>();
    std::map<uint16_t, int> &filemap = dbFileMap;
    std::map<uint16_t, int> vbmap;

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        if (discoverDbFiles(dbname, files)) {
            populateFileNameMap(files);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: data directory is empty, %s\n",
                             dbname.c_str());
            return;
        }
    }

    if (vbids) {
        // get entries for given vbucket(s) from dbFileMap
        std::string dirname = dbname;
        getFileNameMap(vbids, dirname, vbmap);
        filemap = vbmap;
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    int keyNum = 0;
    std::map<uint16_t, int>::iterator itr = filemap.begin();
    for (; itr != filemap.end(); itr++, keyNum++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (errorCode != COUCHSTORE_SUCCESS) {
            std::stringstream rev, vbid;
            rev  << itr->second;
            vbid << itr->first;
            std::string dbName = dbname + "/" + vbid.str() + ".couch." +
                                 rev.str();
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database, name=%s error=%s",
                             dbName.c_str(),
                             couchstore_strerror(errorCode));
            remVBucketFromDbFileMap(itr->first);
        } else {
            LoadResponseCtx ctx;
            ctx.vbucketId = itr->first;
            ctx.keysonly = keysOnly;
            ctx.callback = cb;
            errorCode = couchstore_changes_since(db, 0, 0, recordDbDumpC,
                                                 static_cast<void *>(&ctx));
            if (errorCode != COUCHSTORE_SUCCESS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: couchstore_changes_since failed, error=%s\n",
                                 couchstore_strerror(errorCode));
                remVBucketFromDbFileMap(itr->first);
            }
            closeDatabaseHandle(db);
        }
        db = NULL;
    }

    bool success = true;
    cb->complete->callback(success);
}

void CouchKVStore::open()
{
    // TODO intransaction, is it needed?
    intransaction = false;
    delete mc;
    mc = new MemcachedEngine(&engine, configuration);
}

void CouchKVStore::close()
{
    intransaction = false;
    delete mc;
    mc = NULL;
}

bool CouchKVStore::getDbFile(uint16_t vbucketId,
                             std::string &dbFileName)
{
    std::stringstream fileName;
    fileName << dbname << "/" << vbucketId << ".couch";
    std::map<uint16_t, int>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr == dbFileMap.end()) {
        dbFileName = fileName.str();
        int rev = checkNewRevNum(dbFileName, true);
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

int CouchKVStore::checkNewRevNum(std::string &dbFileName, bool newFile)
{
    int newrev = 0;
    glob_t fglob;

    std::string filename, revnum, nameKey;
    size_t secondDot;

    if (!newFile) {
        // extract out the file revision number first
        secondDot = dbFileName.rfind(".");
        nameKey = dbFileName.substr(0, secondDot);
        nameKey.append(".*", 2);
    } else {
        nameKey = dbFileName;
        nameKey.append(".*", 2);
    }

    if (glob(nameKey.c_str(), GLOB_ERR | GLOB_MARK, NULL, &fglob)) {
        return newrev;
    }

    // found file(s) whoes name has the same key name pair with different
    // revision number
    int max = fglob.gl_pathc;
    for (int count = 0; count < max; count++) {
        filename = std::string(fglob.gl_pathv[count]);
        if (endWithCompact(filename)) {
            continue;
        }

        secondDot = filename.rfind(".");
        revnum = filename.substr(secondDot + 1);
        if (newrev < atoi(revnum.c_str())) {
            newrev = atoi(revnum.c_str());
            dbFileName = filename;
        }
    }
    globfree(&fglob);
    return newrev;
}

void CouchKVStore::updateDbFileMap(uint16_t vbucketId, int newFileRev,
                                   bool insertImmediately)
{
    if (insertImmediately) {
        dbFileMap.insert(std::pair<uint16_t, int>(vbucketId, newFileRev));
        return;
    }

    std::map<uint16_t, int>::iterator itr;
    itr = dbFileMap.find(vbucketId);
    if (itr != dbFileMap.end()) {
        itr->second = newFileRev;
    } else {
        dbFileMap.insert(std::pair<uint16_t, int>(vbucketId, newFileRev));
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
                                 uint16_t rev)
{
    std::stringstream ss;
    ss << dbname << "/" << vbid << ".couch." << rev;
    return ss.str();
}

couchstore_error_t CouchKVStore::openDB(uint16_t vbucketId,
                                        uint16_t fileRev,
                                        Db **db,
                                        uint64_t options,
                                        uint16_t *newFileRev)
{
    couchstore_error_t errorCode;
    std::string dbFileName = getDBFileName(dbname, vbucketId, fileRev);

    int newRevNum = fileRev;
    // first try to open database without options, we don't want to create
    // a duplicate db that has the same name with different revision number
    if ((errorCode = couchstore_open_db(dbFileName.c_str(), 0, db))) {
        if ((newRevNum = checkNewRevNum(dbFileName))) {
            errorCode = couchstore_open_db(dbFileName.c_str(), 0, db);
            if (errorCode == COUCHSTORE_SUCCESS) {
                updateDbFileMap(vbucketId, newRevNum);
            }
        } else {
            if (options) {
                newRevNum = fileRev;
                errorCode = couchstore_open_db(dbFileName.c_str(), options, db);
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
                         "open_db() failed, name=%s error=%d\n",
                         dbFileName.c_str(), errorCode);
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
                                  std::map<uint16_t, int> &filemap)
{
    std::vector<uint16_t>::iterator vbidItr;
    std::map<uint16_t, int>::iterator dbFileItr;

    for (vbidItr = vbids->begin(); vbidItr != vbids->end(); vbidItr++) {
        std::string dbFileName = getDBFileName(dirname, *vbidItr);
        dbFileItr = dbFileMap.find(*vbidItr);
        if (dbFileItr == dbFileMap.end()) {
            int rev = checkNewRevNum(dbFileName, true);
            if (rev > 0) {
                filemap.insert(std::pair<uint16_t, int>(*vbidItr, rev));
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: database file does not exist, %s\n",
                                 dbFileName.c_str());
            }
        } else {
            filemap.insert(std::pair<uint16_t, int>(dbFileItr->first,
                                                    dbFileItr->second));
        }
    }
}

void CouchKVStore::populateFileNameMap(std::vector<std::string> &filenames)
{
    std::string filename;
    std::string nameKey;
    std::string revNumStr;
    std::string vbIdStr;

    int revNum, vbId;
    size_t secondDot, firstDot, firstSlash;

    std::map<uint16_t, int>::iterator mapItr;
    std::vector<std::string>::iterator fileItr;

    for (fileItr = filenames.begin(); fileItr != filenames.end(); fileItr++) {
        filename = *fileItr;
        secondDot = filename.rfind(".");
        nameKey = filename.substr(0, secondDot);
        firstDot = nameKey.rfind(".");
        firstSlash = nameKey.rfind("/");

        revNumStr = filename.substr(secondDot + 1);
        revNum = atoi(revNumStr.c_str());
        vbIdStr = nameKey.substr(firstSlash + 1, (firstDot - firstSlash));
        vbId = atoi(vbIdStr.c_str());

        mapItr = dbFileMap.find(vbId);
        if (mapItr == dbFileMap.end()) {
            dbFileMap.insert(std::pair<uint16_t, int>(vbId, revNum));
        } else {
            // duplicate key
            if (mapItr->second < revNum) {
                mapItr->second = revNum;
            }
        }
    }
}

int CouchKVStore::recordDbDump(Db *db, DocInfo *docinfo, void *ctx)
{
    Item *it = NULL;
    Doc *doc = NULL;
    LoadResponseCtx *loadCtx = (LoadResponseCtx *)ctx;
    shared_ptr<LoadCallback> callback = loadCtx->callback;

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

    if (docinfo->deleted) {
        // skip deleted doc
        return 0;
    }

    if (!loadCtx->keysonly) {
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
                             "database, vBucket=%d key=%s error=%s\n",
                             vbucketId, key.buf, couchstore_strerror(errCode));
            return 0;
        }
    }

    it = new Item((void *)key.buf,
                  key.size,
                  itemflags,
                  (time_t)exptime,
                  valuePtr, valuelen,
                  cas,
                  1,
                  vbucketId);

    GetValue rv(it, ENGINE_SUCCESS, -1, -1, NULL, loadCtx->keysonly);
    callback->cb->callback(rv);

    couchstore_free_document(doc);
    return 0;
}

bool CouchKVStore::commit2couchstore(void)
{
    Doc **docs;
    DocInfo **docinfos;
    uint16_t vbucket2flush, vbucketId;
    int reqIndex,  flushStartIndex, numDocs2save;
    couchstore_error_t errCode;
    bool success = true;

    std::string dbName;
    CouchRequest *req = NULL;
    CouchRequest **committedReqs;

    if (pendingCommitCnt == 0) {
        return success;
    }

    committedReqs = new CouchRequest *[pendingCommitCnt];
    docs = new Doc *[pendingCommitCnt];
    docinfos = new DocInfo *[pendingCommitCnt];

    if ((req = pendingReqsQ.front()) == NULL) {
        abort();
    }
    vbucket2flush = req->getVBucketId();
    for (reqIndex = 0, flushStartIndex = 0, numDocs2save = 0;
            pendingCommitCnt > 0;
            reqIndex++, numDocs2save++, pendingCommitCnt--) {
        if ((req = pendingReqsQ.front()) == NULL) {
            abort();
        }
        committedReqs[reqIndex] = req;
        docs[reqIndex] = req->getDbDoc();
        docinfos[reqIndex] = req->getDbDocInfo();
        vbucketId = req->getVBucketId();
        pendingReqsQ.pop_front();

        if (vbucketId != vbucket2flush) {
            errCode = saveDocs(vbucket2flush,
                               committedReqs[flushStartIndex]->getRevNum(),
                               &docs[flushStartIndex],
                               &docinfos[flushStartIndex],
                               numDocs2save);
            if (errCode) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warning: commit failed, cannot save CouchDB "
                                 "docs for vbucket = %d rev = %d error = %d\n",
                                 vbucket2flush,
                                 committedReqs[flushStartIndex]->getRevNum(),
                                 (int)errCode);
            }
            commitCallback(&committedReqs[flushStartIndex], numDocs2save, errCode);
            numDocs2save = 0;
            flushStartIndex = reqIndex;
            vbucket2flush = vbucketId;
        }
    }

    if (reqIndex - flushStartIndex) {
        // flush the rest
        errCode = saveDocs(vbucket2flush,
                           committedReqs[flushStartIndex]->getRevNum(),
                           &docs[flushStartIndex],
                           &docinfos[flushStartIndex],
                           numDocs2save);
        if (errCode) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: commit failed, cannot save CouchDB docs "
                             "for vbucket = %d rev = %d error = %d\n",
                             vbucket2flush,
                             committedReqs[flushStartIndex]->getRevNum(),
                             (int)errCode);
        }
        commitCallback(&committedReqs[flushStartIndex], numDocs2save, errCode);
    }

    while (reqIndex--) {
        delete committedReqs[reqIndex];
    }
    delete [] docs;
    delete [] docinfos;
    return success;
}

couchstore_error_t CouchKVStore::saveDocs(uint16_t vbid, int rev, Doc **docs,
                                          DocInfo **docinfos, int docCount)
{
    couchstore_error_t errCode;
    int fileRev;
    uint16_t newFileRev;
    uint16_t vbucket2save = vbid;
    Db *db = NULL;
    bool retry_save_docs;

    fileRev = rev;
    assert(fileRev);

    do {
        retry_save_docs = false;
        errCode = openDB(vbucket2save, fileRev, &db, 0, &newFileRev);
        if (errCode != COUCHSTORE_SUCCESS) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: failed to open database, vbucketId = %d "
                             "fileRev = %d error = %d\n",
                             vbucket2save, fileRev, errCode);
            return errCode;
        } else {
            uint32_t max = computeMaxDeletedSeqNum(docinfos, docCount);

            // update max_deleted_seq in the local doc (vbstate)
            // before save docs for the given vBucket
            if (max > 0) {
                vbucket_state vbState;
                readVBState(db, vbucket2save, vbState);
                assert(vbState.state != vbucket_state_dead);
                if (vbState.maxDeletedSeqno < max) {
                    vbState.maxDeletedSeqno = max;
                    errCode = saveVBState(db, vbState);
                    if (errCode != COUCHSTORE_SUCCESS) {
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "Warning: failed to save local doc for, "
                                         "vBucket = %d error = %s\n",
                                         vbucket2save, couchstore_strerror(errCode));
                        closeDatabaseHandle(db);
                        return errCode;
                    }
                }
            }

            errCode = couchstore_save_documents(db, docs, docinfos, docCount,
                                                0 /* no options */);
            if (errCode != COUCHSTORE_SUCCESS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to save docs to database, error = %s\n",
                                 couchstore_strerror(errCode));
                closeDatabaseHandle(db);
                return errCode;
            }

            errCode = couchstore_commit(db);
            if (errCode) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Commit failed: %s",
                                 couchstore_strerror(errCode));
                closeDatabaseHandle(db);
                return errCode;
            }

            RememberingCallback<uint16_t> cb;
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            mc->notify_headerpos_update(vbucket2save, newFileRev, newHeaderPos, cb);
            if (cb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (cb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                   "Retry notify CouchDB of update, vbucket=%d rev=%d\n",
                                     vbucket2save, newFileRev);
                    fileRev = newFileRev;
                    retry_save_docs = true;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warning: failed to notify CouchDB of "
                                     "update for vbucket=%d, error=0x%x\n",
                                     vbucket2save, cb.val);
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

    return errCode;
}

void CouchKVStore::queue(CouchRequest &req)
{
    pendingReqsQ.push_back(&req);
    pendingCommitCnt++;
}

void CouchKVStore::remVBucketFromDbFileMap(uint16_t vbucketId)
{
    std::map<uint16_t, int>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr != dbFileMap.end()) {
        dbFileMap.erase(itr);
    }
}

void CouchKVStore::commitCallback(CouchRequest **committedReqs, int numReqs,
                                  int errCode)
{
    for (int index = 0; index < numReqs; index++) {
        size_t dataSize = committedReqs[index]->getNBytes();
        size_t keySize = committedReqs[index]->getKey().length();
        /* update ep stats */
        ++epStats.io_num_write;
        epStats.io_write_bytes += keySize + dataSize;

        if (committedReqs[index]->isDelete()) {
            int rv = (errCode) ? -1 : 1;
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta() / 1000);
            }
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            int rv = (errCode) ? 0 : 1;
            if (errCode) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(committedReqs[index]->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + keySize);
            }
            mutation_result p(rv, committedReqs[index]->getItemId());
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
                         "Warning: failed to retrieve stat info for vBucket=%d error=%d\n",
                         vbId, (int)errCode);
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

    return couchstore_save_local_document(db, &lDoc);
}

void CouchKVStore::closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to close database handle: %s",
                         couchstore_strerror(ret));
    }
    st.numClose++;
}

/* end of couch-kvstore.cc */
