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
#include "couch_db.h"
#include "tools/cJSON.h"
#include "common.hh"

static void closeDatabaseHandle(Db *db) {
    couchstore_error_t ret = couchstore_close_db(db);
    if (ret != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to close database handle: %s",
                         couchstore_strerror(ret));
    }
}

static bool isJSON(const value_t &value) {
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

extern "C" int recordDbDumpC(Db* db, DocInfo* docinfo, void *ctx);
int recordDbDumpC(Db* db, DocInfo* docinfo, void *ctx) {
    return CouchKVStore::recordDbDump(db, docinfo, ctx);
}

extern "C" int recordDbStatC(Db* db, DocInfo* docinfo, void *ctx);
int recordDbStatC(Db* db, DocInfo* docinfo, void *ctx) {
    return CouchKVStore::recordDbStat(db, docinfo, ctx);
}


static const std::string getJSONObjString(cJSON *i) {
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
            (filename.size()  - sizeof(".compact")) != pos) {
            return false;
        }
        return true;
}

static int dbFileRev(const std::string &dbname) {
    size_t secondDot = dbname.rfind(".");
    return atoi(dbname.substr(secondDot+1).c_str());
}

static bool discoverDbFiles(const std::string &dir,
                            std::vector<std::string> &v) {
    DIR *dhdl = NULL;
    struct dirent *direntry = NULL;

    if ((dhdl = opendir(dir.c_str())) == NULL) {
        return false;
    } else {
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
    }
    closedir(dhdl);
    return true;
}

struct StatResponseCtx {
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &statMap;
    uint16_t vbId;
};

struct TapResponseCtx {
    shared_ptr<TapCallback> callback;
    uint16_t vbucketId;
    bool keysonly;
};

CouchRequest::CouchRequest(const Item &it, int rev, CouchRequestCallback &cb, bool del) :
                           value(it.getValue()), valuelen(it.getNBytes()),
                           vbucketId(it.getVBucketId()), fileRevNum(rev),
                           key(it.getKey()), deleteItem(del) {
    bool isjson = false;
    uint64_t cas = it.getCas();
    uint32_t flags = it.getFlags();
    uint32_t exptime = it.getExptime();

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
    dbDocInfo.rev_meta.buf = reinterpret_cast<char*>(meta);
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
                           mc(NULL), pendingCommitCnt(0),
                           intransaction(false),
                           vbBatchCount(configuration.getCouchVbucketBatchCount()) {
    vbBatchSize = configuration.getMaxTxnSize() / vbBatchCount;
    if (vbBatchSize == 0) {
        vbBatchSize = configuration.getCouchDefaultBatchSize();
    }
    open();
}

CouchKVStore::CouchKVStore(const CouchKVStore &copyFrom) :
                           KVStore(copyFrom), engine(copyFrom.engine),
                           epStats(copyFrom.epStats),
                           configuration(copyFrom.configuration), mc(NULL),
                           pendingCommitCnt(0), intransaction(false),
                           vbBatchCount(copyFrom.vbBatchCount),
                           vbBatchSize(copyFrom.vbBatchSize) {
    open();
    dbFileMap = copyFrom.dbFileMap;
}

void CouchKVStore::reset() {
    // TODO CouchKVStore::flush() when couchstore api ready
    RememberingCallback<bool> cb;
    mc->flush(cb);
    cb.waitForValue();
}

void CouchKVStore::set(const Item &itm, uint16_t, Callback<mutation_result> &cb) {
    assert(intransaction);
    bool deleteItem = false;
    CouchRequestCallback requestcb;
    std::string dbFile;

    requestcb.setCb = &cb;
    if (!(getDbFile(itm, dbFile))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to set data, cannot locate database file %s\n",
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
                       Callback<GetValue> &cb) {
    Db *db = NULL;
    DocInfo *docInfo = NULL;
    Doc *doc = NULL;
    std::string dbFile;
    sized_buf id;

    if (!(getDbFile(vb, dbFile))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to retrieve data from vBucketId = %d, key = %s ",
                         "cannot locate database file %s\n",
                         vb, key.c_str(), dbFile.c_str());
        GetValue rv;
        cb.callback(rv);
        return;
    }

    couchstore_error_t errCode = openDB(vb, dbFileRev(dbFile), &db, 0, NULL);
    if (errCode != COUCHSTORE_SUCCESS) {
        // TODO return error or assert?
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to open database, name=%s error=%s\n",
                         dbFile.c_str(),
                         couchstore_strerror(errCode));
        abort();
    }

    id.size = key.size();
    id.buf = const_cast<char *>(key.c_str());
    errCode = couchstore_docinfo_by_id(db, (uint8_t *)id.buf,
                                       id.size, &docInfo);
    if (errCode != COUCHSTORE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to retrieve doc info from database, "
                         "name=%s key=%s error=%s\n",
                         dbFile.c_str(), id.buf,
                         couchstore_strerror(errCode));
        abort();
    } else {
        assert(docInfo);
        errCode = couchstore_open_doc_with_docinfo(db, docInfo, &doc, 0);
        if (errCode != COUCHSTORE_SUCCESS) {
            // TODO return error or assert?
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to retrieve key value from database, "
                             "name=%s key=%s error=%s\n",
                             dbFile.c_str(), id.buf,
                             couchstore_strerror(errCode));
            abort();
        }
        assert(doc && (doc->id.size <= UINT16_MAX));
        assert(strncmp(doc->id.buf, key.c_str(), doc->id.size) == 0);
    }

    uint32_t itemFlags;
    void *valuePtr = NULL;
    size_t valuelen = 0;
    sized_buf metadata;

    metadata = docInfo->rev_meta;
    assert(metadata.size == 16);
    memcpy(&itemFlags, (metadata.buf) + 12, 4);
    itemFlags = ntohl(itemFlags);
    if (doc->data.size) {
        valuelen = doc->data.size;
        valuePtr = doc->data.buf;
    }

    Item *it = new Item(key, itemFlags, 0, valuePtr, valuelen, 0, -1, vb);
    GetValue rv(it);

    couchstore_free_docinfo(docInfo);
    couchstore_free_document(doc);
    closeDatabaseHandle(db);
    cb.callback(rv);
}

void CouchKVStore::del(const Item &itm,
                       uint64_t,
                       uint16_t,
                       Callback<int> &cb) {
    assert(intransaction);
    bool deleteItem = true;
    std::string dbFile;
    CouchRequestCallback requestcb;

    if (!(getDbFile(itm, dbFile))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to delete data, cannot locate database file %s\n",
                         dbFile.c_str());
        int value = -1;
        cb.callback(value);
    }

    requestcb.delCb = &cb;
    // each req will be de-allocated after commit
    CouchRequest *req = new CouchRequest(itm, dbFileRev(dbFile),
                                         requestcb, deleteItem);
    this->queue(*req);
}

bool CouchKVStore::delVBucket(uint16_t,
                              uint16_t,
                              std::pair<int64_t, int64_t>) {
    // noop, it is required for abstract base class
    return true;
}

bool CouchKVStore::delVBucket(uint16_t vbucket, uint16_t) {
    assert(mc);
    RememberingCallback<bool> cb;
    mc->delVBucket(vbucket, cb);
    cb.waitForValue();
    remVBucketFromDbFileMap(vbucket);
    return cb.val;
}

vbucket_map_t CouchKVStore::listPersistedVbuckets() {
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    std::string dirname = configuration.getDbname();
    std::vector<std::string> files = std::vector<std::string>();

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        if (discoverDbFiles(dirname, files)) {
            populateFileNameMap(files);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Warning: Data diretory does not exist, %s\n",
                    dirname.c_str());
            return rv;
        }
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    std::map<uint16_t, int>::iterator itr = dbFileMap.begin();
    for (; itr != dbFileMap.end(); itr++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (!errorCode) {
            StatResponseCtx ctx = { rv, itr->first };
            errorCode = couchstore_changes_since(db, 0, 0, recordDbStatC,
                                                 static_cast<void *>(&ctx));
            if (errorCode) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                        "Warning: changes_since failed, vBucket=%d rev=%d error=%s\n",
                        itr->first, itr->second,
                        couchstore_strerror(errorCode));
                //TODO abort or continue?
                abort();
            }
        } else {
            if (errorCode == COUCHSTORE_ERROR_OPEN_FILE ||
                errorCode == COUCHSTORE_ERROR_NO_HEADER) {
                db = NULL;
                continue;
            } else {
                std::stringstream rev, vbid;
                rev  << itr->second;
                vbid << itr->first;
                std::string dbName = dirname + "/" + vbid.str() + ".couch." +
                                     rev.str();
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open database, name=%s error=%s\n",
                        dbName.c_str(), couchstore_strerror(errorCode));
                // TODO abort of return error?
                abort();
            }
        }
        couchstore_close_db(db);
        db = NULL;
    }
    return rv;
}

void CouchKVStore::vbStateChanged(uint16_t vbucket, vbucket_state_t newState) {
    if (!(setVBucketState(vbucket, newState, 0))) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warning: Failed to set new state, %s, for vbucket %d\n",
                         VBucket::toString(newState), vbucket);
    }
}

bool CouchKVStore::snapshotVBuckets(const vbucket_map_t &m) {

    vbucket_map_t::const_iterator iter;
    uint16_t vbucketId;
    vbucket_state_t state;
    uint64_t checkpointId;
    bool success = false;
    hrtime_t start = gethrtime();

    for (iter = m.begin(); iter != m.end(); ++iter) {
        const vbucket_state vbstate = iter->second;
        vbucketId = iter->first.first;
        state = vbstate.state;
        checkpointId = vbstate.checkpointId;

        success = setVBucketState(vbucketId, state, checkpointId);
        if (!success) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: Failed to set new state, %s, for vbucket %d\n",
                             VBucket::toString(state), vbucketId);
            break;
        }
    }

    epStats.snapshotVbucketHisto.add((gethrtime() - start) / 1000);
    return success;
}

bool CouchKVStore::snapshotStats(const std::map<std::string, std::string> &) {
    // noop, virtual function implementation for abstract base class
    return true;
}

bool CouchKVStore::setVBucketState(uint16_t vbucketId, vbucket_state_t state,
                                   uint64_t checkpointId) {
    Db *db = NULL;
    LocalDoc lDoc;

    int rev;
    couchstore_error_t errorCode;
    uint16_t fileRev, newFileRev;
    std::stringstream id;
    std::string dbFileName;
    std::map<uint16_t, int>::iterator mapItr;

    id << vbucketId;
    dbFileName = configuration.getDbname() + "/" + id.str() + ".couch";
    mapItr = dbFileMap.find(vbucketId);
    if (mapItr == dbFileMap.end()) {
        rev = checkNewDbFile(dbFileName);
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
            std::stringstream fileName;
            fileName << vbucketId << ".couch." << fileRev;
            dbFileName = configuration.getDbname() + "/" + fileName.str();
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to open database, name=%s error=%s\n",
                             dbFileName.c_str(),
                             couchstore_strerror(errorCode));
            return false;
        }

        fileRev = newFileRev;
        std::stringstream jsonState;
        jsonState << "{\"state\": \"" << VBucket::toString(state)
                  << "\", \"checkpoint_id\": \"" << checkpointId << "\"}";

        lDoc.id.buf =  (char *)"_local/vbstate";
        lDoc.id.size = sizeof("_local/vbstate") - 1;
        lDoc.json.buf = (char *)jsonState.str().c_str();
        lDoc.json.size = jsonState.str().size();
        lDoc.deleted = 0;

        errorCode = couchstore_save_local_document(db, &lDoc);
        if (errorCode != COUCHSTORE_SUCCESS) {
           getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to save local doc, name=%s error=%s\n",
                            dbFileName.c_str(),
                            couchstore_strerror(errorCode));
           closeDatabaseHandle(db);
           return false;
        }

        errorCode = couchstore_commit(db);
        if (errorCode != COUCHSTORE_SUCCESS) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Commit failed: %s",
                             couchstore_strerror(errorCode));
            // TODO, error handling
            abort();
        } else {
            uint64_t newHeaderPos = couchstore_get_header_position(db);
            RememberingCallback<uint16_t> lcb;

            mc->notify_update(vbucketId, fileRev, newHeaderPos,
                              true, state, checkpointId, lcb);
            if (lcb.val != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                if (lcb.val == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Retry notify CouchDB of update, "
                                     "vbid=%u, rev=%u\n", vbucketId, fileRev);
                    retry = true;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Failed to notify CouchDB of update, "
                                     "vbid=%u, rev=%u, error=0x%x\n",
                                     vbucketId, fileRev, lcb.val);
                    // TODO, better error handling
                    abort();
                }
            }
        }
        closeDatabaseHandle(db);
    }
    return true;
}


void CouchKVStore::dump(shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    tap(callback, false, NULL);
}

void CouchKVStore::dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    tap(callback, false, &vbids);
}

void CouchKVStore::dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    (void)vbids;
    tap(callback, true, NULL);
}


StorageProperties CouchKVStore::getStorageProperties() {
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}

bool CouchKVStore::commit(void) {
    // TODO get rid of bogus intransaction business
    assert(intransaction);
    intransaction = commit2couchstore() ? false : true;
    return !intransaction;

}

void CouchKVStore::addStats(const std::string &prefix,
                            ADD_STAT add_stat,
                            const void *c) {
    //TODO CouchKVStore::addState()
    KVStore::addStats(prefix, add_stat, c);
    addStat(prefix, "vbucket_batch_count", vbBatchCount, add_stat, c);
    addStat(prefix, "vbucket_batch_size", vbBatchSize, add_stat, c);
    mc->addStats(prefix, add_stat, c);
}

template <typename T>
void CouchKVStore::addStat(const std::string &prefix, const char *nm, T val,
                        ADD_STAT add_stat, const void *c) {
    std::stringstream name;
    name << prefix << ":" << nm;
    std::stringstream value;
    value << val;
    std::string n = name.str();
    add_stat(n.data(), static_cast<uint16_t>(n.length()),
             value.str().data(),
             static_cast<uint32_t>(value.str().length()),
             c);

    return;
}

void CouchKVStore::optimizeWrites(std::vector<queued_item> &items) {
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsByVBAndKey cq;
    std::sort(items.begin(), items.end(), cq);
}

void CouchKVStore::processTxnSizeChange(size_t txn_size) {
    // TODO remove this once CouchKVStore::addStat is ready
    size_t new_batch_size = txn_size / vbBatchCount;
    vbBatchSize = new_batch_size == 0 ? vbBatchSize : new_batch_size;
}

void CouchKVStore::setVBBatchCount(size_t batch_count) {
    if (vbBatchCount == batch_count) {
        return;
    }
    vbBatchCount = batch_count;
    size_t new_batch_size = engine.getEpStore()->getTxnSize() / vbBatchCount;
    vbBatchSize = new_batch_size == 0 ? vbBatchSize : new_batch_size;
}

void CouchKVStore::tap(shared_ptr<TapCallback> cb, bool keysOnly,
                       std::vector<uint16_t> *vbids) {
    std::string dirname = configuration.getDbname();
    std::vector<std::string> files = std::vector<std::string>();
    std::map<uint16_t, int> &filemap = dbFileMap;
    std::map<uint16_t, int> vbmap;

    if (dbFileMap.empty()) {
        // warmup, first discover db files from local directory
        if (discoverDbFiles(dirname, files)) {
            populateFileNameMap(files);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: Data diretory is empty, %s\n",
                             dirname.c_str());
            return;
        }
    }

    if (vbids) {
        // get entries for given vbucket(s) from dbFileMap
        getFileNameMap(vbids, dirname, vbmap);
        filemap = vbmap;
    }

    Db *db = NULL;
    couchstore_error_t errorCode;
    int keyNum = 0;
    std::map<uint16_t, int>::iterator itr = filemap.begin();
    for (; itr != filemap.end(); itr++, keyNum++) {
        errorCode = openDB(itr->first, itr->second, &db, 0);
        if (!errorCode) {
            TapResponseCtx ctx;
            ctx.vbucketId = itr->first;
            ctx.keysonly = keysOnly;
            ctx.callback = cb;
            errorCode = couchstore_changes_since(db, 0, 0, recordDbDumpC,
                                                 static_cast<void *>(&ctx));
            if (errorCode) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "couchstore_changes_since failed, error=%s\n",
                                 couchstore_strerror(errorCode));
                // TODO abort or return error?
                abort();
            }
        } else {
            if (errorCode == COUCHSTORE_ERROR_OPEN_FILE ||
                errorCode == COUCHSTORE_ERROR_NO_HEADER)
            {
                db = NULL;
                continue;
            } else {
                std::stringstream rev, vbid;
                rev  << itr->second;
                vbid << itr->first;
                std::string dbName = dirname + "/" + vbid.str() + ".couch." +
                                     rev.str();
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to open database, name=%s error=%s",
                                 dbName.c_str(),
                                 couchstore_strerror(errorCode));
                // TODO abort of return error?
                abort();
            }
        }
        closeDatabaseHandle(db);
        db = NULL;
    }

    bool success = true;
    cb->complete->callback(success);
}

void CouchKVStore::open() {
    // TODO intransaction, is it needed?
    intransaction = false;
    delete mc;
    mc = new MemcachedEngine(&engine, configuration);
}

void CouchKVStore::close() {
    intransaction = false;
    delete mc;
    mc = NULL;
}

bool CouchKVStore::getDbFile(uint16_t vbucketId,
                             std::string &dbFileName) {
    std::stringstream fileName;
    fileName << configuration.getDbname() << "/" << vbucketId << ".couch";
    std::map<uint16_t, int>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr == dbFileMap.end()) {
       dbFileName = fileName.str();
       int rev = checkNewDbFile(dbFileName);
       if (rev > 0) {
           updateDbFileMap(vbucketId, rev, true);
           fileName << "." << rev;
       } else {
           getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "Database file does not exist, %s\n",
                            dbFileName.c_str());
           return false;
       }
    } else {
           fileName  <<  "." << itr->second;
    }

    dbFileName = fileName.str();
    return true;
}

int CouchKVStore::checkNewRevNum(std::string &dbname, bool newFile) {
    int newrev = 0;
    glob_t fglob;

    std::string filename, revnum, nameKey;
    size_t secondDot;

    if (!newFile) {
        // extract out the file revision number first
        secondDot = dbname.rfind(".");
        nameKey = dbname.substr(0, secondDot);
        nameKey.append(".*", 2);
    } else {
        nameKey = dbname;
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
        revnum = filename.substr(secondDot+1);
        if (newrev < atoi(revnum.c_str())) {
            newrev = atoi(revnum.c_str());
            dbname = filename;
        }
    }
    globfree(&fglob);
    return newrev;
}

void CouchKVStore::updateDbFileMap(uint16_t vbucketId, int newFileRev,
                                  bool insertImmediately) {
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


couchstore_error_t CouchKVStore::openDB(uint16_t vbucketId,
                                        uint16_t fileRev,
                                        Db **db,
                                        uint64_t options,
                                        uint16_t *newFileRev) {
    couchstore_error_t errorCode;
    std::stringstream fileName;
    fileName << vbucketId << ".couch." << fileRev;
    std::string dbName = configuration.getDbname() + "/" + fileName.str();

    int newRevNum = fileRev;
    // first try to open database without options, we don't want to create
    // a duplicate db that has the same name with different revision number
    if ((errorCode = couchstore_open_db(dbName.c_str(), 0, db))) {
        if ((newRevNum = checkNewRevNum(dbName))) {
            errorCode = couchstore_open_db(dbName.c_str(), 0, db);
            if (errorCode == COUCHSTORE_SUCCESS) {
                updateDbFileMap(vbucketId, newRevNum);
            }
        } else {
            if (options) {
                newRevNum = fileRev;
                errorCode = couchstore_open_db(dbName.c_str(), options, db);
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
                         "open_db() failed to open database file, %s\n",
                          dbName.c_str());
    }
    return errorCode;
}

void CouchKVStore::getFileNameMap(std::vector<uint16_t> *vbids,
                                  std::string &dirname,
                                  std::map<uint16_t, int> &filemap) {
    std::stringstream nameKey;
    std::string dbName;
    std::vector<uint16_t>::iterator vbidItr;
    std::map<uint16_t, int>::iterator dbFileItr;

    for (vbidItr = vbids->begin(); vbidItr != vbids->end(); vbidItr++) {
        nameKey << dirname << "/" << *vbidItr << ".couch";
        dbName = nameKey.str();
        dbFileItr = dbFileMap.find(*vbidItr);
        if (dbFileItr == dbFileMap.end()) {
            int rev = checkNewDbFile(dbName);
            if (rev > 0) {
                filemap.insert(std::pair<uint16_t, int>(*vbidItr, rev));
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Database file does not exist, %s\n",
                                  dbName.c_str());
                // TODO abort or continue?
                abort();
            }
        } else {
            filemap.insert(std::pair<uint16_t, int>(dbFileItr->first,
                                                    dbFileItr->second));
        }
        nameKey.flush();
    }
}

void CouchKVStore::populateFileNameMap(std::vector<std::string> &filenames) {
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

        revNumStr = filename.substr(secondDot+1);
        revNum = atoi(revNumStr.c_str());
        vbIdStr = nameKey.substr(firstSlash+1, (firstDot - firstSlash));
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

int CouchKVStore::recordDbDump(Db* db, DocInfo* docinfo, void *ctx) {
    Item *it = NULL;
    Doc *doc = NULL;
    TapResponseCtx *tapCtx = (TapResponseCtx *)ctx;
    shared_ptr<TapCallback> callback = tapCtx->callback;

    // TODO enable below when couchstore is ready
    // bool compressed = (docinfo->content_meta & 0x80);
    // uint8_t metaflags = (docinfo->content_meta & ~0x80);

    void *valuePtr;
    size_t valuelen;
    sized_buf  metadata = docinfo->rev_meta;
    uint32_t itemflags;
    uint16_t vbucketId = tapCtx->vbucketId;
    sized_buf key = docinfo->id;
    uint64_t cas;
    uint32_t exptime;

    assert(key.size <= UINT16_MAX);
    assert(metadata.size == 16);

    memcpy(&cas, metadata.buf, 8);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    memcpy(&itemflags, (metadata.buf) + 12, 4);
    itemflags = ntohs(itemflags);

    valuePtr = NULL;
    valuelen = 0;
    if (!tapCtx->keysonly) {
        couchstore_error_t errCode;
        errCode = couchstore_open_doc_with_docinfo(db, docinfo, &doc, 0);

        if (errCode == COUCHSTORE_SUCCESS) {
            if (doc->data.size) {
                valuelen = doc->data.size;
                valuePtr = doc->data.buf;
            }
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to retrieve key value from database, "
                             "vBucket=%d key=%s error=%s\n",
                             vbucketId, key.buf, couchstore_strerror(errCode));
            abort();
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

    GetValue rv(it, ENGINE_SUCCESS, -1, -1, NULL, tapCtx->keysonly);
    callback->cb->callback(rv);

    couchstore_free_document(doc);
    return 0;
}

bool CouchKVStore::commit2couchstore(void) {
    Doc **docs;
    DocInfo **docinfos;
    uint16_t vbucket2flush, vbucketId;
    int reqIndex,  flushStartIndex, numDocs2save;
    int errCode = 0;
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
         reqIndex++, numDocs2save++, pendingCommitCnt-- ) {
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
                // TODO error handling instead of assert
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to save CouchDB docs, vbucket = %d rev = %d\n",
                                 vbucket2flush,
                                 committedReqs[flushStartIndex]->getRevNum());
                abort();
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
                             "Failed to save CouchDB docs, vbucket = %d rev = %d\n",
                             vbucket2flush,
                             committedReqs[flushStartIndex]->getRevNum());
            abort();
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

int CouchKVStore::saveDocs(uint16_t vbid, int rev, Doc **docs, DocInfo **docinfos,
                           int docCount) {
    int errCode, fileRev;
    uint16_t newFileRev;
    uint16_t vbucket2save = vbid;
    Db *db = NULL;
    bool retry_save_docs;

    fileRev = rev;
    assert(fileRev);

    do {
        retry_save_docs = false;
        errCode = openDB(vbucket2save, fileRev, &db, 0, &newFileRev);
        if (errCode) {
            //TODO ERROR RETURN OR ABORT??
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to open database, vbucketId = %d fileRev = %d "
                             "error = %d\n",
                             vbucket2save, fileRev, errCode);
            return errCode;
        } else {
            couchstore_error_t err;
            err = couchstore_save_documents(db, docs, docinfos, docCount,
                                            0 /* no options */);
            if (err != COUCHSTORE_SUCCESS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to save docs to database, error = %s\n",
                                 couchstore_strerror(err));
                return (int)err;
            }

            err = couchstore_commit(db);
            if (err) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Commit failed: %s",
                                 couchstore_strerror(err));
                return (int)err;
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
                                     "Failed to notify CouchDB of update, vbucket=%d, "
                                     "error= %d\n",
                                     vbucket2save, cb.val);
                    abort();
                }
            }
            closeDatabaseHandle(db);
        }
    } while (retry_save_docs);

    return errCode;
}

void CouchKVStore::queue(CouchRequest &req) {

    pendingReqsQ.push_back(&req);
    pendingCommitCnt++;

    return;
}

void CouchKVStore::remVBucketFromDbFileMap(uint16_t vbucketId) {
    std::map<uint16_t, int>::iterator itr;

    itr = dbFileMap.find(vbucketId);
    if (itr != dbFileMap.end()) {
        dbFileMap.erase(itr);
    }

    return;
}

void CouchKVStore::commitCallback(CouchRequest **committedReqs, int numReqs,
                                  int errCode) {
    bool isDelete;
    size_t dataSize;
    hrtime_t spent;

    Callback<mutation_result> *setCb = NULL;
    Callback<int> *delCb = NULL;

    for (int index = 0; index < numReqs; index++) {
        isDelete = committedReqs[index]->isDelete();
        dataSize = committedReqs[index]->getNBytes();
        dataSize = (dataSize) ? dataSize : 1;
        spent = committedReqs[index]->getDelta() / dataSize;

        if (isDelete) {
            int value = 1;
            epStats.couchDelqHisto.add(spent);
            delCb = committedReqs[index]->getDelCallback();
            delCb->callback(value);
        } else {
            if (errCode) {
                epStats.couchSetFailHisto.add(spent);
            } else {
                epStats.couchSetHisto.add(spent);
            }
            setCb = committedReqs[index]->getSetCallback();
            mutation_result p(1, committedReqs[index]->getItemId());
            setCb->callback(p);
        }
    }
}

int CouchKVStore::recordDbStat(Db* db, DocInfo*, void *ctx) {
    int errCode;
    sized_buf id;
    LocalDoc *ldoc = NULL;
    StatResponseCtx *statCtx = (StatResponseCtx *)ctx;
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &rv = statCtx->statMap;
    std::pair<uint16_t, uint16_t> vb(statCtx->vbId, -1);

    id.buf = (char *)"_local/vbstate";
    id.size = sizeof("_local/vbstate") - 1;
    errCode = couchstore_open_local_document(db, (uint8_t *)id.buf,
                                             id.size, &ldoc);
    if (errCode) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to retrieve stat info for vBucket=%d error=%d\n",
                         statCtx->vbId, errCode);
        abort();
    }

    const std::string statjson(ldoc->json.buf, ldoc->json.size);
    cJSON *jsonObj = cJSON_Parse(statjson.c_str());
    const std::string state = getJSONObjString(cJSON_GetObjectItem(jsonObj, "state"));
    const std::string checkpoint_id = getJSONObjString(cJSON_GetObjectItem(jsonObj,
                                                       "checkpoint_id"));
    cJSON_Delete(jsonObj);
    if (state.compare("") == 0 || checkpoint_id.compare("") == 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Warning: State JSON doc for vbucket %d is in the wrong format: %s",
                vb.first, statjson.c_str());
    }

    vbucket_state vb_state;
    vb_state.state = VBucket::fromString(state.c_str());
    char *ptr = NULL;
    vb_state.checkpointId = strtoull(checkpoint_id.c_str(), &ptr, 10);
    // TODO: fix the maxDeletedSeqno in the vbstate document. We are
    // just fixing compiler warnings here.
    vb_state.maxDeletedSeqno = 0;
    rv[vb] = vb_state;

    couchstore_free_local_document(ldoc);
    return 0;
}
