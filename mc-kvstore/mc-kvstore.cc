/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include "mc-kvstore/mc-kvstore.hh"
#include "mc-kvstore/mc-engine.hh"
#include "ep_engine.h"
#include "tools/cJSON.h"


#define STATWRITER_NAMESPACE mckv_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE


static std::string getStringFromJSONObj(cJSON *i) {
    if (i == NULL) {
        return "";
    }
    assert(i->type == cJSON_String);
    return i->valuestring;
}


MCKVStore::MCKVStore(EventuallyPersistentEngine &theEngine, bool read_only) :
    KVStore(read_only), stats(theEngine.getEpStats()), intransaction(false), mc(NULL),
    config(theEngine.getConfiguration()), engine(theEngine),
    vbBatchCount(config.getCouchVbucketBatchCount()) {

    vbBatchSize = config.getMaxTxnSize() / vbBatchCount;
    vbBatchSize = vbBatchSize == 0 ? config.getCouchDefaultBatchSize() : vbBatchSize;
    open();
}

MCKVStore::MCKVStore(const MCKVStore &from) :
    KVStore(from), stats(from.stats), intransaction(false), mc(NULL),
    config(from.config), engine(from.engine),
    vbBatchCount(from.vbBatchCount), vbBatchSize(from.vbBatchSize) {
    open();
}

void MCKVStore::reset() {
    assert(!isReadOnly());
    // @todo what is a clean state?
    // I guess we should probably create a new message to send
    //       directly to mcd to avoid having a lot of ping-pongs
    RememberingCallback<bool> cb;
    mc->flush(cb);
    cb.waitForValue();
}

void MCKVStore::set(const Item &itm, Callback<mutation_result> &cb) {
    assert(!isReadOnly());
    assert(intransaction);
    mc->setmq(itm, cb);
}

void MCKVStore::get(const std::string &key, uint64_t, uint16_t vb,
        Callback<GetValue> &cb) {
    if (dynamic_cast<RememberingCallback<GetValue> *> (&cb)) {
        mc->get(key, vb, cb);
    } else {
        RememberingCallback<GetValue> mcb;
        mc->get(key, vb, mcb);
        mcb.waitForValue();
        cb.callback(mcb.val);
    }
}

void MCKVStore::del(const Item &itm, uint64_t, Callback<int> &cb) {
    assert(!isReadOnly());
    assert(intransaction);
    mc->delmq(itm, cb);
}

bool MCKVStore::delVBucket(uint16_t vbucket) {
    assert(!isReadOnly());
    RememberingCallback<bool> cb;
    mc->delVBucket(vbucket, cb);
    cb.waitForValue();
    return cb.val;
}

vbucket_map_t MCKVStore::listPersistedVbuckets() {
    RememberingCallback<std::map<std::string, std::string> > cb;
    mc->stats("vbucket", cb);

    cb.waitForValue();
    std::map<uint16_t, vbucket_state> rv;
    std::map<std::string, std::string>::const_iterator iter;
    for (iter = cb.val.begin(); iter != cb.val.end(); ++iter) {
        uint16_t vbid = (uint16_t)atoi(iter->first.c_str() + 3);
        const std::string &state_json = iter->second;
        cJSON *jsonObj = cJSON_Parse(state_json.c_str());
        std::string state =
            getStringFromJSONObj(cJSON_GetObjectItem(jsonObj, "state"));
        std::string checkpoint_id =
            getStringFromJSONObj(cJSON_GetObjectItem(jsonObj, "checkpoint_id"));
        std::string max_deleted_seqno =
            getStringFromJSONObj(cJSON_GetObjectItem(jsonObj,
                                                     "max_deleted_seqno"));
        cJSON_Delete(jsonObj);
        if (state.compare("") == 0
            || checkpoint_id.compare("") == 0
            || max_deleted_seqno.compare("") == 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Warning: State JSON doc for vbucket %d is in the wrong format: %s",
                vbid, state_json.c_str());
            continue;
        }

        vbucket_state vb_state;
        vb_state.state = VBucket::fromString(state.c_str());
        char *ptr = NULL;
        vb_state.checkpointId = strtoull(checkpoint_id.c_str(), &ptr, 10);
        vb_state.maxDeletedSeqno = strtoul(max_deleted_seqno.c_str(), &ptr,
                                            10);
        rv[vbid] = vb_state;
    }

    return rv;
}

bool MCKVStore::snapshotVBuckets(const vbucket_map_t &m) {
    assert(!isReadOnly());
    if (m.size() == 0) {
        return true;
    }
    RememberingCallback<bool> cb;
    mc->snapshotVBuckets(m, cb);
    cb.waitForValue();
    return cb.val;
}

bool MCKVStore::snapshotStats(const std::map<std::string, std::string> &m) {
    (void)m;
    // abort();
    return true;
}

void MCKVStore::dump(shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    mc->tap(callback);
    if (!isKeyDumpSupported()) {
        wait->waitForValue();
    }
}

void MCKVStore::dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    mc->tap(vbids, true, callback);
    wait->waitForValue();
}

void MCKVStore::dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb) {
    shared_ptr<RememberingCallback<bool> > wait(new RememberingCallback<bool>());
    shared_ptr<TapCallback> callback(new TapCallback(cb, wait));
    (void)vbids;
    mc->tapKeys(callback);
    wait->waitForValue();
}

StorageProperties MCKVStore::getStorageProperties() {
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true,
                         true, false);
    return rv;
}

void MCKVStore::open() {
    // Wake Up!
    intransaction = false;
    delete mc;
    mc = new MemcachedEngine(&engine, config);
    RememberingCallback<bool> cb;
    mc->setVBucketBatchCount(vbBatchCount, &cb);
    cb.waitForValue();
}

void MCKVStore::close() {
    intransaction = false;
    delete mc;
    mc = NULL;
}


bool MCKVStore::commit(void) {
    assert(!isReadOnly());
    // Trying to commit without an begin is a semantically bogus
    // way to do stuff
    assert(intransaction);

    RememberingCallback<bool> wait;
    mc->noop(wait);
    wait.waitForValue();
    intransaction = wait.val ? false : true;
    // This is somewhat bogus, because we don't support "real"
    // transactions.. Some of the objects in the transaction
    // may have failed so the caller needs to check the
    // callback status for all of the items it added..
    return !intransaction;
}

void MCKVStore::addStats(const std::string &prefix,
                         ADD_STAT add_stat,
                         const void *c)
{
    KVStore::addStats(prefix, add_stat, c);
    addStat(prefix, "vbucket_batch_count", vbBatchCount, add_stat, c);
    addStat(prefix, "vbucket_batch_size", vbBatchSize, add_stat, c);
    mc->addStats(prefix, add_stat, c);
}

template <typename T>
void MCKVStore::addStat(const std::string &prefix, const char *nm, T val,
                        ADD_STAT add_stat, const void *c) {
    std::stringstream name;
    name << prefix << ":" << nm;
    std::stringstream value;
    value << val;
    std::string n = name.str();

    add_casted_stat(n.data(), value.str().data(), add_stat, c);
}

void MCKVStore::optimizeWrites(std::vector<queued_item> &items) {
    assert(!isReadOnly());
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsByVBAndKey cq;
    std::sort(items.begin(), items.end(), cq);

    size_t pos = 0;
    uint16_t current_vbid = items[0]->getVBucketId();
    std::vector< std::vector<queued_item> > vb_chunks;
    std::vector<queued_item> chunk;
    std::vector<queued_item>::iterator it = items.begin();
    for (; it != items.end(); ++it) {
        bool moveToNextVB = current_vbid != (*it)->getVBucketId() ? true : false;
        if (!moveToNextVB) {
            chunk.push_back(*it);
        }
        if (chunk.size() == vbBatchSize || moveToNextVB || (it + 1) == items.end()) {
            if (pos < vb_chunks.size()) {
                std::vector<queued_item> &chunk_items = vb_chunks[pos];
                chunk_items.insert(chunk_items.end(), chunk.begin(), chunk.end());
            } else {
                vb_chunks.push_back(chunk);
            }
            chunk.clear();
            if (moveToNextVB) {
                chunk.push_back(*it);
                current_vbid = (*it)->getVBucketId();
                pos = 0;
            } else {
                ++pos;
            }
        }
    }
    if (!chunk.empty()) {
        assert(pos < vb_chunks.size());
        std::vector<queued_item> &chunk_items = vb_chunks[pos];
        chunk_items.insert(chunk_items.end(), chunk.begin(), chunk.end());
    }

    items.clear();
    std::vector< std::vector<queued_item> >::iterator iter = vb_chunks.begin();
    for (; iter != vb_chunks.end(); ++iter) {
        items.insert(items.end(), iter->begin(), iter->end());
    }
}

void MCKVStore::processTxnSizeChange(size_t txn_size) {
    size_t new_batch_size = txn_size / vbBatchCount;
    vbBatchSize = new_batch_size == 0 ? vbBatchSize : new_batch_size;
}

void MCKVStore::setVBBatchCount(size_t batch_count) {
    if (vbBatchCount == batch_count) {
        return;
    }
    vbBatchCount = batch_count;
    size_t new_batch_size = engine.getEpStore()->getTxnSize() / vbBatchCount;
    vbBatchSize = new_batch_size == 0 ? vbBatchSize : new_batch_size;
    mc->setVBucketBatchCount(vbBatchCount, NULL);
}
