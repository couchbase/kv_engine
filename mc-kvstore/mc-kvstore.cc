/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include "mc-kvstore.hh"
#include "ep_engine.h"

MCKVStore::MCKVStore(EventuallyPersistentEngine &theEngine) :
    KVStore(), stats(theEngine.getEpStats()), intransaction(false), mc(NULL),
            config(theEngine.getConfiguration()), engine(theEngine) {
    open();
}

MCKVStore::MCKVStore(const MCKVStore &from) :
    KVStore(from), stats(from.stats), intransaction(false), mc(NULL),
            config(from.config), engine(from.engine) {
    open();
}

void MCKVStore::reset() {
    // @todo what is a clean state?
    // I guess we should probably create a new message to send
    //       directly to mcd to avoid having a lot of ping-pongs
    RememberingCallback<bool> cb;
    mc->flush(cb);
    cb.waitForValue();
}

void MCKVStore::set(const Item &itm, uint16_t, Callback<mutation_result> &cb) {
    if (!intransaction) {
        std::cout << "Set outside transaction!" << std::endl;
    }

    if (dynamic_cast<RememberingCallback<mutation_result>*> (&cb)) {
        mc->set(itm, cb);
    } else {
        RememberingCallback<mutation_result> mcb;

        mc->set(itm, mcb);

        mcb.waitForValue();
        cb.callback(mcb.val);
    }
}

void MCKVStore::get(const std::string &key, uint64_t, uint16_t vb, uint16_t,
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

void MCKVStore::del(const std::string &key, uint64_t, uint16_t vb, uint16_t,
        Callback<int> &cb) {
    if (dynamic_cast<RememberingCallback<int> *> (&cb)) {
        mc->del(key, vb, cb);
    } else {
        RememberingCallback<int> mcb;
        mc->del(key, vb, mcb);
        mcb.waitForValue();
        cb.callback(mcb.val);
    }
}

bool MCKVStore::delVBucket(uint16_t vbucket, uint16_t vb_version,
        std::pair<int64_t, int64_t> row_range) {
    (void)vbucket;
    (void)vb_version;
    (void)row_range;

    bool rv = true;
    //abort();
    return rv;
}

bool MCKVStore::delVBucket(uint16_t vbucket, uint16_t) {
    RememberingCallback<bool> cb;
    mc->delVBucket(vbucket, cb);
    cb.waitForValue();
    return cb.val;
}

vbucket_map_t MCKVStore::listPersistedVbuckets() {
    RememberingCallback<std::map<std::string, std::string> > cb;
    mc->stats("vbucket", cb);

    cb.waitForValue();
    // @todo We need to figure out the checkpoints!!!
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    std::map<std::string, std::string>::const_iterator iter;
    for (iter = cb.val.begin(); iter != cb.val.end(); ++iter) {
        std::pair<uint16_t, uint16_t> vb(
                (uint16_t)atoi(iter->first.c_str() + 3), -1);
        vbucket_state vb_state;
        vb_state.state = VBucket::fromString(iter->second.c_str());
        vb_state.checkpointId = 0;
        rv[vb] = vb_state;
    }

    return rv;
}

bool MCKVStore::snapshotVBuckets(const vbucket_map_t &m) {
    vbucket_map_t::const_iterator iter;
    std::list<RememberingCallback<bool>*> callbacks;
    RememberingCallback<bool> *cb = NULL;

    for (iter = m.begin(); iter != m.end(); ++iter) {
        std::pair<uint16_t, uint16_t> first = iter->first;
        const vbucket_state state = iter->second;
        cb = new RememberingCallback<bool>;
        callbacks.push_back(cb);
        mc->setVBucket(first.first, state.state, *cb);
    }

    bool ret = true;
    if (cb != NULL) {
        cb->waitForValue();
        std::list<RememberingCallback<bool>*>::iterator ii;
        for (ii = callbacks.begin(); ii != callbacks.end(); ++ii) {
            if (!(*ii)->val) {
                ret = false;
            }
            delete *ii;
        }
    }
    return ret;
}

bool MCKVStore::snapshotStats(const std::map<std::string, std::string> &m) {
    (void)m;
    // abort();
    return true;
}

void MCKVStore::dump(Callback<GetValue> &cb) {
    RememberingCallback<bool> wait;
    TapCallback callback(cb, wait);
    mc->tap(callback);
    wait.waitForValue();
}

void MCKVStore::dump(uint16_t vb, Callback<GetValue> &cb) {
    RememberingCallback<bool> wait;
    TapCallback callback(cb, wait);
    mc->tap(vb, callback);
    wait.waitForValue();
}

StorageProperties MCKVStore::getStorageProperties() {
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}

void MCKVStore::open() {
    // Wake Up!
    intransaction = false;
    delete mc;
    mc = new MemcachedEngine(&engine, config);
    mc->start();
}

bool MCKVStore::commit(void) {
    // Trying to commit without an begin is a semantically bogus
    // way to do stuff
    assert(intransaction);

    RememberingCallback<bool> wait;
    mc->noop(wait);
    wait.waitForValue();
    intransaction = false;
    // This is somewhat bogus, because we don't support "real"
    // transactions.. Some of the objects in the transaction
    // may have failed so the caller needs to check the
    // callback status for all of the items it added..
    return wait.val;
}
