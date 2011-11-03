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
    assert(intransaction);
    mc->setmq(itm, cb);
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

    assert(intransaction);
    mc->delq(key, vb, cb);
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

void MCKVStore::vbStateChanged(uint16_t vbucket, vbucket_state_t newState) {
    RememberingCallback<bool> cb;
    mc->setVBucket(vbucket, newState, cb);
    cb.waitForValue();
}

bool MCKVStore::snapshotVBuckets(const vbucket_map_t &m) {
    if (m.size() == 0) {
        return true;
    }
    hrtime_t start = gethrtime();
    RememberingCallback<bool> cb;
    mc->snapshotVBuckets(m, cb);
    cb.waitForValue();
    stats.snapshotVbucketHisto.add((gethrtime() - start) / 1000);
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
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}

void MCKVStore::open() {
    // Wake Up!
    intransaction = false;
    delete mc;
    mc = new MemcachedEngine(&engine, config);
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

void MCKVStore::addStats(const std::string &prefix,
                         ADD_STAT add_stat,
                         const void *c)
{
    KVStore::addStats(prefix, add_stat, c);
    mc->addStats(prefix, add_stat, c);
}

void MCKVStore::optimizeWrites(std::vector<queued_item> &items) {
    CompareQueuedItemsByVBAndKey cq;
    // Make sure that the items are sorted in the ascending order.
    assert(sorted(items.begin(), items.end(), cq));
}
