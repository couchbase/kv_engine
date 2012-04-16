/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>

#include "common.hh"
#include "ep_engine.h"
#include "stats.hh"
#include "kvstore.hh"
#include "sqlite-kvstore.hh"
#include "mc-kvstore/mc-kvstore.hh"
#include "blackhole-kvstore/blackhole.hh"
#include "warmup.hh"

KVStore *KVStoreFactory::create(EventuallyPersistentEngine &theEngine) {
    Configuration &c = theEngine.getConfiguration();

    KVStore *ret = NULL;
    std::string backend = c.getBackend();
    if (backend.compare("sqlite") == 0) {
        ret = SqliteKVStoreFactory::create(theEngine);
    } else if (backend.compare("couchdb") == 0) {
        ret = new MCKVStore(theEngine);
    } else if (backend.compare("blackhole") == 0) {
        ret = new BlackholeKVStore(theEngine);
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Unknown backend: [%s]",
                backend.c_str());
    }

    if (ret != NULL) {
        ret->setEngine(&theEngine);
    }

    return ret;
}

struct WarmupCookie {
    WarmupCookie(KVStore *s, Callback<GetValue>&c) :
        store(s), cb(c), loaded(0), skipped(0), error(0)
    { /* EMPTY */ }
    KVStore *store;
    Callback<GetValue> &cb;
    size_t loaded;
    size_t skipped;
    size_t error;
};

static void warmupCallback(void *arg, uint16_t vb, uint16_t vbver,
                           const std::string &key, uint64_t rowid)
{
    WarmupCookie *cookie = static_cast<WarmupCookie*>(arg);

    /* @todo check if we should start skipping items!!! */
    RememberingCallback<GetValue> cb;
    cookie->store->get(key, rowid, vb, vbver, cb);
    cb.waitForValue();

    if (cb.getStatus() == ENGINE_SUCCESS) {
        cookie->cb.callback(cb.val);
        cookie->loaded++;
    } else {
        cookie->error++;
    }
}

size_t KVStore::warmup(MutationLog &lf,
                       const std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &vbmap,
                       Callback<GetValue> &cb,
                       Callback<size_t> &estimate)
{
    MutationLogHarvester harvester(lf);
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state>::const_iterator it;
    for (it = vbmap.begin(); it != vbmap.end(); ++it) {
        harvester.setVbVer(it->first.first, it->first.second);
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

    WarmupCookie cookie(this, cb);
    start = gethrtime();
    harvester.apply(&cookie, &warmupCallback);
    end = gethrtime();

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Populated log in %s with (l: %ld, s: %ld, e: %ld)",
                     hrtime2text(end - start).c_str(),
                     cookie.loaded, cookie.skipped, cookie.error);

    return cookie.loaded;
}

bool KVStore::getEstimatedItemCount(size_t &) {
    // Not supported
    return false;
}
