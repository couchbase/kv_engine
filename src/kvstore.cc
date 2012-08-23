/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>

#include "common.hh"
#include "ep_engine.h"
#include "stats.hh"
#include "kvstore.hh"
#include "blackhole-kvstore/blackhole.hh"
#include "warmup.hh"
#ifdef HAVE_LIBCOUCHSTORE
#include "couch-kvstore/couch-kvstore.hh"
#else
#include "couch-kvstore/couch-kvstore-dummy.hh"
#endif

KVStore *KVStoreFactory::create(EventuallyPersistentEngine &theEngine,
                                bool read_only) {
    Configuration &c = theEngine.getConfiguration();

    KVStore *ret = NULL;
    std::string backend = c.getBackend();
    if (backend.compare("couchdb") == 0) {
        ret = new CouchKVStore(theEngine, read_only);
    } else if (backend.compare("blackhole") == 0) {
        ret = new BlackholeKVStore(read_only);
    } else {
        LOG(EXTENSION_LOG_WARNING, "Unknown backend: [%s]", backend.c_str());
    }

    if (ret != NULL) {
        ret->setEngine(&theEngine);
    }

    return ret;
}

bool KVStore::getEstimatedItemCount(size_t &) {
    // Not supported
    return false;
}
