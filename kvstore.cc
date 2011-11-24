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

KVStore *KVStoreFactory::create(EventuallyPersistentEngine &theEngine) {
    Configuration &c = theEngine.getConfiguration();

    std::string backend = c.getBackend();
    if (backend.compare("sqlite") == 0) {
        return SqliteKVStoreFactory::create(theEngine);
    } else if (backend.compare("couchdb") == 0) {
        return new MCKVStore(theEngine);
    } else if (backend.compare("blackhole") == 0) {
        return new BlackholeKVStore(theEngine);
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Unknown backend: [%s]",
                backend.c_str());
    }

    return NULL;
}

