/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"

#include <map>
#include <string>

#include "blackhole-kvstore/blackhole.h"
#include "common.h"
#ifdef HAVE_LIBCOUCHSTORE
#include "couch-kvstore/couch-kvstore.h"
#else
#include "couch-kvstore/couch-kvstore-dummy.h"
#endif
#include "ep_engine.h"
#include "kvstore.h"
#include "stats.h"
#include "warmup.h"


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
