/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <platform/dirutils.h>

#include "kvstore.h"

// Regression test for MB-17517 - ensure that if a couchstore file has a max
// CAS of -1, it is detected and reset to zero when file is loaded.
static void mb_17517_max_cas_of_minus_1_test() {
    std::string data_dir("/tmp/kvstore-test");

    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    Configuration config;
    config.setDbname(data_dir);
    KVStore* kvstore = KVStoreFactory::create(config);
    cb_assert(kvstore);

    // Activate vBucket.
    std::string failoverLog("[]");
    vbucket_state state(vbucket_state_active, /*ckid*/0, /*maxDelSeqNum*/0,
                        /*highSeqno*/0, /*purgeSeqno*/0, /*lastSnapStart*/0,
                        /*lastSnapEnd*/0, /*maxCas*/-1, /*driftCounter*/0,
                        failoverLog);
    cb_assert(kvstore->snapshotVBucket(/*vbid*/0, state, NULL));
    cb_assert(kvstore->listPersistedVbuckets()[0]->maxCas == ~0ull);

    // Close the file, then re-open.
    delete kvstore;
    kvstore = KVStoreFactory::create(config);
    cb_assert(kvstore);

    // Check that our max CAS was repaired on startup.
    cb_assert(kvstore->listPersistedVbuckets()[0]->maxCas == 0);

    // Cleanup
    delete kvstore;
}


int main(int argc, char **argv) {
    (void)argc; (void)argv;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    mb_17517_max_cas_of_minus_1_test();
}
