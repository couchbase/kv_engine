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

#include <platform/dirutils.h>

#include "callbacks.h"
#include "common.h"
#include "kvstore.h"

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL);
    }
}

class WriteCallback : public Callback<mutation_result> {
public:
    WriteCallback() {}

    void callback(mutation_result &result) {

    }

};

class StatsCallback : public Callback<kvstats_ctx> {
public:
    StatsCallback() {}

    void callback(kvstats_ctx &result) {

    }

};

class GetCallback : public Callback<GetValue> {
public:
    GetCallback() {}

    void callback(GetValue &result) {
        cb_assert(strncmp("value", result.getValue()->getData(), 5) == 0);
    }

};

void basic_kvstore_test(std::string& backend) {
    std::string data_dir("/tmp/kvstore-test");

    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, backend, 0);
    KVStore* kvstore = KVStoreFactory::create(config);

    StatsCallback sc;
    std::string failoverLog("");
    vbucket_state state(vbucket_state_active, 0, 0, 0, 0, 0, 0, 0, 0,
                        failoverLog);
    kvstore->snapshotVBucket(0, state, &sc);

    kvstore->begin();

    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->set(item, wc);

    kvstore->commit(&sc);

    GetCallback gc;
    kvstore->get("key", 0, gc);
    delete kvstore;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));
    std::string backend("couchdb");
    basic_kvstore_test(backend);
    //backend = "forestdb";
    //basic_kvstore_test(backend);
}
