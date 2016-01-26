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

#include "callbacks.h"
#include "compress.h"
#include "kvstore.h"

#include <gtest/gtest.h>
#include <unordered_map>

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

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(int64_t s, int64_t e, uint16_t vbid) :
        start(s), end(e), vb(vbid) { }

    void callback(CacheLookup &lookup) {
        EXPECT_EQ(vb, lookup.getVBucketId());
        EXPECT_LE(start, lookup.getBySeqno());
        EXPECT_LE(lookup.getBySeqno(), end);
    }

private:
    int64_t start;
    int64_t end;
    uint16_t vb;
};

class GetCallback : public Callback<GetValue> {
public:
    GetCallback() :
        expectCompressed(false) { }

    GetCallback(bool expect_compressed) :
        expectCompressed(expect_compressed) { }

    void callback(GetValue &result) {
        if (expectCompressed) {
            EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_COMPRESSED,
                      result.getValue()->getDataType());
            snap_buf output;
            EXPECT_EQ(SNAP_SUCCESS,
                      doSnappyUncompress(result.getValue()->getData(),
                                         result.getValue()->getNBytes(),
                                         output));
            EXPECT_EQ(0,
                      strncmp("value",
                              output.buf.get(),
                              output.len));
        } else {
            EXPECT_EQ(0,
                      strncmp("value",
                              result.getValue()->getData(),
                              result.getValue()->getNBytes()));
        }
        delete result.getValue();
    }

private:
    bool expectCompressed;
};

// Creates and initializes a KVStore with the given config
static std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config) {
    auto kvstore = std::unique_ptr<KVStore>(KVStoreFactory::create(config));

    StatsCallback sc;
    std::string failoverLog("");
    vbucket_state state(vbucket_state_active, 0, 0, 0, 0, 0, 0, 0, 0,
                        failoverLog);
    kvstore->snapshotVBucket(0, state, &sc);

    return kvstore;
}

class CouchAndForestTest : public ::testing::TestWithParam<std::string> {
};

class CouchKVStoreTest : public ::testing::TestWithParam<std::string> {
};

/* Test basic set / get of a document */
TEST_P(CouchAndForestTest, BasicTest) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, GetParam(), 0);
    auto kvstore = setup_kv_store(config);

    kvstore->begin();

    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->set(item, wc);

    EXPECT_TRUE(kvstore->commit(nullptr));

    GetCallback gc;
    kvstore->get("key", 0, gc);
}

TEST(CouchKVStoreTest, CompressedTest) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    kvstore->begin();

    uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    WriteCallback wc;
    for (int i = 1; i <= 5; i++) {
        std::string key("key" + std::to_string(i));
        Item item(key.c_str(), key.length(),
                  0, 0, "value", 5, &datatype, 1, 0, i);
        kvstore->set(item, wc);
    }
    StatsCallback sc;
    kvstore->commit(&sc);

    std::shared_ptr<Callback<GetValue> > cb(new GetCallback(true));
    std::shared_ptr<Callback<CacheLookup> > cl(new CacheCallback(1, 5, 0));
    ScanContext* scanCtx;
    scanCtx = kvstore->initScanContext(cb, cl, 0, 1,
                                       DocumentFilter::ALL_ITEMS,
                                       ValueFilter::VALUES_COMPRESSED);

    ASSERT_NE(nullptr, scanCtx);
    EXPECT_EQ(scan_success, kvstore->scan(scanCtx));
    kvstore->destroyScanContext(scanCtx);
}

// Test cases which run on both Couchstore and ForestDB
INSTANTIATE_TEST_CASE_P(CouchstoreAndForestDB,
                        CouchAndForestTest,
                        ::testing::Values("couchdb", "forestdb"));

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    putenv(allow_no_stats_env);

    return RUN_ALL_TESTS();
}
