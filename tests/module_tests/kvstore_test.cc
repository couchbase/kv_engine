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
#include "couch-kvstore/couch-kvstore.h"

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

class BloomFilterCallback : public Callback<std::string&, bool&> {
public:
    BloomFilterCallback() {}
    void callback(std::string& ra, bool& rb) override {}
};

class ExpiryCallback : public Callback<std::string&, uint64_t&> {
public:
    ExpiryCallback() {}
    void callback(std::string& ra, uint64_t& rb) override {}
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

/* Test callback for stats handling.
 * 'cookie' is a std::unordered_map<std::string, std::string) which stats
 * are accumulated in.
 */
static void add_stat_callback(const char *key, const uint16_t klen,
                              const char *val, const uint32_t vlen,
                              const void *cookie) {
    auto* map = reinterpret_cast<std::map<std::string, std::string>*>(
            const_cast<void*>(cookie));
    ASSERT_NE(nullptr, map);
    map->insert(std::make_pair(std::string(key, klen),
                               std::string(val, vlen)));
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

// Verify the stats returned from operations are accurate.
TEST(CouchKVStoreTest, StatsTest) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    kvstore->begin();
    const std::string key{"key"};
    const std::string value{"value"};
    Item item(key.c_str(), key.size(), 0, 0, value.c_str(), value.size());
    WriteCallback wc;
    kvstore->set(item, wc);

    StatsCallback sc;
    EXPECT_TRUE(kvstore->commit(&sc));
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats("", add_stat_callback, &stats);
    EXPECT_EQ("1", stats[":io_num_write"]);
    const size_t io_write_bytes = stoul(stats[":io_write_bytes"]);
    EXPECT_EQ(key.size() + value.size() + COUCHSTORE_METADATA_SIZE,
              io_write_bytes);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and least as many as the actual documents.
    const size_t io_total_write_bytes = stoul(stats[":io_total_write_bytes"]);
    EXPECT_GT(io_total_write_bytes, 0);
    EXPECT_GE(io_total_write_bytes, io_write_bytes);
}

// Verify the compaction stats returned from operations are accurate.
TEST(CouchKVStoreTest, CompactStatsTest) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1, 4, data_dir, "couchdb", 0);
    auto kvstore = setup_kv_store(config);

    // Perform a transaction with a single mutation (set) in it.
    kvstore->begin();
    const std::string key{"key"};
    const std::string value{"value"};
    Item item(key.c_str(), key.size(), 0, 0, value.c_str(), value.size());
    WriteCallback wc;
    kvstore->set(item, wc);

    StatsCallback sc;
    EXPECT_TRUE(kvstore->commit(&sc));

    std::shared_ptr<Callback<std::string&, bool&> >
        filter(new BloomFilterCallback());
    std::shared_ptr<Callback<std::string&, uint64_t&> >
        expiry(new ExpiryCallback());

    compaction_ctx cctx;
    cctx.purge_before_seq = 0;
    cctx.purge_before_ts = 0;
    cctx.curr_time = 0;
    cctx.drop_deletes = 0;
    cctx.db_file_id = 0;
    cctx.max_purged_seq = 0;
    cctx.bloomFilterCallback = filter;
    cctx.expiryCallback = expiry;


    EXPECT_TRUE(kvstore->compactDB(&cctx, sc));
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats("", add_stat_callback, &stats);
    EXPECT_EQ("1", stats[":io_num_write"]);
    const size_t io_write_bytes = stoul(stats[":io_write_bytes"]);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and at least twice as many as the actual documents for
    // the total and once as many for compaction alone.
    const size_t io_total_write_bytes = stoul(stats[":io_total_write_bytes"]);
    const size_t io_compaction_write_bytes = stoul(stats[":io_compaction_write_bytes"]);
    EXPECT_GT(io_total_write_bytes, 0);
    EXPECT_GT(io_compaction_write_bytes, 0);
    EXPECT_GT(io_total_write_bytes, io_compaction_write_bytes);
    EXPECT_GE(io_total_write_bytes, io_write_bytes * 2);
    EXPECT_GE(io_compaction_write_bytes, io_write_bytes);
}

// Regression test for MB-17517 - ensure that if a couchstore file has a max
// CAS of -1, it is detected and reset to zero when file is loaded.
TEST(CouchKVStoreTest, MB_17517MaxCasOfMinus1) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    KVStore* kvstore = KVStoreFactory::create(config);
    ASSERT_NE(nullptr, kvstore);

    // Activate vBucket.
    std::string failoverLog("[]");
    vbucket_state state(vbucket_state_active, /*ckid*/0, /*maxDelSeqNum*/0,
                        /*highSeqno*/0, /*purgeSeqno*/0, /*lastSnapStart*/0,
                        /*lastSnapEnd*/0, /*maxCas*/-1, /*driftCounter*/0,
                        failoverLog);
    EXPECT_TRUE(kvstore->snapshotVBucket(/*vbid*/0, state, NULL));
    EXPECT_EQ(~0ull, kvstore->listPersistedVbuckets()[0]->maxCas);

    // Close the file, then re-open.
    delete kvstore;
    kvstore = KVStoreFactory::create(config);
    EXPECT_NE(nullptr, kvstore);

    // Check that our max CAS was repaired on startup.
    EXPECT_EQ(0u, kvstore->listPersistedVbuckets()[0]->maxCas);

    // Cleanup
    delete kvstore;
}

// Regression test for MB-19430 - ensure that an attempt to get the
// item count from a file which doesn't exist yet propagates the
// error so the caller can detect (and retry as necessary).
TEST(CouchKVStoreTest, MB_18580_ENOENT) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    KVStoreConfig config(1024, 4, data_dir, "couchdb", 0);
    // Create a read-only kvstore (which disables item count caching), then
    // attempt to get the count from a non-existent vbucket.
    KVStore* kvstore = KVStoreFactory::create(config, /*readOnly*/true);
    ASSERT_NE(nullptr, kvstore);

    // Expect to get a system_error (ENOENT)
    EXPECT_THROW(kvstore->getDbFileInfo(0), std::system_error);

    // Cleanup
    delete kvstore;
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
