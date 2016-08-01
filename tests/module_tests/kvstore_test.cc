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
#include "tests/test_fileops.h"
#include "src/internal.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>
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

/**
 * Utility template for generating callbacks for various
 * KVStore functions from a lambda/std::function
 */
template <typename... RV>
class CustomCallback : public Callback<RV...> {
public:
    CustomCallback(std::function<void(RV...)> _cb)
        : cb(_cb) {}
    CustomCallback()
        : cb([](RV... val){}) {}

    void callback(RV&...result) {
        cb(result...);
    }

protected:
    std::function<void(RV...)> cb;
};

/**
 * Callback that can be given a lambda to use, specifically
 * for the Rollback callback
 */
class CustomRBCallback : public RollbackCB {
public:
    CustomRBCallback(std::function<void(GetValue)> _cb)
        : cb(_cb) {}
    CustomRBCallback()
        : cb([](GetValue val){}) {}

    void callback(GetValue &result) {
        cb(result);
    }

protected:
    std::function<void(GetValue)> cb;
};

// Initializes a KVStore
static void initialize_kv_store(KVStore* kvstore) {
    std::string failoverLog("");
    vbucket_state state(vbucket_state_active, 0, 0, 0, 0, 0, 0, 0, 0,
                        failoverLog);
    kvstore->snapshotVBucket(0, state,
                            VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT);
}

// Creates and initializes a KVStore with the given config
static std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config) {
    auto kvstore = std::unique_ptr<KVStore>(KVStoreFactory::create(config));
    initialize_kv_store(kvstore.get());
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

    EXPECT_TRUE(kvstore->commit());

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
    kvstore->commit();

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
    EXPECT_TRUE(kvstore->commit());
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats(add_stat_callback, &stats);
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_write_bytes"]);
    EXPECT_EQ(key.size() + value.size() + COUCHSTORE_METADATA_SIZE,
              io_write_bytes);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and least as many as the actual documents.
    const size_t io_total_write_bytes = stoul(stats["rw_0:io_total_write_bytes"]);
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

    EXPECT_TRUE(kvstore->commit());

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

    EXPECT_TRUE(kvstore->compactDB(&cctx));
    // Check statistics are correct.
    std::map<std::string, std::string> stats;
    kvstore->addStats(add_stat_callback, &stats);
    EXPECT_EQ("1", stats["rw_0:io_num_write"]);
    const size_t io_write_bytes = stoul(stats["rw_0:io_write_bytes"]);

    // Hard to determine exactly how many bytes should have been written, but
    // expect non-zero, and at least twice as many as the actual documents for
    // the total and once as many for compaction alone.
    const size_t io_total_write_bytes = stoul(stats["rw_0:io_total_write_bytes"]);
    const size_t io_compaction_write_bytes = stoul(stats["rw_0:io_compaction_write_bytes"]);
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
    EXPECT_TRUE(kvstore->snapshotVBucket(/*vbid*/0, state,
                                         VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT));
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

/**
 * The CouchKVStoreErrorInjectionTest cases utilise GoogleMock to inject
 * errors into couchstore as if they come from the filesystem in order
 * to observe how CouchKVStore handles the error and logs it.
 *
 * The GoogleMock framework allows expectations to be set on how an object
 * will be called and how it will respond. Generally we will set a Couchstore
 * FileOps instance to return an error code on the 'nth' call as follows:
 *
 *      EXPECT_CALL(ops, open(_, _, _, _)).Times(AnyNumber());
 *      EXPECT_CALL(ops, open(_, _, _, _))
 *          .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();
 *      EXPECT_CALL(ops, open(_, _, _, _)).Times(n).RetiresOnSaturation();
 *
 * We will additionally set an expectation on the LoggerMock regarding how it
 * will be called by CouchKVStore. In this instance we have set an expectation
 * that the logger will be called with a logging level greater than or equal
 * to info, and the log message will contain the error string that corresponds
 * to `COUCHSTORE_ERROR_OPEN_FILE`.
 *
 *      EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
 *      EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
 *                               VCE(COUCHSTORE_ERROR_OPEN_FILE))
 *      ).Times(1).RetiresOnSaturation();
 */

using namespace testing;

/**
 * The MockLogger is used to verify that the logger is called with certain
 * parameters / messages.
 *
 * The MockLogger is slightly misleading in that it mocks a function that
 * is not on the API of the logger, instead mocking a function that is
 * called with the preformatted log message.
 */
class MockLogger : public Logger {
public:
    MockLogger() {
        ON_CALL(*this, mlog(_, _)).WillByDefault(Invoke([](EXTENSION_LOG_LEVEL sev,
                                                           const std::string& msg){
        }));
    }

    void vlog(EXTENSION_LOG_LEVEL severity, const char* fmt, va_list va) const override {
        mlog(severity, vatos(fmt, va));
    }

    MOCK_CONST_METHOD2(mlog, void(EXTENSION_LOG_LEVEL severity,
                                  const std::string& message));

private:
    /**
     * Convert fmt cstring and a variadic arguments list to a string
     */
    static std::string vatos(const char* fmt, va_list va) {
        std::vector<char> buffer;
        va_list cpy;

        // Calculate Size
        va_copy(cpy, va);
        buffer.resize(vsnprintf(nullptr, 0, fmt, cpy) + 1);
        va_end(cpy);

        // Write to vector and return as string
        vsnprintf(buffer.data(), buffer.size(), fmt, va);
        return std::string(buffer.data());
    }

};

/**
 * VCE: Verify Couchstore Error
 *
 * This is a GoogleMock matcher which will match against a string
 * which has the corresponding message for the passed couchstore
 * error code in it. e.g.
 *
 *     VCE(COUCHSTORE_ERROR_WRITE)
 *
 * will match against a string which contains 'error writing to file'.
 */
MATCHER_P(VCE, value, "is string of %(value)") {
    return arg.find(couchstore_strerror(value)) != std::string::npos;
}

/**
 * CouchKVStoreErrorInjectionTest is used for tests which verify
 * log messages from error injection in couchstore.
 */
class CouchKVStoreErrorInjectionTest : public ::testing::Test {
public:
    CouchKVStoreErrorInjectionTest()
        : data_dir("/tmp/kvstore-test"),
          ops(create_default_file_ops()),
          config(KVStoreConfig(1024, 4, data_dir, "couchdb", 0)
                     .setLogger(logger)
                     .setBuffered(false)) {

        CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());
        kvstore.reset(new CouchKVStore(config, ops, false));
        initialize_kv_store(kvstore.get());
    }
    ~CouchKVStoreErrorInjectionTest() {
        CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());
    }

protected:
    void generate_items(size_t count) {
        for(unsigned i(0); i < count; i++) {
            std::string key("key" + std::to_string(i));
            items.push_back(Item(key.data(), key.length(), 0, 0, "value", 5,
                                 nullptr, 0, 0, i + 1));
        }
    }

    void populate_items(size_t count) {
        generate_items(count);
        CustomCallback<mutation_result> set_callback;
        kvstore->begin();
        for(const auto& item: items) {
            kvstore->set(item, set_callback);
        }
        kvstore->commit();
    }

    vb_bgfetch_queue_t make_bgfetch_queue() {
        vb_bgfetch_queue_t itms;
        vb_bgfetch_item_ctx_t ctx;
        ctx.isMetaOnly = false;
        for(const auto& item: items) {
            itms[item.getKey()] = ctx;
        }
        return itms;
    }


    const std::string data_dir;

    ::testing::NiceMock<MockOps> ops;
    ::testing::NiceMock<MockLogger> logger;

    KVStoreConfig config;
    std::unique_ptr<CouchKVStore> kvstore;
    std::vector<Item> items;
};


/**
 * Injects error during CouchKVStore::openDB_retry/couchstore_open_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, openDB_retry_open_db_ex) {
    generate_items(1);
    CustomCallback<mutation_result> set_callback;

    kvstore->begin();
    kvstore->set(items.front(), set_callback);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_NOTICE),
                                 VCE(COUCHSTORE_ERROR_OPEN_FILE))
        ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _)).Times(AnyNumber());
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();

        kvstore->commit();
    }
}

/**
 * Injects error during CouchKVStore::openDB/couchstore_open_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, openDB_open_db_ex) {
    generate_items(1);
    CustomCallback<mutation_result> set_callback;

    kvstore->begin();
    kvstore->set(items.front(), set_callback);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_OPEN_FILE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillRepeatedly(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();

        kvstore->commit();
    }
}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_documents
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_save_documents) {
    generate_items(1);
    CustomCallback<mutation_result> set_callback;

    kvstore->begin();
    kvstore->set(items.front(), set_callback);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_WRITE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();

        kvstore->commit();
    }

}

/**
 * Injects error during CouchKVStore::commit/couchstore_save_local_document
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_save_local_document) {
    generate_items(1);
    CustomCallback<mutation_result> set_callback;

    kvstore->begin();
    kvstore->set(items.front(), set_callback);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_WRITE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(6).RetiresOnSaturation();

        kvstore->commit();
    }

}

/**
 * Injects error during CouchKVStore::commit/couchstore_commit
 */
TEST_F(CouchKVStoreErrorInjectionTest, commit_commit) {
    generate_items(1);
    CustomCallback<mutation_result> set_callback;

    kvstore->begin();
    kvstore->set(items.front(), set_callback);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_WRITE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pwrite(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_WRITE)).RetiresOnSaturation();
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(8).RetiresOnSaturation();

        kvstore->commit();
    }
}

/**
 * Injects error during CouchKVStore::get/couchstore_docinfo_by_id
 */
TEST_F(CouchKVStoreErrorInjectionTest, get_docinfo_by_id) {
    populate_items(1);
    CustomCallback<GetValue> get_callback;
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();
        kvstore->get(items.front().getKey(), 0, get_callback);

    }
}

/**
 * Injects error during CouchKVStore::get/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, get_open_doc_with_docinfo) {
    populate_items(1);
    CustomCallback<GetValue> get_callback;
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(5).RetiresOnSaturation();
        kvstore->get(items.front().getKey(), 0, get_callback);

    }
}

/**
 * Injects error during CouchKVStore::getMulti/couchstore_docinfos_by_id
 */
TEST_F(CouchKVStoreErrorInjectionTest, getMulti_docinfos_by_id) {
    populate_items(1);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());
    {

        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();
        kvstore->getMulti(0, itms);

    }
}


/**
 * Injects error during CouchKVStore::getMulti/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, getMulti_open_doc_with_docinfo) {
    populate_items(1);
    vb_bgfetch_queue_t itms(make_bgfetch_queue());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(5).RetiresOnSaturation();
        kvstore->getMulti(0, itms);

    }
}

/**
 * Injects error during CouchKVStore::compactDB/couchstore_compact_db_ex
 */
TEST_F(CouchKVStoreErrorInjectionTest, compactDB_compact_db_ex) {
    populate_items(1);

    compaction_ctx cctx;
    cctx.purge_before_seq = 0;
    cctx.purge_before_ts = 0;
    cctx.curr_time = 0;
    cctx.drop_deletes = 0;
    cctx.db_file_id = 0;

    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_OPEN_FILE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, open(_, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE)).RetiresOnSaturation();
        EXPECT_CALL(ops, open(_, _, _, _)).Times(1).RetiresOnSaturation();
        kvstore->compactDB(&cctx);
    }
}

/**
 * Injects error during CouchKVStore::getNumItems/couchstore_changes_count
 */
TEST_F(CouchKVStoreErrorInjectionTest, getNumItems_changes_count) {
    populate_items(1);
    {
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();
        try {
            kvstore->getNumItems(0, 0, 100000);
            EXPECT_TRUE(false) << "kvstore->getNumItems(0, 0, 100000); should "
                                  "have thrown a runtime_error";
        } catch (const std::runtime_error& e) {
            EXPECT_THAT(std::string(e.what()), VCE(COUCHSTORE_ERROR_READ));
        }

    }
}

/**
 * Injects error during CouchKVStore::reset/couchstore_commit
 */
TEST_F(CouchKVStoreErrorInjectionTest, reset_commit) {
    populate_items(1);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, sync(_, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();

        kvstore->reset(0);
    }
}

/**
 * Injects error during CouchKVStore::initScanContext/couchstore_changes_count
 */
TEST_F(CouchKVStoreErrorInjectionTest, initScanContext_changes_count) {
    populate_items(1);
    auto cb(std::make_shared<CustomCallback<GetValue>>());
    auto cl(std::make_shared<CustomCallback<CacheLookup>>());
    {
        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();

        ScanContext* scanCtx = nullptr;
        try {
            scanCtx = kvstore->initScanContext(cb, cl, 0, 0,
                                               DocumentFilter::ALL_ITEMS,
                                               ValueFilter::VALUES_DECOMPRESSED);
            EXPECT_TRUE(false) << "kvstore->initScanContext(cb, cl, 0, 0, "
                                  "DocumentFilter::ALL_ITEMS, "
                                  "ValueFilter::VALUES_DECOMPRESSED); should "
                                  "have thrown a runtime_error";
        } catch (const std::runtime_error& e) {
            EXPECT_THAT(std::string(e.what()), VCE(COUCHSTORE_ERROR_READ));
        }

        kvstore->destroyScanContext(scanCtx);
    }
}

/**
 * Injects error during CouchKVStore::scan/couchstore_changes_since
 */
TEST_F(CouchKVStoreErrorInjectionTest, scan_changes_since) {
    populate_items(1);
    auto cb(std::make_shared<CustomCallback<GetValue>>());
    auto cl(std::make_shared<CustomCallback<CacheLookup>>());
    auto scan_context = kvstore->initScanContext(cb, cl, 0, 0,
                                              DocumentFilter::ALL_ITEMS,
                                              ValueFilter::VALUES_DECOMPRESSED);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();

        kvstore->scan(scan_context);
    }

    kvstore->destroyScanContext(scan_context);
}

/**
 * Injects error during CouchKVStore::recordDbDump/couchstore_open_doc_with_docinfo
 */
TEST_F(CouchKVStoreErrorInjectionTest, recordDbDump_open_doc_with_docinfo) {
    populate_items(1);
    auto cb(std::make_shared<CustomCallback<GetValue>>());
    auto cl(std::make_shared<CustomCallback<CacheLookup>>());
    auto scan_context = kvstore->initScanContext(cb, cl, 0, 0,
                                                 DocumentFilter::ALL_ITEMS,
                                                 ValueFilter::VALUES_DECOMPRESSED);
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(2).RetiresOnSaturation();

        kvstore->scan(scan_context);
    }

    kvstore->destroyScanContext(scan_context);
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/1
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_changes_count1) {
    generate_items(6);
    CustomCallback<mutation_result> set_callback;

    for(const auto item: items) {
        kvstore->begin();
        kvstore->set(item, set_callback);
        kvstore->commit();
    }

    auto rcb(std::make_shared<CustomRBCallback>());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();

        kvstore->rollback(0, 5, rcb);
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_rewind_header
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_rewind_header) {
    generate_items(6);
    CustomCallback<mutation_result> set_callback;

    for(const auto item: items) {
        kvstore->begin();
        kvstore->set(item, set_callback);
        kvstore->commit();
    }

    auto rcb(std::make_shared<CustomRBCallback>());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_DB_NO_LONGER_VALID))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            /* Doing an ALLOC_FAIL as Couchstore will just
             * keep rolling back otherwise */
            .WillOnce(Return(COUCHSTORE_ERROR_ALLOC_FAIL)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(9).RetiresOnSaturation();

        kvstore->rollback(0, 5, rcb);
    }
}

/**
 * Injects error during CouchKVStore::rollback/couchstore_changes_count/2
 */
TEST_F(CouchKVStoreErrorInjectionTest, rollback_changes_count2) {
    generate_items(6);
    CustomCallback<mutation_result> set_callback;

    for(const auto item: items) {
        kvstore->begin();
        kvstore->set(item, set_callback);
        kvstore->commit();
    }

    auto rcb(std::make_shared<CustomRBCallback>());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(11).RetiresOnSaturation();

        kvstore->rollback(0, 5, rcb);
    }
}

/**
 * Injects error during CouchKVStore::readVBState/couchstore_open_local_document
 */
TEST_F(CouchKVStoreErrorInjectionTest, readVBState_open_local_document) {
    generate_items(6);
    CustomCallback<mutation_result> set_callback;

    for(const auto item: items) {
        kvstore->begin();
        kvstore->set(item, set_callback);
        kvstore->commit();
    }

    auto rcb(std::make_shared<CustomRBCallback>());
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(20).RetiresOnSaturation();

        kvstore->rollback(0, 5, rcb);
    }
}

/**
 * Injects error during CouchKVStore::getAllKeys/couchstore_all_docs
 */
TEST_F(CouchKVStoreErrorInjectionTest, getAllKeys_all_docs) {
    populate_items(1);

    auto adcb(std::make_shared<CustomCallback<uint16_t&, char*&>>());
    std::string start("");
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_READ))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, pread(_, _, _, _, _))
            .WillOnce(Return(COUCHSTORE_ERROR_READ)).RetiresOnSaturation();
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(3).RetiresOnSaturation();


        kvstore->getAllKeys(0, start, 1, adcb);
    }
}

/**
 * Injects error during CouchKVStore::closeDB/couchstore_close_file
 */
TEST_F(CouchKVStoreErrorInjectionTest, closeDB_close_file) {
    {
        /* Establish Logger expectation */
        EXPECT_CALL(logger, mlog(_, _)).Times(AnyNumber());
        EXPECT_CALL(logger, mlog(Ge(EXTENSION_LOG_WARNING),
                                 VCE(COUCHSTORE_ERROR_FILE_CLOSE))
                   ).Times(1).RetiresOnSaturation();

        /* Establish FileOps expectation */
        EXPECT_CALL(ops, close(_, _)).Times(AnyNumber());
        EXPECT_CALL(ops, close(_, _))
            .WillOnce(Return(COUCHSTORE_ERROR_FILE_CLOSE)).RetiresOnSaturation();

        populate_items(1);
    }
}

// Test cases which run on both Couchstore and ForestDB
INSTANTIATE_TEST_CASE_P(CouchstoreAndForestDB,
                        CouchAndForestTest,
                        ::testing::Values("couchdb", "forestdb"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    putenv(allow_no_stats_env);

    return RUN_ALL_TESTS();
}
