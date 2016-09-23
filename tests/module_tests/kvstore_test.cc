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

static time_t start_time;

static rel_time_t mock_current_time(void) {
    rel_time_t result = (rel_time_t)(time(NULL) - start_time);
    return result;
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
        if (result.getStatus() == ENGINE_SUCCESS) {
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
    EXPECT_TRUE(kvstore->snapshotVBucket(/*vbid*/0, state, nullptr));
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

class MockCouchRequest : public CouchRequest {
public:
    class MetaData {
    public:
        MetaData()
            : cas(0),
              expiry(0),
              flags(0),
              ext1(0),
              ext2(0),
              legacyDeleted(0) {
        }

        uint64_t cas;
        uint32_t expiry;
        uint32_t flags;
        uint8_t ext1;
        uint8_t ext2;
        uint8_t legacyDeleted; // allow testing via 19byte meta document

        static const size_t sizeofV0 = 16;
        static const size_t sizeofV1 = 18;
        static const size_t sizeofV2 = 19;
    };

    MockCouchRequest(const Item& it,
                     uint64_t rev,
                     MutationRequestCallback &cb,
                     bool del)
        :  CouchRequest(it, rev, cb, del) {
    }

    ~MockCouchRequest() {}

    // Update what will be written as 'metadata'
    void writeMetaData(MetaData& meta, size_t size) {
        allocatedMeta.reset(new char[size]());
        std::memcpy(allocatedMeta.get(), &meta, size);
        dbDocInfo.rev_meta.buf = allocatedMeta.get();
        dbDocInfo.rev_meta.size = size;
    }

    std::unique_ptr<char[]> allocatedMeta;
};

class MockCouchKVStore : public CouchKVStore {
public:
    MockCouchKVStore(KVStoreConfig& config)
        : CouchKVStore(config, false) {}

    // Mocks original code but returns the IORequest for fuzzing
    MockCouchRequest* setAndReturnRequest(const Item &itm, Callback<mutation_result> &cb) {
        if (isReadOnly()) {
            throw std::logic_error("MockCouchKVStore::set: Not valid on a read-only "
                            "object.");
        }
        if (!intransaction) {
            throw std::invalid_argument("MockCouchKVStore::set: intransaction must be "
                            "true to perform a set operation.");
        }

        bool deleteItem = false;
        MutationRequestCallback requestcb;
        uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];

        // each req will be de-allocated after commit
        requestcb.setCb = &cb;
        MockCouchRequest *req = new MockCouchRequest(itm, fileRev, requestcb, deleteItem);
        pendingReqsQ.push_back(req);
        return req;
    }

    couchstore_error_t public_openDB(uint16_t vbucketId, uint64_t fileRev, Db **db,
                              uint64_t options, uint64_t *newFileRev = NULL,
                              bool reset=false, const couch_file_ops* ops = nullptr) {
        return openDB(vbucketId, fileRev, db, options, newFileRev, reset, ops);
    }

    void public_getWithHeader(void *dbHandle, const std::string &key,
                       uint16_t vb, Callback<GetValue> &cb,
                       bool fetchDelete = false) {
        getWithHeader(dbHandle, key, vb, cb, fetchDelete);
    }

    void public_closeDatabaseHandle(Db *db) {
        closeDatabaseHandle(db);
    }
};

//
// Explicitly test couchstore (not valid for ForestDB)
// Intended to ensure we can read and write couchstore files and
// parse metadata we store in them.
//
class CouchstoreTest : public ::testing::Test {
public:
    CouchstoreTest()
        : data_dir("/tmp/kvstore-test"),
          vbid(0) {

        CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());
        KVStoreConfig config(1024, 4, data_dir, "couchdb", 0);

        kvstore.reset(new MockCouchKVStore(config));
        StatsCallback sc;
        std::string failoverLog("");
        vbucket_state state(vbucket_state_active, 0, 0, 0, 0, 0, 0, 0, 0,
                            failoverLog);
        kvstore->snapshotVBucket(0, state, &sc, true);
    }

    ~CouchstoreTest() {
        CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());
    }

protected:
    std::string data_dir;
    std::unique_ptr<MockCouchKVStore> kvstore;
    uint16_t vbid;
};

template<class T>
class MockedGetCallback : public Callback<T> {
    public:
        MockedGetCallback() {}

        ~MockedGetCallback() {
            delete savedValue.getValue();
        }

        void callback(GetValue& value){
            status(value.getStatus());
            if (value.getStatus() == ENGINE_SUCCESS) {
                EXPECT_CALL(*this, value("value"));
                cas(value.getValue()->getCas());
                expTime(value.getValue()->getExptime());
                flags(value.getValue()->getFlags());
                datatype(protocol_binary_datatypes(value.getValue()->getDataType()));
                this->value(std::string(value.getValue()->getData(),
                                        value.getValue()->getNBytes()));
                savedValue = value;
            }
        }

        Item* getValue() {
            return savedValue.getValue();
        }

        /*
         * Define a number of mock methods that will be invoked by the
         * callback method. Functions can then setup expectations of the
         * value of each method e.g. expect cas to be -1
         */
        MOCK_METHOD1_T(status, void(ENGINE_ERROR_CODE));
        MOCK_METHOD1_T(cas, void(uint64_t));
        MOCK_METHOD1_T(expTime, void(uint32_t));
        MOCK_METHOD1_T(flags, void(uint32_t));
        MOCK_METHOD1_T(datatype, void(protocol_binary_datatypes));
        MOCK_METHOD1_T(value, void(std::string));
    private:
        GetValue savedValue;
};

/*
 * The overall aim of these tests is to create an Item, write it to disk
 * then read it back from disk and look at various fields which are
 * built from the couchstore rev_meta feature.
 *
 * Validation of the Item read from disk is performed by the GetCallback.
 * A number of validators can be called upon which compare the disk Item
 * against an expected Item.
 *
 * The MockCouchKVStore exposes some of the internals of the class so we
 * can inject custom metadata by using ::setAndReturnRequest instead of ::set
 *
 */
// Disabled in watson as couchstore and throw will leak DocInfo
// safe for master (with changes to expect a failure, not a throw)
TEST_F(CouchstoreTest, DISABLED_noMeta) {
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 0); // no meta!

    StatsCallback sc;
    kvstore->commit(&sc);

    GetCallback gc;
    // Note: master does not throw, it creates an error code
    // when merged up to master this can be changed to just call get
    // and does not need the open/close
    Db* db = nullptr;
    kvstore->public_openDB(0, 0, &db, COUCHSTORE_OPEN_FLAG_RDONLY);
    EXPECT_THROW(kvstore->public_getWithHeader(db, "key", 0, gc),
                 std::invalid_argument);
    kvstore->public_closeDatabaseHandle(db);
}

// Disabled in watson as couchstore and throw will leak DocInfo
// safe for master (with changes to expect a failure, not a throw)
TEST_F(CouchstoreTest, DISABLED_shortMeta) {
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 4); // not enough meta!
    StatsCallback sc;
    kvstore->commit(&sc);

    GetCallback gc;
    // Note: master does not throw, it creates an error code
    // when merged up to master this can be changed to just call get
    // and does not need the open/close
    Db* db = nullptr;
    kvstore->public_openDB(0, 0, &db, COUCHSTORE_OPEN_FLAG_RDONLY);
    EXPECT_THROW(kvstore->public_getWithHeader(db, "key", 0, gc),
                 std::invalid_argument);
    kvstore->public_closeDatabaseHandle(db);
}

TEST_F(CouchstoreTest, testV0MetaThings) {
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11/*expiry*/,
              "value", 5,
              nullptr, 0,
              0xf00fcafe11225566ull);

    WriteCallback wc;
    kvstore->begin();
    kvstore->set(item, wc);
    StatsCallback sc;
    kvstore->commit(&sc);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, testV1MetaThings) {
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11,/*expiry*/
              "value", 5,
              &datatype, 1, /*ext_meta is v1 extension*/
              0xf00fcafe11225566ull);
    EXPECT_NE(0, datatype); // make sure we writing non-zero
    WriteCallback wc;
    kvstore->begin();
    kvstore->set(item, wc);
    StatsCallback sc;
    kvstore->commit(&sc);

    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_DATATYPE_JSON));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, fuzzV0) {
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV0);
    StatsCallback sc;
    kvstore->commit(&sc);

    // CAS is byteswapped when read back
    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, fuzzV1) {
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = 2;
    meta.ext2 = 33;
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV1);
    StatsCallback sc;
    kvstore->commit(&sc);
    MockedGetCallback<GetValue> gc;
    uint8_t expectedDataType = 33;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(expectedDataType)));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, testV0WriteReadWriteRead) {
    // Ensure CAS, exptime and flags are set to something.
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11,/*expiry*/
              "value", 5,
              &datatype, 1, /*ext_meta is v1 extension*/
              0xf00fcafe11225566ull);

    EXPECT_NE(0, datatype); // make sure we writing non-zero values

    // Write an item with forced (valid) V0 meta
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;

    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Force the meta to be V0
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV0);

    // Commit it
    StatsCallback sc;
    kvstore->commit(&sc);

    // Read back, are V1 fields sane?
    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(meta.ext2)));
    kvstore->get("key", 0, gc);

    // Write back the item we read (this will write out V1 meta)
    kvstore->begin();
    kvstore->set(*gc.getValue(), wc);
    kvstore->commit(&sc);

    // Read back, is conf_res_mode sane?
    MockedGetCallback<GetValue> gc2;
    EXPECT_CALL(gc2, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc2, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc2, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc2, flags(0x01020304));
    EXPECT_CALL(gc2, datatype(protocol_binary_datatypes(meta.ext2)));
    kvstore->get("key", 0, gc2);
}

TEST_F(CouchstoreTest, testV2WriteRead) {
    // Ensure CAS, exptime and flags are set to something.
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11,/*expiry*/
              "value", 5,
              &datatype, 1, /*ext_meta is v1 extension*/
              0xf00fcafe11225566ull);

    EXPECT_NE(0, datatype); // make sure we writing non-zero values

    // Write an item with forced (valid) V2 meta
    // In 4.6 we removed the extra conflict resolution byte, so be sure we
    // operate correctly if a document has V2 meta.
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = FLEX_META_CODE;
    meta.ext1 = datatype;
    meta.legacyDeleted = 0x01;

    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Force the meta to be V2 (19 bytes)
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV2);

    // Commit it
    StatsCallback sc;
    kvstore->commit(&sc);

    // Read back successful, the extra byte will of been dropped.
    MockedGetCallback<GetValue> gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(meta.ext2)));
    kvstore->get("key", 0, gc);
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
    /* Setup mock time functions */
    start_time = time(0);
    ep_current_time = mock_current_time;
    putenv(allow_no_stats_env);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
