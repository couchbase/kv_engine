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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <platform/dirutils.h>

#include "kvstore.h"
#include "couch-kvstore/couch-kvstore.h"

static time_t start_time;

static time_t mock_abstime(const rel_time_t exptime) {
    return start_time + exptime;
}

static rel_time_t mock_current_time(void) {
    rel_time_t result = (rel_time_t)(time(NULL) - start_time);
    return result;
}

class CouchKVStoreTest : public ::testing::TestWithParam<std::string> {
};

// Regression test for MB-17517 - ensure that if a couchstore file has a max
// CAS of -1, it is detected and reset to zero when file is loaded.
TEST(CouchKVStoreTest, MB_17517MaxCasOfMinus1) {
    std::string data_dir("/tmp/kvstore-test");
    CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());

    Configuration config;
    config.setDbname(data_dir);
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

class WriteCallback : public Callback<mutation_result> {
public:
    WriteCallback() {}

    void callback(mutation_result &result) {

    }
};

class KVStatsCallback : public Callback<kvstats_ctx> {
public:
    KVStatsCallback() {}

    void callback(kvstats_ctx &result) {

    }
};

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
              conflictResMode(0) {
        }

        uint64_t cas;
        uint32_t expiry;
        uint32_t flags;
        uint8_t ext1;
        uint8_t ext2;
        uint8_t conflictResMode;

        static const size_t sizeofV0 = 16;
        static const size_t sizeofV1 = 18;
        static const size_t sizeofV2 = 19;
    };

    MockCouchRequest(const Item& it,
                     uint64_t rev,
                     CouchRequestCallback &cb,
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
    MockCouchKVStore(Configuration& config)
        : CouchKVStore(config) {
    }
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
        CouchRequestCallback requestcb;
        uint64_t fileRev = dbFileRevMap[itm.getVBucketId()];

        // each req will be de-allocated after commit
        requestcb.setCb = &cb;
        MockCouchRequest *req = new MockCouchRequest(itm, fileRev, requestcb, deleteItem);
        pendingReqsQ.push_back(req);
        return req;
    }

    // For sherlock testing just wrap the 5 arg commit.
    // We drop this in Watson as commit is refactored
    bool commit1() {
        KVStatsCallback cb;
        return commit(&cb, 0, 1, 1, 1);
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
        Configuration config;
        config.setDbname(data_dir);
        kvstore.reset(new MockCouchKVStore(config));
        // Activate vBucket.
        std::string failoverLog("[]");
        vbucket_state state(vbucket_state_active, /*ckid*/0, /*maxDelSeqNum*/0,
                            /*highSeqno*/0, /*purgeSeqno*/0, /*lastSnapStart*/0,
                            /*lastSnapEnd*/0, /*maxCas*/-1, /*driftCounter*/0,
                            failoverLog);
        EXPECT_TRUE(kvstore->snapshotVBucket(/*vbid*/0, state, nullptr));
    }

    ~CouchstoreTest() {
        CouchbaseDirectoryUtilities::rmrf(data_dir.c_str());
    }

protected:
    std::string data_dir;
    std::unique_ptr<MockCouchKVStore> kvstore;
    uint16_t vbid;
};


MATCHER(IsValidConflictMode, "") {
    return (arg == last_write_wins || arg == revision_seqno);
}

class MockedGetCallback : public Callback<GetValue> {
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
                conflictResMode(value.getValue()->getConflictResMode());
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
        MOCK_METHOD1(status, void(ENGINE_ERROR_CODE));
        MOCK_METHOD1(cas, void(uint64_t));
        MOCK_METHOD1(expTime, void(uint32_t));
        MOCK_METHOD1(flags, void(uint32_t));
        MOCK_METHOD1(datatype, void(protocol_binary_datatypes));
        MOCK_METHOD1(conflictResMode, void(conflict_resolution_mode));
        MOCK_METHOD1(value, void(std::string));
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
TEST_F(CouchstoreTest, DISABLED_noMeta) { // DISABLED in sherlock due to assert
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 0); // no meta!
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_TMPFAIL));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, DISABLED_shortMeta) { // DISABLED in sherlock due to assert
    Item item("key", 3, 0, 0, "value", 5);
    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Now directly mess with the metadata of the value which will be written
    MockCouchRequest::MetaData meta;
    request->writeMetaData(meta, 4); // not enough meta!
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_TMPFAIL));
    kvstore->get("key", 0, gc);
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
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    EXPECT_CALL(gc, conflictResMode(revision_seqno));
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
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_CALL(gc, conflictResMode(revision_seqno));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, testV2MetaThings) {
    // Baseline test, just writes meta things and reads them
    // via standard interfaces
    // Ensure CAS, exptime and flags are set to something.
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11,/*expiry*/
              "value", 5,
              &datatype, 1, /*ext_meta is v1 extension*/
              0xf00fcafe11225566ull, 1, vbid, 1, INITIAL_NRU_VALUE,
              last_write_wins /*non zero conflict mode*/);
    EXPECT_NE(0, last_write_wins);
    WriteCallback wc;
    kvstore->begin();
    kvstore->set(item, wc);
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(0xf00fcafe11225566ull));
    EXPECT_CALL(gc, expTime(0xaa00bb11));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_CALL(gc, conflictResMode(last_write_wins));
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
    kvstore->commit1();

    // CAS is byteswapped when read back
    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(PROTOCOL_BINARY_RAW_BYTES));
    EXPECT_CALL(gc, conflictResMode(revision_seqno));
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
    kvstore->commit1();
    MockedGetCallback gc;
    uint8_t expectedDataType = 33;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(expectedDataType)));
    EXPECT_CALL(gc, conflictResMode(revision_seqno));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, fuzzV2) {
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
    meta.conflictResMode = 99;
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV2);
    kvstore->commit1();

    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(meta.ext2)));
    EXPECT_CALL(gc, conflictResMode(conflict_resolution_mode(3)));
    kvstore->get("key", 0, gc);
}

TEST_F(CouchstoreTest, testV1WriteReadWriteRead) {
    // Ensure CAS, exptime and flags are set to something.
    uint8_t datatype = PROTOCOL_BINARY_DATATYPE_JSON; //lies, but non-zero
    Item item("key", 3,
              0x01020304/*flags*/, 0xaa00bb11,/*expiry*/
              "value", 5,
              &datatype, 1, /*ext_meta is v1 extension*/
              0xf00fcafe11225566ull);

    EXPECT_NE(0, datatype); // make sure we writing non-zero values

    // Write an item with forced (valid) V1 meta
    MockCouchRequest::MetaData meta;
    meta.cas = 0xf00fcafe11225566ull;
    meta.expiry = 0xaa00bb11;
    meta.flags = 0x01020304;
    meta.ext1 = FLEX_META_CODE;
    meta.ext1 = datatype;

    WriteCallback wc;
    kvstore->begin();
    auto request = kvstore->setAndReturnRequest(item, wc);

    // Force the meta to be V1
    request->writeMetaData(meta, MockCouchRequest::MetaData::sizeofV1);

    // Commit it
    kvstore->commit1();

    // Read back, is conf_res_mode sane?
    MockedGetCallback gc;
    EXPECT_CALL(gc, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc, flags(0x01020304));
    EXPECT_CALL(gc, datatype(protocol_binary_datatypes(meta.ext2)));
    EXPECT_CALL(gc, conflictResMode(IsValidConflictMode()));
    kvstore->get("key", 0, gc);

    // Write back the item we read (this will write out V2 meta)
    kvstore->begin();
    kvstore->set(*gc.getValue(), wc);
    kvstore->commit1();

    // Read back, is conf_res_mode sane?
    MockedGetCallback gc2;
    EXPECT_CALL(gc2, status(ENGINE_SUCCESS));
    EXPECT_CALL(gc2, cas(htonll(0xf00fcafe11225566ull)));
    EXPECT_CALL(gc2, expTime(htonl(0xaa00bb11)));
    EXPECT_CALL(gc2, flags(0x01020304));
    EXPECT_CALL(gc2, datatype(protocol_binary_datatypes(meta.ext2)));
    EXPECT_CALL(gc2, conflictResMode(IsValidConflictMode()));
    kvstore->get("key", 0, gc2);
}

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
     /* Setup mock time functions */
    start_time = time(0);
    ep_abs_time = mock_abstime;
    ep_current_time = mock_current_time;
    putenv(allow_no_stats_env);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
