/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/*
 * Testsuite for XDCR-related functionality in ep-engine.
 */
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "hlc.h"
#include <platform/cb_malloc.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

// Helper functions ///////////////////////////////////////////////////////////

static void verifyMetaData(const ItemMetaData& imd, const item_info& metadata) {
    checkeq(uint64_t(imd.revSeqno), metadata.seqno, "Seqno didn't match");
    checkeq(imd.cas, metadata.cas, "Cas didn't match");
    checkeq(imd.exptime,
            static_cast<time_t>(metadata.exptime),
            "Expiration time didn't match");
    checkeq(imd.flags, metadata.flags, "Flags didn't match");
}

/**
 * Create an XATTR document using the supplied string as the body
 * @returns vector containing the body bytes
 */
static std::vector<char> createXattrValue(const std::string& body) {
    cb::xattr::Blob blob;

    //Add a few XAttrs
    blob.set("user", R"({"author":"bubba"})");
    blob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");
    blob.set("meta", R"({"content-type":"text"})");

    auto xattr_value = blob.finalize();

    // append body to the xattrs and store in data
    std::vector<char> data;
    std::copy(xattr_value.begin(), xattr_value.end(), std::back_inserter(data));
    std::copy(body.c_str(), body.c_str() + body.size(),
              std::back_inserter(data));

    return data;
}


// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_get_meta(EngineIface* h) {
    char const *key = "test_get_meta";
    item *i = nullptr;
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    // check the stat
    auto temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");

    ItemMetaData metadata(it->getCas(), it->getRevSeqno(),
                          it->getFlags(), it->getExptime());
    verifyMetaData(metadata, errorMetaPair.second);

    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta op");

    h->release(i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_extras(EngineIface* h) {
    const char *key1 = "test_getm_one";
    item *i = nullptr;
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");

    wait_for_flusher_to_settle(h);

    Item *it1 = reinterpret_cast<Item*>(i);
    // check the stat
    auto temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    ItemMetaData metadata1(it1->getCas(), it1->getRevSeqno(),
                           it1->getFlags(), it1->getExptime());
    verifyMetaData(metadata1, errorMetaPair.second);
    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta op");
    h->release(i);

    if (isWarmupEnabled(h)) {
        // restart
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   true);

        wait_for_warmup_complete(h);

        check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
        verifyMetaData(metadata1, errorMetaPair.second);
    }

    return SUCCESS;
}

static enum test_result test_get_meta_deleted(EngineIface* h) {
    char const *key = "k1";
    item *i = nullptr;

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, "somevalue"),
            "Failed set.");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");

    Item *it = reinterpret_cast<Item*>(i);
    wait_for_flusher_to_settle(h);

    checkeq(ENGINE_SUCCESS,
            del(h, key, it->getCas(), Vbid(0)),
            "Delete failed");
    wait_for_flusher_to_settle(h);

    // check the stat
    int temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(errorMetaPair.second.seqno, (it->getRevSeqno() + 1),
          "Expected seqno to match");
    checkne(errorMetaPair.second.cas, it->getCas(),
          "Expected cas to be different");
    checkeq(errorMetaPair.second.flags, it->getFlags(),
          "Expected flags to match");

    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta op");

    h->release(i);
    return SUCCESS;
}

static enum test_result test_get_meta_nonexistent(EngineIface* h) {
    char const *key = "k1";

    // check the stat
    int temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(!get_meta(h, key, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");

    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_get(EngineIface* h) {
    char const *key1 = "key1";
    char const *key2 = "key2";

    // test get_meta followed by get for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    // check the stat
    int temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    auto ret = get(h, nullptr, key1, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first,
            "Expected get success");
    ret.second.reset();
    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(1, temp, "Expect one getMeta op");

    // test get_meta followed by get for a deleted key. should fail.
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, key1, Vbid(0)).first,
            "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(2, temp, "Expect more getMeta ops");

    // test get_meta followed by get for a nonexistent key. should fail.
    check(!get_meta(h, key2, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, key2, Vbid(0)).first,
            "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(3, temp, "Expected one extra getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_set(EngineIface* h) {
    char const *key1 = "key1";
    char const *key2 = "key2";

    ItemMetaData itm_meta;

    // test get_meta followed by set for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 1);

    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_get_meta"),
            "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");
    // check the stat
    checkeq(1, get_int_stat(h, "ep_num_ops_get_meta"), "Expect one getMeta op");
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // check curr, temp item counts
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // test get_meta followed by set for a deleted key. should pass.
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);

    wait_for_stat_to_be(h, "curr_items", 0);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // check the stat
    checkeq(2,
            get_int_stat(h, "ep_num_ops_get_meta"),
            "Expect more getMeta ops");

    // test get_meta followed by set for a nonexistent key. should pass.
    check(!get_meta(h, key2, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key2, "someothervalue"),
            "Failed set.");
    // check the stat again
    checkeq(3,
            get_int_stat(h, "ep_num_ops_get_meta"),
            "Expected one extra getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_delete(EngineIface* h) {
    char const *key1 = "key1";
    char const *key2 = "key2";

    // test get_meta followed by delete for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    // check the stat
    int temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(0, temp, "Expect zero getMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_get_meta");
   checkeq(1, temp, "Expect one getMeta op");

    // test get_meta followed by delete for a deleted key. should fail.
    wait_for_flusher_to_settle(h);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(ENGINE_KEY_ENOENT, del(h, key1, 0, Vbid(0)), "Expected enoent");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(2, temp, "Expect more getMeta op");

    // test get_meta followed by delete for a nonexistent key. should fail.
    check(!get_meta(h, key2, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");
    checkeq(ENGINE_KEY_ENOENT, del(h, key2, 0, Vbid(0)), "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, "ep_num_ops_get_meta");
    checkeq(3, temp, "Expected one extra getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_xattr(EngineIface* h) {
    const char* key = "get_meta_key";
    std::vector<char> data = createXattrValue({"test_expiry_value"});

    const void* cookie = testHarness->create_cookie(h);

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         cookie,
                         OPERATION_SET,
                         key,
                         reinterpret_cast<char*>(data.data()),
                         data.size(),
                         9258,
                         0,
                         Vbid(0),
                         0,
                         PROTOCOL_BINARY_DATATYPE_XATTR)
                    .first,
            "Failed to store xattr document");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
    }

    cb::EngineErrorMetadataPair errorMetaPair;

    // Check that the datatype is XATTR (at engine level the datatype is always
    // returned).
    check(get_meta(h, key, errorMetaPair, cookie), "Get meta command failed");
    checkeq(PROTOCOL_BINARY_DATATYPE_XATTR,
            errorMetaPair.second.datatype,
            "Datatype is not XATTR");

    if (isPersistentBucket(h)) {
        //Evict the key
        evict_key(h, key);

        // This should result in a bg fetch
        check(get_meta(h, key, errorMetaPair, cookie),
              "Get meta command failed");
        checkeq(PROTOCOL_BINARY_DATATYPE_XATTR,
                errorMetaPair.second.datatype,
                "Datatype is not XATTR");
    }

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Test that we can still get datatype of the deleted item after compaction
 */
static enum test_result test_get_meta_mb23905(EngineIface* h) {
    const char* key = "get_meta_key";
    std::vector<char> data = createXattrValue({"test_expiry_value"});

    const void* cookie = testHarness->create_cookie(h);

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         cookie,
                         OPERATION_SET,
                         key,
                         reinterpret_cast<char*>(data.data()),
                         data.size(),
                         9258,
                         0,
                         Vbid(0),
                         0,
                         PROTOCOL_BINARY_DATATYPE_XATTR)
                    .first,
            "Failed to store xattr document");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
    }

    if (isPersistentBucket(h)) {
        cb::xattr::Blob systemXattrBlob;
        systemXattrBlob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");
        auto deletedValue = systemXattrBlob.finalize();

        checkeq(ENGINE_SUCCESS,
                delete_with_value(h,
                                  cookie,
                                  0,
                                  key,
                                  deletedValue,
                                  cb::mcbp::Datatype::Xattr),
                "delete_with_value() failed");

        // Run compaction to start using the bloomfilter
        useconds_t sleepTime = 128;
        compact_db(h, Vbid(0), Vbid(0), 1, 1, 0);
        while (get_int_stat(h, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        cb::EngineErrorMetadataPair errorMetaPair;
        check(get_meta(h, key, errorMetaPair, cookie),
              "Get meta command failed");
        checkeq(PROTOCOL_BINARY_DATATYPE_XATTR,
                errorMetaPair.second.datatype,
                "Datatype is not XATTR");
        checkeq(DocumentState::Deleted,
                errorMetaPair.second.document_state,
                "Expected deleted flag to be set");
    }

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_add_with_meta(EngineIface* h) {
    const char *key = "mykey";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    int temp = 0;

    // put some random metadata
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(0, temp, "Expect zero setMeta ops");

    // store an item with meta data
    checkeq(ENGINE_SUCCESS,
            add_with_meta(h, key, keylen, nullptr, 0, Vbid(0), &itemMeta),
            "Expected to add item");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // store the item again, expect key exists
    checkeq(ENGINE_KEY_EEXISTS,
            add_with_meta(h, key, keylen, nullptr, 0, Vbid(0), &itemMeta, true),
            "Expected add to fail when the item exists already");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(1, temp, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta(EngineIface* h) {
    const char *key1 = "delete_with_meta_key1";
    const char *key2 = "delete_with_meta_key2";
    const char *key3 = "delete_with_meta_key3";
    const size_t keylen = strlen(key1);
    ItemMetaData itemMeta;
    uint64_t vb_uuid;
    uint32_t high_seqno;
    // check the stat
    auto temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(0, temp, "Expect zero setMeta ops");

    // put some random meta data
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key2, "somevalue2"),
            "Failed set.");

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key3, "somevalue3"),
            "Failed set.");

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    const void* cookie = testHarness->create_cookie(h);

    // delete an item with meta data
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key1,
                          keylen,
                          Vbid(0),
                          &itemMeta,
                          0 /*cas*/,
                          0 /*options*/,
                          cookie),
            "Expected delete OK");

    checkeq(last_uuid.load(), vb_uuid, "Expected valid vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 1),
            "Expected valid sequence number");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(1, temp, "Expect more setMeta ops");

    testHarness->set_mutation_extras_handling(cookie, false);

    // delete an item with meta data
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key2,
                          keylen,
                          Vbid(0),
                          &itemMeta,
                          0 /*cas*/,
                          0 /*options*/,
                          cookie),
            "Expected delete OK");

    checkeq(last_uuid.load(), vb_uuid, "Expected valid vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 1),
            "Expected valid sequence number");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // delete an item with meta data
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key3, keylen, Vbid(0), &itemMeta),
            "Expected delete OK");

    checkeq(last_uuid.load(), vb_uuid, "Expected valid vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 3),
            "Expected valid sequence number");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_deleted(EngineIface* h) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);

    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_del_meta"),
            "Expect zero setMeta ops");

    // add a key
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // delete the key
    checkeq(ENGINE_SUCCESS, del(h, key, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    cb::EngineErrorMetadataPair errorMetaPair;

    // get metadata of deleted key
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;
    // put some random metadata and delete the item with new meta data
    ItemMetaData itm_meta(
            0xdeadbeef, 10, 0xdeadbeef, 1735689600); // expires in 2025

    // do delete with meta with an incorrect cas value. should fail.
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, key, keylen, Vbid(0), &itm_meta, invalid_cas),
            "Expected invalid cas error");
    checkeq(0,
            get_int_stat(h, "ep_num_ops_del_meta"),
            "Faild ops does not count");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    // do delete with meta with the correct cas value. should pass.
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key, keylen, Vbid(0), &itm_meta, valid_cas),
            "Expected delete oK");
    wait_for_flusher_to_settle(h);

    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(1, get_int_stat(h, "ep_num_ops_del_meta"), "Expect some ops");
    wait_for_stat_to_be(h, "curr_items", 0);
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(static_cast<uint64_t>(itm_meta.revSeqno), errorMetaPair.second.seqno,
            "Expected seqno to match");
    checkeq(itm_meta.cas, errorMetaPair.second.cas, "Expected cas to match");
    checkeq(itm_meta.flags, errorMetaPair.second.flags,
            "Expected flags to match");

    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent(EngineIface* h) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);

    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_del_meta"),
            "Expect zero setMeta ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    // get metadata of nonexistent key
    check(!get_meta(h, key, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;

    // do delete with meta
    // put some random metadata and delete the item with new meta data
    ItemMetaData itm_meta(
            0xdeadbeef, 10, 0xdeadbeef, 1735689600); // expires in 2025

    // do delete with meta with an incorrect cas value. should fail.
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, key, keylen, Vbid(0), &itm_meta, invalid_cas),
            "Expected invalid cas error");
    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_del_meta"),
            "Failed op does not count");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    // do delete with meta with the correct cas value. should pass.
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key, keylen, Vbid(0), &itm_meta, valid_cas),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    // check the stat
    checkeq(1, get_int_stat(h, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, "curr_items", 0);
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(static_cast<uint64_t>(itm_meta.revSeqno),
            errorMetaPair.second.seqno,
            "Expected seqno to match");
    checkeq(itm_meta.cas, errorMetaPair.second.cas, "Expected cas to match");
    checkeq(itm_meta.flags, errorMetaPair.second.flags,
            "Expected flags to match");

    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent_no_temp(
        EngineIface* h) {
    const char *key1 = "delete_with_meta_no_temp_key1";
    const size_t keylen1 = strlen(key1);
    ItemMetaData itm_meta1;

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, Vbid(0), Vbid(0), 1, 1, 0);
    while (get_int_stat(h, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    // put some random metadata and delete the item with new meta data
    itm_meta1.revSeqno = 10;
    itm_meta1.cas = 0xdeadbeef;
    itm_meta1.exptime = 1735689600; // expires in 2025
    itm_meta1.flags = 0xdeadbeef;

    // do delete with meta with the correct cas value.
    // skipConflictResolution false
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key1, keylen1, Vbid(0), &itm_meta1, 0, false),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    checkeq(1, get_int_stat(h, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, "curr_items", 0);
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // do delete with meta with the correct cas value.
    // skipConflictResolution true
    const char *key2 = "delete_with_meta_no_temp_key2";
    const size_t keylen2 = strlen(key2);
    ItemMetaData itm_meta2;

    // put some random metadata and delete the item with new meta data
    itm_meta2.revSeqno = 10;
    itm_meta2.cas = 0xdeadbeef;
    itm_meta2.exptime = 1735689600; // expires in 2025
    itm_meta2.flags = 0xdeadbeef;

    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key2, keylen2, Vbid(0), &itm_meta2, 0, true),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    checkeq(2, get_int_stat(h, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, "curr_items", 0);
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_set(EngineIface* h) {
    char const *key1 = "key1";
    const size_t keylen1 = strlen(key1);

    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;
    // check the stat
    size_t temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{0}, temp, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");

    // attempt delete_with_meta. should fail since cas is no longer valid.
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h,
                          key1,
                          keylen1,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{0}, temp, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");

    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h,
                          key1,
                          keylen1,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{0}, temp, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_delete(EngineIface* h) {
    char const *key1 = "key1";
    auto keylen1 = (uint16_t)strlen(key1);
    char const *key2 = "key2";
    auto keylen2 = (uint16_t)strlen(key2);

    // check the stat
    size_t temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{0}, temp, "Expect zero ops");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");

    //Store the CAS. This will be used in a subsequent delete_with_meta call
    uint64_t cas_from_store = errorMetaPair.second.cas;

    //Do a concurrent delete. This should modify the CAS
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");

    //Get the latest meta data
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");

    //Populate the item meta data in such a way, so that we will pass
    //conflict resolution
    ItemMetaData itm_meta(errorMetaPair.second.cas,
                          errorMetaPair.second.seqno + 1,
                          errorMetaPair.second.flags,
                          errorMetaPair.second.exptime);

    // attempt delete_with_meta. should fail since cas is no longer valid.
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, key1, keylen1, Vbid(0), &itm_meta, cas_from_store),
            "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{0}, temp, "Failed op does not count");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete itself will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");

    // do a concurrent delete
    checkeq(ENGINE_KEY_ENOENT, del(h, key1, 0, Vbid(0)), "Delete failed");

    // attempt delete_with_meta. should pass.
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key1, keylen1, Vbid(0), &itm_meta, last_cas),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{1}, temp, "Expect some ops");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete itself will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, key2, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");

    // do a concurrent delete
    checkeq(ENGINE_KEY_ENOENT, del(h, key1, 0, Vbid(0)), "Delete failed");

    // attempt delete_with_meta. should pass.
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key2,
                          keylen2,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(size_t{2}, temp, "Expect some ops");

    return SUCCESS;
}

static enum test_result test_set_with_meta(EngineIface* h) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = R"({"json":"yes"})";
    size_t newValLen = strlen(newVal);
    uint64_t vb_uuid;
    uint32_t high_seqno;

    // check the stat
    checkeq(0, get_int_stat(h, "ep_num_ops_set_meta"), "Expect zero ops");
    checkeq(0,
            get_int_stat(h, "ep_num_ops_get_meta_on_set_meta"),
            "Expect zero ops");
    checkeq(0, get_int_stat(h, "curr_items"), "Expect zero items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expect zero temp items");

    // create a new key
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, val),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // get metadata for the key
    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(1, get_int_stat(h, "curr_items"), "Expect one item");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expect zero temp item");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = errorMetaPair.second.cas;
    // init some random metadata
    ItemMetaData itm_meta(0xdeadbeef, 10, 0xdeadbeef, time(nullptr) + 300);

    char *bigValue = new char[32*1024*1024];
    // do set with meta with the value size bigger than the max size allowed.
    checkeq(ENGINE_E2BIG,
            set_with_meta(h,
                          key,
                          keylen,
                          bigValue,
                          32 * 1024 * 1024,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set),
            "Expected the max value size exceeding error");
    delete []bigValue;

    // do set with meta with an incorrect cas value. should fail.
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          1229),
            "Expected invalid cas error");
    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Failed op does not count");

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    const void* cookie = testHarness->create_cookie(h);
    // We are explicitly going to test the !datatype paths, so turn it off.
    testHarness->set_datatype_support(cookie, false);

    // do set with meta with the correct cas value. should pass.
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set,
                          0,
                          0,
                          cookie),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(last_uuid.load(), vb_uuid, "Expected valid vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 1),
            "Expected valid sequence number");

    // Check that set_with_meta has marked the JSON input as JSON
    item_info info;
    check(get_item_info(h, &info, key, Vbid(0)), "get_item_info failed");
    checkeq(int(PROTOCOL_BINARY_DATATYPE_JSON), int(info.datatype),
        "Expected datatype to now include JSON");

    // check the stat
    checkeq(1, get_int_stat(h, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(1, get_int_stat(h, "curr_items"), "Expect one item");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expect zero temp item");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(uint64_t{10},
            errorMetaPair.second.seqno,
            "Expected seqno to match");
    checkeq(uint64_t{0xdeadbeef},
            errorMetaPair.second.cas,
            "Expected cas to match");
    checkeq(uint32_t{0xdeadbeef},
            errorMetaPair.second.flags,
            "Expected flags to match");

    //disable getting vb uuid and seqno as extras
    testHarness->set_mutation_extras_handling(cookie, false);
    itm_meta.revSeqno++;
    cas_for_set = errorMetaPair.second.cas;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set,
                          false,
                          0,
                          cookie),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(last_uuid.load(), vb_uuid, "Expected same vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 1),
            "Expected same sequence number");

    itm_meta.revSeqno++;
    cas_for_set = last_meta.cas;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(last_uuid.load(), vb_uuid, "Expected valid vbucket uuid");
    checkeq(last_seqno.load(),
            static_cast<uint64_t>(high_seqno + 3),
            "Expected valid sequence number");

    // Make sure the item expiration was processed correctly
    testHarness->time_travel(301);
    auto ret = get(h, nullptr, key, Vbid(0));
    checkeq(cb::engine_errc::no_such_key, ret.first, "Failed to get value.");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_set_with_meta_by_force(EngineIface* h) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";

    // init some random metadata
    ItemMetaData itm_meta(0xdeadbeef, 10, 0xdeadbeef, time(nullptr) + 300);

    // Pass true to force SetWithMeta.
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          val,
                          strlen(val),
                          Vbid(0),
                          &itm_meta,
                          0,
                          true),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    // get metadata again to verify that the warmup loads an item correctly.
    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(uint64_t{10},
            errorMetaPair.second.seqno,
            "Expected seqno to match");
    checkeq(uint64_t{0xdeadbeef},
            errorMetaPair.second.cas,
            "Expected cas to match");
    checkeq(uint32_t{0xdeadbeef},
            errorMetaPair.second.flags,
            "Expected flags to match");

    check_key_value(h, key, val, strlen(val));

    return SUCCESS;
}

static enum test_result test_set_with_meta_deleted(EngineIface* h) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    auto newValLen = (uint16_t)strlen(newVal);

    // check the stat
    checkeq(0, get_int_stat(h, "ep_num_ops_set_meta"), "Expect zero ops");
    checkeq(0,
            get_int_stat(h, "ep_num_ops_get_meta_on_set_meta"),
            "Expect zero ops");

    // create a new key
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, val),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // delete the key
    checkeq(ENGINE_SUCCESS, del(h, key, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    cb::EngineErrorMetadataPair errorMetaPair;

    // get metadata for the key
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = errorMetaPair.second.cas;
    // init some random metadata
    ItemMetaData itm_meta(
            0xdeadbeef, 10, 0xdeadbeef, 1735689600); // expires in 2025

    // do set_with_meta with an incorrect cas for a deleted item. should fail.
    checkeq(ENGINE_KEY_ENOENT,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          1229),
            "Expected key_not_found error");
    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Failed op does not count");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkPersistentBucketTempItems(h, 1);

    // do set with meta with the correct cas value. should pass.
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    // check the stat
    checkeq(1, get_int_stat(h, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(0,
            get_int_stat(h, "ep_num_ops_get_meta_on_set_meta"),
            "Expect some ops");
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    ItemMetaData metadata(0xdeadbeef, 10, 0xdeadbeef, 1735689600);
    verifyMetaData(metadata, errorMetaPair.second);
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    return SUCCESS;
}

static enum test_result test_set_with_meta_nonexistent(EngineIface* h) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    size_t valLen = strlen(val);

    // check the stat
    checkeq(0, get_int_stat(h, "ep_num_ops_set_meta"), "Expect zero ops");

    cb::EngineErrorMetadataPair errorMetaPair;

    // get metadata for the key
    check(!get_meta(h, key, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = errorMetaPair.second.cas;
    // init some random metadata
    ItemMetaData itm_meta(
            0xdeadbeef, 10, 0xdeadbeef, 1735689600); // expires in 2025

    // do set_with_meta with an incorrect cas for a non-existent item. should fail.
    checkeq(ENGINE_KEY_ENOENT,
            set_with_meta(
                    h, key, keylen, val, valLen, Vbid(0), &itm_meta, 1229),
            "Expected key_not_found error");
    // check the stat
    checkeq(0,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Failed op does not count");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");

    // do set with meta with the correct cas value. should pass.
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          keylen,
                          val,
                          valLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);

    // check the stat
    checkeq(1, get_int_stat(h, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    ItemMetaData metadata(0xdeadbeef, 10, 0xdeadbeef, 1735689600);
    verifyMetaData(metadata, errorMetaPair.second);
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");
    checkeq(1, get_int_stat(h, "curr_items"), "Expected single curr_items");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_set(EngineIface* h) {
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    // check the stat
    size_t temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{0}, temp, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");

    // attempt set_with_meta. should fail since cas is no longer valid.
    ItemMetaData meta(errorMetaPair.second.cas,
                      errorMetaPair.second.seqno + 2,
                      errorMetaPair.second.flags,
                      errorMetaPair.second.exptime);
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h,
                          key1,
                          keylen1,
                          nullptr,
                          0,
                          Vbid(0),
                          &meta,
                          errorMetaPair.second.cas),
            "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{0}, temp, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "someothervalue"),
            "Failed set.");

    // attempt set_with_meta. should fail since cas is no longer valid.
    meta = ItemMetaData(errorMetaPair.second.cas,
                        errorMetaPair.second.seqno + 2,
                        errorMetaPair.second.flags,
                        errorMetaPair.second.exptime);
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h,
                          key1,
                          keylen1,
                          nullptr,
                          0,
                          Vbid(0),
                          &meta,
                          errorMetaPair.second.cas),
            "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{0}, temp, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_delete(EngineIface* h) {
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    size_t keylen2 = strlen(key2);
    // check the stat
    size_t temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{0}, temp, "Expect zero op");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");

    // do a concurrent delete that changes the cas
    checkeq(ENGINE_SUCCESS, del(h, key1, 0, Vbid(0)), "Delete failed");

    // attempt set_with_meta. should fail since cas is no longer valid.
    ItemMetaData meta(errorMetaPair.second.cas,
                      errorMetaPair.second.seqno,
                      errorMetaPair.second.flags,
                      errorMetaPair.second.exptime);
    checkeq(ENGINE_KEY_ENOENT,
            set_with_meta(h,
                          key1,
                          keylen1,
                          nullptr,
                          0,
                          Vbid(0),
                          &meta,
                          errorMetaPair.second.cas,
                          true),
            (std::string{"Expected invalid cas error (KEY_EXISTS or"
                         " KEY_ENOENT), got: "} +
             ::to_string(last_status.load()))
                    .c_str());

    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{0}, temp, "Expect zero op");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h);
    check(get_meta(h, key1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");

    // do a concurrent delete. should fail.
    checkeq(ENGINE_KEY_ENOENT, del(h, key1, 0, Vbid(0)), "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    meta = ItemMetaData(errorMetaPair.second.cas,
                        errorMetaPair.second.seqno,
                        errorMetaPair.second.flags,
                        errorMetaPair.second.exptime);
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key1,
                          keylen1,
                          nullptr,
                          0,
                          Vbid(0),
                          &meta,
                          errorMetaPair.second.cas,
                          true),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{1}, temp, "Expect some op");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, key2, errorMetaPair),
          "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");

    // do a concurrent delete. should fail.
    checkeq(ENGINE_KEY_ENOENT, del(h, key2, 0, Vbid(0)), "Delete failed");

    // Attempt set_with_meta. This should pass as we set a new key passing 0 as
    // command CAS.
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h, key2, keylen2, nullptr, 0, Vbid(0), &meta, 0, true),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, "ep_num_ops_set_meta");
    checkeq(size_t{2}, temp, "Expect some ops");

    return SUCCESS;
}

static enum test_result test_set_with_meta_xattr(EngineIface* h) {
    const char* key = "set_with_meta_xattr_key";

    // Create XATTR doc with JSON body
    std::string value_data = R"({"json":"yes"})";
    std::vector<char> data = createXattrValue(value_data);

    const void* cookie = testHarness->create_cookie(h);

    // store a value (so we can get its metadata)
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, value_data.c_str()),
            "Failed set.");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, key, errorMetaPair), "Expected to get meta");

    //init the meta data
    ItemMetaData itm_meta(errorMetaPair.second.cas,
                          errorMetaPair.second.seqno,
                          errorMetaPair.second.flags,
                          errorMetaPair.second.exptime);

    int force = 0;
    if (testHarness->get_current_testcase()->cfg.find(
                "conflict_resolution_type=lww") != std::string::npos) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Only enable XATTR
    testHarness->set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_XATTR);

    // Set with the same meta data but now with the xattr/json value
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key,
                          strlen(key),
                          data.data(),
                          data.size(),
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas,
                          force,
                          PROTOCOL_BINARY_DATATYPE_XATTR,
                          cookie),
            "Expected item to be stored");

    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected the set_with_meta to be successful");

    // set_with_meta will mark JSON input as JSON
    item_info info;
    check(get_item_info(h, &info, key, Vbid(0)), "get_item_info failed");
    checkeq(int(PROTOCOL_BINARY_DATATYPE_JSON|PROTOCOL_BINARY_DATATYPE_XATTR),
        int(info.datatype),
        "Expected datatype to be JSON and XATTR");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
        //evict the key
        evict_key(h, key);

        //set with the same meta data but now as RAW BYTES.
        //This should result in a bg fetch
        checkeq(ENGINE_KEY_EEXISTS,
                set_with_meta(h,
                              key,
                              strlen(key),
                              data.data(),
                              data.size(),
                              Vbid(0),
                              &itm_meta,
                              last_meta.cas,
                              force,
                              PROTOCOL_BINARY_RAW_BYTES,
                              cookie),
                "Expected return code to be EEXISTS");
    }

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_delete_with_meta_xattr(EngineIface* h) {
    const char* key1 = "delete_with_meta_xattr_key1";

    const void* cookie = testHarness->create_cookie(h);

    // Create XATTR doc with a JSON body
    // In practice a del_with_meta should come along with only XATTR, but
    // the command should work with a complete xattr/body blob
    std::string body = R"({"key1":"value","key2":"value"})";
    std::vector<char> data = createXattrValue(body);
    cb::xattr::Blob xattr({data.data(), data.size()}, false);
    xattr.prune_user_keys();
    data.resize(xattr.finalize().size());

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key1, body.data()),
            "Failed to store key1.");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
    }

    // Get the metadata so we can build a del_with_meta
    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key1, errorMetaPair), "Failed get_meta(key1)");

    // Init the meta data for a successful delete
    ItemMetaData itm_meta(
            errorMetaPair.second.cas + 1, // +1 for CAS conflicts
            errorMetaPair.second.seqno + 1, // +1 for seqno conflicts
            errorMetaPair.second.flags,
            errorMetaPair.second.exptime);

    int force = 0;
    if (testHarness->get_current_testcase()->cfg.find(
                "conflict_resolution_type=lww") != std::string::npos) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Now enable XATTR
    testHarness->set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_XATTR);

    // Now delete with a value (marked with XATTR)
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key1,
                          strlen(key1),
                          Vbid(0),
                          &itm_meta,
                          0, // cas
                          force,
                          cookie,
                          {}, // nmeta
                          PROTOCOL_BINARY_DATATYPE_XATTR,
                          data),
            "Expected delete OK");

    /* @todo this should fail, but doesn't. We've just deleted the item, but
    it remains in the hash-table (marked deleted), the subsequent set finds the
    item and is happy to process this SET_CAS operation (returns success).
    A delete with no body would of dropped it from the hash-table and the
    SET_CAS would return enoent, delete_with_meta /should/ I think have the same
    effect.
    checkeq(ENGINE_KEY_ENOENT,
            store(h,
                  h1,
                  nullptr,
                  OPERATION_SET,
                  key1,
                  body.data(),
                  nullptr,
                  last_cas.load()),
            "Failed to store key1.");
    */

    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected delete_with_meta(key1) to succeed");

    // Verify the new value is as expected
    item_info info;
    auto ret = get(h, nullptr, key1, Vbid(0), DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success, ret.first, "Failed to get(key1)");

    check(h->get_item_info(ret.second.get(), &info),
          "Failed get_item_info of key1");

    checkeq(data.size(), info.value[0].iov_len, "Value length mismatch");
    checkeq(0, memcmp(info.value[0].iov_base, data.data(), data.size()),
           "New body mismatch");
    checkeq(int(DocumentState::Deleted), int(info.document_state),
          "document_state is not DocumentState::Deleted");

    // The new value is XATTR
    checkeq(int(PROTOCOL_BINARY_DATATYPE_XATTR),
            int(info.datatype),
            "datatype isn't XATTR");

    // @todo implement test for the deletion of a value that has xattr using
    // a delete that has none (i.e. non-xattr/!spock client)

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_exp_persisted_set_del(EngineIface* h) {
    cb::EngineErrorMetadataPair errorMetaPair;

    check(!get_meta(h, "key3", errorMetaPair), "Expected get_meta() to fail");

    ItemMetaData itm_meta(1, 1, 0, 0);
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          "key3",
                          4,
                          "val0",
                          4,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected item to be stored");

    itm_meta.revSeqno = 2;
    itm_meta.cas = 2;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(
                    h, "key3", 4, "val1", 4, Vbid(0), &itm_meta, last_meta.cas),
            "Expected item to be stored");

    // MB-21725 Depending on how fast the flusher is, we may see 1 or 2.
    wait_for_stat_to_be_gte(h, "ep_total_persisted", 1);

    itm_meta.revSeqno = 3;
    itm_meta.cas = 3;
    itm_meta.exptime = 1735689600; // expires in 2025
    checkeq(ENGINE_SUCCESS,
            set_with_meta(
                    h, "key3", 4, "val1", 4, Vbid(0), &itm_meta, last_meta.cas),
            "Expected item to be stored");

    testHarness->time_travel(500000000);
    // Wait for the item to be expired, either by the pager,
    // or by access (as part of persistence callback from a
    // previous set - slow disk), or the compactor (unlikely).
    wait_for_expired_items_to_be(h, 1);

    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    check(get_meta(h, "key3", errorMetaPair), "Expected to get meta");
    checkeq(uint64_t{4}, errorMetaPair.second.seqno, "Expected seqno to match");
    checkne(uint64_t{3},
            errorMetaPair.second.cas,
            "Expected cas to be different");
    checkeq(uint32_t{0}, errorMetaPair.second.flags, "Expected flags to match");

    return SUCCESS;
}

static enum test_result test_temp_item_deletion(EngineIface* h) {
    // Do get_meta for an existing key
    char const *k1 = "k1";

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, k1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    checkeq(ENGINE_SUCCESS, del(h, k1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    // Issue a get_meta for a deleted key. This will need to bring in a temp
    // item into the hashtable as a placeholder for the (deleted) metadata
    // which needs to be loaded from disk via BG fetch
    // We need to temporarily disable the reader threads as to prevent the
    // BGfetch from immediately running and removing our temp_item before
    // we've had chance to validate its existence.
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_reader_threads",
              "0");

    // Disable nonio so that we have better control of the expirypager
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_nonio_threads",
              "0");

    // Tell the harness not to handle EWOULDBLOCK for us - we want it to
    // be outstanding while we check the below stats.
    const void* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);

    cb::EngineErrorMetadataPair errorMetaPair;

    check(!get_meta(h, k1, errorMetaPair, cookie),
          "Expected get_meta to fail (EWOULDBLOCK)");
    checkeq(cb::engine_errc::would_block,
            errorMetaPair.first,
            "Expected EWOULDBLOCK");

    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkeq(1,
            get_int_stat(h, "curr_temp_items"),
            "Expected single temp_items");

    // Re-enable EWOULDBLOCK handling (and reader threads), and re-issue.
    testHarness->set_ewouldblock_handling(cookie, true);
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_reader_threads",
              "1");

    check(get_meta(h, k1, errorMetaPair, cookie),
          "Expected get_meta to succeed");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");

    // Even though 2 get_meta calls are made, we may have one or two bg fetches
    // done. That is if first bgfetch restores in HT, the deleted item from
    // disk, before the second get_meta call tries to find that item in HT,
    // we will have only 1 bgfetch
    wait_for_stat_to_be_gte(h, "ep_bg_meta_fetched", 1);
    int exp_get_meta_ops = get_int_stat(h, "ep_num_ops_get_meta");

    // Do get_meta for a non-existing key.
    char const *k2 = "k2";
    check(!get_meta(h, k2, errorMetaPair), "Expected get meta to return false");
    checkeq(cb::engine_errc::no_such_key,
            errorMetaPair.first,
            "Expected no_such_key");

    // This call for get_meta may or may not result in bg fetch because
    // bloomfilter may predict that key does not exist.
    // However we still must increment the ep_num_ops_get_meta count
    checkeq(exp_get_meta_ops + 1,
            get_int_stat(h, "ep_num_ops_get_meta"),
            "Num get meta ops not as expected");

    // Trigger the expiry pager and verify that two temp items are deleted
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_nonio_threads",
              "1");

    // When bloom filters are on, it skips 1 of the expired items.
    int ep_expired_pager = get_bool_stat(h, "ep_bfilter_enabled") ? 1 : 2;
    wait_for_stat_to_be(h, "ep_expired_pager", ep_expired_pager);

    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Expected zero temp_items");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_add_meta_conflict_resolution(EngineIface* h) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(ENGINE_SUCCESS,
            add_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta),
            "Expected to add item");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    int expected_bg_meta_fetched =
            get_bool_stat(h, "ep_bfilter_enabled") ? 0 : 1;
    checkeq(expected_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "ep_bg_meta_fetched");

    checkeq(ENGINE_SUCCESS, del(h, "key", 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    // Check all meta data is the same
    itemMeta.revSeqno++;
    itemMeta.cas++;
    checkeq(ENGINE_KEY_EEXISTS,
            add_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta),
            "Expected exists");
    expected_bg_meta_fetched = 0;
    if (isPersistentBucket(h)) {
        expected_bg_meta_fetched =
                get_bool_stat(h, "ep_bfilter_enabled") ? 1 : 2;
    }
    checkeq(expected_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "ep_bg_meta_fetched");

    checkeq(1,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    checkeq(ENGINE_KEY_EEXISTS,
            add_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    checkeq(ENGINE_KEY_EEXISTS,
            add_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    checkeq(ENGINE_SUCCESS,
            add_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta),
            "Expected to add item");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_set_meta_conflict_resolution(EngineIface* h) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(0,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Expect zero setMeta ops");

    checkeq(ENGINE_SUCCESS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    int expected_bg_meta_fetched = 0;
    if (isPersistentBucket(h)) {
        expected_bg_meta_fetched =
                get_bool_stat(h, "ep_bfilter_enabled") ? 0 : 1;
    }
    checkeq(expected_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "ep_bg_meta_fetched");

    // Check all meta data is the same
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected exists");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected exists");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check has newer flags passes
    itemMeta.flags = 0xdeadbeff;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check that newer exptime wins
    itemMeta.exptime = time(nullptr) + 10;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected exists");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected exists");
    checkeq(4,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(4,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    expected_bg_meta_fetched =
            (!isPersistentBucket(h) || get_bool_stat(h, "ep_bfilter_enabled"))
                    ? 0
                    : 1;
    checkeq(expected_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "ep_bg_meta_fetched");

    return SUCCESS;
}

static enum test_result test_set_meta_lww_conflict_resolution(EngineIface* h) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(0,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Expect zero setMeta ops");

    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    int expected_bg_meta_fetched =
            (!isPersistentBucket(h) || get_bool_stat(h, "ep_bfilter_enabled"))
                    ? 0
                    : 1;
    checkeq(expected_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "ep_bg_meta_fetched");

    // Check all meta data is the same
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected exists");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check that an older cas fails
    itemMeta.cas = 0xdeadbeee;
    checkeq(ENGINE_KEY_EEXISTS,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected exists");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_set_meta_res_fail"),
            "Expected set meta conflict resolution failure");

    // Check that a higher cas passes
    itemMeta.cas = 0xdeadbeff;
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check that we fail requests if the force flag is not set
    itemMeta.cas = 0xdeadbeff + 1;
    checkeq(ENGINE_EINVAL,
            set_with_meta(
                    h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0, 0 /*options*/),
            "Expected EINVAL");

    return SUCCESS;
}

static enum test_result test_del_meta_conflict_resolution(EngineIface* h) {
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, "key", "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected delete ok");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    // Check all meta data is the same
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
            "Expected delete meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
            "Expected delete meta conflict resolution failure");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
            "Expected delete meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected exists");
    checkeq(4, get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(4, get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_del_meta_lww_conflict_resolution(EngineIface* h) {
    item *i = nullptr;
    item_info info;

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");

    h->get_item_info(i, &info);
    wait_for_flusher_to_settle(h);
    h->release(i);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = info.cas + 1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // first check the command fails if no force is set
    checkeq(ENGINE_EINVAL,
            del_with_meta(h, "key", 3, Vbid(0), &itemMeta, 0, 0 /*options*/),
            "Expected EINVAL");

    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          "key",
                          3,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    // Check all meta data is the same
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h,
                          "key",
                          3,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected exists");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
            "Expected delete meta conflict resolution failure");

    // Check that higher rev seqno but lower cas fails
    itemMeta.cas = info.cas;
    itemMeta.revSeqno = 11;
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h,
                          "key",
                          3,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected exists");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_del_meta_res_fail"),
            "Expected delete meta conflict resolution failure");

    // Check that a higher cas and lower rev seqno passes
    itemMeta.cas = info.cas + 2;
    itemMeta.revSeqno = 9;
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          "key",
                          3,
                          Vbid(0),
                          &itemMeta,
                          0,
                          FORCE_ACCEPT_WITH_META_OPS),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected sucess");

    return SUCCESS;
}

static enum test_result test_getMeta_with_item_eviction(EngineIface* h) {
    char const *key = "test_get_meta";
    item *i = nullptr;
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, key, Vbid(0), "Ejected.");

    Item *it = reinterpret_cast<Item*>(i);

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");
    ItemMetaData metadata(it->getCas(), it->getRevSeqno(),
                          it->getFlags(), it->getExptime());
    verifyMetaData(metadata, errorMetaPair.second);

    h->release(i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_and_check_drift_stats(
        EngineIface* h) {
    // Activate n vbuckets (vb 0 is already)
    const int n_vbuckets = 10;
    for (int ii = 1; ii < n_vbuckets; ii++) {
        check(set_vbucket_state(h, Vbid(ii), vbucket_state_active),
              "Failed to set vbucket state.");
    }

    // Let's make vbucket n/2 be the one who is ahead, n/3 is behind
    const int aheadVb = n_vbuckets/2;
    const int behindVb = n_vbuckets/3;
    checkne(aheadVb, behindVb, "Cannot have the same VB as ahead/behind");

    HLC hlc(0 /*init HLC*/,
            HlcCasSeqnoUninitialised,
            std::chrono::microseconds(0) /*ahead threshold*/,
            std::chrono::microseconds(0) /*behind threshold*/);

    // grab the drift behind threshold
    uint64_t driftBehindThreshold =
            get_ull_stat(h, "ep_hlc_drift_ahead_threshold_us", nullptr);
    // Create n keys
    const int n_keys = 5;
    for (int ii = 0 ; ii < n_vbuckets; ii++) {
        for (int k = 0; k < n_keys; k++) {
            std::string key = "key_" + std::to_string(k);
            ItemMetaData itm_meta;
            itm_meta.cas = hlc.nextHLC();
            if (ii == aheadVb) {
                // Push this guy *far* ahead (1 year)
                itm_meta.cas += 3154E10;
            } else if(ii == behindVb) {
                // just be sure it was already greater then 1 + driftthreshold
                checkge(itm_meta.cas, uint64_t(1) + driftBehindThreshold,
                        "HLC was already zero");
                // set to be way way behind...
                itm_meta.cas = 1;
            }
            checkeq(ENGINE_SUCCESS,
                    set_with_meta(h,
                                  key.data(),
                                  key.size(),
                                  nullptr,
                                  0,
                                  Vbid(ii),
                                  &itm_meta,
                                  0,
                                  FORCE_ACCEPT_WITH_META_OPS),
                    "Expected item to be stored");
            checkeq(cb::mcbp::Status::Success, last_status.load(),
                    "Expected success");
        }
    }

    // Bucket stats should report drift
    checkge(get_ull_stat(h, "ep_active_hlc_drift"),
            uint64_t(0),
            "Expected drift above zero");
    checkeq(uint64_t(n_keys * n_vbuckets),
            get_ull_stat(h, "ep_active_hlc_drift_count"),
            "Expected ahead counter to match mutations");

    // Victim VBs should have exceptions
    {
        std::string vbAheadName = "vb_" + std::to_string(aheadVb);
        std::string ahead_threshold_exceeded = vbAheadName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbAheadName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbAheadName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(aheadVb);
        checkeq(uint64_t(n_keys),
                get_ull_stat(
                        h, ahead_threshold_exceeded.data(), details.data()),
                "Expected ahead threshold to match mutations");
        checkeq(uint64_t(0),
                get_ull_stat(
                        h, behind_threshold_exceeded.data(), details.data()),
                "Expected no behind exceptions");
        checkge(get_ull_stat(h, total_abs_drift.data(), details.data()),
                uint64_t(0),
                "Expected some drift");
    }

    {
        std::string vbBehindName = "vb_" + std::to_string(behindVb);
        std::string ahead_threshold_exceeded = vbBehindName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbBehindName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbBehindName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(behindVb);
        checkeq(uint64_t(n_keys),
                get_ull_stat(
                        h, behind_threshold_exceeded.data(), details.data()),
                "Expected behind threshold to match mutations");
        checkeq(uint64_t(0),
                get_ull_stat(
                        h, ahead_threshold_exceeded.data(), details.data()),
                "Expected no ahead exceptions");
        checkge(get_ull_stat(h, total_abs_drift.data(), details.data()),
                uint64_t(0),
                "Expected some drift");
    }


    return SUCCESS;
}

static enum test_result test_del_with_meta_and_check_drift_stats(
        EngineIface* h) {
    // Activate n vbuckets (vb 0 is already)
    const int n_vbuckets = 10;
    for (int ii = 1; ii < n_vbuckets; ii++) {
        check(set_vbucket_state(h, Vbid(ii), vbucket_state_active),
              "Failed to set vbucket state.");
    }

    // Let's make vbucket n/2 be the one who is ahead, n/3 is behind
    const int aheadVb = n_vbuckets/2;
    const int behindVb = n_vbuckets/3;
    checkne(aheadVb, behindVb, "Cannot have the same VB as ahead/behind");

    HLC hlc(0 /*init HLC*/,
            HlcCasSeqnoUninitialised,
            std::chrono::microseconds(0) /*ahead threshold*/,
            std::chrono::microseconds(0) /*behind threshold*/);

    // grab the drift behind threshold
    uint64_t driftBehindThreshold =
            get_ull_stat(h, "ep_hlc_drift_ahead_threshold_us", nullptr);
    // Create n keys * n_vbuckets
    const int n_keys = 5;
    for (int ii = 0 ; ii < n_vbuckets; ii++) {
        for (int k = 0; k < n_keys; k++) {
            std::string key = "key_" + std::to_string(k);

            // In the del_with_meta test we want to pretend a del_wm came from
            // the past, so we want to ensure a delete doesn't get rejected
            // by LWW conflict resolution, thus write all documents that are
            // going to be deleted with set_with_meta, and write them way in the past.
            // This will trigger threshold and increment drift stats... so we
            // account for these later
            ItemMetaData itm_meta;
            itm_meta.cas = 1; // set to 1
            checkeq(ENGINE_SUCCESS,
                    set_with_meta(h,
                                  key.data(),
                                  key.size(),
                                  nullptr,
                                  0,
                                  Vbid(ii),
                                  &itm_meta,
                                  0,
                                  FORCE_ACCEPT_WITH_META_OPS),
                    "Expected item to be stored");
            checkeq(cb::mcbp::Status::Success, last_status.load(),
                    "Expected success");
        }
    }

    checkeq(uint64_t(0),
            get_ull_stat(h, "ep_active_ahead_exceptions"),
            "Expected ahead counter to match mutations");
    checkeq(uint64_t(n_keys * n_vbuckets),
            get_ull_stat(h, "ep_active_behind_exceptions"),
            "Expected behind counter to match mutations");

    // Del_with_meta n_keys to n_vbuckets
    for (int ii = 0 ; ii < n_vbuckets; ii++) {
        for (int k = 0; k < n_keys; k++) {
            std::string key = "key_" + std::to_string(k);
            ItemMetaData itm_meta;
            itm_meta.cas = hlc.nextHLC();
            if (ii == aheadVb) {
                // Push this guy *far* ahead (1 year)
                itm_meta.cas += 3154E10;
            } else if(ii == behindVb) {
                // just be sure it was already greater than 1 + driftthreshold
                checkge(itm_meta.cas, uint64_t(1) + driftBehindThreshold,
                        "HLC was already zero");
                // set to be way way behind, but ahead of the documents we have set
                itm_meta.cas = 2;
            }
            checkeq(ENGINE_SUCCESS,
                    del_with_meta(h,
                                  key.data(),
                                  key.size(),
                                  Vbid(ii),
                                  &itm_meta,
                                  1,
                                  FORCE_ACCEPT_WITH_META_OPS),
                    "Expected delete OK");
            checkeq(cb::mcbp::Status::Success, last_status.load(),
                    "Expected success");
        }
    }

    // Bucket stats should report drift
    checkge(get_ull_stat(h, "ep_active_hlc_drift"),
            uint64_t(0),
            "Expected drift above zero");
    checkeq(2 * uint64_t(n_keys * n_vbuckets),
            get_ull_stat(h, "ep_active_hlc_drift_count"),
            "Expected ahead counter to match mutations");

    // and should report total exception of all VBs
    checkeq(uint64_t(n_keys),
            get_ull_stat(h, "ep_active_ahead_exceptions"),
            "Expected ahead counter to match mutations");
    checkeq(uint64_t(n_keys + (n_keys * n_vbuckets)),
            get_ull_stat(h, "ep_active_behind_exceptions"),
            "Expected behind counter to match mutations");

    // Victim VBs should have exceptions
    {
        std::string vbAheadName = "vb_" + std::to_string(aheadVb);
        std::string ahead_threshold_exceeded = vbAheadName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbAheadName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbAheadName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(aheadVb);

        checkeq(uint64_t(n_keys),
                get_ull_stat(
                        h, ahead_threshold_exceeded.data(), details.data()),
                "Expected ahead threshold to match mutations");
        checkge(get_ull_stat(h, total_abs_drift.data(), details.data()),
                uint64_t(0),
                "Expected some drift");
    }

    {
        std::string vbBehindName = "vb_" + std::to_string(behindVb);
        std::string ahead_threshold_exceeded = vbBehindName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbBehindName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbBehindName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(behindVb);

        // *2 behind due to the initial set_with_meta
        checkeq(uint64_t(n_keys * 2),
                get_ull_stat(
                        h, behind_threshold_exceeded.data(), details.data()),
                "Expected behind threshold to match mutations");
        checkeq(uint64_t(0),
                get_ull_stat(
                        h, ahead_threshold_exceeded.data(), details.data()),
                "Expected no ahead exceptions");
        checkge(get_ull_stat(h, total_abs_drift.data(), details.data()),
                uint64_t(0),
                "Expected some drift");
    }


    return SUCCESS;
}

static enum test_result test_setting_drift_threshold(EngineIface* h) {
    std::vector<std::tuple<std::string, std::string, std::string> > configData =
        {std::make_tuple("ep_hlc_drift_ahead_threshold_us",
                         "hlc_drift_ahead_threshold_us",
                         "vb_0:drift_ahead_threshold"),
         std::make_tuple("ep_hlc_drift_behind_threshold_us",
                         "hlc_drift_behind_threshold_us",
                         "vb_0:drift_behind_threshold")};

    std::vector<std::pair<std::string, std::chrono::microseconds> > values = {
        {"0", std::chrono::microseconds(0)},
        {"1", std::chrono::microseconds(1)},
        {"-1", std::chrono::microseconds(-1)},
        {"-0", std::chrono::microseconds(0)},
        {"18446744073709551615",
         std::chrono::microseconds(18446744073709551615ull)}};

    for (auto data : values) {
        for (auto conf : configData) {
            check(set_param(h,
                            cb::mcbp::request::SetParamPayload::Type::Vbucket,
                            std::get<1>(conf).c_str(),
                            data.first.data()),
                  "Expected set_param success");

            checkeq(int64_t(data.second.count()),
                    int64_t(get_ull_stat(
                            h, std::get<0>(conf).c_str(), nullptr)),
                    "Expected the stat to change to the new value");

            // The VB stat values are in nanoseconds
            checkeq(int64_t(std::chrono::nanoseconds(data.second).count()),
                    int64_t(get_ull_stat(
                            h, std::get<2>(conf).c_str(), "vbucket-details 0")),
                    "Expected the VB stats to change to the new value");
        }
    }
    return SUCCESS;
}

/*
 * Perform set_with_meta and check CAS regeneration is ok.
 */
static enum test_result test_cas_regeneration(EngineIface* h) {
    // First store a key from the past (small CAS).
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    int force = 0;

    if (testHarness->get_current_testcase()->cfg.find(
                "conflict_resolution_type=lww") != std::string::npos) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Set the key with a low CAS value
    checkeq(ENGINE_SUCCESS,
            set_with_meta(
                    h, "key", 3, nullptr, 0, Vbid(0), &itemMeta, 0, force),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, "key", errorMetaPair), "Failed to get_meta");

    // CAS must be what we set.
    checkeq(itemMeta.cas,
            errorMetaPair.second.cas,
            "CAS is not the value we stored");

    itemMeta.cas++;

    // Check that the code requires skip
    checkeq(ENGINE_EINVAL,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          REGENERATE_CAS /*but no skip*/),
            "Expected EINVAL");

    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG),
            "Expected item to be stored");

    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected success");

    check(get_meta(h, "key", errorMetaPair), "Failed to get_meta");

    uint64_t cas = errorMetaPair.second.cas;
    // Check item has a new CAS
    checkne(itemMeta.cas, cas, "CAS was not regenerated");

    itemMeta.cas++;
    // All flags set should still regen the cas (lww and seqno)
    checkeq(ENGINE_SUCCESS,
            set_with_meta(
                    h,
                    "key",
                    3,
                    nullptr,
                    0,
                    Vbid(0),
                    &itemMeta,
                    0,
                    REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | force),
            "Expected item to be stored");

    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected success");

    check(get_meta(h, "key", errorMetaPair), "Failed to get_meta");
    // Check item has a new CAS
    checkne(itemMeta.cas, errorMetaPair.second.cas, "CAS was not regenerated");
    checkne(cas, errorMetaPair.second.cas, "CAS was not regenerated");
    return SUCCESS;
}

/*
 * Perform del_with_meta and check CAS regeneration is ok.
 */
static enum test_result test_cas_regeneration_del_with_meta(EngineIface* h) {
    const std::string key("key");
    // First store a key from the past (small CAS).
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    int force = 0;

    if (testHarness->get_current_testcase()->cfg.find(
                "conflict_resolution_type=lww") != std::string::npos) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Set the key with a low CAS value
    checkeq(ENGINE_SUCCESS,
            set_with_meta(h,
                          key.c_str(),
                          key.length(),
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          force),
            "Expected item to be stored");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected success");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key.c_str(), errorMetaPair), "Failed to get_meta");
    // CAS must be what we set.
    checkeq(itemMeta.cas,
            errorMetaPair.second.cas,
            "CAS is not the value we stored");

    itemMeta.cas++;

    // Check that the code requires skip
    checkeq(ENGINE_EINVAL,
            del_with_meta(h,
                          key.c_str(),
                          key.length(),
                          Vbid(0),
                          &itemMeta,
                          0,
                          REGENERATE_CAS /*but no skip*/),
            "Expected EINVAL");

    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key.c_str(),
                          key.length(),
                          Vbid(0),
                          &itemMeta,
                          0,
                          REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected success");

    check(get_meta(h, key.c_str(), errorMetaPair), "Failed to get_meta");
    uint64_t cas = errorMetaPair.second.cas;
    // Check item has a new CAS
    checkne(itemMeta.cas, cas, "CAS was not regenerated");

    itemMeta.cas++;
    // All flags set should still regen the cas (lww and seqno)
    checkeq(ENGINE_SUCCESS,
            del_with_meta(
                    h,
                    key.c_str(),
                    key.length(),
                    Vbid(0),
                    &itemMeta,
                    0,
                    REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | force),
            "Expected delete OK");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected success");

    check(get_meta(h, key.c_str(), errorMetaPair), "Failed to get_meta");
    // Check item has a new CAS
    checkne(itemMeta.cas, errorMetaPair.second.cas, "CAS was not regenerated");
    checkne(cas, errorMetaPair.second.cas, "CAS was not regenerated");

    return SUCCESS;
}

/*
 * Test that we can send options and nmeta
 * The nmeta is just going to be ignored though, but should not fail
 */
static enum test_result test_cas_options_and_nmeta(EngineIface* h) {
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // Watson (4.6) accepts valid encodings, but ignores them
    std::vector<char> junkMeta = {-2,-1,2,3};

    int force = 0;

    if (testHarness->get_current_testcase()->cfg.find(
                "conflict_resolution_type=lww") != std::string::npos) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Set the key and junk nmeta
    checkeq(ENGINE_EINVAL,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          force,
                          PROTOCOL_BINARY_RAW_BYTES,
                          nullptr,
                          junkMeta),
            "Expected EINVAL");

    // Set the key and junk nmeta that's quite large
    junkMeta.resize(std::numeric_limits<uint16_t>::max());
    checkeq(ENGINE_EINVAL,
            set_with_meta(h,
                          "key",
                          3,
                          nullptr,
                          0,
                          Vbid(0),
                          &itemMeta,
                          0,
                          force,
                          PROTOCOL_BINARY_RAW_BYTES,
                          nullptr,
                          junkMeta),
            "Expected EINVAL");

    // Test that valid meta can be sent. It should be ignored and success
    // returned
    // Encodings which should not fail, see ext_meta_parser.cc
#pragma pack(1)
    struct adjusted_time_metadata {
        uint8_t type;
        uint16_t length;
        int64_t value;
    };
    struct conf_res_metadata {
        uint8_t type;
        uint16_t length;
        uint8_t value;
    };
    struct with_cas_metadata1 {
        uint8_t version;
        adjusted_time_metadata adjusted_time;
    };
    struct with_cas_metadata2 {
        uint8_t version;
        conf_res_metadata conf_res;
    };
    struct with_cas_metadata3 {
        uint8_t version;
        conf_res_metadata conf_res;
        adjusted_time_metadata adjusted_time;
    };
    struct with_cas_metadata4 {
        uint8_t version;
        adjusted_time_metadata adjusted_time;
        conf_res_metadata conf_res;
    };
#pragma pack()

    {
        with_cas_metadata1 validMetaData = {META_EXT_VERSION_ONE,
                                            {CMD_META_ADJUSTED_TIME,
                                             htons(sizeof(int64_t)), -1}};
        std::vector<char> validMetaVector(reinterpret_cast<char*>(&validMetaData),
                                          reinterpret_cast<char*>(&validMetaData) +
                                          sizeof(validMetaData));

        // Set the key with a low CAS value and real nmeta
        checkeq(ENGINE_SUCCESS,
                set_with_meta(h,
                              "key1",
                              4,
                              nullptr,
                              0,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              PROTOCOL_BINARY_RAW_BYTES,
                              nullptr,
                              validMetaVector),
                "Expected item to be stored");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        checkeq(ENGINE_SUCCESS,
                del_with_meta(h,
                              "key1",
                              4,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              nullptr,
                              validMetaVector),
                "Expected delete success");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");
    }

    {
        with_cas_metadata2 validMetaData = {META_EXT_VERSION_ONE,
                                            {CMD_META_CONFLICT_RES_MODE,
                                             htons(sizeof(uint8_t)), 0xff}};
        std::vector<char> validMetaVector(reinterpret_cast<char*>(&validMetaData),
                                          reinterpret_cast<char*>(&validMetaData) +
                                          sizeof(validMetaData));

        // Set the key with a low CAS value and real nmeta
        checkeq(ENGINE_SUCCESS,
                set_with_meta(h,
                              "key2",
                              4,
                              nullptr,
                              0,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              PROTOCOL_BINARY_RAW_BYTES,
                              nullptr,
                              validMetaVector),
                "Expected item to be stored");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        checkeq(ENGINE_SUCCESS,
                del_with_meta(h,
                              "key2",
                              4,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              nullptr,
                              validMetaVector),
                "Expected delete success");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");
    }

    {
        with_cas_metadata3 validMetaData = {META_EXT_VERSION_ONE,
                                            {CMD_META_CONFLICT_RES_MODE,
                                             htons(sizeof(uint8_t)), 0xff},
                                            {CMD_META_ADJUSTED_TIME,
                                             htons(sizeof(int64_t)), -1}};
        std::vector<char> validMetaVector(reinterpret_cast<char*>(&validMetaData),
                                          reinterpret_cast<char*>(&validMetaData) +
                                          sizeof(validMetaData));

        // Set the key with a low CAS value and real nmeta
        checkeq(ENGINE_SUCCESS,
                set_with_meta(h,
                              "key3",
                              4,
                              nullptr,
                              0,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              PROTOCOL_BINARY_RAW_BYTES,
                              nullptr,
                              validMetaVector),
                "Expected item to be stored");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        checkeq(ENGINE_SUCCESS,
                del_with_meta(h,
                              "key3",
                              4,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              nullptr,
                              validMetaVector),
                "Expected delete OK");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");
    }

    {
        with_cas_metadata4 validMetaData = {META_EXT_VERSION_ONE,
                                            {CMD_META_ADJUSTED_TIME,
                                             htons(sizeof(int64_t)), -1},
                                            {CMD_META_CONFLICT_RES_MODE,
                                             htons(sizeof(uint8_t)), 0xff}};
        std::vector<char> validMetaVector(reinterpret_cast<char*>(&validMetaData),
                                          reinterpret_cast<char*>(&validMetaData) +
                                          sizeof(validMetaData));

        // Set the key with a low CAS value and real nmeta
        checkeq(ENGINE_SUCCESS,
                set_with_meta(h,
                              "key4",
                              4,
                              nullptr,
                              0,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              PROTOCOL_BINARY_RAW_BYTES,
                              nullptr,
                              validMetaVector),
                "Expected item to be stored");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        checkeq(ENGINE_SUCCESS,
                del_with_meta(h,
                              "key4",
                              4,
                              Vbid(0),
                              &itemMeta,
                              0,
                              force,
                              nullptr,
                              validMetaVector),
                "Expected delete ok");
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "Expected success");
    }

    return SUCCESS;
}

// A delete_with_meta with a large seqno (upper 16-bits dirty) should trigger
// a conflict if evicted and a second identical delete_with_meta occurs
static enum test_result test_MB29119(EngineIface* h) {
    const char* key1 = "delete_with_meta_key1";
    const size_t keylen = strlen(key1);
    RawItemMetaData itemMeta;

    // Overflow seqno from 48-bits
    itemMeta.revSeqno = 0x0080a80000000001;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    const void* cookie = testHarness->create_cookie(h);

    // delete an item with meta data
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h,
                          key1,
                          keylen,
                          Vbid(0),
                          &itemMeta,
                          0 /*cas*/,
                          0 /*options*/,
                          cookie),
            "Expected delete OK");

    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected success");

    wait_for_flusher_to_settle(h);
    // evict the key
    evict_key(h, key1);

    // Same key
    checkeq(ENGINE_KEY_EEXISTS,
            del_with_meta(h,
                          key1,
                          keylen,
                          Vbid(0),
                          &itemMeta,
                          0 /*cas*/,
                          0 /*options*/,
                          cookie),
            "Expected EEXISTS");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

/*
 * Test that we can send option of IS_EXPIRATION
 */
static enum test_result test_expiration_options(EngineIface* h) {
    const char* key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 10;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    checkeq(ENGINE_SUCCESS,
            store(h,
                  nullptr,
                  OPERATION_SET,
                  key,
                  "somevalue",
                  nullptr,
                  0,
                  Vbid(0),
                  100),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // delete an item with meta data indicating expiration
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key, keylen, Vbid(0), &itemMeta, 0, IS_EXPIRATION),
            "Expected delete success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_xdcr";

BaseTestCase testsuite_testcases[] = {

        // XDCR unit tests
        TestCase("get meta",
                 test_get_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta with extras",
                 test_get_meta_with_extras,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta deleted",
                 test_get_meta_deleted,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta nonexistent",
                 test_get_meta_nonexistent,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta followed by get",
                 test_get_meta_with_get,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta followed by set",
                 test_get_meta_with_set,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("get meta followed by delete",
                 test_get_meta_with_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get meta with xattr",
                 test_get_meta_with_xattr,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("add with meta",
                 test_add_with_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete with meta",
                 test_delete_with_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete with meta deleted",
                 test_delete_with_meta_deleted,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("delete with meta nonexistent",
                 test_delete_with_meta_nonexistent,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete with meta nonexistent no temp",
                 test_delete_with_meta_nonexistent_no_temp,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete_with_meta race with concurrent delete",
                 test_delete_with_meta_race_with_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete_with_meta race with concurrent set",
                 test_delete_with_meta_race_with_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set with meta",
                 test_set_with_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks when full
                  * eviction */
                 prepare_ep_bucket_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("set with meta by force",
                 test_set_with_meta_by_force,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set with meta deleted",
                 test_set_with_meta_deleted,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks when full
                  * eviction */
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("set with meta nonexistent",
                 test_set_with_meta_nonexistent,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set_with_meta race with concurrent set",
                 test_set_with_meta_race_with_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set_with_meta race with concurrent delete",
                 test_set_with_meta_race_with_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test set_with_meta exp persisted",
                 test_exp_persisted_set_del,
                 test_setup,
                 teardown,
                 "exp_pager_stime=3",
                 /* TODO RDB: curr_items not correct under Rocks in full
                  * eviction */
                 /* Requires persistence */
                 prepare_ep_bucket_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("test del meta conflict resolution",
                 test_del_meta_conflict_resolution,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks in full
                  * eviction */
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("test add meta conflict resolution",
                 test_add_meta_conflict_resolution,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks in full
                  * eviction */
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("test set meta conflict resolution",
                 test_set_meta_conflict_resolution,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test del meta lww conflict resolution",
                 test_del_meta_lww_conflict_resolution,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 /* TODO RDB: curr_items not correct under Rocks in full
                  * eviction */
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("test set meta lww conflict resolution",
                 test_set_meta_lww_conflict_resolution,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("set with meta xattr",
                 test_set_with_meta_xattr,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set with meta lww xattr",
                 test_set_with_meta_xattr,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("delete with meta xattr",
                 test_delete_with_meta_xattr,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete with meta lww xattr",
                 test_delete_with_meta_xattr,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("temp item deletion",
                 test_temp_item_deletion,
                 test_setup,
                 teardown,
                 "exp_pager_stime=1",
                 /* TODO RDB: curr_items not correct under Rocks full eviction*/
                 /* related to temp items in hash table */
                 prepare_ep_bucket_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("test get_meta with item_eviction",
                 test_getMeta_with_item_eviction,
                 test_setup,
                 teardown,
                 "item_eviction_policy=full_eviction",
                 prepare_full_eviction,
                 cleanup),

        TestCase("test set_with_meta and drift stats",
                 test_set_with_meta_and_check_drift_stats,
                 test_setup,
                 teardown,
                 "hlc_drift_ahead_threshold_us=5000000;"
                 "hlc_drift_behind_threshold_us=0;conflict_resolution_type=lww;"
                 "max_vbuckets=10;max_num_shards=4",
                 prepare,
                 cleanup),
        TestCase("test del_with_meta and drift stats",
                 test_del_with_meta_and_check_drift_stats,
                 test_setup,
                 teardown,
                 "hlc_drift_ahead_threshold_us=0;hlc_drift_behind_threshold_us="
                 "5000000;conflict_resolution_type=lww;max_vbuckets=10;max_num_"
                 "shards=4",
                 prepare,
                 cleanup),
        TestCase("test setting drift threshold",
                 test_setting_drift_threshold,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test CAS regeneration lww",
                 test_cas_regeneration,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("test CAS regeneration seqno",
                 test_cas_regeneration,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=seqno",
                 prepare,
                 cleanup),
        TestCase("test CAS regeneration seqno del_with_meta lww",
                 test_cas_regeneration_del_with_meta,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("test CAS regeneration seqno del_with_meta seqno",
                 test_cas_regeneration_del_with_meta,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=seqno",
                 prepare,
                 cleanup),
        TestCase("test CAS options and nmeta (lww)",
                 test_cas_options_and_nmeta,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=lww",
                 prepare,
                 cleanup),
        TestCase("test CAS options and nmeta (seqno)",
                 test_cas_options_and_nmeta,
                 test_setup,
                 teardown,
                 "conflict_resolution_type=seqno",
                 prepare,
                 cleanup),
        TestCase("getMetaData mb23905",
                 test_get_meta_mb23905,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),

        TestCase("MB29119",
                 test_MB29119,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("delete with meta with expiration option",
                 test_expiration_options,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase(nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
