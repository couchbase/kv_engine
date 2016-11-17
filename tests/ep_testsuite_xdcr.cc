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

#include "config.h"

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "hlc.h"

#include <platform/cb_malloc.h>

// Helper functions ///////////////////////////////////////////////////////////

static void verifyLastMetaData(ItemMetaData imd) {
    checkeq(imd.revSeqno, last_meta.revSeqno, "Seqno didn't match");
    checkeq(imd.cas, last_meta.cas, "Cas didn't match");
    checkeq(imd.exptime, last_meta.exptime, "Expiration time didn't match");
    checkeq(imd.flags, last_meta.flags, "Flags didn't match");
}

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "test_get_meta";
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    ItemMetaData metadata(it->getCas(), it->getRevSeqno(),
                          it->getFlags(), it->getExptime());
    verifyLastMetaData(metadata);

    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_extras(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1)
{
    const char *key1 = "test_getm_one";
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");

    wait_for_flusher_to_settle(h, h1);

    Item *it1 = reinterpret_cast<Item*>(i);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key1, true), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    ItemMetaData metadata1(it1->getCas(), it1->getRevSeqno(),
                           it1->getFlags(), it1->getExptime());
    verifyLastMetaData(metadata1);
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");
    h1->release(h, NULL, i);

    // restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    check(get_meta(h, h1, key1, true), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    verifyLastMetaData(metadata1);

    return SUCCESS;
}

static enum test_result test_get_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";
    item *i = NULL;

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");

    Item *it = reinterpret_cast<Item*>(i);
    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS, del(h, h1, key, it->getCas(), 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    // check the stat
    int temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(last_meta.revSeqno == it->getRevSeqno() + 1, "Expected seqno to match");
    check(last_meta.cas != it->getCas() , "Expected cas to be different");
    check(last_meta.flags == it->getFlags(), "Expected flags to match");

    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";

    // check the stat
    int temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(1, temp, "Expect one getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    // test get_meta followed by get for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    int temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(ENGINE_SUCCESS,
            h1->get(h, NULL, &i, key1, strlen(key1), 0), "Expected get success");
    h1->release(h, NULL, i);
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by get for a deleted key. should fail.
    checkeq(ENGINE_SUCCESS,
            del(h, h1, key1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &i, key1, strlen(key1), 0), "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(2, temp, "Expect more getMeta ops");

    // test get_meta followed by get for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            "Expected enoent");
    checkeq(ENGINE_KEY_ENOENT,
            h1->get(h, NULL, &i, key2, strlen(key2), 0), "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(3, temp, "Expected one extra getMeta ops");

    return SUCCESS;
}

static enum test_result test_get_meta_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    ItemMetaData itm_meta;

    // test get_meta followed by set for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 1);

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_get_meta"), "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");
    // check the stat
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_get_meta"), "Expect one getMeta op");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");
    h1->release(h, NULL, i);

    // check curr, temp item counts
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // test get_meta followed by set for a deleted key. should pass.
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    wait_for_stat_to_be(h, h1, "curr_items", 0);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");

    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // check the stat
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_get_meta"), "Expect more getMeta ops");
    h1->release(h, NULL, i);

    // test get_meta followed by set for a nonexistent key. should pass.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key2, "someothervalue", &i),
            "Failed set.");
    // check the stat again
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_get_meta"),
            "Expected one extra getMeta ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;

    // test get_meta followed by delete for an existing key. should pass.
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    int temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by delete for a deleted key. should fail.
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key1, 0, 0), "Expected enoent");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(2, temp, "Expect more getMeta op");

    // test get_meta followed by delete for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key2, 0, 0), "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    checkeq(3, temp, "Expected one extra getMeta ops");

    return SUCCESS;
}

static enum test_result test_add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const char *key = "mykey";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    size_t temp = 0;

    // put some random metadata
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // store an item with meta data
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // store the item again, expect key exists
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
            "Expected add to fail when the item exists already");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key1 = "delete_with_meta_key1";
    const char *key2 = "delete_with_meta_key2";
    const char *key3 = "delete_with_meta_key3";
    const size_t keylen = strlen(key1);
    ItemMetaData itemMeta;
    uint64_t vb_uuid;
    uint32_t high_seqno;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // put some random meta data
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1,
                  "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key2,
                  "somevalue2", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key3,
                  "somevalue3", &i), "Failed set.");
    h1->release(h, NULL, i);

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    const void *cookie = testHarness.create_cookie();

    // delete an item with meta data
    del_with_meta(h, h1, key1, keylen, 0, &itemMeta, 0/*cas*/, 0/*options*/, cookie);

    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected valid sequence number");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect more setMeta ops");

    testHarness.set_mutation_extras_handling(cookie, false);

    // delete an item with meta data
    del_with_meta(h, h1, key2, keylen, 0, &itemMeta, 0/*cas*/, 0/*options*/, cookie);

    check(last_uuid == vb_uuid, "Expected same vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected same sequence number");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // delete an item with meta data
    del_with_meta(h, h1, key3, keylen, 0, &itemMeta);

    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 3, "Expected valid sequence number");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_deleted(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    item *i = NULL;

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_del_meta"),
            "Expect zero setMeta ops");

    // add a key
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    // delete the key
    checkeq(ENGINE_SUCCESS, del(h, h1, key, 0, 0),
            "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // get metadata of deleted key
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1,get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;
    // put some random metadata and delete the item with new meta data
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
            "Expected invalid cas error");
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Faild ops does not count");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1,get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    wait_for_flusher_to_settle(h, h1);

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Expect some ops");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.revSeqno == last_meta.revSeqno, "Expected seqno to match");
    check(itm_meta.cas == last_meta.cas, "Expected cas to match");
    check(itm_meta.flags == last_meta.flags, "Expected flags to match");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itm_meta;

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_del_meta"),
            "Expect zero setMeta ops");

    // get metadata of nonexistent key
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            "Expected enoent");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;

    // do delete with meta
    // put some random metadata and delete the item with new meta data
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
            "Expected invalid cas error");
    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Failed op does not count");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);

    // check the stat
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that delete with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.revSeqno == last_meta.revSeqno, "Expected seqno to match");
    check(itm_meta.cas == last_meta.cas, "Expected cas to match");
    check(itm_meta.flags == last_meta.flags, "Expected flags to match");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent_no_temp(ENGINE_HANDLE *h,
                                                                  ENGINE_HANDLE_V1 *h1) {
    const char *key1 = "delete_with_meta_no_temp_key1";
    const size_t keylen1 = strlen(key1);
    ItemMetaData itm_meta1;

    // Run compaction to start using the bloomfilter
    useconds_t sleepTime = 128;
    compact_db(h, h1, 0, 1, 1, 0);
    while (get_int_stat(h, h1, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    // put some random metadata and delete the item with new meta data
    itm_meta1.revSeqno = 10;
    itm_meta1.cas = 0xdeadbeef;
    itm_meta1.exptime = 1735689600; // expires in 2025
    itm_meta1.flags = 0xdeadbeef;

    // do delete with meta with the correct cas value.
    // skipConflictResolution false
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta1, 0, false);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);

    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

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

    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta2, 0, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);

    checkeq(2, get_int_stat(h, h1, "ep_num_ops_del_meta"), "Expect one op");
    wait_for_stat_to_be(h, h1, "curr_items", 0);
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    const size_t keylen1 = strlen(key1);

    item *i = NULL;
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    uint16_t keylen1 = (uint16_t)strlen(key1);
    char const *key2 = "key2";
    uint16_t keylen2 = (uint16_t)strlen(key2);
    item *i = NULL;
    ItemMetaData itm_meta;
    itm_meta.cas = 0x1;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // do a concurrent delete
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete itself will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key1, 0, 0), "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect some ops");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete itself will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");

    // do a concurrent delete
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key1, 0, 0), "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta, last_cas, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    size_t newValLen = strlen(newVal);
    uint64_t vb_uuid;
    uint32_t high_seqno;

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect zero ops");
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta"),
            "Expect zero ops");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expect zero items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expect zero temp items");

    // create a new key
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, val, &i),
            "Failed set.");

    // get metadata for the key
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expect one item");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expect zero temp item");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = time(NULL) + 300;
    itm_meta.flags = 0xdeadbeef;

    char *bigValue = new char[32*1024*1024];
    // do set with meta with the value size bigger than the max size allowed.
    set_with_meta(h, h1, key, keylen, bigValue, 32*1024*1024, 0, &itm_meta, cas_for_set);
    checkeq(PROTOCOL_BINARY_RESPONSE_E2BIG, last_status.load(),
          "Expected the max value size exceeding error");
    delete []bigValue;

    // do set with meta with an incorrect cas value. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Failed op does not count");

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    const void *cookie = testHarness.create_cookie();

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set,
                  0, 0, cookie);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected valid sequence number");

    // check the stat
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expect one item");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expect zero temp item");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");

    //disable getting vb uuid and seqno as extras
    testHarness.set_mutation_extras_handling(cookie, false);
    itm_meta.revSeqno++;
    cas_for_set = last_meta.cas;
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set,
                  false, 0, cookie);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_uuid == vb_uuid, "Expected same vbucket uuid");
    check(last_seqno == high_seqno + 1, "Expected same sequence number");

    itm_meta.revSeqno++;
    cas_for_set = last_meta.cas;
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_uuid == vb_uuid, "Expected valid vbucket uuid");
    check(last_seqno == high_seqno + 3, "Expected valid sequence number");

    // Make sure the item expiration was processed correctly
    testHarness.time_travel(301);
    checkeq(ENGINE_KEY_ENOENT, h1->get(h, NULL, &i, key, keylen, 0),
            "Failed to get value.");

    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_set_with_meta_by_force(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";

    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = time(NULL) + 300;
    itm_meta.flags = 0xdeadbeef;

    // Pass true to force SetWithMeta.
    set_with_meta(h, h1, key, keylen, val, strlen(val), 0, &itm_meta,
                  0, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);

    // get metadata again to verify that the warmup loads an item correctly.
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_meta.revSeqno == 10, "Expected seqno to match");
    check(last_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(last_meta.flags == 0xdeadbeef, "Expected flags to match");

    check_key_value(h, h1, key, val, strlen(val));

    return SUCCESS;
}

static enum test_result test_set_with_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    uint16_t newValLen = (uint16_t)strlen(newVal);

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect zero ops");
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta"),
            "Expect zero ops");

    // create a new key
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, val, &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // delete the key
    checkeq(ENGINE_SUCCESS, del(h, h1, key, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // get metadata for the key
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set_with_meta with an incorrect cas for a deleted item. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
          "Expected key_not_found error");
    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Failed op does not count");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    // check the stat
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_get_meta_on_set_meta"),
            "Expect some ops");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    ItemMetaData metadata(0xdeadbeef, 10, 0xdeadbeef, 1735689600);
    verifyLastMetaData(metadata);
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    size_t valLen = strlen(val);

    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect zero ops");

    // get metadata for the key
    check(!get_meta(h, h1, key), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set_with_meta with an incorrect cas for a non-existent item. should fail.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, 1229);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
          "Expected key_not_found error");
    // check the stat
    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Failed op does not count");
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, cas_for_set);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    // check the stat
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta"), "Expect some ops");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    ItemMetaData metadata(0xdeadbeef, 10, 0xdeadbeef, 1735689600);
    verifyLastMetaData(metadata);
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");
    checkeq(1, get_int_stat(h, h1, "curr_items"), "Expected single curr_items");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    item *i = NULL;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    // attempt set_with_meta. should fail since cas is no longer valid.
    last_meta.revSeqno += 2;
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);

    // attempt set_with_meta. should fail since cas is no longer valid.
    last_meta.revSeqno += 2;
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    size_t keylen2 = strlen(key2);
    item *i = NULL;
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // do a concurrent delete that changes the cas
    checkeq(ENGINE_SUCCESS, del(h, h1, key1, 0, 0), "Delete failed");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas, true);

    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(),
            (std::string{"Expected invalid cas error (KEY_EXISTS or"
                         " KEY_ENOENT), got: "} +
             std::to_string(last_status.load())).c_str());

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete. should fail.
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key1, 0, 0), "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &last_meta, last_cas, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some op");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");

    // do a concurrent delete. should fail.
    checkeq(ENGINE_KEY_ENOENT, del(h, h1, key2, 0, 0), "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key2, keylen2, NULL, 0, 0, &last_meta, last_cas, true);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_exp_persisted_set_del(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    check(!get_meta(h, h1, "key3"), "Expected to get meta");

    ItemMetaData itm_meta;
    itm_meta.revSeqno = 1;
    itm_meta.cas = 1;
    itm_meta.exptime = 0;
    itm_meta.flags = 0;
    set_with_meta(h, h1, "key3", 4, "val0", 4, 0, &itm_meta, last_meta.cas);

    itm_meta.revSeqno = 2;
    itm_meta.cas = 2;
    set_with_meta(h, h1, "key3", 4, "val1", 4, 0, &itm_meta, last_meta.cas);

    // MB-21725 Depending on how fast the flusher is, we may see 1 or 2.
    wait_for_stat_to_be_gte(h, h1, "ep_total_persisted", 1);

    itm_meta.revSeqno = 3;
    itm_meta.cas = 3;
    itm_meta.exptime = 1735689600; // expires in 2025
    set_with_meta(h, h1, "key3", 4, "val1", 4, 0, &itm_meta, last_meta.cas);

    testHarness.time_travel(500000000);
    // Wait for the item to be expired, either by the pager,
    // or by access (as part of persistence callback from a
    // previous set - slow disk), or the compactor (unlikely).
    wait_for_expired_items_to_be(h, h1, 1);

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    check(get_meta(h, h1, "key3"), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(last_meta.revSeqno == 4, "Expected seqno to match");
    check(last_meta.cas != 3, "Expected cas to be different");
    check(last_meta.flags == 0, "Expected flags to match");

    return SUCCESS;
}

static enum test_result test_temp_item_deletion(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    // Do get_meta for an existing key
    char const *k1 = "k1";
    item *i = NULL;

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, k1, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    checkeq(ENGINE_SUCCESS, del(h, h1, k1, 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Issue a get_meta for a deleted key. This will need to bring in a temp
    // item into the hashtable as a placeholder for the (deleted) metadata
    // which needs to be loaded from disk via BG fetch
    // We need to temporarily disable the reader threads as to prevent the
    // BGfetch from immediately running and removing our temp_item before
    // we've had chance to validate its existence.
    set_param(h, h1, protocol_binary_engine_param_flush,
              "max_num_readers", "0");

    // Disable nonio so that we have better control of the expirypager
    set_param(h, h1, protocol_binary_engine_param_flush,
              "max_num_nonio", "0");

    // Tell the harness not to handle EWOULDBLOCK for us - we want it to
    // be outstanding while we check the below stats.
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    checkeq(false, get_meta(h, h1, k1, /*reqExtMeta*/false, cookie),
            "Expected get_meta to fail (EWOULDBLOCK)");
    checkeq(static_cast<protocol_binary_response_status>(ENGINE_EWOULDBLOCK),
            last_status.load(), "Expected EWOULDBLOCK");

    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(1, get_int_stat(h, h1, "curr_temp_items"), "Expected single temp_items");

    // Re-enable EWOULDBLOCK handling (and reader threads), and re-issue.
    testHarness.set_ewouldblock_handling(cookie, true);
    set_param(h, h1, protocol_binary_engine_param_flush,
              "max_num_readers", "1");

    check(get_meta(h, h1, k1, /*reqExtMeta*/false, cookie),
          "Expected get_meta to succeed");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // Do get_meta for a non-existing key.
    char const *k2 = "k2";
    check(!get_meta(h, h1, k2), "Expected get meta to return false");
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, last_status.load(), "Expected enoent");

    // Ensure all bg fetches completed (two were requested)
    wait_for_stat_to_be(h, h1, "ep_bg_meta_fetched", 2);

    // Trigger the expiry pager and verify that two temp items are deleted
    set_param(h, h1, protocol_binary_engine_param_flush,
              "max_num_nonio", "1");

    wait_for_stat_to_be(h, h1, "ep_expired_pager", 1);
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Expected zero curr_items");
    checkeq(0, get_int_stat(h, h1, "curr_temp_items"), "Expected zero temp_items");

    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_add_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"),
            "Expected no bg meta fetches, thanks to bloom filters");

    checkeq(ENGINE_SUCCESS, del(h, h1, "key", 0, 0), "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    itemMeta.revSeqno++;
    itemMeta.cas++;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(1, get_int_stat(h, h1, "ep_bg_meta_fetched"),
          "Expected two be meta fetches");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    add_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_set_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"),
          "Expect zero setMeta ops");

    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"),
            "Expected no bg meta fetches, thanks to bloom filters");

    // Check all meta data is the same
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check has newer flags passes
    itemMeta.flags = 0xdeadbeff;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // Check that newer exptime wins
    itemMeta.exptime = time(NULL) + 10;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(4, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(4, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"),
            "Expect no bg meta fetches");

    return SUCCESS;
}

static enum test_result test_set_meta_lww_conflict_resolution(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {
    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(0, get_int_stat(h, h1, "ep_num_ops_set_meta"),
          "Expect zero setMeta ops");

    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    checkeq(0, get_int_stat(h, h1, "ep_bg_meta_fetched"),
            "Expected no bg meta fetchs, thanks to bloom filters");

    // Check all meta data is the same
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check that an older cas fails
    itemMeta.cas = 0xdeadbeee;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_set_meta_res_fail"),
          "Expected set meta conflict resolution failure");

    // Check that a higher cas passes
    itemMeta.cas = 0xdeadbeff;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    // Check that we fail requests if the force flag is not set
    itemMeta.cas = 0xdeadbeff + 1;
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  0/*options*/);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected EINVAL");

    return SUCCESS;
}

static enum test_result test_del_meta_conflict_resolution(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    h1->release(h, NULL, i);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    // Check has older flags fails
    itemMeta.flags = 0xdeadbeee;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    // Check that smaller exptime loses
    itemMeta.exptime = 0;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(3, get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    // Check testing with old seqno
    itemMeta.revSeqno--;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 4,
          "Expected delete meta conflict resolution failure");

    itemMeta.revSeqno += 10;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    check(get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail") == 4,
          "Expected delete meta conflict resolution failure");

    return SUCCESS;
}

static enum test_result test_del_meta_lww_conflict_resolution(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {

    item *i = NULL;
    item_info info;

    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");

    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);
    wait_for_flusher_to_settle(h, h1);
    h1->release(h, NULL, i);

    // put some random metadata
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = info.cas + 1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // first check the command fails if no force is set
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, 0/*options*/);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected EINVAL");

    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 0);

    // Check all meta data is the same
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(1, get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    // Check that higher rev seqno but lower cas fails
    itemMeta.cas = info.cas;
    itemMeta.revSeqno = 11;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(), "Expected exists");
    checkeq(2, get_int_stat(h, h1, "ep_num_ops_del_meta_res_fail"),
          "Expected delete meta conflict resolution failure");

    // Check that a higher cas and lower rev seqno passes
    itemMeta.cas = info.cas + 2;
    itemMeta.revSeqno = 9;
    del_with_meta(h, h1, "key", 3, 0, &itemMeta, 0, FORCE_ACCEPT_WITH_META_OPS);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected sucess");

    return SUCCESS;
}

static enum test_result test_getMeta_with_item_eviction(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1)
{
    char const *key = "test_get_meta";
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, key, 0, "Ejected.");

    Item *it = reinterpret_cast<Item*>(i);

    check(get_meta(h, h1, key), "Expected to get meta");
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");
    ItemMetaData metadata(it->getCas(), it->getRevSeqno(),
                          it->getFlags(), it->getExptime());
    verifyLastMetaData(metadata);

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_and_check_drift_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Activate n vbuckets (vb 0 is already)
    const int n_vbuckets = 10;
    for (int ii = 1; ii < n_vbuckets; ii++) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    // Let's make vbucket n/2 be the one who is ahead, n/3 is behind
    const int aheadVb = n_vbuckets/2;
    const int behindVb = n_vbuckets/3;
    checkne(aheadVb, behindVb, "Cannot have the same VB as ahead/behind");

    HLC hlc(0/*init HLC*/,
            std::chrono::microseconds(0)/*ahead threshold*/,
            std::chrono::microseconds(0)/*behind threshold*/);

    // grab the drift behind threshold
    uint64_t driftBehindThreshold = get_ull_stat(h, h1,
                                                 "ep_hlc_drift_ahead_threshold_us",
                                                 nullptr);
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
            set_with_meta(h, h1, key.data(), key.size(), NULL, 0, ii, &itm_meta,
                          0, FORCE_ACCEPT_WITH_META_OPS);
            checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                    "Expected success");
        }
    }

    // Bucket stats should report drift
    checkge(get_ull_stat(h, h1, "ep_active_hlc_drift"), uint64_t(0),
            "Expected drift above zero");
    checkeq(uint64_t(n_keys*n_vbuckets), get_ull_stat(h, h1, "ep_active_hlc_drift_count"),
            "Expected ahead counter to match mutations");

    // Victim VBs should have exceptions
    {
        std::string vbAheadName = "vb_" + std::to_string(aheadVb);
        std::string ahead_threshold_exceeded = vbAheadName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbAheadName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbAheadName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(aheadVb);
            checkeq(uint64_t(n_keys), get_ull_stat(h, h1, ahead_threshold_exceeded.data(), details.data()),
                "Expected ahead threshold to match mutations");
        checkeq(uint64_t(0), get_ull_stat(h, h1, behind_threshold_exceeded.data(), details.data()),
                "Expected no behind exceptions");
        checkge(get_ull_stat(h, h1, total_abs_drift.data(), details.data()), uint64_t(0),
                "Expected some drift");
    }

    {
        std::string vbBehindName = "vb_" + std::to_string(behindVb);
        std::string ahead_threshold_exceeded = vbBehindName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbBehindName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbBehindName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(behindVb);
        checkeq(uint64_t(n_keys), get_ull_stat(h, h1, behind_threshold_exceeded.data(), details.data()),
                "Expected behind threshold to match mutations");
        checkeq(uint64_t(0), get_ull_stat(h, h1, ahead_threshold_exceeded.data(), details.data()),
                "Expected no ahead exceptions");
        checkge(get_ull_stat(h, h1, total_abs_drift.data(), details.data()), uint64_t(0),
                "Expected some drift");
    }


    return SUCCESS;
}

static enum test_result test_del_with_meta_and_check_drift_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Activate n vbuckets (vb 0 is already)
    const int n_vbuckets = 10;
    for (int ii = 1; ii < n_vbuckets; ii++) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    // Let's make vbucket n/2 be the one who is ahead, n/3 is behind
    const int aheadVb = n_vbuckets/2;
    const int behindVb = n_vbuckets/3;
    checkne(aheadVb, behindVb, "Cannot have the same VB as ahead/behind");

    HLC hlc(0/*init HLC*/,
            std::chrono::microseconds(0)/*ahead threshold*/,
            std::chrono::microseconds(0)/*behind threshold*/);

    // grab the drift behind threshold
    uint64_t driftBehindThreshold = get_ull_stat(h, h1,
                                                 "ep_hlc_drift_ahead_threshold_us",
                                                 nullptr);
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
            set_with_meta(h, h1, key.data(), key.size(), NULL, 0, ii, &itm_meta,
                          0, FORCE_ACCEPT_WITH_META_OPS);
            checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                    "Expected success");
        }
    }

    checkeq(uint64_t(0), get_ull_stat(h, h1, "ep_active_ahead_exceptions"),
            "Expected ahead counter to match mutations");
    checkeq(uint64_t(n_keys*n_vbuckets), get_ull_stat(h, h1, "ep_active_behind_exceptions"),
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
            del_with_meta(h, h1, key.data(), key.size(), ii, &itm_meta,
                          1, FORCE_ACCEPT_WITH_META_OPS);
            checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                    "Expected success");
        }
    }

    // Bucket stats should report drift
    checkge(get_ull_stat(h, h1, "ep_active_hlc_drift"), uint64_t(0),
            "Expected drift above zero");
    checkeq(2*uint64_t(n_keys*n_vbuckets), get_ull_stat(h, h1, "ep_active_hlc_drift_count"),
            "Expected ahead counter to match mutations");

    // and should report total exception of all VBs
    checkeq(uint64_t(n_keys), get_ull_stat(h, h1, "ep_active_ahead_exceptions"),
            "Expected ahead counter to match mutations");
    checkeq(uint64_t(n_keys + (n_keys*n_vbuckets)), get_ull_stat(h, h1, "ep_active_behind_exceptions"),
            "Expected behind counter to match mutations");

    // Victim VBs should have exceptions
    {
        std::string vbAheadName = "vb_" + std::to_string(aheadVb);
        std::string ahead_threshold_exceeded = vbAheadName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbAheadName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbAheadName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(aheadVb);

        checkeq(uint64_t(n_keys),
                get_ull_stat(h, h1, ahead_threshold_exceeded.data(), details.data()),
                "Expected ahead threshold to match mutations");
        checkge(get_ull_stat(h, h1, total_abs_drift.data(), details.data()), uint64_t(0),
                "Expected some drift");
    }

    {
        std::string vbBehindName = "vb_" + std::to_string(behindVb);
        std::string ahead_threshold_exceeded = vbBehindName + ":drift_ahead_threshold_exceeded";
        std::string behind_threshold_exceeded = vbBehindName + ":drift_behind_threshold_exceeded";
        std::string total_abs_drift = vbBehindName + ":total_abs_drift";
        std::string details = "vbucket-details " + std::to_string(behindVb);

        // *2 behind due to the initial set_with_meta
        checkeq(uint64_t(n_keys*2), get_ull_stat(h, h1, behind_threshold_exceeded.data(), details.data()),
                "Expected behind threshold to match mutations");
        checkeq(uint64_t(0), get_ull_stat(h, h1, ahead_threshold_exceeded.data(), details.data()),
                "Expected no ahead exceptions");
        checkge(get_ull_stat(h, h1, total_abs_drift.data(), details.data()), uint64_t(0),
                "Expected some drift");
    }


    return SUCCESS;
}

static enum test_result test_setting_drift_threshold(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    std::vector<std::pair<std::string, std::string> > configData =
        {{"ep_hlc_drift_ahead_threshold_us", "hlc_drift_ahead_threshold_us"},
         {"ep_hlc_drift_behind_threshold_us", "hlc_drift_behind_threshold_us"}};

    std::vector<std::pair<std::string, uint64_t> > values =
        {{"0", 0}, {"1", 1}, {"-1", -1}, {"-0", 0},
         {"18446744073709551615", 18446744073709551615ull}};

    for (auto data : values) {
        for (auto conf : configData) {
            check(set_param(h, h1, protocol_binary_engine_param_vbucket,
                    conf.second.data(), data.first.data()),
                "Expected set_param success");

            checkeq(data.second,
                    get_ull_stat(h, h1, conf.first.data(), nullptr),
                    "Expected the stat to change to the new value");
        }
    }
    return SUCCESS;
}

/*
 * Perform set_with_meta and check CAS regeneration is ok.
 */
static enum test_result test_cas_regeneration(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    // First store a key from the past (small CAS).
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    int force = 0;

    if (strstr(testHarness.get_current_testcase()->cfg,
               "conflict_resolution_type=lww") != nullptr) {
        force = FORCE_ACCEPT_WITH_META_OPS;
    }

    // Set the key with a low CAS value
    set_with_meta(h, h1, "key", 3, nullptr, 0, 0, &itemMeta, 0, force);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(), "Expected success");

    check(get_meta(h, h1, "key"), "Failed to get_meta");

    // CAS must be what we set.
    checkeq(itemMeta.cas, last_meta.cas, "CAS is not the value we stored");

    itemMeta.cas++;

    // Check that the code requires skip
    set_with_meta(h, h1, "key", 3, nullptr, 0, 0, &itemMeta, 0,
                  REGENERATE_CAS/*but no skip*/);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
            "Expected EINVAL");

    set_with_meta(h, h1, "key", 3, nullptr, 0, 0, &itemMeta, 0,
                  REGENERATE_CAS|SKIP_CONFLICT_RESOLUTION_FLAG);

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");

    check(get_meta(h, h1, "key"), "Failed to get_meta");

    uint64_t cas = last_meta.cas;
    // Check item has a new CAS
    checkne(itemMeta.cas, cas, "CAS was not regenerated");

    itemMeta.cas++;
    // All flags set should still regen the cas (lww and seqno)
    set_with_meta(h, h1, "key", 3, nullptr, 0, 0, &itemMeta, 0,
                  REGENERATE_CAS|SKIP_CONFLICT_RESOLUTION_FLAG|force);

    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected success");

    check(get_meta(h, h1, "key"), "Failed to get_meta");
    // Check item has a new CAS
    checkne(itemMeta.cas, last_meta.cas, "CAS was not regenerated");
    checkne(cas, last_meta.cas, "CAS was not regenerated");
    return SUCCESS;
}

/*
 * Test that we can send options and nmeta
 * The nmeta is just going to be ignored though, but should not fail
 */
static enum test_result test_cas_options_and_nmeta(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // Watson (4.6) accepts valid encodings, but ignores them
    std::vector<char> junkMeta = {-2,-1,2,3};

    // Set the key and junk nmeta
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                  nullptr, junkMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected EINVAL");

    // Set the key and junk nmeta that's quite large
    junkMeta.resize(std::numeric_limits<uint16_t>::max());
    set_with_meta(h, h1, "key", 3, NULL, 0, 0, &itemMeta, 0,
                  FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                  nullptr, junkMeta);
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(), "Expected EINVAL");

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
        set_with_meta(h, h1, "key1", 4, nullptr, 0, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                      nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        del_with_meta(h, h1, "key1", 4, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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
        set_with_meta(h, h1, "key2", 4, nullptr, 0, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                      nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        del_with_meta(h, h1, "key2", 4, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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
        set_with_meta(h, h1, "key3", 4, nullptr, 0, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                      nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        del_with_meta(h, h1, "key3", 4, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
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
        set_with_meta(h, h1, "key4", 4, NULL, 0, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, PROTOCOL_BINARY_RAW_BYTES,
                      nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");

        itemMeta.cas++;
        del_with_meta(h, h1, "key4", 4, 0, &itemMeta, 0,
                      FORCE_ACCEPT_WITH_META_OPS, nullptr, validMetaVector);
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success");
    }

    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_xdcr";

BaseTestCase testsuite_testcases[] = {

        // XDCR unit tests
        TestCase("get meta", test_get_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("get meta with extras", test_get_meta_with_extras,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta deleted", test_get_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta nonexistent", test_get_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by get", test_get_meta_with_get,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by set", test_get_meta_with_set,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("get meta followed by delete", test_get_meta_with_delete,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("add with meta", test_add_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete with meta", test_delete_with_meta,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete with meta deleted", test_delete_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("delete with meta nonexistent",
                 test_delete_with_meta_nonexistent, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete with meta nonexistent no temp",
                 test_delete_with_meta_nonexistent_no_temp, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete_with_meta race with concurrent delete",
                 test_delete_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("delete_with_meta race with concurrent delete",
                 test_delete_with_meta_race_with_delete, test_setup,
                 teardown, "item_eviction_policy=full_eviction",
                 prepare, cleanup),
        TestCase("delete_with_meta race with concurrent set",
                 test_delete_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set with meta", test_set_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set with meta by force", test_set_with_meta_by_force,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set with meta deleted", test_set_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set with meta nonexistent", test_set_with_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("set_with_meta race with concurrent set",
                 test_set_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("set_with_meta race with concurrent delete",
                 test_set_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test set_with_meta exp persisted", test_exp_persisted_set_del,
                 test_setup, teardown, "exp_pager_stime=3", prepare, cleanup),
        TestCase("test del meta conflict resolution",
                 test_del_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test add meta conflict resolution",
                 test_add_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test set meta conflict resolution",
                 test_set_meta_conflict_resolution, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test del meta lww conflict resolution",
                 test_del_meta_lww_conflict_resolution, test_setup, teardown,
                 "conflict_resolution_type=lww",prepare, cleanup),
        TestCase("test set meta lww conflict resolution",
                 test_set_meta_lww_conflict_resolution, test_setup, teardown,
                 "conflict_resolution_type=lww",prepare, cleanup),
        TestCase("temp item deletion", test_temp_item_deletion,
                 test_setup, teardown,
                 "exp_pager_stime=1", prepare, cleanup),
        TestCase("test get_meta with item_eviction",
                 test_getMeta_with_item_eviction, test_setup, teardown,
                 "item_eviction_policy=full_eviction", prepare, cleanup),

        TestCase("test set_with_meta and drift stats",
                 test_set_with_meta_and_check_drift_stats, test_setup,
                 teardown, "hlc_drift_ahead_threshold_us=5000000;"
                 "hlc_drift_behind_threshold_us=0;conflict_resolution_type=lww",
                 prepare, cleanup),
        TestCase("test del_with_meta and drift stats",
                 test_del_with_meta_and_check_drift_stats, test_setup,
                 teardown, "hlc_drift_ahead_threshold_us=0;"
                 "hlc_drift_behind_threshold_us=5000000;conflict_resolution_type=lww",
                 prepare, cleanup),
        TestCase("test setting drift threshold",
                 test_setting_drift_threshold, test_setup,
                 teardown, nullptr,
                 prepare, cleanup),
        TestCase("test CAS regeneration lww",
                 test_cas_regeneration, test_setup, teardown,
                 "conflict_resolution_type=lww",
                 prepare, cleanup),
        TestCase("test CAS regeneration seqno",
                 test_cas_regeneration, test_setup, teardown,
                 "conflict_resolution_type=seqno",
                 prepare, cleanup),
        TestCase("test CAS options and nmeta",
                 test_cas_options_and_nmeta, test_setup, teardown,
                 "conflict_resolution_type=lww",
                 prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
