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
 * Testsuite for TAP functionality in ep-engine.
 */

#include "config.h"

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

// Helper functions ///////////////////////////////////////////////////////////

static enum test_result verify_item(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    item *i,
                                    const char* key, size_t klen,
                                    const char* val, size_t vlen)
{
    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "get item info failed");
    check(info.nvalue == 1, "iovectors not supported");
    // We can pass in a NULL key to avoid the key check (for tap streams)
    if (key) {
        check(klen == info.nkey, "Incorrect key length");
        check(memcmp(info.key, key, klen) == 0, "Incorrect key value");
    }
    check(vlen == info.value[0].iov_len, "Incorrect value length");
    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");

    return SUCCESS;
}

// Testcases //////////////////////////////////////////////////////////////////
static enum test_result test_set_tap_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    set_param(h, h1, protocol_binary_engine_param_tap, "tap_keepalive", "600");
    checkeq(600, get_int_stat(h, h1, "ep_tap_keepalive"),
            "Incorrect tap_keepalive value.");
    set_param(h, h1, protocol_binary_engine_param_tap, "tap_keepalive", "5000");
    checkeq(PROTOCOL_BINARY_RESPONSE_EINVAL, last_status.load(),
        "Expected an invalid value error due to exceeding a max value allowed");
    return SUCCESS;
}

static enum test_result test_tap_noop_config_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    checkeq(200, get_int_stat(h, h1, "ep_tap_noop_interval", "tap"),
            "Expected tap_noop_interval == 200");
    return SUCCESS;
}

static enum test_result test_tap_noop_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    checkeq(10, get_int_stat(h, h1, "ep_tap_noop_interval", "tap"),
            "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_notify(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    int ii = 0;
    char buffer[1024];
    ENGINE_ERROR_CODE r;

    const void *cookie = testHarness.create_cookie();
    memset(&buffer, 0, sizeof(buffer));
    do {
        std::stringstream ss;
        ss << "Key-"<< ++ii;
        std::string key = ss.str();

        char eng_specific[9];
        memset(eng_specific, 0, sizeof(eng_specific));
        r = h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific), 1, 0,
                           TAP_MUTATION, 0, key.c_str(), key.length(), 0, 0, 0x1,
                           0, buffer, 1024, 0);
    } while (r == ENGINE_SUCCESS);
    checkeq(ENGINE_TMPFAIL, r, "non-acking streams should etmpfail");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    std::vector<char> data(8192, 'x');
    for (size_t i = 0; i < 8192; ++i) {

        checkeq(ENGINE_SUCCESS,
                h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                               1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 1,
                               PROTOCOL_BINARY_RAW_BYTES,
                               data.data(), i, 0),
                "Failed tap notify.");
        check_key_value(h, h1, "key", data.data(), i);
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    for (uint64_t checkpoint_id = 0; checkpoint_id < 10; checkpoint_id++) {
        checkeq(ENGINE_SUCCESS,
                h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                               1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 1,
                               PROTOCOL_BINARY_RAW_BYTES,
                               &checkpoint_id, sizeof(checkpoint_id), 1),
                "Failed tap notify.");
        checkeq(ENGINE_SUCCESS,
                h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                               1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 1,
                               PROTOCOL_BINARY_RAW_BYTES,
                               &checkpoint_id, sizeof(checkpoint_id), 1),
                "Failed tap notify.");
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_set_vbstate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[8];
    vbucket_state_t vb_state = static_cast<vbucket_state_t>(htonl(vbucket_state_active));
    memcpy(eng_specific, &vb_state, sizeof(vb_state));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    // Get the vbucket UUID before vbucket takeover.
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(vb_state),
                           1, 0, TAP_VBUCKET_SET, 1, "", 0, 828, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           "", 0, 1),
            "Failed tap notify.");
    // Get the vbucket UUID after vbucket takeover.
    check(get_ull_stat(h, h1, "vb_1:0:id", "failovers") != vb_uuid,
          "A new vbucket uuid should be created after TAP-based vbucket takeover.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           "data", 4, 1),
            "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           "data", 4, 1),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           "data", 4, 1),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica_locked(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"key", "value", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    getl(h, h1, "key", 0, 10); // 10 secs of lock expiration
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS,
            last_status.load(),
            "expected the key to be locked...");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           "data", 4, 0),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_DELETION, 0, "key", 3, 0, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           0, 0, 0),
            "Failed tap notify.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 1),
            "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 1),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 1),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica_locked(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET,"key", "value", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    getl(h, h1, "key", 0, 10); // 10 secs of lock expiration
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "expected the key to be locked...");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    char* eng_specific[8];
    memset(eng_specific, 0, sizeof(eng_specific));
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 1,
                           PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0),
            "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + num_keys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            testHarness.unlock_cookie(cookie);
            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_agg_stats(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    std::vector<const void*> cookies;

    cookies.push_back(createTapConn(h, h1, "replica_a"));
    cookies.push_back(createTapConn(h, h1, "replica_b"));
    cookies.push_back(createTapConn(h, h1, "replica_c"));
    cookies.push_back(createTapConn(h, h1, "rebalance_a"));
    cookies.push_back(createTapConn(h, h1, "userconnn"));

    checkeq(3, get_int_stat(h, h1, "replica:count", "tapagg _"),
            "Incorrect replica count on tap agg");
    checkeq(1, get_int_stat(h, h1, "rebalance:count", "tapagg _"),
            "Incorrect rebalance count on tap agg");
    checkeq(5, get_int_stat(h, h1, "_total:count", "tapagg _"),
            "Incorrect total count on tap agg");

    for (auto& cookie : cookies) {
        testHarness.unlock_cookie(cookie);
        testHarness.destroy_cookie(cookie);
    }

    return SUCCESS;
}

static enum test_result test_tap_sends_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int ii = 0; ii < num_keys - 2; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS, del(h, h1, ss.str().c_str(), 0, 0), "Delete failed");
    }
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", 2);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    int num_mutations = 0;
    int num_deletes = 0;

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
        case TAP_DISCONNECT:
            break;
        case TAP_MUTATION:
            h1->release(h, NULL, it);
            num_mutations++;
            break;
        case TAP_DELETION:
            h1->release(h, NULL, it);
            num_deletes++;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    checkeq(2, num_mutations, "Incorrect number of remaining mutations");
    checkeq((num_keys - 2), num_deletes, "Incorrect number of deletes");

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_sent_from_vb(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
        case TAP_DISCONNECT:
            break;
        case TAP_MUTATION:
        case TAP_DELETION:
            h1->release(h, NULL, it);
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    checkeq(5, get_int_stat(h, h1,
                            "eq_tapq:tap_client_thread:sent_from_vb_0",
                            "tap"),
            "Incorrect number of items sent");


    std::map<uint16_t, uint64_t> vbmap;
    vbmap[0] = 0;
    changeVBFilter(h, h1, "tap_client_thread", vbmap);
    checkeq(0, get_int_stat(h, h1,
                            "eq_tapq:tap_client_thread:sent_from_vb_0",
                            "tap"),
            "Incorrect number of items sent");

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_tap_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    int initializedKeys = 0;

    memset(keys, 0, sizeof(keys));

    for (; initializedKeys < num_keys / 2; ++initializedKeys) {
        keys[initializedKeys] = false;
        std::stringstream ss;
        ss << initializedKeys;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + initializedKeys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < initializedKeys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    uint16_t vbucketfilter[2];
    vbucketfilter[0] = ntohs(1);
    vbucketfilter[1] = ntohs(0);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS |
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;
    bool allows_more_mutations(true);

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (initializedKeys < num_keys) {
            keys[initializedKeys] = false;
            std::stringstream ss;
            ss << initializedKeys;
            checkeq(ENGINE_SUCCESS,
                    store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                          "value", NULL, 0, 0),
                    "Failed to store an item.");
            ++initializedKeys;
        }

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            // This will be false if we've seen a vbucket set state.
            cb_assert(allows_more_mutations);

            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            break;
        case TAP_DISCONNECT:
            break;
        case TAP_VBUCKET_SET:
            cb_assert(nengine_specific == 4);
            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine_specific);
            state = static_cast<vbucket_state_t>(ntohl(state));
            if (state == vbucket_state_active) {
                allows_more_mutations = false;
            }
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_filter_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint16_t vbid;
    for (vbid = 0; vbid < 4; ++vbid) {
        check(set_vbucket_state(h, h1, vbid, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    const int num_keys = 40;
    bool keys[num_keys];
    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, ii % 4),
                "Failed to store an item.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::map<uint16_t, uint64_t> filtermap;
    filtermap[0] = 0;
    filtermap[1] = 0;
    filtermap[2] = 0;

    uint16_t numOfVBs = htons(2); // Start with vbuckets 0 and 1
    char *userdata = static_cast<char*>(calloc(1, 28));
    char *ptr = userdata;
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids
        uint16_t vb = htons(i);
        memcpy(ptr, &vb, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
    }
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids and their checkpoint ids
        uint16_t vb = htons(i);
        memcpy(ptr, &vb, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        uint64_t chkid = htonll(filtermap[i]);
        memcpy(ptr, &chkid, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
    }

    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT,
                                             static_cast<void*>(userdata),
                                             28); // userdata length
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;

    tap_event_t event;
    int found = 0;

    uint16_t unlikely_vbucket_identifier = 17293;
    std::string key;
    bool done = false;
    bool filter_change_done = false;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            done = true;
            // Check if all the items except for vbucket 3 are received
            for (int ii = 0; ii < num_keys; ++ii) {
                if ((ii % 4) != 3 && !keys[ii]) {
                    done = false;
                    break;
                }
            }
            if (!done) {
                testHarness.waitfor_cookie(cookie);
            }
            break;
        case TAP_NOOP:
            break;
        case TAP_OPAQUE:
            if (nengine_specific == sizeof(uint32_t)) {
                uint32_t opaque_code;
                memcpy(&opaque_code, engine_specific, sizeof(opaque_code));
                opaque_code = ntohl(opaque_code);
                if (opaque_code == TAP_OPAQUE_COMPLETE_VB_FILTER_CHANGE) {
                    filter_change_done = true;
                }
            }
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
            h1->release(h, cookie, it);
            break;

        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out the key");
            vbid = atoi(key.c_str()) % 4;
            checkeq(vbid, vbucket, "Incorrect vbucket id");
            check(vbid != 3,
                  "Received an item for a vbucket we don't subscribe to");
            keys[atoi(key.c_str())] = true;
            ++found;
            cb_assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            // We've got some of the elements.. Let's change the filter
            // and get the rest
            if (found == 10) {
                changeVBFilter(h, h1, name, filtermap);
                checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "Expected success response from changing the TAP VB filter.");
            }
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    cb_assert(filter_change_done);
    checkeq(0,
            get_int_stat(h, h1, "eq_tapq:tap_client_thread:qlen", "tap"),
            "queue should be empty");
    free(userdata);

    return SUCCESS;
}

static enum test_result test_tap_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_SUCCESS, h1->get_stats(h, NULL, "tap", 3, add_stats),
            "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");
    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "0.05") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "2") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect grace period value");
    return SUCCESS;
}

static enum test_result test_tap_default_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    checkeq(ENGINE_SUCCESS, h1->get_stats(h, NULL, "tap", 3, add_stats),
            "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");

    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "5") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "1000") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "300") == 0, "Incorrect grace period value");

    return SUCCESS;
}

static enum test_result test_tap_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    bool receivedKeys[nkeys];
    bool nackKeys[nkeys];

    for (int i = 0; i < nkeys; ++i) {
        nackKeys[i] = true;
        receivedKeys[i] = false;
        std::stringstream ss;
        ss << i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_item_info(h, h1, &info, ss.str().c_str()), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int index;
    int numRollbacks = 1000;

    do {
        if (numRollbacks > 0) {
            if (random() % 4 == 0) {
                iter = NULL;
            }
        }

        if (iter == NULL) {
            testHarness.unlock_cookie(cookie);
            iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                        name.length(),
                                        TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                        TAP_CONNECT_CHECKPOINT |
                                        TAP_CONNECT_SUPPORT_ACK |
                                        TAP_CONNECT_FLAG_DUMP,
                                        static_cast<void*>(vbucketfilter),
                                        4);
            check(iter != NULL, "Failed to create a tap iterator");
            testHarness.lock_cookie(cookie);
        }

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
            if (numRollbacks > 0) {
                if (random() % 4 == 0) {
                    iter = NULL;
                }
            }
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, NULL, 0,
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out key");
            index = atoi(key.c_str());
            check(index >= 0 && index <= nkeys, "Illegal key returned");

            testHarness.unlock_cookie(cookie);
            if (nackKeys[index]) {
                nackKeys[index] = false;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
            } else {
                receivedKeys[index] = true;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
            }
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);

            break;
        case TAP_CHECKPOINT_START:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_CHECKPOINT_END:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                           NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    for (int ii = 0; ii < nkeys; ++ii) {
        check(receivedKeys[ii], "Did not receive all of the keys");
    }

    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_tap_implicit_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_item_info(h, h1, &info, ss.str().c_str()), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int mutations = 0;
    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            }
            if (seqno == static_cast<uint32_t>(4294967294UL)) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < static_cast<uint32_t>(4294967295UL));

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            }
            if (seqno == 1) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < 1);

    /* Now just get the rest */
    do {

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);
        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
                h1->release(h, cookie, it);
            } else if (event == TAP_DISCONNECT) {
                done = true;
            }
            if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, PROTOCOL_BINARY_RAW_BYTES,
                               NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (!done);
    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);
    checkeq(11, mutations, "Expected 11 mutations to be returned");
    return SUCCESS;
}

static enum test_result test_est_vb_move(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    check(estimateVBucketMove(h, h1, 0) == 0, "Empty VB estimate is wrong");

    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0, 0),
                "Failed to store an item.");
    }
    check(estimateVBucketMove(h, h1, 0) == 11, "Invalid estimate");
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(1801);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");

    for (int ii = 0; ii < 2; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS,
                del(h, h1, ss.str().c_str(), 0, 0),
                "Failed to remove a key");
    }

    check(estimateVBucketMove(h, h1, 0) == 8, "Invalid estimate");
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(1801);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 4, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:persisted_checkpoint_id", 3, "checkpoint");

    stop_persistence(h, h1);
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "longerkey" << ii;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                      "value", NULL, 0, 0, 0),
                "Failed to store an item.");
    }
    check(estimateVBucketMove(h, h1, 0) >= 15, "Invalid estimate");

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    size_t backfillAge = 0;
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_BACKFILL |
                                             TAP_CONNECT_CHECKPOINT,
                                             static_cast<void*>(&backfillAge),
                                             8);
    check(iter != NULL, "Failed to create a tap iterator");
    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    std::string key;
    tap_event_t event;
    bool backfillphase = true;
    bool done = false;
    uint32_t opaque;
    int total_sent = 0;
    int mutations = 0;
    int deletions = 0;

    size_t chk_items;
    size_t remaining;

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, NULL, 0, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        int64_t byseq = -1;
        if (event == TAP_CHECKPOINT_START ||
            event == TAP_DELETION || event == TAP_MUTATION) {
            check(nengine_specific >= 8,
                  (std::string("Unexpected engine_specific size (") +
                   std::to_string(nengine_specific)).c_str());
            uint8_t *es = ((uint8_t*)engine_specific);
            memcpy(&byseq, (void*)es, 8);
            byseq = ntohll(byseq);
        }

        switch (event) {
        case TAP_PAUSE:
            if (total_sent == 10) {
                done = true;
            }
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_OPAQUE:
            opaque = ntohl(*(static_cast<int*> (engine_specific)));
            if (opaque == TAP_OPAQUE_CLOSE_BACKFILL) {
                checkeq(3, mutations, "Invalid number of backfill mutations");
                checkeq(2, deletions, "Invalid number of backfill deletions");
                backfillphase = false;
            }
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
            h1->release(h, NULL, it);
            break;
        case TAP_MUTATION:
        case TAP_DELETION:
            total_sent++;
            if (event == TAP_DELETION) {
                deletions++;
            } else {
                mutations++;
            }

            if (!backfillphase) {
                chk_items = estimateVBucketMove(h, h1, 0, name.c_str());
                remaining = 10 - total_sent;
                checkeq(chk_items, remaining, "Invalid Estimate of chk items");
            }
            h1->release(h, NULL, it);
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    checkeq(10, get_int_stat(h, h1, "eq_tapq:tap_client_thread:sent_from_vb_0",
                             "tap"),
            "Incorrect number of items sent");
    testHarness.unlock_cookie(cookie);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}


// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_tap";

BaseTestCase testsuite_testcases[] = {

        // tap tests
        TestCase("set tap param", test_set_tap_param, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap_noop_interval default config",
                 test_tap_noop_config_default,
                 test_setup, teardown, NULL , prepare, cleanup),
        TestCase("tap_noop_interval config", test_tap_noop_config,
                 test_setup, teardown,
                 "tap_noop_interval=10", prepare, cleanup),
        TestCase("tap receiver mutation", test_tap_rcvr_mutate, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap receiver checkpoint start/end", test_tap_rcvr_checkpoint,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver vbucket state", test_tap_rcvr_set_vbstate,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (dead)", test_tap_rcvr_mutate_dead,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (pending)",
                 test_tap_rcvr_mutate_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (replica)",
                 test_tap_rcvr_mutate_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver mutation (replica) on a locked item",
                 test_tap_rcvr_mutate_replica_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete", test_tap_rcvr_delete,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (dead)", test_tap_rcvr_delete_dead,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (pending)", test_tap_rcvr_delete_pending,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (replica)", test_tap_rcvr_delete_replica,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap receiver delete (replica) on a locked item",
                 test_tap_rcvr_delete_replica_locked,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap stream", test_tap_stream, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap stream send deletes", test_tap_sends_deleted, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test tap sent from vb", test_sent_from_vb, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap agg stats", test_tap_agg_stats, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("tap takeover (with concurrent mutations)", test_tap_takeover,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("tap filter stream", test_tap_filter_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3", prepare, cleanup),
        TestCase("tap default config", test_tap_default_config,
                 test_setup, teardown, NULL , prepare, cleanup),
        TestCase("tap config", test_tap_config, test_setup,
                 teardown,
                 "tap_backoff_period=0.05;tap_ack_interval=10;tap_ack_window_size=2;tap_ack_grace_period=10",
                 prepare, cleanup),
        TestCase("tap acks stream", test_tap_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;chk_max_items=500",
                 prepare, cleanup),
        TestCase("tap implicit acks stream", test_tap_implicit_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;tap_ack_initial_sequence_number=4294967290;chk_max_items=500",
                 prepare, cleanup),
        TestCase("tap notify", test_tap_notify, test_setup,
                 teardown, "max_size=1048576", prepare, cleanup),

        TestCase("test estimate vb move", test_est_vb_move,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
