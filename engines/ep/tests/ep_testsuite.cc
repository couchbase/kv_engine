/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// mock_cookie.h & mock_server.h must be included before ep_test_apis.h as
// ep_test_apis.h define a macro named check the mock_* headers also use the
// name 'check' indirectly.
#include <ep_engine.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>

// Usage: (to run just a single test case)
// make engine_tests EP_TEST_NUM=3

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "kvstore/couch-kvstore/couch-kvstore-metadata.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "module_tests/thread_gate.h"
#include <libcouchstore/couch_db.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/engine_testapp.h>
#include <memcached/types.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/platform_thread.h>
#include <platform/platform_time.h>
#include <programs/engine_testapp/mock_engine.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#ifdef linux
/* /usr/include/netinet/in.h defines macros from ntohs() to _bswap_nn to
 * optimize the conversion functions, but the prototypes generate warnings
 * from gcc. The conversion methods isn't the bottleneck for my app, so
 * just remove the warnings by undef'ing the optimization ..
 */
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

using namespace std::string_literals;
using namespace std::string_view_literals;

class ThreadData {
public:
    explicit ThreadData(EngineIface* eh, int e = 0) : h(eh), extra(e) {
    }
    EngineIface* h;
    int extra;
};

enum class EPBucketType { EP, Ephemeral };

static void check_observe_seqno(bool failover,
                                EPBucketType bucket_type,
                                uint8_t format_type,
                                Vbid vb_id,
                                uint64_t vb_uuid,
                                uint64_t last_persisted_seqno,
                                uint64_t current_seqno,
                                uint64_t failover_vbuuid = 0,
                                uint64_t failover_seqno = 0) {
    uint8_t recv_format_type;
    Vbid recv_vb_id;
    uint64_t recv_vb_uuid;
    uint64_t recv_last_persisted_seqno;
    uint64_t recv_current_seqno;
    uint64_t recv_failover_vbuuid;
    uint64_t recv_failover_seqno;

    memcpy(&recv_format_type, last_body.data(), sizeof(uint8_t));
    checkeq(format_type, recv_format_type, "Wrong format type in result");
    memcpy(&recv_vb_id, last_body.data() + 1, sizeof(uint16_t));
    checkeq(vb_id.get(), ntohs(recv_vb_id.get()), "Wrong vbucket id in result");
    memcpy(&recv_vb_uuid, last_body.data() + 3, sizeof(uint64_t));
    checkeq(vb_uuid, ntohll(recv_vb_uuid), "Wrong vbucket uuid in result");
    memcpy(&recv_last_persisted_seqno, last_body.data() + 11, sizeof(uint64_t));

    switch (bucket_type) {
    case EPBucketType::EP:
        // Should get the "real" persisted seqno:
        checkeq(last_persisted_seqno,
                ntohll(recv_last_persisted_seqno),
                "Wrong persisted seqno in result (EP)");
        break;
    case EPBucketType::Ephemeral:
        // For ephemeral, this should always be zero, as there is no
        // persistence.
        checkeq(uint64_t(0),
                ntohll(recv_last_persisted_seqno),
                "Wrong persisted seqno in result (Ephemeral)");
        break;
    }

    memcpy(&recv_current_seqno, last_body.data() + 19, sizeof(uint64_t));
    checkeq(current_seqno, ntohll(recv_current_seqno), "Wrong current seqno in result");

    if (failover) {
        memcpy(&recv_failover_vbuuid, last_body.data() + 27, sizeof(uint64_t));
        checkeq(failover_vbuuid, ntohll(recv_failover_vbuuid),
                "Wrong failover uuid in result");
        memcpy(&recv_failover_seqno, last_body.data() + 35, sizeof(uint64_t));
        checkeq(failover_seqno, ntohll(recv_failover_seqno),
                "Wrong failover seqno in result");
    }
}

static enum test_result test_replace_with_eviction(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed to set value.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "key");
    int numBgFetched = get_int_stat(h, "ep_bg_fetched");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Replace, "key", "somevalue1"),
            "Failed to replace existing value.");

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "full_eviction") {
        numBgFetched++;
    }

    checkeq(numBgFetched,
            get_int_stat(h, "ep_bg_fetched"),
            "Bg fetched value didn't match");

    check_key_value(h, "key", "somevalue1", 10);
    return SUCCESS;
}

static enum test_result test_wrong_vb_mutation(EngineIface* h,
                                               StoreSemantics op) {
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");
    uint64_t cas = 11;
    if (op == StoreSemantics::Add) {
        // Add operation with cas != 0 doesn't make sense
        cas = 0;
    }
    checkeq(cb::engine_errc::not_my_vbucket,
            store(h, nullptr, op, "key", "somevalue", nullptr, cas, Vbid(1)),
            "Expected not_my_vbucket");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_pending_vb_mutation(EngineIface* h,
                                                 StoreSemantics op) {
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, Vbid(1), vbucket_state_pending),
          "Failed to set vbucket state.");
    check(verify_vbucket_state(h, Vbid(1), vbucket_state_pending),
          "Bucket state was not set to pending.");
    uint64_t cas = 11;
    if (op == StoreSemantics::Add) {
        // Add operation with cas != 0 doesn't make sense..
        cas = 0;
    }
    checkeq(cb::engine_errc::would_block,
            store(h, cookie, op, "key", "somevalue", nullptr, cas, Vbid(1)),
            "Expected ewouldblock");
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(EngineIface* h,
                                                 StoreSemantics op) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    check(verify_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Bucket state was not set to replica.");
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");

    uint64_t cas = 11;
    if (op == StoreSemantics::Add) {
        // performing add with a CAS != 0 doesn't make sense...
        cas = 0;
    }
    checkeq(cb::engine_errc::not_my_vbucket,
            store(h, nullptr, op, "key", "somevalue", nullptr, cas, Vbid(1)),
            "Expected not my vbucket");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

//
// ----------------------------------------------------------------------
// The actual tests are below.
// ----------------------------------------------------------------------
//

static int checkCurrItemsAfterShutdown(EngineIface* h,
                                       int numItems2Load,
                                       bool shutdownForce) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    std::vector<std::string> keys;
    for (int index = 0; index < numItems2Load; ++index) {
        std::stringstream s;
        s << "keys_2_load-" << index;
        std::string key(s.str());
        keys.push_back(key);
    }

    // Check preconditions.
    checkeq(0,
            get_int_stat(h, "ep_total_persisted"),
            "Expected ep_total_persisted equals 0");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected curr_items equals 0");

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    // stop flusher before loading new items
    auto pkt = createPacket(cb::mcbp::ClientOpcode::StopPersistence);
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "CMD_STOP_PERSISTENCE failed!");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Failed to stop persistence!");

    std::vector<std::string>::iterator itr;
    for (itr = keys.begin(); itr != keys.end(); ++itr) {
        checkeq(cb::engine_errc::success,
                store(h, nullptr, StoreSemantics::Set, itr->c_str(), "oracle"),
                "Failed to store a value");
    }

    checkeq(0,
            get_int_stat(h, "ep_total_persisted"),
            "Incorrect ep_total_persisted, expected 0");

    // Can only check curr_items in value_only eviction; full-eviction
    // relies on persistence to complete (via flusher) to update count.
    const auto evictionPolicy = get_str_stat(h, "ep_item_eviction_policy");
    if (evictionPolicy == "value_only") {
        checkeq(numItems2Load,
                get_int_stat(h, "curr_items"),
                "Expected curr_items to reflect item count");
    }

    // resume flusher before shutdown + warmup
    pkt = createPacket(cb::mcbp::ClientOpcode::StartPersistence);
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "CMD_START_PERSISTENCE failed!");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Failed to start persistence!");

    // shutdown engine force and restart
    testHarness->reload_engine(&h,
                               testHarness->get_current_testcase()->cfg,
                               true,
                               shutdownForce);

    wait_for_warmup_complete(h);
    return get_int_stat(h, "curr_items");
}

static enum test_result test_flush_shutdown_force(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    int numItems2load = 3000;
    bool shutdownForce = true;
    int currItems =
            checkCurrItemsAfterShutdown(h, numItems2load, shutdownForce);
    checkle(currItems, numItems2load,
           "Number of curr items should be <= 3000, unless previous "
           "shutdown force had to wait for the flusher");
    return SUCCESS;
}

static enum test_result test_flush_shutdown_noforce(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    int numItems2load = 3000;
    bool shutdownForce = false;
    int currItems =
            checkCurrItemsAfterShutdown(h, numItems2load, shutdownForce);
    checkeq(numItems2load, currItems,
           "Number of curr items should be equal to 3000, unless previous "
           "shutdown did not wait for the flusher");
    return SUCCESS;
}

static enum test_result test_shutdown_snapshot_range(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    const int num_items = 100;
    for (int j = 0; j < num_items; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "data"),
                "Failed to store a value");
    }

    wait_for_flusher_to_settle(h);
    int end = get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    /* change vb state to replica before restarting (as it happens in graceful
       failover)*/
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed set vbucket 0 to replica state.");

    /* trigger persist vb state task */
    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Flush, "vb_state_persist_run", "0"),
            "Failed to trigger vb state persist");

    /* restart the engine */
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    /* Check if snapshot range is persisted correctly */
    checkeq(end,
            get_int_stat(h, "vb_0:last_persisted_snap_start", "vbucket-seqno"),
            "Wrong snapshot start persisted");
    checkeq(end,
            get_int_stat(h, "vb_0:last_persisted_snap_end", "vbucket-seqno"),
            "Wrong snapshot end persisted");

    return SUCCESS;
}

static enum test_result test_restart(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    static const char val[] = "somevalue";
    cb::engine_errc ret = store(h, nullptr, StoreSemantics::Set, "key", val);
    checkeq(cb::engine_errc::success, ret, "Failed set.");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    check_key_value(h, "key", val, strlen(val));
    return SUCCESS;
}

static enum test_result test_specialKeys(EngineIface* h) {
    cb::engine_errc ret;

    // Simplified Chinese "Couchbase"
    static const char key0[] = "沙发数据库";
    static const char val0[] = "some Chinese value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key0, val0)),
            "Failed set Chinese key");
    check_key_value(h, key0, val0, strlen(val0));

    // Traditional Chinese "Couchbase"
    static const char key1[] = "沙發數據庫";
    static const char val1[] = "some Traditional Chinese value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key1, val1)),
            "Failed set Traditional Chinese key");

    // Korean "couch potato"
    static const char key2[] = "쇼파감자";
    static const char val2[] = "some Korean value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key2, val2)),
            "Failed set Korean key");

    // Russian "couch potato"
    static const char key3[] = "лодырь, лентяй";
    static const char val3[] = "some Russian value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key3, val3)),
            "Failed set Russian key");

    // Japanese "couch potato"
    static const char key4[] = "カウチポテト";
    static const char val4[] = "some Japanese value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key4, val4)),
            "Failed set Japanese key");

    // Indian char key, and no idea what it is
    static const char key5[] = "हरियानवी";
    static const char val5[] = "some Indian value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key5, val5)),
            "Failed set Indian key");

    // Portuguese translation "couch potato"
    static const char key6[] = "sedentário";
    static const char val6[] = "some Portuguese value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key6, val6)),
            "Failed set Portuguese key");

    // Arabic translation "couch potato"
    static const char key7[] = "الحافلةالبطاطة";
    static const char val7[] = "some Arabic value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key7, val7)),
            "Failed set Arabic key");

    if (isWarmupEnabled(h)) {
        // Check that after warmup the keys are still present.
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   false);

        wait_for_warmup_complete(h);
        check_key_value(h, key0, val0, strlen(val0));
        check_key_value(h, key1, val1, strlen(val1));
        check_key_value(h, key2, val2, strlen(val2));
        check_key_value(h, key3, val3, strlen(val3));
        check_key_value(h, key4, val4, strlen(val4));
        check_key_value(h, key5, val5, strlen(val5));
        check_key_value(h, key6, val6, strlen(val6));
        check_key_value(h, key7, val7, strlen(val7));
    }
    return SUCCESS;
}

static enum test_result test_binKeys(EngineIface* h) {
    cb::engine_errc ret;

    // binary key with char values beyond 0x7F
    static const char key0[] = "\xe0\xed\xf1\x6f\x7f\xf8\xfa";
    static const char val0[] = "some value val8";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key0, val0)),
            "Failed set binary key0");
    check_key_value(h, key0, val0, strlen(val0));

    // binary keys with char values beyond 0x7F
    static const char key1[] = "\xf1\xfd\xfe\xff\xf0\xf8\xef";
    static const char val1[] = "some value val9";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key1, val1)),
            "Failed set binary key1");
    check_key_value(h, key1, val1, strlen(val1));

    // binary keys with special utf-8 BOM (Byte Order Mark) values 0xBB 0xBF
    // 0xEF
    static const char key2[] = "\xff\xfe\xbb\xbf\xef";
    static const char val2[] = "some utf-8 bom value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key2, val2)),
            "Failed set binary utf-8 bom key");
    check_key_value(h, key2, val2, strlen(val2));

    // binary keys with special utf-16BE BOM values "U+FEFF"
    static const char key3[] = "U+\xfe\xff\xefU+\xff\xfe";
    static const char val3[] = "some utf-16 bom value";
    checkeq(cb::engine_errc::success,
            (ret = store(h, nullptr, StoreSemantics::Set, key3, val3)),
            "Failed set binary utf-16 bom key");
    check_key_value(h, key3, val3, strlen(val3));

    if (isWarmupEnabled(h)) {
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   false);

        wait_for_warmup_complete(h);
        check_key_value(h, key0, val0, strlen(val0));
        check_key_value(h, key1, val1, strlen(val1));
        check_key_value(h, key2, val2, strlen(val2));
        check_key_value(h, key3, val3, strlen(val3));
    }
    return SUCCESS;
}

static enum test_result test_restart_bin_val(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         "key",
                         binaryData,
                         sizeof(binaryData),
                         82758,
                         0,
                         Vbid(0))
                    .first,
            "Failed set.");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    check_key_value(h, "key", binaryData, sizeof(binaryData));
    return SUCCESS;
}

static enum test_result test_wrong_vb_get(EngineIface* h) {
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");
    checkeq(cb::engine_errc::not_my_vbucket,
            verify_key(h, "key", Vbid(1)),
            "Expected wrong bucket.");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_get_pending(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_pending),
          "Failed to set vbucket state.");
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);

    checkeq(cb::engine_errc::would_block,
            get(h, cookie, "key", Vbid(1)).first,
            "Expected wouldblock.");
    checkeq(1, get_int_stat(h, "vb_pending_ops_get"), "Expected 1 get");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");
    checkeq(cb::engine_errc::not_my_vbucket,
            verify_key(h, "key", Vbid(1)),
            "Expected not my bucket.");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_set(EngineIface* h) {
    return test_wrong_vb_mutation(h, StoreSemantics::Set);
}

static enum test_result test_wrong_vb_cas(EngineIface* h) {
    return test_wrong_vb_mutation(h, StoreSemantics::CAS);
}

static enum test_result test_wrong_vb_add(EngineIface* h) {
    return test_wrong_vb_mutation(h, StoreSemantics::Add);
}

static enum test_result test_wrong_vb_replace(EngineIface* h) {
    return test_wrong_vb_mutation(h, StoreSemantics::Replace);
}

static enum test_result test_wrong_vb_del(EngineIface* h) {
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");
    checkeq(cb::engine_errc::not_my_vbucket,
            del(h, "key", 0, Vbid(1)),
            "Expected wrong bucket.");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

/* Returns a string in the format "%Y-%m-%d %H:%M:%S" of the specified
 * time point.
 */
std::string make_time_string(std::chrono::system_clock::time_point time_point) {
    time_t tt = std::chrono::system_clock::to_time_t(time_point);
#ifdef _MSC_VER
    // Windows' gmtime() is already thread-safe.
    struct tm* split = gmtime(&tt);
#else
    struct tm local_storage;
    struct tm* split = gmtime_r(&tt, &local_storage);
#endif
    char timeStr[20];
    strftime(timeStr, 20, "%Y-%m-%d %H:%M:%S", split);
    return timeStr;
}

static enum test_result test_expiry_pager_settings(EngineIface* h) {
    cb_assert(!get_bool_stat(h, "ep_exp_pager_enabled"));
    checkeq(600,
            get_int_stat(h, "ep_exp_pager_stime"),
            "Expiry pager sleep time not expected");
    set_param(h, EngineParamCategory::Flush, "exp_pager_stime", "1");
    checkeq(1,
            get_int_stat(h, "ep_exp_pager_stime"),
            "Expiry pager sleep time not updated");
    cb_assert(!get_bool_stat(h, "ep_exp_pager_enabled"));
    using namespace std::chrono;
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    checkeq(0,
            get_int_stat(h, "ep_num_expiry_pager_runs"),
            "Expiry pager run count is not zero");

    set_param(h, EngineParamCategory::Flush, "exp_pager_enabled", "true");
    checkeq(1,
            get_int_stat(h, "ep_exp_pager_stime"),
            "Expiry pager sleep time not updated");
    wait_for_stat_to_be_gte(h, "ep_num_expiry_pager_runs", 1);

    // Reload engine
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    cb_assert(!get_bool_stat(h, "ep_exp_pager_enabled"));

    // Enable expiry pager again
    set_param(h, EngineParamCategory::Flush, "exp_pager_enabled", "true");

    checkeq(get_int_stat(h, "ep_exp_pager_initial_run_time"),
            -1,
            "Task time should be disable upon warmup");

    std::string err_msg;
    // Update exp_pager_initial_run_time and ensure the update is successful
    set_param(h, EngineParamCategory::Flush, "exp_pager_initial_run_time", "3");
    std::string expected_time = "03:00";
    std::string str;
    // [MB-21806] - Need to repeat the fetch as the set_param for
    // "exp_pager_initial_run_time" schedules a task that sets the stats later
    repeat_till_true([&]() {
        str = get_str_stat(h, "ep_expiry_pager_task_time");
        return 0 == str.substr(11, 5).compare(expected_time);
    });
    err_msg.assign("Updated time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update exp_pager_stime by 30 minutes and ensure that the update is successful
    const auto update_by = 30min;
    // the calculated task time depends on memcached_uptime ticks, so can be
    // at most 1 second behind based on exactly when it is checked;
    // reduce the lower bound to allow this.
    std::string targetTaskTime1{
            make_time_string(system_clock::now() + update_by - 1s)};

    // clear the initial task time, and test setting stime results in an exact
    // result of
    //   task_time = now + stime
    set_param(
            h, EngineParamCategory::Flush, "exp_pager_initial_run_time", "-1");
    set_param(
            h,
            EngineParamCategory::Flush,
            "exp_pager_stime",
            std::to_string(duration_cast<seconds>(update_by).count()).c_str());
    str = get_str_stat(h, "ep_expiry_pager_task_time");

    std::string targetTaskTime2{
            make_time_string(system_clock::now() + update_by)};

    // ep_expiry_pager_task_time should fall within the range of
    // targetTaskTime1 and targetTaskTime2
    err_msg.assign("Unexpected task time range, expect: " +
                   targetTaskTime1 + " <= " + str + " <= " + targetTaskTime2);
    checkle(targetTaskTime1, str, err_msg.c_str());
    checkle(str, targetTaskTime2, err_msg.c_str());

    return SUCCESS;
}

static enum test_result test_expiry_with_xattr(EngineIface* h) {
    const char* key = "test_expiry";
    cb::xattr::Blob blob;

    //Add a few XAttrs
    blob.set("user", R"({"author":"bubba"})");
    blob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");
    blob.set("meta", R"({"content-type":"text"})");

    auto xattr_value = blob.finalize();

    //Now, append user data to the xattrs and store the data
    std::string value_data("test_expiry_value");
    std::vector<char> data;
    std::copy(xattr_value.begin(), xattr_value.end(), std::back_inserter(data));
    std::copy(value_data.c_str(), value_data.c_str() + value_data.length(),
              std::back_inserter(data));

    auto* cookie = testHarness->create_cookie(h);

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         cookie,
                         StoreSemantics::Set,
                         key,
                         reinterpret_cast<char*>(data.data()),
                         data.size(),
                         9258,
                         0,
                         Vbid(0),
                         10,
                         PROTOCOL_BINARY_DATATYPE_XATTR)
                    .first,
            "Failed to store xattr document");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
    }

    testHarness->time_travel(11);

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, "test_expiry", errorMetaPair, cookie),
          "Get meta command failed");
    auto prev_revseqno = errorMetaPair.second.seqno;

    checkeq(PROTOCOL_BINARY_DATATYPE_XATTR,
            errorMetaPair.second.datatype,
            "Datatype is not XATTR");

    auto ret = get(h, cookie, key, Vbid(0), DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success,
            ret.first,
            "Unable to get a deleted item");

    check(get_meta(h, "test_expiry", errorMetaPair, cookie),
          "Get meta command failed");

    checkeq(errorMetaPair.second.seqno,
            prev_revseqno + 1,
            "rev seqno must have incremented by 1");

    /* Retrieve the item info and create a new blob out of the data */
    item_info info;
    checkeq(true,
            h->get_item_info(*ret.second.get(), info),
            "Unable to retrieve item info");

    cb::char_buffer value_buf{static_cast<char*>(info.value[0].iov_base),
                              info.value[0].iov_len};

    cb::xattr::Blob new_blob(value_buf, false);

    /* Only system extended attributes need to be present at this point.
     * Thus, check the blob length with the system size.
     */
    const auto systemsize = new_blob.finalize().size();

    checkeq(systemsize, new_blob.get_system_size(),
            "The size of the blob doesn't match the size of system attributes");

    const std::string& cas_str{R"({"cas":"0xdeadbeefcafefeed"})"};
    const std::string& sync_str = to_string(blob.get("_sync"));

    checkeq(cas_str, sync_str , "system xattr is invalid");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_expiry(EngineIface* h) {
    const char *key = "test_expiry";
    const char *data = "some test data here.";

    auto* cookie = testHarness->create_cookie(h);
    auto ret = allocate(h,
                        cookie,
                        key,
                        strlen(data),
                        0,
                        2,
                        PROTOCOL_BINARY_RAW_BYTES,
                        Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    auto rv = h->store(*cookie,
                       *ret.second.get(),
                       cas,
                       StoreSemantics::Set,
                       {},
                       DocumentState::Alive,
                       false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    check_key_value(h, key, data, strlen(data));

    testHarness->time_travel(5);
    checkeq(cb::engine_errc::no_such_key,
            get(h, cookie, key, Vbid(0)).first,
            "Item didn't expire");

    int expired_access = get_int_stat(h, "ep_expired_access");
    int expired_pager = get_int_stat(h, "ep_expired_pager");
    int active_expired = get_int_stat(h, "vb_active_expired");
    checkeq(0, expired_pager, "Expected zero expired item by pager");
    checkeq(1, expired_access, "Expected an expired item on access");
    checkeq(1, active_expired, "Expected an expired active item");
    checkeq(cb::engine_errc::success,
            store(h, cookie, StoreSemantics::Set, key, data),
            "Failed set.");

    // When run under full eviction, the total item stats are set from the
    // flusher. So we need to wait for it to finish before checking the
    // total number of items.
    wait_for_flusher_to_settle(h);

    std::stringstream ss;
    ss << "curr_items stat should be still 1 after ";
    ss << "overwriting the key that was expired, but not purged yet";
    checkeq(1, get_int_stat(h, "curr_items"), ss.str().c_str());

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_expiry_loader(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }
    const char *key = "test_expiry_loader";
    const char *data = "some test data here.";

    auto* cookie = testHarness->create_cookie(h);
    auto ret = allocate(h,
                        cookie,
                        key,
                        strlen(data),
                        0,
                        2,
                        PROTOCOL_BINARY_RAW_BYTES,
                        Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    auto rv = h->store(*cookie,
                       *ret.second.get(),
                       cas,
                       StoreSemantics::Set,
                       {},
                       DocumentState::Alive,
                       false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    check_key_value(h, key, data, strlen(data));

    testHarness->time_travel(3);

    ret = get(h, cookie, key, Vbid(0));
    checkeq(cb::engine_errc::no_such_key, ret.first, "Item didn't expire");
    testHarness->destroy_cookie(cookie);

    // Restart the engine to ensure the above expired item is not loaded
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    cb_assert(0 == get_int_stat(h, "ep_warmup_value_count", "warmup"));

    return SUCCESS;
}

static enum test_result test_expiration_on_compaction(EngineIface* h) {
    if (get_bool_stat(h, "ep_exp_pager_enabled")) {
        set_param(h, EngineParamCategory::Flush, "exp_pager_enabled", "false");
    }

    checkeq(1,
            get_int_stat(h, "vb_0:persistence:num_visits", "checkpoint"),
            "Cursor moved before item load");

    for (int i = 0; i < 25; i++) {
        std::stringstream ss;
        ss << "key" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "somevalue",
                      nullptr,
                      0,
                      Vbid(0),
                      10,
                      PROTOCOL_BINARY_RAW_BYTES),
                "Set failed.");
    }

    // Throw an xattr document in (and later compressed)
    cb::xattr::Blob builder;
    builder.set("_ep", R"({"foo":"bar"})");
    builder.set("key", R"({"foo":"bar"})");
    builder.set("_sync", R"({"foo":"bar"})");
    builder.set("stuff", R"({"foo":"bar"})");
    builder.set("misc", R"({"foo":"bar"})");
    builder.set("things", R"({"foo":"bar"})");
    builder.set("that", R"({"foo":"bar"})");

    auto blob = builder.finalize();
    std::string data;
    std::copy(blob.begin(), blob.end(), std::back_inserter(data));
    for (int i = 0; i < 12; i++) {
        std::stringstream ss;
        ss << "xattr_key" << i;

        checkeq(cb::engine_errc::success,
                storeCasVb11(h,
                             nullptr,
                             StoreSemantics::Set,
                             ss.str().c_str(),
                             data.data(),
                             data.size(),
                             0,
                             0,
                             Vbid(0),
                             10,
                             PROTOCOL_BINARY_DATATYPE_XATTR,
                             DocumentState::Alive)
                        .first,
                "Unable to store item");
    }

    cb::compression::Buffer compressedDoc;
    cb::compression::deflate(
            cb::compression::Algorithm::Snappy, data, compressedDoc);

    for (int i = 0; i < 13; i++) {
        std::stringstream ss;
        ss << "compressed_xattr_key" << i;

        checkeq(cb::engine_errc::success,
                storeCasVb11(h,
                             nullptr,
                             StoreSemantics::Set,
                             ss.str().c_str(),
                             compressedDoc.data(),
                             compressedDoc.size(),
                             0,
                             0,
                             Vbid(0),
                             10,
                             PROTOCOL_BINARY_DATATYPE_XATTR |
                                     PROTOCOL_BINARY_DATATYPE_SNAPPY,
                             DocumentState::Alive)
                        .first,
                "Unable to store item");
    }

    wait_for_flusher_to_settle(h);
    checkeq(50,
            get_int_stat(h, "curr_items"),
            "Unexpected number of items on database");
    checklt(1, get_int_stat(h, "vb_0:persistence:num_visits", "checkpoint"),
            "Cursor not moved even after flusher runs");

    testHarness->time_travel(15);

    // Compaction on VBucket
    compact_db(h, Vbid(0), 0, 0, 0);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);

    checkeq(50,
            get_int_stat(h, "ep_expired_compactor"),
            "Unexpected expirations by compactor");

    return SUCCESS;
}

static enum test_result test_expiration_on_warmup(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    auto* cookie = testHarness->create_cookie(h);
    set_param(h, EngineParamCategory::Flush, "exp_pager_enabled", "false");
    int pager_runs = get_int_stat(h, "ep_num_expiry_pager_runs");

    const char *key = "KEY";
    const char *data = "VALUE";

    auto ret = allocate(h,
                        cookie,
                        key,
                        strlen(data),
                        0,
                        10,
                        PROTOCOL_BINARY_RAW_BYTES,
                        Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    auto rv = h->store(*cookie,
                       *ret.second.get(),
                       cas,
                       StoreSemantics::Set,
                       {},
                       DocumentState::Alive,
                       false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    testHarness->destroy_cookie(cookie);

    check_key_value(h, key, data, strlen(data));
    ret.second.reset();
    wait_for_flusher_to_settle(h);

    checkeq(1, get_int_stat(h, "curr_items"), "Failed store item");
    testHarness->time_travel(15);

    checkeq(pager_runs,
            get_int_stat(h, "ep_num_expiry_pager_runs"),
            "Expiry pager shouldn't have run during this time");

    // Restart the engine to ensure the above item is expired
    testHarness->reload_engine(&h,
                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    check(get_bool_stat(h, "ep_exp_pager_enabled"),
          "Expiry pager should be enabled on warmup");

    // Wait for the expiry pager to run and expire our item.
    wait_for_stat_to_be_gte(
            h, "ep_expired_pager", 1, nullptr, std::chrono::seconds{10});

    // Note: previously we checked that curr_items was zero here (immediately
    // after waiting for ep_expired_pager == 1), however we cannot assume that
    // - items are actually expired asynchronously.
    // See EPStore::processExpiredItem - for non-temporary, expired items we
    // call processSoftDelete (soft-marking the item as deleted in the
    // hashtable), and then call queueDirty to queue a deletion, and then
    // increment the expired stat. Only when that delete is actually persisted
    // and the deleted callback is invoked -
    // PeristenceCallback::callback(int&) - is curr_items finally decremented.
    // Therefore we need to wait for the flusher to settle (i.e. delete
    // callback to be called) for the curr_items stat to be accurate.
    wait_for_flusher_to_settle(h);

    checkeq(0,
            get_int_stat(h, "curr_items"),
            "The item should have been expired.");

    return SUCCESS;
}

static enum test_result test_bug3454(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    const char *key = "test_expiry_duplicate_warmup";
    const char *data = "some test data here.";

    auto* cookie = testHarness->create_cookie(h);
    auto ret = allocate(h,
                        cookie,
                        key,
                        strlen(data),
                        0,
                        5,
                        PROTOCOL_BINARY_RAW_BYTES,
                        Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    auto rv = h->store(*cookie,
                       *ret.second.get(),
                       cas,
                       StoreSemantics::Set,
                       {},
                       DocumentState::Alive,
                       false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    check_key_value(h, key, data, strlen(data));
    wait_for_flusher_to_settle(h);

    // Advance the ep_engine time by 10 sec for the above item to be expired.
    testHarness->time_travel(10);
    ret = get(h, cookie, key, Vbid(0));
    checkeq(cb::engine_errc::no_such_key, ret.first, "Item didn't expire");

    ret = allocate(h,
                   cookie,
                   key,
                   strlen(data),
                   0,
                   0,
                   PROTOCOL_BINARY_RAW_BYTES,
                   Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    cas = 0;
    // Add a new item with the same key.
    rv = h->store(*cookie,
                  *ret.second.get(),
                  cas,
                  StoreSemantics::Add,
                  {},
                  DocumentState::Alive,
                  false);
    checkeq(cb::engine_errc::success, rv, "Add failed.");
    check_key_value(h, key, data, strlen(data));
    ret.second.reset();
    wait_for_flusher_to_settle(h);

    checkeq(cb::engine_errc::success,
            get(h, cookie, key, Vbid(0)).first,
            "Item shouldn't expire");
    testHarness->destroy_cookie(cookie);

    // Restart the engine to ensure the above unexpired new item is loaded
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    cb_assert(1 == get_int_stat(h, "ep_warmup_value_count", "warmup"));
    cb_assert(0 == get_int_stat(h, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static enum test_result test_bug3522(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    const char *key = "test_expiry_no_items_warmup";
    const char *data = "some test data here.";

    auto* cookie = testHarness->create_cookie(h);
    auto ret = allocate(h,
                        cookie,
                        key,
                        strlen(data),
                        0,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    item_info info;
    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    auto rv = h->store(*cookie,
                       *ret.second.get(),
                       cas,
                       StoreSemantics::Set,
                       {},
                       DocumentState::Alive,
                       false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    check_key_value(h, key, data, strlen(data));
    wait_for_flusher_to_settle(h);

    // Add a new item with the same key and 2 sec of expiration.
    const char *new_data = "new data here.";
    ret = allocate(h,
                   cookie,
                   key,
                   strlen(new_data),
                   0,
                   2,
                   PROTOCOL_BINARY_RAW_BYTES,
                   Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    if (!h->get_item_info(*ret.second.get(), info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, new_data, strlen(new_data));

    int pager_runs = get_int_stat(h, "ep_num_expiry_pager_runs");
    cas = 0;
    rv = h->store(*cookie,
                  *ret.second.get(),
                  cas,
                  StoreSemantics::Set,
                  {},
                  DocumentState::Alive,
                  false);
    checkeq(cb::engine_errc::success, rv, "Set failed.");
    testHarness->destroy_cookie(cookie);

    check_key_value(h, key, new_data, strlen(new_data));
    ret.second.reset();
    testHarness->time_travel(3);
    wait_for_stat_change(h, "ep_num_expiry_pager_runs", pager_runs);
    wait_for_flusher_to_settle(h);

    // Restart the engine.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    // TODO: modify this for a better test case
    cb_assert(0 == get_int_stat(h, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static enum test_result test_get_replica_active_state(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    auto pkt = prepare_get_replica(h, vbucket_state_active);
    checkeq(cb::engine_errc::not_my_vbucket,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Get Replica Failed");

    return SUCCESS;
}

static enum test_result test_get_replica_pending_state(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);
    auto pkt = prepare_get_replica(h, vbucket_state_pending);
    checkeq(cb::engine_errc::not_my_vbucket,
            h->unknown_command(cookie, *pkt, add_response),
            "Should have returned NOT_MY_VBUCKET for pending state");
    checkeq(1, get_int_stat(h, "ep_num_not_my_vbuckets"), "Expected 1 get");
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_get_replica_dead_state(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    auto pkt = prepare_get_replica(h, vbucket_state_dead);
    checkeq(cb::engine_errc::not_my_vbucket,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Get Replica Failed");
    return SUCCESS;
}

static enum test_result test_get_replica(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    auto pkt = prepare_get_replica(h, vbucket_state_replica);
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Get Replica Failed");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected cb::mcbp::Status::Success response.");
    checkeq("replicadata"s, last_body, "Should have returned identical value");
    checkeq(1, get_int_stat(h, "vb_replica_ops_get"), "Expected 1 get");

    return SUCCESS;
}

static enum test_result test_get_replica_non_resident(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Store Failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "ep_total_persisted", 1);

    evict_key(h, "key", Vbid(0), "Ejected.");
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket to replica");

    get_replica(h, "key", Vbid(0));
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected success");
    checkeq(1, get_int_stat(h, "vb_replica_ops_get"), "Expected 1 get");

    return SUCCESS;
}

static enum test_result test_get_replica_invalid_key(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    bool makeinvalidkey = true;
    auto pkt = prepare_get_replica(h, vbucket_state_replica, makeinvalidkey);
    checkeq(cb::engine_errc::not_my_vbucket,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Get Replica Failed");
    return SUCCESS;
}

static enum test_result test_vb_del_pending(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, Vbid(1), vbucket_state_pending),
          "Failed to set vbucket state.");
    checkeq(cb::engine_errc::would_block,
            del(h, "key", 0, Vbid(1), cookie),
            "Expected woodblock.");
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, "ep_num_not_my_vbuckets");
    checkeq(cb::engine_errc::not_my_vbucket,
            del(h, "key", 0, Vbid(1)),
            "Expected not my vbucket.");
    wait_for_stat_change(h, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(EngineIface* h) {
    return verify_vbucket_missing(h, Vbid(1)) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(EngineIface* h) {
    return verify_vbucket_state(h, Vbid(0), vbucket_state_active) ? SUCCESS
                                                                  : FAIL;
}

static enum test_result test_vbucket_create(EngineIface* h) {
    if (!verify_vbucket_missing(h, Vbid(1))) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, Vbid(1), vbucket_state_active)) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    return verify_vbucket_state(h, Vbid(1), vbucket_state_active) ? SUCCESS
                                                                  : FAIL;
}

static enum test_result test_takeover_stats_race_with_vb_create_DCP(
        EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state information");

    checkeq(0,
            get_int_stat(h, "on_disk_deletes", "dcp-vbtakeover 1"),
            "Invalid number of on-disk deletes");

    return SUCCESS;
}

static enum test_result test_takeover_stats_num_persisted_deletes(
        EngineIface* h) {
    /* set an item */
    std::string key("key");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key.c_str(), "data"),
            "Failed to store an item");

    /* delete the item */
    checkeq(cb::engine_errc::success,
            del(h, key.c_str(), 0, Vbid(0)),
            "Failed to delete the item");

    /* wait for persistence */
    wait_for_flusher_to_settle(h);

    /* check if persisted deletes stats is got correctly */
    checkeq(1,
            get_int_stat(h, "on_disk_deletes", "dcp-vbtakeover 0"),
            "Invalid number of on-disk deletes");

    return SUCCESS;
}

static enum test_result test_vbucket_compact(EngineIface* h) {
    const char* exp_key = "Carss";
    const char* exp_value = "pollute";
    const char* non_exp_key = "trees";
    const char* non_exp_value = "cleanse";

    // Set two keys - one to be expired and other to remain...
    // Set expiring key
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, exp_key, exp_value),
            "Failed to set expiring key");
    check_key_value(h, exp_key, exp_value, strlen(exp_value));

    // Set a non-expiring key...
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, non_exp_key, non_exp_value),
            "Failed to set non-expiring key");
    check_key_value(h, non_exp_key, non_exp_value, strlen(non_exp_value));

    // Touch expiring key with an expire time
    const int exp_time = 11;
    checkeq(cb::engine_errc::success,
            touch(h, exp_key, Vbid(0), exp_time),
            "Touch expiring key failed");

    // Move beyond expire time
    testHarness->time_travel(exp_time + 1);

    // Touch the expiring key to trigger the expiry
    auto* cookie = testHarness->create_cookie(h);
    checkeq(cb::engine_errc::no_such_key,
            get(h, cookie, exp_key, Vbid(0), DocStateFilter::Alive).first,
            "Expiration trigger via touch failed");
    testHarness->destroy_cookie(cookie);

    // Expect the item to now be expired
    checkeq(1,
            get_int_stat(h, "vb_active_expired"),
            "Incorrect number of expired keys");
    const int exp_purge_seqno =
            get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    // non_exp_key and its value should be intact...
    checkeq(cb::engine_errc::success,
            verify_key(h, non_exp_key),
            "key trees should be found.");
    // exp_key should have disappeared...
    cb::engine_errc val = verify_key(h, exp_key);
    checkeq(cb::engine_errc::no_such_key, val, "Key Carss has not expired.");

    // Store a dummy item since we do not purge the item with highest seqno
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "dummykey", "dummyvalue"),
            "Error setting dummy key");
    wait_for_flusher_to_settle(h);

    checkeq(0,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno not found to be zero before compaction");

    // Compaction on VBucket
    compact_db(
            h,
            Vbid(0) /* vbucket_id */,
            2 /* purge_before_ts */,
            exp_purge_seqno - 1 /* purge_before_seq */,
            1 /* drop deletes (forces purge irrespective purge_before_seq) */);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(exp_purge_seqno,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    return SUCCESS;
}

static enum test_result test_compaction_config(EngineIface* h) {
    checkeq(10000,
            get_int_stat(h, "ep_compaction_write_queue_cap"),
            "Expected compaction queue cap to be 10000");
    set_param(h,
              EngineParamCategory::Flush,
              "compaction_write_queue_cap",
              "100000");
    checkeq(100000,
            get_int_stat(h, "ep_compaction_write_queue_cap"),
            "Expected compaction queue cap to be 100000");
    return SUCCESS;
}

static enum test_result test_MB_33919(EngineIface* h) {
    const char* expKey = "expiryKey";
    const char* otherKey = "otherKey";
    const char* value = "value";

    // Test runs in the future as we need to set times in the past without
    // falling foul of the expiry time being behind server start
    auto fiveDays =
            std::chrono::system_clock::now() + std::chrono::hours(5 * 24);

    // Calculate expiry and purgeBefore times
    auto threeDaysAgo = std::chrono::system_clock::to_time_t(
            fiveDays - std::chrono::hours(3 * 24));
    auto fourDaysAgo = std::chrono::system_clock::to_time_t(
            fiveDays - std::chrono::hours(4 * 24));
    // Time travel forwards by 5 days to give the illusion of 5 days uptime.
    testHarness->time_travel((86400 * 5));

    // Set expiring key, expiry 4 days ago
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  expKey,
                  value,
                  nullptr,
                  0,
                  Vbid(0),
                  fourDaysAgo),
            "Failed to set expiring key");

    // Force it to expire
    cb::engine_errc val = verify_key(h, expKey);
    checkeq(cb::engine_errc::no_such_key, val, "expiryKey has not expired.");

    // And a second key (after expiry), compaction won't purge the high-seqno
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, otherKey, value),
            "Failed to set non-expiring key");

    // Wait for the item to be expired
    wait_for_stat_to_be(h, "vb_active_expired", 1);

    wait_for_flusher_to_settle(h);

    checkeq(0,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno not found to be zero before compaction");

    // Compaction on VBucket
    compact_db(h, Vbid(0), threeDaysAgo, 0, 0);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);

    // Purge-seqno should not of moved, without the fix it would
    checkeq(0,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno not found to be zero before compaction");

    return SUCCESS;
}

struct comp_thread_ctx {
    EngineIface* h;
    Vbid db_file_id;
};

void makeCouchstoreFileInaccessible(const std::string& dbname);
extern "C" {
    static void compaction_thread(void *arg) {
        auto *ctx = static_cast<comp_thread_ctx *>(arg);
        compact_db(ctx->h, ctx->db_file_id, 0, 0, 0);
    }
}

static enum test_result test_multiple_vb_compactions(EngineIface* h) {
    for (uint16_t i = 0; i < 4; ++i) {
        if (!set_vbucket_state(h, Vbid(i), vbucket_state_active)) {
            fprintf(stderr, "set state failed for vbucket %d.\n", i);
            return FAIL;
        }
        check(verify_vbucket_state(h, Vbid(i), vbucket_state_active),
              "VBucket state not active");
    }

    std::vector<std::string> keys;
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int count = 0;
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        Vbid vbid = Vbid(count % 4);
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      nullptr,
                      0,
                      vbid),
                "Failed to store a value");
        ++count;
    }

    wait_for_flusher_to_settle(h);

    // Compact multiple vbuckets.
    const int n_threads = 4;
    cb_thread_t threads[n_threads];
    struct comp_thread_ctx ctx[n_threads];

    const int num_shards =
            get_int_stat(h, "ep_workload:num_shards", "workload");

    for (int i = 0; i < n_threads; i++) {
        ctx[i].h = h;
        ctx[i].db_file_id = Vbid(static_cast<Vbid>(i).get() % num_shards);
        int r = cb_create_thread(&threads[i], compaction_thread, &ctx[i], 0);
        cb_assert(r == 0);
    }

    for (auto thread : threads) {
        int r = cb_join_thread(thread);
        cb_assert(r == 0);
    }

    wait_for_stat_to_be(h, "ep_pending_compactions", 0);

    return SUCCESS;
}

static enum test_result test_multi_vb_compactions_with_workload(
        EngineIface* h) {
    for (uint16_t i = 0; i < 4; ++i) {
        if (!set_vbucket_state(h, Vbid(i), vbucket_state_active)) {
            fprintf(stderr, "set state failed for vbucket %d.\n", i);
            return FAIL;
        }
        check(verify_vbucket_state(h, Vbid(i), vbucket_state_active),
              "VBucket state not active");
    }

    std::vector<std::string> keys;
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int count = 0;
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        Vbid vbid = Vbid(count % 4);
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      nullptr,
                      0,
                      vbid),
                "Failed to store a value");
        ++count;
    }
    wait_for_flusher_to_settle(h);

    for (int i = 0; i < 2; ++i) {
        count = 0;
        for (it = keys.begin(); it != keys.end(); ++it) {
            Vbid vbid = Vbid(count % 4);
            checkeq(cb::engine_errc::success,
                    get(h, nullptr, it->c_str(), vbid).first,
                    "Unable to get stored item");
            ++count;
        }
    }
    wait_for_stat_to_be(h, "ep_workload_pattern", std::string{"read_heavy"});

    // Compact multiple vbuckets.
    const int n_threads = 4;
    cb_thread_t threads[n_threads];
    struct comp_thread_ctx ctx[n_threads];

    for (int i = 0; i < n_threads; i++) {
        ctx[i].h = h;
        ctx[i].db_file_id = static_cast<Vbid>(i);
        int r = cb_create_thread(&threads[i], compaction_thread, &ctx[i], 0);
        cb_assert(r == 0);
    }

    for (auto thread : threads) {
        int r = cb_join_thread(thread);
        cb_assert(r == 0);
    }

    wait_for_stat_to_be(h, "ep_pending_compactions", 0);

    return SUCCESS;
}

static enum test_result vbucket_destroy(EngineIface* h,
                                        const char* value = nullptr) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    checkeq(cb::engine_errc::not_my_vbucket,
            vbucketDelete(h, Vbid(2), value),
            "Expected NMVB");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1), value),
            "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, Vbid(1)),
          "vbucket 1 was not missing after deleting it.");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_stats(EngineIface* h) {
    int cacheSize = get_int_stat(h, "ep_total_cache_size");
    int overhead = get_int_stat(h, "ep_overhead");
    int nonResident = get_int_stat(h, "ep_num_non_resident");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int itemsRemoved = get_int_stat(h, "ep_items_rm_from_checkpoints");
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      nullptr,
                      0,
                      Vbid(1)),
                "Failed to store a value");
    }
    wait_for_flusher_to_settle(h);
    testHarness->time_travel(65);
    wait_for_stat_change(h, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    int vbucketDel = get_int_stat(h, "ep_vbucket_del");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, Vbid(1)),
          "vbucket 1 was not missing after deleting it.");

    wait_for_stat_change(h, "ep_vbucket_del", vbucketDel);

    wait_for_stat_to_be(h, "ep_total_cache_size", cacheSize);
    wait_for_stat_to_be(h, "ep_overhead", overhead);
    wait_for_stat_to_be(h, "ep_num_non_resident", nonResident);

    return SUCCESS;
}

static enum test_result vbucket_destroy_restart(EngineIface* h,
                                                const char* value = nullptr) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    // Store a value so the restart will try to resurrect it.
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key",
                  "somevalue",
                  nullptr,
                  0,
                  Vbid(1)),
            "Failed to set a value");
    check_key_value(h, "key", "somevalue", 9, Vbid(1));

    // Reload to get a flush forced.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    check(verify_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Bucket state was what it was initially, after restart.");
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");
    check_key_value(h, "key", "somevalue", 9, Vbid(1));

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed set set vbucket 1 state.");

    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1), value),
            "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, Vbid(1)),
          "vbucket 1 was not missing after deleting it.");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    if (verify_vbucket_state(h, Vbid(1), vbucket_state_pending, true)) {
        std::cerr << "Bucket came up in pending state after delete." << std::endl;
        abort();
    }

    check(verify_vbucket_missing(h, Vbid(1)),
          "vbucket 1 was not missing after restart.");

    return SUCCESS;
}

static enum test_result test_async_vbucket_destroy(EngineIface* h) {
    return vbucket_destroy(h);
}

static enum test_result test_sync_vbucket_destroy(EngineIface* h) {
    return vbucket_destroy(h, "async=0");
}

static enum test_result test_async_vbucket_destroy_restart(EngineIface* h) {
    return vbucket_destroy_restart(h);
}

static enum test_result test_sync_vbucket_destroy_restart(EngineIface* h) {
    return vbucket_destroy_restart(h, "async=0");
}

static enum test_result test_vb_set_pending(EngineIface* h) {
    return test_pending_vb_mutation(h, StoreSemantics::Set);
}

static enum test_result test_vb_add_pending(EngineIface* h) {
    return test_pending_vb_mutation(h, StoreSemantics::Add);
}

static enum test_result test_vb_cas_pending(EngineIface* h) {
    return test_pending_vb_mutation(h, StoreSemantics::CAS);
}

static enum test_result test_vb_set_replica(EngineIface* h) {
    return test_replica_vb_mutation(h, StoreSemantics::Set);
}

static enum test_result test_vb_replace_replica(EngineIface* h) {
    return test_replica_vb_mutation(h, StoreSemantics::Replace);
}

static enum test_result test_vb_replace_pending(EngineIface* h) {
    return test_pending_vb_mutation(h, StoreSemantics::Replace);
}

static enum test_result test_vb_add_replica(EngineIface* h) {
    return test_replica_vb_mutation(h, StoreSemantics::Add);
}

static enum test_result test_vb_cas_replica(EngineIface* h) {
    return test_replica_vb_mutation(h, StoreSemantics::CAS);
}

static enum test_result test_stats_seqno(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    int num_keys = 100;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "value",
                      nullptr,
                      0,
                      Vbid(0)),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h);

    // MB-47105: help identifing if anything related to MB-37920
    if (isPersistentBucket(h)) {
        checkeq(0, get_int_stat(h, "ep_item_commit_failed"), "Flush failures");
    }

    checkeq(100,
            get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno"),
            "Invalid seqno");

    if (isPersistentBucket(h)) {
        checkeq(100,
                get_int_stat(h, "vb_0:last_persisted_seqno", "vbucket-seqno"),
                "Unexpected last_persisted_seqno");
    }
    checkeq(0,
            get_int_stat(h, "vb_1:high_seqno", "vbucket-seqno"),
            "Invalid seqno");
    checkeq(0,
            get_int_stat(h, "vb_1:high_seqno", "vbucket-seqno 1"),
            "Invalid seqno");
    if (isPersistentBucket(h)) {
        checkeq(0,
                get_int_stat(h, "vb_1:last_persisted_seqno", "vbucket-seqno 1"),
                "Invalid last_persisted_seqno");
    }

    uint64_t vb_uuid = get_ull_stat(h, "vb_1:0:id", "failovers");

    auto seqno_stats = get_all_stats(h, "vbucket-seqno 1");
    checkeq(vb_uuid, uint64_t(std::stoull(seqno_stats.at("vb_1:uuid"))),
            "Invalid uuid");

    // Check invalid vbucket
    checkeq(cb::engine_errc::not_my_vbucket,
            get_stats(h, "vbucket-seqno 2"sv, {}, add_stats),
            "Expected not my vbucket");

    // Check bad vbucket parameter (not numeric)
    checkeq(cb::engine_errc::invalid_arguments,
            get_stats(h, "vbucket-seqno tt2"sv, {}, add_stats),
            "Expected invalid");

    // Check extra spaces at the end
    checkeq(cb::engine_errc::invalid_arguments,
            get_stats(h, "vbucket-seqno    "sv, {}, add_stats),
            "Expected invalid");

    return SUCCESS;
}

static enum test_result test_stats_diskinfo(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    int num_keys = 100;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "value",
                      nullptr,
                      0,
                      Vbid(1)),
                "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h);

    size_t file_size = get_int_stat(h, "ep_db_file_size", "diskinfo");
    size_t data_size = get_int_stat(h, "ep_db_data_size", "diskinfo");
    checklt(size_t{0}, file_size, "DB file size should be greater than 0");
    checklt(size_t{0}, data_size, "DB data size should be greater than 0");
    checkge(file_size, data_size, "DB file size should be >= DB data size");
    checkgt(get_int_stat(h, "vb_1:data_size", "diskinfo detail"), 0,
          "VB 1 data size should be greater than 0");

    checkeq(cb::engine_errc::invalid_arguments,
            get_stats(h, "diskinfo "sv, {}, add_stats),
            "Expected invalid");

    checkeq(cb::engine_errc::invalid_arguments,
            get_stats(h, "diskinfo detai"sv, {}, add_stats),
            "Expected invalid");

    checkeq(cb::engine_errc::invalid_arguments,
            get_stats(h, "diskinfo detaillll"sv, {}, add_stats),
            "Expected invalid");

    return SUCCESS;
}

static enum test_result test_uuid_stats(EngineIface* h) {
    vals.clear();
    checkeq(cb::engine_errc::success,
            get_stats(h, "uuid"sv, {}, add_stats),
            "Failed to get stats.");
    checkeq(std::string_view("foobar"),
            static_cast<std::string_view>(vals["uuid"]),
            "Incorrect uuid");
    return SUCCESS;
}

static enum test_result test_item_stats(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalueX"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key1", "somevalueY"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    check_key_value(h, "key", "somevalueX", 10);
    check_key_value(h, "key1", "somevalueY", 10);

    checkeq(cb::engine_errc::success,
            del(h, "key1", 0, Vbid(0)),
            "Failed remove with value.");
    wait_for_flusher_to_settle(h);

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key1", "someothervalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    check_key_value(h, "key1", "someothervalue", 14);

    checkeq(3, get_int_stat(h, "vb_active_ops_create"), "Expected 3 creations");
    checkeq(1, get_int_stat(h, "vb_active_ops_update"), "Expected 1 updation");
    checkeq(1, get_int_stat(h, "vb_active_ops_delete"), "Expected 1 deletion");
    checkeq(3, get_int_stat(h, "vb_active_ops_get"), "Expected 3 gets");

    return SUCCESS;
}

static enum test_result test_stats(EngineIface* h) {
    vals.clear();
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    checklt(size_t{10}, vals.size(), "Kind of expected more stats than that.");

    return SUCCESS;
}

static enum test_result test_mem_stats(EngineIface* h) {
    std::string value(4096, 'b');

    // If active compression - make a value that doesn't compress very well.
    if (isActiveCompressionEnabled(h)) {
        std::mt19937 generator; // Using the default seed is fine
        std::uniform_int_distribution<int> distribution{'0', 'z'};
        for (auto& dis : value) {
            dis = distribution(generator);
        }
    }

    int itemsRemoved = get_int_stat(h, "ep_items_rm_from_checkpoints");
    wait_for_persisted_value(h, "key", value.c_str());
    testHarness->time_travel(65);
    if (isPersistentBucket(h)) {
        wait_for_stat_change(h, "ep_items_rm_from_checkpoints", itemsRemoved);
    }

    if (isActiveCompressionEnabled(h)) {
        wait_for_item_compressor_to_settle(h);
    }

    int mem_used = get_int_stat(h, "mem_used");
    int cache_size = get_int_stat(h, "ep_total_cache_size");
    int overhead = get_int_stat(h, "ep_overhead");
    int value_size = get_int_stat(h, "ep_value_size");
    checkgt((mem_used - overhead), cache_size,
            "ep_kv_size should be greater than the hashtable cache size due to "
            "the checkpoint overhead");

    if (isPersistentBucket(h)) {
        evict_key(h, "key", Vbid(0), "Ejected.");

        checkge(cache_size, get_int_stat(h, "ep_total_cache_size"),
                "Evict a value shouldn't increase the total cache size");
        checkgt(mem_used, get_int_stat(h, "mem_used"),
              "Expected mem_used to decrease when an item is evicted");

        check_key_value(h,
                        "key",
                        value.c_str(),
                        value.size(),
                        Vbid(0)); // Load an item from disk again.

        if (isActiveCompressionEnabled(h)) {
            wait_for_item_compressor_to_settle(h);
        }
        checkeq(value_size, get_int_stat(h, "ep_value_size"),
                "Expected ep_value_size to remain the same after item is "
                "loaded from disk");
    }

    return SUCCESS;
}

static enum test_result test_io_stats(EngineIface* h) {
    int exp_write_bytes;
    std::string backend = get_str_stat(h, "ep_backend");
    if (backend == "couchdb") {
        exp_write_bytes = 22; /* TBD: Do not hard code the value */
    } else if (backend == "rocksdb") {
        // TODO RDB:
        return SKIPPED_UNDER_ROCKSDB;
    }
    else {
        return SKIPPED;
    }

    std::string collections = get_str_stat(h, "ep_collections_enabled");
    if (collections == "true") {
        // 1 byte of meta data for the collection-ID
        exp_write_bytes += 1;
    }

    reset_stats(h);

    checkeq(0,
            get_int_stat(h, "rw_0:io_bg_fetch_docs_read", "kvstore"),
            "Expected reset stats to set io_bg_fetch_docs_read to zero");
    checkeq(0,
            get_int_stat(h, "rw_0:io_num_write", "kvstore"),
            "Expected reset stats to set io_num_write to zero");
    checkeq(0,
            get_int_stat(h, "rw_0:io_bg_fetch_doc_bytes", "kvstore"),
            "Expected reset stats to set io_bg_fetch_doc_bytes to zero");
    checkeq(0,
            get_int_stat(h, "rw_0:io_document_write_bytes", "kvstore"),
            "Expected reset stats to set io_document_write_bytes to zero");

    const std::string key("a");
    const std::string value("b\r\n");
    wait_for_persisted_value(h, key.c_str(), value.c_str());
    checkeq(0,
            get_int_stat(h, "rw_0:io_bg_fetch_docs_read", "kvstore"),
            "Expected storing one value to not change the read counter");
    checkeq(0,
            get_int_stat(h, "rw_0:io_bg_fetch_doc_bytes", "kvstore"),
            "Expected storing one value to not change the bgfetch doc bytes");
    checkeq(1,
            get_int_stat(h, "rw_0:io_num_write", "kvstore"),
            "Expected storing the key to update the write counter");
    checkeq(exp_write_bytes,
            get_int_stat(h, "rw_0:io_document_write_bytes", "kvstore"),
            "Expected storing the key to update the write bytes");

    // Exact write amplification varies, bur expect it to be at least 10.0x
    // given we are writing a single byte key and single byte value.
    checkge(get_stat<float>(
                    h, "rw_0:io_flusher_write_amplification", "kvstore"),
            10.0f,
            "Expected storing the key to update Flusher Write Amplification");

    evict_key(h, key.c_str(), Vbid(0), "Ejected.");

    check_key_value(h, "a", value.c_str(), value.size(), Vbid(0));

    checkeq(1,
            get_int_stat(h, "rw_0:io_bg_fetch_docs_read", "kvstore"),
            "Expected reading the value back in to update the read counter");

    uint64_t exp_read_bytes = key.size() + value.size() +
                              MetaData::getMetaDataSize(MetaData::Version::V1);
    if (collections == "true") {
        // 1 byte of meta data for the collection-ID
        exp_read_bytes += 1;
    }
    checkeq(exp_read_bytes,
            get_stat<uint64_t>(h, "rw_0:io_bg_fetch_doc_bytes", "kvstore"),
            "Expected reading the value back in to update the read bytes");

    // For read amplification, exact value depends on couchstore file layout,
    // but generally see a value of 2 here.
    checkge(get_float_stat(h, "ep_bg_fetch_avg_read_amplification"),
            2.0f,
            "Expected sensible bgFetch read amplification value");

    checkeq(1,
            get_int_stat(h, "rw_0:io_num_write", "kvstore"),
            "Expected reading the value back in to not update the write "
            "counter");
    checkeq(exp_write_bytes,
            get_int_stat(h, "rw_0:io_document_write_bytes", "kvstore"),
            "Expected reading the value back in to not update the write bytes");

    return SUCCESS;
}

static enum test_result test_vb_file_stats(EngineIface* h) {
    wait_for_flusher_to_settle(h);
    std::string backend = get_str_stat(h, "ep_backend");
    // Some tests just insert vbstate into local db and magma doesn't track
    // the data size until items are put into the key and seq indexes so
    // use the file size to determine when data has been put into the db store.
    if (backend == "magma") {
        wait_for_stat_change(h, "ep_db_file_size", 0);
    } else {
        wait_for_stat_change(h, "ep_db_data_size", 0);
    }

    int old_data_size = get_int_stat(h, "ep_db_data_size");
    int old_file_size = get_int_stat(h, "ep_db_file_size");
    checkne(0, old_file_size, "Expected a non-zero value for ep_db_file_size");

    // Write a value and test ...
    wait_for_persisted_value(h, "a", "b\r\n");
    checklt(old_data_size, get_int_stat(h, "ep_db_data_size"),
            "Expected the DB data size to increase");
    checklt(old_file_size, get_int_stat(h, "ep_db_file_size"),
            "Expected the DB file size to increase");

    checklt(0, get_int_stat(h, "vb_0:db_data_size", "vbucket-details 0"),
            "Expected the vbucket DB data size to non-zero");
    checklt(0, get_int_stat(h, "vb_0:db_file_size", "vbucket-details 0"),
            "Expected the vbucket DB file size to non-zero");
    return SUCCESS;
}

static enum test_result test_vb_file_stats_after_warmup(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    for (int i = 0; i < 100; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      key.str().c_str(),
                      "somevalue"),
                "Error setting.");
    }
    wait_for_flusher_to_settle(h);

    int fileSize = get_int_stat(h, "vb_0:db_file_size", "vbucket-details 0");
    int spaceUsed = get_int_stat(h, "vb_0:db_data_size", "vbucket-details 0");

    // Restart the engine.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    int newFileSize = get_int_stat(h, "vb_0:db_file_size", "vbucket-details 0");
    int newSpaceUsed =
            get_int_stat(h, "vb_0:db_data_size", "vbucket-details 0");

    checkle(static_cast<float>(0.9 * fileSize),
            static_cast<float>(newFileSize),
            "Unexpected fileSize for vbucket");
    checkle(static_cast<float>(0.9 * spaceUsed),
            static_cast<float>(newSpaceUsed),
            "Unexpected spaceUsed for vbucket");

    return SUCCESS;
}

static enum test_result test_bg_stats(EngineIface* h) {
    reset_stats(h);
    wait_for_persisted_value(h, "a", "b\r\n");
    evict_key(h, "a", Vbid(0), "Ejected.");
    testHarness->time_travel(43);
    check_key_value(h, "a", "b\r\n", 3, Vbid(0));

    auto stats = get_all_stats(h);
    checkeq(1, std::stoi(stats.at("ep_bg_num_samples")),
               "Expected one sample");

    const char* bg_keys[] = { "ep_bg_min_wait",
                              "ep_bg_max_wait",
                              "ep_bg_wait_avg",
                              "ep_bg_min_load",
                              "ep_bg_max_load",
                              "ep_bg_load_avg"};
    for (const auto* key : bg_keys) {
        check(stats.find(key) != stats.end(),
              (std::string("Found no ") + key).c_str());
    }

    evict_key(h, "a", Vbid(0), "Ejected.");
    check_key_value(h, "a", "b\r\n", 3, Vbid(0));
    checkeq(2, get_int_stat(h, "ep_bg_num_samples"), "Expected one sample");

    reset_stats(h);
    checkeq(0,
            get_int_stat(h, "ep_bg_fetched"),
            "ep_bg_fetched is not reset to 0");
    return SUCCESS;
}

static enum test_result test_bg_meta_stats(EngineIface* h) {
    reset_stats(h);

    wait_for_persisted_value(h, "k1", "v1");
    wait_for_persisted_value(h, "k2", "v2");

    evict_key(h, "k1", Vbid(0), "Ejected.");
    checkeq(cb::engine_errc::success,
            del(h, "k2", 0, Vbid(0)),
            "Failed remove with value.");
    wait_for_flusher_to_settle(h);

    checkeq(0, get_int_stat(h, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(0,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "Expected bg_meta_fetched to be 0");

    check(get_meta(h, "k2"), "Get meta failed");
    checkeq(0, get_int_stat(h, "ep_bg_fetched"), "Expected bg_fetched to be 0");
    checkeq(1,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "Expected bg_meta_fetched to be 1");

    checkeq(cb::engine_errc::success,
            get(h, nullptr, "k1", Vbid(0)).first,
            "Missing key");
    checkeq(1, get_int_stat(h, "ep_bg_fetched"), "Expected bg_fetched to be 1");
    checkeq(1,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "Expected bg_meta_fetched to be 1");

    // store new key with some random metadata
    const size_t keylen = strlen("k3");
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    checkeq(cb::engine_errc::success,
            add_with_meta(h, "k3", keylen, nullptr, 0, Vbid(0), &itemMeta),
            "Expected to add item");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Set meta failed");

    check(get_meta(h, "k2"), "Get meta failed");
    checkeq(1, get_int_stat(h, "ep_bg_fetched"), "Expected bg_fetched to be 1");

    int expected_ep_bg_meta_fetched =
            get_bool_stat(h, "ep_bfilter_enabled") ? 1 : 2;
    checkeq(expected_ep_bg_meta_fetched,
            get_int_stat(h, "ep_bg_meta_fetched"),
            "bg_meta_fetched");
    return SUCCESS;
}

static enum test_result test_key_stats(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to store an item.");

    // set (k2,v2) in vbucket 1
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "k2",
                  "v2",
                  nullptr,
                  0,
                  Vbid(1)),
            "Failed to store an item.");

    auto* cookie = testHarness->create_cookie(h);

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey1, strlen(statkey1)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "key k2 1";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey2, strlen(statkey2)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_key_stats_eaccess(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to store an item.");

    // set (k2,v2) in vbucket 1
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "k2",
                  "v2",
                  nullptr,
                  0,
                  Vbid(1)),
            "Failed to store an item.");

    auto* cookie = testHarness->create_cookie(h);

    MockCookie::setCheckPrivilegeFunction(
            [](const CookieIface&,
               cb::rbac::Privilege,
               std::optional<ScopeID>,
               std::optional<CollectionID>) -> cb::rbac::PrivilegeAccess {
                return cb::rbac::PrivilegeAccessFail;
            });

    const auto ret = h->get_stats(*cookie, "key k1 0"sv, {}, add_stats);
    // Reset priv check function
    MockCookie::setCheckPrivilegeFunction({});
    checkeq(cb::engine_errc::no_access,
            ret,
            "Expected stats key to return no access");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vkey_stats(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket 1 state.");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed set vbucket 2 state.");
    check(set_vbucket_state(h, Vbid(3), vbucket_state_active),
          "Failed set vbucket 3 state.");
    check(set_vbucket_state(h, Vbid(4), vbucket_state_active),
          "Failed set vbucket 4 state.");

    wait_for_persisted_value(h, "k1", "v1");
    wait_for_persisted_value(h, "k2", "v2", Vbid(1));
    wait_for_persisted_value(h, "k3", "v3", Vbid(2));
    wait_for_persisted_value(h, "k4", "v4", Vbid(3));
    wait_for_persisted_value(h, "k5", "v5", Vbid(4));

    check(set_vbucket_state(h, Vbid(2), vbucket_state_replica),
          "Failed to set VB2 state.");
    check(set_vbucket_state(h, Vbid(3), vbucket_state_pending),
          "Failed to set VB3 state.");
    check(set_vbucket_state(h, Vbid(4), vbucket_state_dead),
          "Failed to set VB4 state.");

    auto* cookie = testHarness->create_cookie(h);

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "vkey k1 0";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey1, strlen(statkey1)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "vkey k2 1";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey2, strlen(statkey2)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k3" and vbucket "2"
    const char *statkey3 = "vkey k3 2";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey3, strlen(statkey3)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k4" and vbucket "3"
    const char *statkey4 = "vkey k4 3";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey4, strlen(statkey4)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k5" and vbucket "4"
    const char *statkey5 = "vkey k5 4";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey5, strlen(statkey5)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_warmup_conf(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    checkeq(100,
            get_int_stat(h, "ep_warmup_min_items_threshold"),
            "Incorrect initial warmup min items threshold.");
    checkeq(100,
            get_int_stat(h, "ep_warmup_min_memory_threshold"),
            "Incorrect initial warmup min memory threshold.");

    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h,
                      EngineParamCategory::Flush,
                      "warmup_min_items_threshold",
                      "a"),
            "Set warmup_min_items_threshold should have failed");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h,
                      EngineParamCategory::Flush,
                      "warmup_min_items_threshold",
                      "a"),
            "Set warmup_min_memory_threshold should have failed");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "warmup_min_items_threshold",
                      "80"),
            "Set warmup_min_items_threshold should have worked");
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "warmup_min_memory_threshold",
                      "80"),
            "Set warmup_min_memory_threshold should have worked");

    checkeq(80,
            get_int_stat(h, "ep_warmup_min_items_threshold"),
            "Incorrect smaller warmup min items threshold.");
    checkeq(80,
            get_int_stat(h, "ep_warmup_min_memory_threshold"),
            "Incorrect smaller warmup min memory threshold.");

    for (int i = 0; i < 100; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      key.str().c_str(),
                      "somevalue"),
                "Error setting.");
    }

    // Restart the server.
    std::string config(testHarness->get_current_testcase()->cfg);
    config = config + "warmup_min_memory_threshold=0";
    testHarness->reload_engine(&h, config.c_str(), true, false);

    wait_for_warmup_complete(h);

    const std::string eviction_policy =
            get_str_stat(h, "ep_item_eviction_policy");
    if (eviction_policy == "value_only") {
        checkeq(100,
                get_int_stat(h, "ep_warmup_key_count", "warmup"),
                "Expected 100 keys loaded after warmup");
    } else { // Full eviction mode
        checkeq(0,
                get_int_stat(h, "ep_warmup_key_count", "warmup"),
                "Expected 0 keys loaded after warmup");
    }

    checkeq(0,
            get_int_stat(h, "ep_warmup_value_count", "warmup"),
            "Expected 0 values loaded after warmup");

    return SUCCESS;
}

// Test that all the configuration parameters associated with the ItemPager,
// can be set.
static enum test_result test_itempager_conf(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "pager_active_vb_pcnt",
                      "50"),
            "Setting pager_active_vb_pcnt should have worked");
    checkeq(50,
            get_int_stat(h, "ep_pager_active_vb_pcnt"),
            "pager_active_vb_pcnt did not get set to the correct value");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "pager_sleep_time_ms",
                      "1000"),
            "Setting pager_sleep_time_ms should have worked");
    checkeq(1000,
            get_int_stat(h, "ep_pager_sleep_time_ms"),
            "pager_sleep_time_ms did not get set to the correct value");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "item_eviction_age_percentage",
                      "100"),
            "Set item_eviction_age_percentage should have worked");
    checkeq(100,
            get_int_stat(h, "ep_item_eviction_age_percentage"),
            "item_eviction_age_percentage did not get set to the correct "
            "value");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "item_eviction_freq_counter_age_threshold",
                      "10"),
            "Set item_eviction_freq_counter_age_threshold should have worked");
    checkeq(10,
            get_int_stat(h, "ep_item_eviction_freq_counter_age_threshold"),
            "item_eviction_freq_counter_age_threshold did not get set to the "
            "correct value");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "item_freq_decayer_chunk_duration",
                      "1000"),
            "Set item_freq_decayer_chunk_duration should have worked");
    checkeq(1000,
            get_int_stat(h, "ep_item_freq_decayer_chunk_duration"),
            "item_freq_decayer_chunk_duration did not get set to the correct "
            "value");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "item_freq_decayer_percent",
                      "100"),
            "Set item_freq_decayer_percent should have worked");
    checkeq(100,
            get_int_stat(h, "ep_item_freq_decayer_percent"),
            "item_freq_decayer_percent did not get set to the correct value");

    return SUCCESS;
}

static enum test_result test_pitr_conf(EngineIface* h) {
    // Check the max age
    checkeq(86400,
            get_int_stat(h, "ep_pitr_max_history_age"),
            "Incorrect default value for pitr_max_history_age");
    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Flush, "pitr_max_history_age", "1"),
            "Setting pitr_max_history_age should have worked");
    checkeq(1,
            get_int_stat(h, "ep_pitr_max_history_age"),
            "pitr_max_history_age did not get set to the correct value");
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "pitr_max_history_age",
                      "172800"),
            "Setting pitr_max_history_age should have worked");
    checkeq(172800,
            get_int_stat(h, "ep_pitr_max_history_age"),
            "pitr_max_history_age did not get set to the correct value");

    checkeq(cb::engine_errc::invalid_arguments,
            set_param(
                    h, EngineParamCategory::Flush, "pitr_max_history_age", "0"),
            "Setting pitr_max_history_age outside the legal range should not "
            "work");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h,
                      EngineParamCategory::Flush,
                      "pitr_max_history_age",
                      "172801"),
            "Setting pitr_max_history_age outside the legal range should not "
            "work");
    checkeq(172800,
            get_int_stat(h, "ep_pitr_max_history_age"),
            "pitr_max_history_age was changed when setting "
            "pitr_max_history_age to invalid value");

    checkeq(cb::engine_errc::invalid_arguments,
            set_param(
                    h, EngineParamCategory::Flush, "pitr_max_history_age", "0"),
            "Setting pitr_max_history_age outside the legal range should not "
            "work");
    checkeq(172800,
            get_int_stat(h, "ep_pitr_max_history_age"),
            "pitr_max_history_age was changed when setting "
            "pitr_max_history_age to invalid value");

    // Check the granularity
    checkeq(600,
            get_int_stat(h, "ep_pitr_granularity"),
            "Incorrect default value for ep_pitr_granularity");

    checkeq(cb::engine_errc::success,
            set_param(h, EngineParamCategory::Flush, "pitr_granularity", "1"),
            "Setting pitr_granularity should have worked");
    checkeq(1,
            get_int_stat(h, "ep_pitr_granularity"),
            "ep_pitr_granularity did not get set to the correct value");
    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Flush, "pitr_granularity", "18000"),
            "Setting pitr_granularity should have worked");
    checkeq(18000,
            get_int_stat(h, "ep_pitr_granularity"),
            "ep_pitr_granularity did not get set to the correct value");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Flush, "pitr_granularity", "0"),
            "Setting pitr_granularity outside the legal range should not work");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(
                    h, EngineParamCategory::Flush, "pitr_granularity", "18001"),
            "Setting pitr_granularity outside the legal range should not work");
    checkeq(18000,
            get_int_stat(h, "ep_pitr_granularity"),
            "pitr_max_history_age was changed when setting pitr_granularity to "
            "invalid value");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Flush, "pitr_granularity", "0"),
            "Setting pitr_granularity outside the legal range should not work");
    checkeq(18000,
            get_int_stat(h, "ep_pitr_granularity"),
            "pitr_max_history_age was changed when setting pitr_granularity to "
            "invalid value");

    // Check the master on off switch
    check(!get_bool_stat(h, "ep_pitr_enabled"),
          "Incorrect default value for ep_pitr_enabled");

    checkeq(cb::engine_errc::success,
            set_param(h, EngineParamCategory::Flush, "pitr_enabled", "true"),
            "Set pitr_enabled should have worked");
    checkeq(true,
            get_bool_stat(h, "ep_pitr_enabled"),
            "ep_pitr_enabled did not get set to the correct value");

    checkeq(cb::engine_errc::success,
            set_param(h, EngineParamCategory::Flush, "pitr_enabled", "false"),
            "Set pitr_enabled should have worked");
    checkeq(false,
            get_bool_stat(h, "ep_pitr_enabled"),
            "ep_pitr_enabled did not get set to the correct value");

    return SUCCESS;
}

static enum test_result test_bloomfilter_conf(EngineIface* h) {
    if (get_bool_stat(h, "ep_bfilter_enabled") == false) {
        checkeq(cb::engine_errc::success,
                set_param(h,
                          EngineParamCategory::Flush,
                          "bfilter_enabled",
                          "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, "ep_bfilter_enabled"),
          "Bloom filter wasn't enabled");

    checkeq(0.1f,
            get_float_stat(h, "ep_bfilter_residency_threshold"),
            "Incorrect initial bfilter_residency_threshold.");

    checkeq(cb::engine_errc::success,
            set_param(
                    h, EngineParamCategory::Flush, "bfilter_enabled", "false"),
            "Set bloomfilter_enabled should have worked.");
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "bfilter_residency_threshold",
                      "0.15"),
            "Set bfilter_residency_threshold should have worked.");

    checkeq(false, get_bool_stat(h, "ep_bfilter_enabled"),
            "Bloom filter should have been disabled.");
    checkeq(0.15f,
            get_float_stat(h, "ep_bfilter_residency_threshold"),
            "Incorrect bfilter_residency_threshold.");

    return SUCCESS;
}

static enum test_result test_bloomfilters(EngineIface* h) {
    if (get_bool_stat(h, "ep_bfilter_enabled") == false) {
        checkeq(cb::engine_errc::success,
                set_param(h,
                          EngineParamCategory::Flush,
                          "bfilter_enabled",
                          "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, "ep_bfilter_enabled"),
          "Bloom filter wasn't enabled");

    // Key is only present if bgOperations is non-zero.
    int num_read_attempts = get_int_stat_or_default(h, 0, "ep_bg_num_samples");

    // Ensure vbucket's bloom filter is enabled
    checkeq("ENABLED"s,
            get_str_stat(h, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    int i;

    // Insert 10 items.
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      key.str().c_str(),
                      "somevalue"),
                "Error setting.");
    }
    wait_for_flusher_to_settle(h);

    // Evict all 10 items.
    for (i = 0; i < 10; ++i) {
        std::stringstream key;
        key << "key-" << i;
        evict_key(h, key.str().c_str(), Vbid(0), "Ejected.");
    }
    wait_for_flusher_to_settle(h);

    // Ensure 10 items are non-resident.
    cb_assert(10 == get_int_stat(h, "ep_num_non_resident"));

    // Issue delete on first 5 items.
    for (i = 0; i < 5; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(cb::engine_errc::success,
                del(h, key.str().c_str(), 0, Vbid(0)),
                "Failed remove with value.");
    }
    wait_for_flusher_to_settle(h);

    // Ensure that there are 5 non-resident items
    cb_assert(5 == get_int_stat(h, "ep_num_non_resident"));
    cb_assert(5 == get_int_stat(h, "curr_items"));

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    std::chrono::microseconds sleepTime{128};

    if (eviction_policy == "value_only") {  // VALUE-ONLY EVICTION MODE

        checkeq(5,
                get_int_stat(
                        h, "vb_0:bloom_filter_key_count", "vbucket-details 0"),
                "Unexpected no. of keys in bloom filter");

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, 0, "ep_bg_num_samples"),
                "Expected bgFetch attempts to remain unchanged");

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, key.str().c_str()), "Get meta failed");
        }

        // GetMeta would cause bgFetches as bloomfilter contains
        // the deleted items.
        checkeq(num_read_attempts + 5,
                get_int_stat(h, "ep_bg_num_samples"),
                "Expected bgFetch attempts to increase by five");

        // Run compaction, with drop_deletes
        compact_db(h, Vbid(0), 15, 15, 1);
        while (get_int_stat(h, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; ++i) {
            std::stringstream key;
            key << "key-" << i;
            check(get_meta(h, key.str().c_str()), "Get meta failed");
        }
        checkeq(num_read_attempts + 5,
                get_int_stat(h, "ep_bg_num_samples"),
                "Expected bgFetch attempts to stay as before");

    } else {                                // FULL EVICTION MODE

        checkeq(10,
                get_int_stat(
                        h, "vb_0:bloom_filter_key_count", "vbucket-details 0"),
                "Unexpected no. of keys in bloom filter");

        // Because of issuing deletes on non-resident items
        checkeq(num_read_attempts + 5,
                get_int_stat(h, "ep_bg_num_samples"),
                "Expected bgFetch attempts to increase by five, after deletes");

        // Run compaction, with drop_deletes, to exclude deleted items
        // from bloomfilter.
        compact_db(h, Vbid(0), 15, 15, 1);
        while (get_int_stat(h, "ep_pending_compactions") != 0) {
            decayingSleep(&sleepTime);
        }

        for (i = 0; i < 5; i++) {
            std::stringstream key;
            key << "key-" << i;
            checkeq(cb::engine_errc::no_such_key,
                    get(h, nullptr, key.str(), Vbid(0)).first,
                    "Unable to get stored item");
        }
        // + 6 because last delete is not purged by the compactor
        checkeq(num_read_attempts + 6,
                get_int_stat(h, "ep_bg_num_samples"),
                "Expected bgFetch attempts to stay as before");
    }

    return SUCCESS;
}

static enum test_result test_bloomfilters_with_store_apis(EngineIface* h) {
    if (get_bool_stat(h, "ep_bfilter_enabled") == false) {
        checkeq(cb::engine_errc::success,
                set_param(h,
                          EngineParamCategory::Flush,
                          "bfilter_enabled",
                          "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, "ep_bfilter_enabled"),
          "Bloom filter wasn't enabled");

    int num_read_attempts = get_int_stat_or_default(h, 0, "ep_bg_num_samples");

    // Ensure vbucket's bloom filter is enabled
    checkeq("ENABLED"s,
            get_str_stat(h, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    for (int i = 0; i < 1000; i++) {
        std::stringstream key;
        key << "key-" << i;
        check(!get_meta(h, key.str().c_str()), "Get meta should fail.");
    }

    checkeq(num_read_attempts,
            get_int_stat_or_default(h, 0, "ep_bg_num_samples"),
            "Expected no bgFetch attempts");

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;

    if (eviction_policy == "full_eviction") {  // FULL EVICTION MODE
        // Set with Meta
        int j;
        for (j = 0; j < 10; j++) {
            // init some random metadata
            ItemMetaData itm_meta;
            itm_meta.revSeqno = 10;
            itm_meta.cas = 0xdeadbeef;
            itm_meta.exptime = time(nullptr) + 300;
            itm_meta.flags = 0xdeadbeef;

            const std::string key = "swm-" + std::to_string(j);
            checkeq(cb::engine_errc::success,
                    set_with_meta(h,
                                  key.data(),
                                  key.size(),
                                  "somevalue",
                                  9,
                                  Vbid(0),
                                  &itm_meta,
                                  0),
                    "Expected to store item");
        }

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, 0, "ep_bg_num_samples"),
                "Expected no bgFetch attempts");

        // Add
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "add-" << j;

            checkeq(cb::engine_errc::success,
                    store(h,
                          nullptr,
                          StoreSemantics::Add,
                          key.str().c_str(),
                          "newvalue"),
                    "Failed to add value again.");
        }

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, 0, "ep_bg_num_samples"),
                "Expected no bgFetch attempts");

        // Delete
        for (j = 0; j < 10; j++) {
            std::stringstream key;
            key << "del-" << j;
            checkeq(cb::engine_errc::no_such_key,
                    del(h, key.str().c_str(), 0, Vbid(0)),
                    "Failed remove with value.");
        }

        checkeq(num_read_attempts,
                get_int_stat_or_default(h, 0, "ep_bg_num_samples"),
                "Expected no bgFetch attempts");
    }

    return SUCCESS;
}

static enum test_result test_bloomfilter_delete_plus_set_scenario(
        EngineIface* h) {
    if (get_bool_stat(h, "ep_bfilter_enabled") == false) {
        checkeq(cb::engine_errc::success,
                set_param(h,
                          EngineParamCategory::Flush,
                          "bfilter_enabled",
                          "true"),
                "Set bloomfilter_enabled should have worked");
    }
    check(get_bool_stat(h, "ep_bfilter_enabled"),
          "Bloom filter wasn't enabled");

    // Ensure vbucket's bloom filter is enabled
    checkeq("ENABLED"s,
            get_str_stat(h, "vb_0:bloom_filter", "vbucket-details 0"),
            "Vbucket 0's bloom filter wasn't enabled upon setup!");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to fail to store an item.");

    wait_for_flusher_to_settle(h);
    int num_writes = get_int_stat(h, "rw_0:io_num_write", "kvstore");
    int num_persisted = get_int_stat(h, "ep_total_persisted");
    cb_assert(num_writes == 1 && num_persisted == 1);

    checkeq(cb::engine_errc::success,
            del(h, "k1", 0, Vbid(0)),
            "Failed remove with value.");
    stop_persistence(h);
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "k1",
                  "v2",
                  nullptr,
                  0,
                  Vbid(0)),
            "Failed to fail to store an item.");
    int key_count =
            get_int_stat(h, "vb_0:bloom_filter_key_count", "vbucket-details 0");

    if (key_count == 0) {
        checkge(2, get_int_stat(h, "rw_0:io_num_write", "kvstore"),
              "Unexpected number of writes");
        start_persistence(h);
        wait_for_flusher_to_settle(h);
        checkeq(0,
                get_int_stat(
                        h, "vb_0:bloom_filter_key_count", "vbucket-details 0"),
                "Unexpected number of keys in bloomfilter");
    } else {
        cb_assert(key_count == 1);
        checkeq(2,
                get_int_stat(h, "rw_0:io_num_write", "kvstore"),
                "Unexpected number of writes");
        start_persistence(h);
        wait_for_flusher_to_settle(h);
        checkeq(1,
                get_int_stat(
                        h, "vb_0:bloom_filter_key_count", "vbucket-details 0"),
                "Unexpected number of keys in bloomfilter");
    }

    return SUCCESS;
}

static enum test_result test_datatype(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_datatype_support(cookie, true);

    ItemIface* itm = nullptr;
    const std::string key(R"({"foo":"bar"})");
    const auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    uint64_t cas = 0;
    std::string value("x");
    checkeq(cb::engine_errc::success,
            storeCasOut(h, nullptr, Vbid(0), key, value, datatype, itm, cas),
            "Expected set to succeed");

    auto ret = get(h, cookie, key, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Unable to get stored item");

    item_info info;
    h->get_item_info(*ret.second.get(), info);
    checkeq(PROTOCOL_BINARY_DATATYPE_JSON, info.datatype, "Invalid datatype");

    const char* key1 = "foo";
    const char* val1 = R"({"foo1":"bar1"})";
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = info.cas;
    itm_meta.exptime = info.exptime;
    itm_meta.flags = info.flags;
    checkeq(cb::engine_errc::success,
            set_with_meta(h,
                          key1,
                          strlen(key1),
                          val1,
                          strlen(val1),
                          Vbid(0),
                          &itm_meta,
                          last_cas,
                          0,
                          info.datatype,
                          cookie),
            "Expected to store item");

    ret = get(h, cookie, key1, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Unable to get stored item");

    h->get_item_info(*ret.second.get(), info);
    checkeq(PROTOCOL_BINARY_DATATYPE_JSON,
            info.datatype,
            "Invalid datatype, when setWithMeta");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_datatype_with_unknown_command(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    testHarness->set_datatype_support(cookie, true);
    const char* key = "foo";
    const char* val = R"({"foo":"bar"})";
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;

    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0x1;
    itm_meta.exptime = 0;
    itm_meta.flags = 0;

    //SET_WITH_META
    checkeq(cb::engine_errc::success,
            set_with_meta(h,
                          key,
                          strlen(key),
                          val,
                          strlen(val),
                          Vbid(0),
                          &itm_meta,
                          0,
                          0,
                          datatype,
                          cookie),
            "Expected to store item");

    auto ret = get(h, cookie, key, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Unable to get stored item");

    item_info info;
    h->get_item_info(*ret.second.get(), info);
    checkeq(PROTOCOL_BINARY_DATATYPE_JSON,
            info.datatype,
            "Invalid datatype, when setWithMeta");

    //SET_RETURN_META
    checkeq(cb::engine_errc::success,
            set_ret_meta(h,
                         "foo1",
                         4,
                         val,
                         strlen(val),
                         Vbid(0),
                         0,
                         0,
                         0,
                         datatype,
                         cookie),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected set returing meta to succeed");
    checkeq(PROTOCOL_BINARY_DATATYPE_JSON,
            last_datatype.load(),
            "Invalid datatype, when set_return_meta");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_access_scanner_settings(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        // Access scanner n/a without warmup.
        return SKIPPED;
    }

    // Create a unique access log path by combining with the db path.
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string dbname = vals.find("ep_dbname")->second;

    const auto alog_path = std::string("alog_path=") + dbname +
                           cb::io::DirectorySeparator + "access.log";
    std::string newconfig =
            std::string(testHarness->get_current_testcase()->cfg) + alog_path;

    testHarness->reload_engine(&h, newconfig.c_str(), true, false);

    wait_for_warmup_complete(h);

    std::string err_msg;
    // Check access scanner is enabled and alog_task_time is at default
    checkeq(true,
            get_bool_stat(h, "ep_access_scanner_enabled"),
            "Expected access scanner to be enabled");
    cb_assert(get_int_stat(h, "ep_alog_task_time") == 2);

    // Ensure access_scanner_task_time is what its expected to be.
    // Need to wait until the AccessScanner task has been setup.
    wait_for_stat_change(
            h, "ep_access_scanner_task_time", std::string{"NOT_SCHEDULED"});

    std::string str = get_str_stat(h, "ep_access_scanner_task_time");
    std::string expected_time = "02:00";
    err_msg.assign("Initial time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update alog_task_time and ensure the update is successful
    expected_time = "05:00";

    // [MB-24422] we need to set this multiple times as the change listeners
    //  may not have been initialized at the time of call
    repeat_till_true([&]() {
        set_param(h, EngineParamCategory::Flush, "alog_task_time", "5");
        str = get_str_stat(h, "ep_access_scanner_task_time");
        return (0 == str.substr(11, 5).compare(expected_time));
    });

    err_msg.assign("Updated time incorrect, expect: " +
                   expected_time + ", actual: " + str.substr(11, 5));
    checkeq(0, str.substr(11, 5).compare(expected_time), err_msg.c_str());

    // Update alog_sleep_time by 10 mins and ensure the update is successful.
    const std::chrono::minutes update_by{10};
    std::string targetTaskTime1{make_time_string(std::chrono::system_clock::now() +
                                                 update_by)};

    set_param(h,
              EngineParamCategory::Flush,
              "alog_sleep_time",
              std::to_string(update_by.count()).c_str());
    str = get_str_stat(h, "ep_access_scanner_task_time");

    // Recalculate now() + 10mins as upper bound on when the task should be
    // scheduled.
    std::string targetTaskTime2{make_time_string(std::chrono::system_clock::now() +
                                                 update_by)};

    // ep_access_scanner_task_time should fall within the range of
    // targetTaskTime1 and targetTaskTime2
    err_msg.assign("Unexpected task time range, expect: " +
                   targetTaskTime1 + " <= " + str + " <= " + targetTaskTime2);
    checkle(targetTaskTime1, str, err_msg.c_str());
    checkle(str, targetTaskTime2, err_msg.c_str());

    return SUCCESS;
}

static enum test_result test_access_scanner(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        // Access scanner not applicable without warmup.
        return SKIPPED;
    }

    // Create a unique access log path by combining with the db path.
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    const auto dbname = vals.find("ep_dbname")->second;

    const auto alog_path = std::string("alog_path=") + dbname +
                           cb::io::DirectorySeparator + "access.log";

    /* We do not want the access scanner task to be running while we initiate it
       explicitly below. Hence set the alog_task_time to about 1 ~ 2 hours
       from now */
    const time_t now = time(nullptr);
    struct tm tm_now;
    cb_gmtime_r(&now, &tm_now);
    const auto two_hours_hence = (tm_now.tm_hour + 2) % 24;

    const auto alog_task_time = std::string("alog_task_time=") +
            std::to_string(two_hours_hence);

    const auto newconfig =
            std::string(testHarness->get_current_testcase()->cfg) + alog_path +
            ";" + alog_task_time;

    testHarness->reload_engine(&h, newconfig.c_str(), true, false);

    wait_for_warmup_complete(h);

    /* Check that alog_task_time was correctly updated. */
    checkeq(get_int_stat(h, "ep_alog_task_time"),
            two_hours_hence,
            "Failed to set alog_task_time to 2 hours in the future");

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string name = vals.find("ep_alog_path")->second;

    /* Check access scanner is enabled */
    checkeq(true,
            get_bool_stat(h, "ep_access_scanner_enabled"),
            "Access scanner task not enabled by default. Check test config");

    const int num_shards =
            get_int_stat(h, "ep_workload:num_shards", "workload");
    name = name + ".0";
    std::string prev(name + ".old");

    /* Get the resident ratio down to below 95% - point at which access.log
     * generation occurs.
     */
    int num_items = 0;
    // Size chosen to create ~2000 items (i.e. 2x more than we sanity-check below)
    // with the given max_size for this test.
    const std::string value(2000, 'x');
    while (true) {
        // Gathering stats on every store is expensive, just check every 100 iterations
        if ((num_items % 100) == 0) {
            if (get_int_stat(h, "vb_active_perc_mem_resident") < 94) {
                break;
            }
        }

        std::string key("key" + std::to_string(num_items));
        cb::engine_errc ret = store(
                h, nullptr, StoreSemantics::Set, key.c_str(), value.c_str());
        switch (ret) {
        case cb::engine_errc::success:
            num_items++;
            break;

        case cb::engine_errc::no_memory:
        case cb::engine_errc::temporary_failure:
            // Returned when at high watermark; simply retry the op.
            break;

        default:
            fprintf(stderr,
                    "test_access_scanner: Unexpected result from store(): %s\n",
                    cb::to_string(ret).c_str());
            abort();
        }

    }

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    if (num_items < 1000) {
        std::cerr << "Error: test_access_scanner: "
            "expected at least 1000 items after filling vbucket, "
            "but only have " << num_items << ". "
            "Check max_size setting for test." << std::endl;
        return FAIL;
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong number of items");
    int num_non_resident = get_int_stat(h, "vb_active_num_non_resident");
    checkge(num_non_resident, num_items * 6 / 100,
            "Expected num_non_resident to be at least 6% of total items");

    /* Run access scanner task once and expect it to generate access log */
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "access_scanner_run",
                      "true"),
            "Failed to trigger access scanner");

    // Wait for the number of runs to equal the number of shards.
    wait_for_stat_to_be(h, "ep_num_access_scanner_runs", num_shards);

    /* This time since resident ratio is < 95% access log should be generated */
    check(cb::io::isFile(name),
          (std::string("access log file (") + name +
           ") should exist (got errno:" + std::to_string(errno))
                  .c_str());

    /* Increase resident ratio by deleting items */
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(0)),
            "Expected success");
    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set VB0 state.");

    /* Run access scanner task once */
    const int access_scanner_skips =
            get_int_stat(h, "ep_num_access_scanner_skips");
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "access_scanner_run",
                      "true"),
            "Failed to trigger access scanner");
    wait_for_stat_to_be(h,
                        "ep_num_access_scanner_skips",
                        access_scanner_skips + num_shards);

    // MB-51240: expect another forced run to still increase the skips
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "access_scanner_run",
                      "true"),
            "Failed to trigger access scanner");
    wait_for_stat_to_be(h,
                        "ep_num_access_scanner_skips",
                        access_scanner_skips + (2 * num_shards));

    /* Access log files should be removed because resident ratio > 95% */
    check(!cb::io::isFile(prev), ".old access log file should not exist");
    check(!cb::io::isFile(name), "access log file should not exist");

    return SUCCESS;
}

static enum test_result test_set_param_message(EngineIface* h) {
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Flush, "alog_task_time", "50"),
            "Expected an invalid value error for an out of bounds "
            "alog_task_time");
    check(std::string("Validation Error").compare(last_body),
          "Expected a "
          "validation error in the response body");
    return SUCCESS;
}

static enum test_result test_warmup_stats(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set VB0 state.");
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set VB1 state.");

    for (int i = 0; i < 5000; ++i) {
        std::stringstream key;
        key << "key-" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      key.str().c_str(),
                      "somevalue"),
                "Error setting.");
    }

    // Restart the server.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    const auto warmup_stats = get_all_stats(h, "warmup");

    // Check all expected warmup stats exists.
    const char* warmup_keys[] = { "ep_warmup_thread",
                                  "ep_warmup_value_count",
                                  "ep_warmup_key_count",
                                  "ep_warmup_dups",
                                  "ep_warmup_oom",
                                  "ep_warmup_time"};
    for (const auto* key : warmup_keys) {
        check(warmup_stats.find(key) != warmup_stats.end(),
              (std::string("Found no ") + key).c_str());
    }

    std::string warmup_time = warmup_stats.at("ep_warmup_time");
    cb_assert(std::stoi(warmup_time) > 0);

    const auto prev_vb_stats = get_all_stats(h, "prev-vbucket");

    check(prev_vb_stats.find("vb_0") != prev_vb_stats.end(),
          "Found no previous state for VB0");
    check(prev_vb_stats.find("vb_1") != prev_vb_stats.end(),
          "Found no previous state for VB1");

    checkeq(std::string("active"), prev_vb_stats.at("vb_0"),
            "Unexpected stats for vb 0");
    checkeq(std::string("replica"), prev_vb_stats.at("vb_1"),
            "Unexpected stats for vb 1");

    const auto vb_details_stats = get_all_stats(h, "vbucket-details");
    checkeq(5000, std::stoi(vb_details_stats.at("vb_0:num_items")),
            "Unexpected item count for vb 0");
    checkeq(0, std::stoi(vb_details_stats.at("vb_1:num_items")),
            "Unexpected item count for vb 1");

    return SUCCESS;
}

static enum test_result test_warmup_with_threshold(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed set vbucket 1 state.");
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket 2 state.");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed set vbucket 3 state.");
    check(set_vbucket_state(h, Vbid(3), vbucket_state_active),
          "Failed set vbucket 4 state.");

    for (int i = 0; i < 10000; ++i) {
        std::stringstream key;
        key << "key+" << i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      key.str().c_str(),
                      "somevalue",
                      nullptr,
                      0,
                      Vbid(i % 4)),
                "Error setting.");
    }

    // Restart the server.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    checkeq(1,
            get_int_stat(h, "ep_warmup_min_item_threshold", "warmup"),
            "Unable to set warmup_min_item_threshold to 1%");

    const std::string policy = get_str_stat(h, "ep_item_eviction_policy");

    if (policy == "full_eviction") {
        checkeq(get_int_stat(h, "ep_warmup_key_count", "warmup"),
                get_int_stat(h, "ep_warmup_value_count", "warmup"),
                "Warmed up key count didn't match warmed up value count");
    } else {
        checkeq(10000,
                get_int_stat(h, "ep_warmup_key_count", "warmup"),
                "Warmup didn't warmup all keys");
    }
    check(get_int_stat(h, "ep_warmup_value_count", "warmup") <= 110,
          "Warmed up value count found to be greater than 1%");

    cb_assert(get_int_stat(h, "ep_warmup_time", "warmup") > 0);

    return SUCCESS;
}

// Test that when a bucket is populated in full-eviction mode; but
// later changed to value-eviction mode, if there isn't sufficient
// memory to load all item metadata we return NOMEM to the
// ENABLE_TRAFFIC command.
static enum test_result test_warmup_oom(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    // Requires memory tracking for us to be able to correctly monitor memory
    // usage.
    if (!get_bool_stat(h, "ep_mem_tracker_enabled")) {
        return SKIPPED;
    }

    write_items(
            h, 20000, 0, "superlongnameofkey1234567890123456789012345678902");

    wait_for_flusher_to_settle(h);

    std::string config(testHarness->get_current_testcase()->cfg);
    config = config + "max_size=2097152;item_eviction_policy=value_only";

    testHarness->reload_engine(&h, config.c_str(), true, false);

    wait_for_warmup_complete(h);

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    auto pkt = createPacket(cb::mcbp::ClientOpcode::EnableTraffic);
    checkeq(cb::engine_errc::no_memory,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Data traffic command should have failed with enomem");

    return SUCCESS;
}

static enum test_result test_cbd_225(EngineIface* h) {
    // get engine startup token
    time_t token1 = get_int_stat(h, "ep_startup_time");
    checkne(time_t{0}, token1, "Expected non-zero startup token");

    // store some random data
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to fail to store an item.");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k2", "v2"),
            "Failed to fail to store an item.");
    wait_for_flusher_to_settle(h);

    // check token again, which should be the same as before
    time_t token2 = get_int_stat(h, "ep_startup_time");
    checkeq(token1, token2, "Expected the same startup token");

    // reload the engine
    testHarness->time_travel(10);
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    // check token, this time we should get a different one
    time_t token3 = get_int_stat(h, "ep_startup_time");
    checkne(token3, token1, "Expected a different startup token");

    return SUCCESS;
}

static enum test_result test_workload_stats(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, "workload"sv, {}, add_stats),
            "Falied to get workload stats");
    testHarness->destroy_cookie(cookie);
    int num_read_threads =
            get_int_stat(h, "ep_workload:num_readers", "workload");
    int num_write_threads =
            get_int_stat(h, "ep_workload:num_writers", "workload");
    int num_auxio_threads =
            get_int_stat(h, "ep_workload:num_auxio", "workload");
    int num_nonio_threads =
            get_int_stat(h, "ep_workload:num_nonio", "workload");
    int num_shards = get_int_stat(h, "ep_workload:num_shards", "workload");
    checkeq(10, num_read_threads, "Incorrect number of readers");
    checkeq(4, num_write_threads, "Incorrect number of writers");
    checkeq(8, num_auxio_threads, "Incorrect number of auxio threads");
    check(num_nonio_threads > 1 && num_nonio_threads <= 8,
          "Incorrect number of nonio threads");
    checkeq(5, num_shards, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_max_workload_stats(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, "workload"sv, {}, add_stats),
            "Failed to get workload stats");
    testHarness->destroy_cookie(cookie);
    int num_read_threads =
            get_int_stat(h, "ep_workload:num_readers", "workload");
    int num_write_threads =
            get_int_stat(h, "ep_workload:num_writers", "workload");
    int num_auxio_threads =
            get_int_stat(h, "ep_workload:num_auxio", "workload");
    int num_nonio_threads =
            get_int_stat(h, "ep_workload:num_nonio", "workload");
    int num_shards = get_int_stat(h, "ep_workload:num_shards", "workload");
    checkeq(14, num_read_threads, "Incorrect number of readers");
    checkeq(4, num_write_threads, "Incorrect number of writers");

    checkeq(1, num_auxio_threads, "Incorrect number of auxio threads");// config
    checkeq(4, num_nonio_threads, "Incorrect number of nonio threads");// config
    checkeq(5, num_shards, "Incorrect number of shards");
    return SUCCESS;
}

static enum test_result test_worker_stats(EngineIface* h) {
    if (isFollyExecutorPool(h)) {
        // FollyExecutorPool doesn't support 'worker' stats.
        return SKIPPED;
    }
    checkeq(cb::engine_errc::success,
            get_stats(h, "dispatcher"sv, {}, add_stats),
            "Failed to get worker stats");

    std::set<std::string> tasklist;
    tasklist.insert("Running a flusher loop");
    tasklist.insert("Updating stat snapshot on disk");
    tasklist.insert("Batching background fetch");
    tasklist.insert("Fetching item from disk for vkey stat");
    tasklist.insert("Fetching item from disk");
    tasklist.insert("Generating access log");
    tasklist.insert("Snapshotting vbucket states");
    tasklist.insert("Warmup - initialize");
    tasklist.insert("Warmup - creating vbuckets");
    tasklist.insert("Warmup - estimate item count");
    tasklist.insert("Warmup - key dump");
    tasklist.insert("Warmup - check for access log");
    tasklist.insert("Warmup - loading access log");
    tasklist.insert("Warmup - loading KV Pairs");
    tasklist.insert("Warmup - loading data");
    tasklist.insert("Warmup - completion");
    tasklist.insert("Not currently running any task");

    std::set<std::string> statelist;
    statelist.insert("creating");
    statelist.insert("running");
    statelist.insert("waiting");
    statelist.insert("sleeping");
    statelist.insert("shutdown");
    statelist.insert("dead");

    for (std::string name : {"worker_0", "worker_1"}) {
        std::string task = vals["Reader_" + name + ":task"];
        task = task.substr(0, task.find(":"));

        std::string state = vals["Reader_" + name + ":state"];

        check(tasklist.find(task) != tasklist.end(),
              (name + "'s Current task incorrect: " + task).c_str());
        check(statelist.find(state) != statelist.end(),
              (name + "'s state incorrect: " + state).c_str());
    }

    checkeq(15,
            get_int_stat(h, "ep_num_workers"), // cannot spawn less
            "Incorrect number of threads spawned");
    return SUCCESS;
}

static enum test_result test_all_keys_api(EngineIface* h) {
    std::vector<std::string> keys;
    const int start_key_idx = 10, del_key_idx = 12, num_keys = 5,
              total_keys = 100;

    for (uint32_t i = 0; i < total_keys; ++i) {
        std::string key("key_" + std::to_string(i));
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemIface* itm;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      &itm,
                      0,
                      Vbid(0)),
                "Failed to store a value");
        h->release(*itm);
    }
    std::string del_key("key_" + std::to_string(del_key_idx));
    checkeq(cb::engine_errc::success,
            del(h, del_key.c_str(), 0, Vbid(0)),
            "Failed to delete key");
    wait_for_flusher_to_settle(h);
    checkeq(total_keys - 1,
            get_int_stat(h, "curr_items"),
            "Item count mismatch");

    std::string start_key("key_" + std::to_string(start_key_idx));
    const uint16_t keylen = start_key.length();
    uint32_t count = htonl(num_keys);

    auto pkt1 = createPacket(cb::mcbp::ClientOpcode::GetKeys,
                             Vbid(0),
                             0,
                             {reinterpret_cast<char*>(&count), sizeof(count)},
                             start_key);

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    if (isPersistentBucket(h)) {
        checkeq(cb::engine_errc::success,
                h->unknown_command(cookie.get(), *pkt1, add_response),
                "Failed to get all_keys, sort: ascending");
    } else {
        /* We intend to support cb::mcbp::ClientOpcode::GetKeys in ephemeral
           buckets in the future */
        checkeq(cb::engine_errc::not_supported,
                h->unknown_command(cookie.get(), *pkt1, add_response),
                "Should return not supported");
        return SUCCESS;
    }

    /* Check the keys. */
    size_t offset = 0;
    /* Since we have one deleted key, we must go till num_keys + 1 */
    for (size_t i = 0; i < num_keys + 1; ++i) {
        if (del_key_idx == start_key_idx + i) {
            continue;
        }
        uint16_t len;
        memcpy(&len, last_body.data() + offset, sizeof(uint16_t));
        len = ntohs(len);
        checkeq(keylen, len, "Key length mismatch in all_docs response");
        std::string key("key_" + std::to_string(start_key_idx + i));
        offset += sizeof(uint16_t);
        checkeq(0, last_body.compare(offset, keylen, key.c_str()),
                "Key mismatch in all_keys response");
        offset += keylen;
    }

    return SUCCESS;
}

static enum test_result test_all_keys_api_during_bucket_creation(
        EngineIface* h) {
    uint32_t count = htonl(5);
    const char key[] = "key_10";

    auto pkt1 = createPacket(cb::mcbp::ClientOpcode::GetKeys,
                             Vbid(1),
                             0,
                             {reinterpret_cast<char*>(&count), sizeof(count)},
                             {key, strlen(key)});

    stop_persistence(h);
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket 1 state.");

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    if (isPersistentBucket(h)) {
        checkeq(cb::engine_errc::success,
                h->unknown_command(cookie.get(), *pkt1, add_response),
                "Unexpected return code from all_keys_api");
    } else {
        /* We intend to support cb::mcbp::ClientOpcode::GetKeys in ephemeral
           buckets in the future */
        checkeq(cb::engine_errc::not_supported,
                h->unknown_command(cookie.get(), *pkt1, add_response),
                "Should return not supported");
        return SUCCESS;
    }

    start_persistence(h);

    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Unexpected response status");

    return SUCCESS;
}

static enum test_result test_curr_items_add_set(EngineIface* h) {
    // Verify initial case.
    verify_curr_items(h, 0, "init");

    const auto initial_enqueued = get_int_stat(h, "ep_total_enqueued");

    // Verify set and add case
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "k1", "v1"),
            "Failed to fail to store an item.");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k2", "v2"),
            "Failed to fail to store an item.");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k3", "v3"),
            "Failed to fail to store an item.");
    if (isPersistentBucket(h) && is_full_eviction(h)) {
        // MB-21957: FE mode - curr_items is only valid once we flush documents
        wait_for_flusher_to_settle(h);
    }
    verify_curr_items(h, 3, "three items stored");
    checkeq(initial_enqueued + 3,
            get_int_stat(h, "ep_total_enqueued"),
            "Expected total_enqueued to increase by 3 after 3 new items");

    return SUCCESS;
}

static enum test_result test_curr_items_delete(EngineIface* h) {
    // Verify initial case.
    verify_curr_items(h, 0, "init");

    // Store some items
    write_items(h, 3);
    wait_for_flusher_to_settle(h);

    // Verify delete case.
    checkeq(cb::engine_errc::success,
            del(h, "key1", 0, Vbid(0)),
            "Failed remove with value.");

    wait_for_stat_change(h, "curr_items", 3);
    verify_curr_items(h, 2, "one item deleted - persisted");

    return SUCCESS;
}

static enum test_result test_curr_items_dead(EngineIface* h) {
    // Verify initial case.
    verify_curr_items(h, 0, "init");

    // Store some items
    write_items(h, 3);
    wait_for_flusher_to_settle(h);

    // Verify dead vbucket case.
    check(set_vbucket_state(h, Vbid(0), vbucket_state_dead),
          "Failed set vbucket 0 state to dead");

    verify_curr_items(h, 0, "dead vbucket");
    checkeq(0,
            get_int_stat(h, "curr_items_tot"),
            "Expected curr_items_tot to be 0 with a dead vbucket");

    // Then resurrect.
    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed set vbucket 0 state to active");

    verify_curr_items(h, 3, "resurrected vbucket");

    // Now completely delete it.
    check(set_vbucket_state(h, Vbid(0), vbucket_state_dead),
          "Failed set vbucket 0 state to dead (2)");
    wait_for_flusher_to_settle(h);
    checkeq(uint64_t(0),
            get_stat<uint64_t>(h, "ep_queue_size"),
            "ep_queue_size is not zero after setting to dead (2)");

    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(0)),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Expected success deleting vbucket.");
    verify_curr_items(h, 0, "del vbucket");
    checkeq(0,
            get_int_stat(h, "curr_items_tot"),
            "Expected curr_items_tot to be 0 after deleting a vbucket");

    return SUCCESS;
}

static enum test_result test_value_eviction(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    reset_stats(h);

    checkeq(0,
            get_int_stat(h, "ep_num_value_ejects"),
            "Expected reset stats to set ep_num_value_ejects to zero");
    checkeq(0,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected all items to be resident");
    checkeq(0,
            get_int_stat(h, "vb_active_num_non_resident"),
            "Expected all active vbucket items to be resident");

    stop_persistence(h);
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to fail to store an item.");
    evict_key(h, "k1", Vbid(0), "Can't eject: Dirty object.", true);
    start_persistence(h);
    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "k2",
                  "v2",
                  nullptr,
                  0,
                  Vbid(1)),
            "Failed to fail to store an item.");
    evict_key(h, "k2", Vbid(1), "Can't eject: Dirty object.", true);
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    evict_key(h, "k1", Vbid(0), "Ejected.");
    evict_key(h, "k2", Vbid(1), "Ejected.");

    checkeq(2,
            get_int_stat(h, "vb_active_num_non_resident"),
            "Expected two non-resident items for active vbuckets");

    evict_key(h, "k1", Vbid(0), "Already ejected.");
    evict_key(h, "k2", Vbid(1), "Already ejected.");

    auto pkt = createPacket(cb::mcbp::ClientOpcode::EvictKey,
                            Vbid(0),
                            0,
                            {},
                            {"missing-key", 11});

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Failed to evict key.");

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    if (eviction_policy == "value_only") {
        checkeq(cb::mcbp::Status::KeyEnoent, last_status.load(),
                "expected the key to be missing...");
    } else {
        // Note that we simply return SUCCESS when EVICT_KEY is issued to
        // a non-resident or non-existent key with full eviction to avoid a disk lookup.
        checkeq(cb::mcbp::Status::Success, last_status.load(),
            "expected the success for evicting a non-existent key with full eviction");
    }

    reset_stats(h);
    checkeq(0,
            get_int_stat(h, "ep_num_value_ejects"),
            "Expected reset stats to set ep_num_value_ejects to zero");

    check_key_value(h, "k1", "v1", 2);
    checkeq(1,
            get_int_stat(h, "vb_active_num_non_resident"),
            "Expected only one active vbucket item to be non-resident");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    checkeq(0,
            get_int_stat(h, "vb_active_num_non_resident"),
            "Expected no non-resident items");

    return SUCCESS;
}

static enum test_result test_duplicate_items_disk(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      "value",
                      nullptr,
                      0,
                      Vbid(1)),
                "Failed to store a value");
    }
    wait_for_flusher_to_settle(h);

    // don't need to explicitly set the vbucket state to dead as this is
    // done as part of the vbucketDelete. See KVBucket::deleteVBucket
    int vb_del_num = get_int_stat(h, "ep_vbucket_del");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, Vbid(1)),
          "vbucket 1 was not missing after deleting it.");
    // wait for the deletion to successfully complete before setting the
    // vbucket state active (which creates the vbucket)
    wait_for_stat_change(h, "ep_vbucket_del", vb_del_num);

    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemIface* i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      &i,
                      0,
                      Vbid(1)),
                "Failed to store a value");
        h->release(*i);
    }
    wait_for_flusher_to_settle(h);

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");
    // Make sure that a key/value item is persisted correctly
    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, it->c_str(), Vbid(1), "Ejected.");
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, it->c_str(), it->data(), it->size(), Vbid(1));
    }
    checkeq(0,
            get_int_stat(h, "ep_warmup_dups"),
            "Expected no duplicate items from disk");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_golden(EngineIface* h) {
    // Check/grab initial state.
    const auto initial_enqueued = get_int_stat(h, "ep_total_enqueued");
    int itemsRemoved = get_int_stat(h, "ep_items_rm_from_checkpoints");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, "k1", "some value");
    testHarness->time_travel(65);
    wait_for_stat_change(h, "ep_items_rm_from_checkpoints", itemsRemoved);

    checkeq(0,
            get_int_stat(h, "ep_bg_fetched"),
            "Should start with zero bg fetches");
    checkeq((initial_enqueued + 1),
            get_int_stat(h, "ep_total_enqueued"),
            "Should have additional item enqueued after store");
    int kv_size = get_int_stat(h, "ep_kv_size");

    // Evict the data.
    evict_key(h, "k1");

    int kv_size2 = get_int_stat(h, "ep_kv_size");

    checkgt(kv_size, kv_size2, "kv_size should have decreased after eviction");

    // Reload the data.
    check_key_value(h, "k1", "some value", 10);

    int kv_size3 = get_int_stat(h, "ep_kv_size");

    checkeq(1,
            get_int_stat(h, "ep_bg_fetched"),
            "BG fetches should be one after reading an evicted key");
    checkeq((initial_enqueued + 1),
            get_int_stat(h, "ep_total_enqueued"),
            "Item should not be marked dirty after reading an evicted key");

    checkeq(kv_size, kv_size3,
            "kv_size should have returned to initial value after restoring evicted item");

    itemsRemoved = get_int_stat(h, "ep_items_rm_from_checkpoints");
    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, "ep_total_persisted");
    checkeq(cb::engine_errc::success,
            del(h, "k1", 0, Vbid(0)),
            "Failed remove with value.");
    wait_for_stat_change(h, "ep_total_persisted", numStored);
    testHarness->time_travel(65);
    wait_for_stat_change(h, "ep_items_rm_from_checkpoints", itemsRemoved);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_paged_rm(EngineIface* h) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, "ep_overhead");
    const auto initial_enqueued = get_int_stat(h, "ep_total_enqueued");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, "k1", "some value");
    checkeq(0,
            get_int_stat(h, "ep_bg_fetched"),
            "bg_fetched should initially be zero");
    checkeq(initial_enqueued + 1,
            get_int_stat(h, "ep_total_enqueued"),
            "Expected total_enqueued to increase by 1 after storing 1 value");
    checkge(get_int_stat(h, "ep_overhead"),
            overhead,
            "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, "k1");

    // Delete the value and make sure things return correctly.
    int itemsRemoved = get_int_stat(h, "ep_items_rm_from_checkpoints");
    int numStored = get_int_stat(h, "ep_total_persisted");
    checkeq(cb::engine_errc::success,
            del(h, "k1", 0, Vbid(0)),
            "Failed remove with value.");
    wait_for_stat_change(h, "ep_total_persisted", numStored);
    testHarness->time_travel(65);
    wait_for_stat_change(h, "ep_items_rm_from_checkpoints", itemsRemoved);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_update_paged_out(EngineIface* h) {
    wait_for_persisted_value(h, "k1", "some value");

    evict_key(h, "k1");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "new value"),
            "Failed to update an item.");

    check_key_value(h, "k1", "new value", 9);

    checkeq(0, get_int_stat(h, "ep_bg_fetched"), "bg fetched something");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_delete_paged_out(EngineIface* h) {
    wait_for_persisted_value(h, "k1", "some value");

    evict_key(h, "k1");

    checkeq(cb::engine_errc::success,
            del(h, "k1", 0, Vbid(0)),
            "Failed to delete.");

    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "k1"),
            "Expected miss.");

    checkeq(0,
            get_int_stat(h, "ep_bg_fetched"),
            "Unexpected bg_fetched after del/get");

    return SUCCESS;
}

extern "C" {
    static void bg_set_thread(void *arg) {
        auto* td(static_cast<ThreadData*>(arg));

        std::this_thread::sleep_for(
                std::chrono::microseconds(2600)); // Exacerbate race condition.

        checkeq(cb::engine_errc::success,
                store(td->h, nullptr, StoreSemantics::Set, "k1", "new value"),
                "Failed to update an item.");

        delete td;
    }

    static void bg_del_thread(void *arg) {
        auto *td(static_cast<ThreadData*>(arg));

        std::this_thread::sleep_for(
                std::chrono::microseconds(2600)); // Exacerbate race condition.

        checkeq(cb::engine_errc::success,
                del(td->h, "k1", 0, Vbid(0)),
                "Failed to delete.");

        delete td;
    }
}

static enum test_result test_disk_gt_ram_set_race(EngineIface* h) {
    wait_for_persisted_value(h, "k1", "some value");

    evict_key(h, "k1");

    cb_thread_t tid;
    if (cb_create_thread(&tid, bg_set_thread, new ThreadData(h), 0) != 0) {
        abort();
    }

    check_key_value(h, "k1", "new value", 9);

    // Should have bg_fetched, but discarded the old value.
    cb_assert(1 == get_int_stat(h, "ep_bg_fetched"));

    cb_assert(cb_join_thread(tid) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_rm_race(EngineIface* h) {
    wait_for_persisted_value(h, "k1", "some value");

    evict_key(h, "k1");

    cb_thread_t tid;
    if (cb_create_thread(&tid, bg_del_thread, new ThreadData(h), 0) != 0) {
        abort();
    }

    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "k1"),
            "Expected miss.");

    // Should have bg_fetched, but discarded the old value.
    cb_assert(1 == get_int_stat(h, "ep_bg_fetched"));

    cb_assert(cb_join_thread(tid) == 0);

    return SUCCESS;
}

static enum test_result test_kill9_bucket(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-0-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str()),
                "Failed to store a value");
    }

    // Last parameter indicates the force shutdown for the engine.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);

    keys.clear();
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-1-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        ItemIface* i;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      it->c_str(),
                      it->c_str(),
                      &i,
                      0,
                      Vbid(0)),
                "Failed to store a value");
        h->release(*i);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, it->c_str(), it->data(), it->size(), Vbid(0));
    }

    return SUCCESS;
}

static enum test_result test_revid(EngineIface* h) {
    ItemMetaData meta;
    for (uint64_t ii = 1; ii < 10; ++ii) {
        checkeq(cb::engine_errc::success,
                store(h, nullptr, StoreSemantics::Set, "test_revid", "foo"),
                "Failed to store a value");

        cb::EngineErrorMetadataPair erroMetaPair;
        check(get_meta(h, "test_revid", erroMetaPair), "Get meta failed");
        checkeq(ii, erroMetaPair.second.seqno, "Unexpected sequence number");
    }

    return SUCCESS;
}

static enum test_result test_regression_mb4314(EngineIface* h) {
    cb::EngineErrorMetadataPair errorMetaPair;
    check(!get_meta(h, "test_regression_mb4314", errorMetaPair),
          "Expected get_meta() to fail");

    ItemMetaData itm_meta(0xdeadbeef, 10, 0xdeadbeef, 0);
    checkeq(cb::engine_errc::success,
            set_with_meta(h,
                          "test_regression_mb4314",
                          22,
                          nullptr,
                          0,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected to store item");

    // Now try to read the item back:
    auto ret = get(h, nullptr, "test_regression_mb4314", Vbid(0));
    checkeq(cb::engine_errc::success,
            ret.first,
            "Expected to get the item back!");

    return SUCCESS;
}

static enum test_result test_mb3466(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    check(vals.find("mem_used") != vals.end(),
          "Expected \"mem_used\" to be returned");
    check(vals.find("bytes") != vals.end(),
          "Expected \"bytes\" to be returned");
    std::string memUsed = vals["mem_used"];
    std::string bytes = vals["bytes"];
    checkeq(memUsed, bytes,
          "Expected mem_used and bytes to have the same value");

    return SUCCESS;
}

static enum test_result test_observe_no_data(EngineIface* h) {
    std::map<std::string, Vbid> obskeys;
    checkeq(cb::engine_errc::success, observe(h, obskeys), "expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    return SUCCESS;
}

static enum test_result test_observe_seqno_basic_tests(EngineIface* h) {
    // Check observe seqno for vbucket with id 1
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    //Check the output when there is no data in the vbucket
    uint64_t vb_uuid = get_ull_stat(h, "vb_1:0:id", "failovers");
    uint64_t high_seqno = get_int_stat(h, "vb_1:high_seqno", "vbucket-seqno");
    checkeq(cb::engine_errc::success,
            observe_seqno(h, Vbid(1), vb_uuid),
            "Expected success");

    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Expected success");

    const auto bucket_type =
            isPersistentBucket(h) ? EPBucketType::EP : EPBucketType::Ephemeral;
    check_observe_seqno(
            false, bucket_type, 0, Vbid(1), vb_uuid, high_seqno, high_seqno);

    //Add some mutations and verify the output
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        // Set an item
        ItemIface* it = nullptr;
        uint64_t cas1;
        std::string value('x', 100);
        checkeq(cb::engine_errc::success,
                storeCasOut(h,
                            nullptr,
                            Vbid(1),
                            "key" + std::to_string(j),
                            value,
                            PROTOCOL_BINARY_RAW_BYTES,
                            it,
                            cas1),
                "Expected set to succeed");
    }

    wait_for_flusher_to_settle(h);

    int total_persisted = 0;
    high_seqno = get_int_stat(h, "vb_1:high_seqno", "vbucket-seqno");

    if (isPersistentBucket(h)) {
        total_persisted = get_int_stat(h, "ep_total_persisted");
        checkeq(total_persisted,
                num_items,
                "Expected ep_total_persisted equals the number of items");
    } else {
        total_persisted = high_seqno;
    }

    checkeq(cb::engine_errc::success,
            observe_seqno(h, Vbid(1), vb_uuid),
            "Expected success");

    check_observe_seqno(false,
                        bucket_type,
                        0,
                        Vbid(1),
                        vb_uuid,
                        total_persisted,
                        high_seqno);
    //Stop persistence. Add more mutations and check observe result
    stop_persistence(h);

    num_items = 20;
    for (int j = 10; j < num_items; ++j) {
        // Set an item
        ItemIface* it = nullptr;
        uint64_t cas1;
        std::string value('x', 100);
        checkeq(cb::engine_errc::success,
                storeCasOut(h,
                            nullptr,
                            Vbid(1),
                            "key" + std::to_string(j),
                            value,
                            PROTOCOL_BINARY_RAW_BYTES,
                            it,
                            cas1),
                "Expected set to succeed");
    }

    high_seqno = get_int_stat(h, "vb_1:high_seqno", "vbucket-seqno");
    checkeq(cb::engine_errc::success,
            observe_seqno(h, Vbid(1), vb_uuid),
            "Expected success");

    if (!isPersistentBucket(h)) {
        /* if bucket is not persistent then total_persisted == high_seqno */
        total_persisted = high_seqno;
    }
    check_observe_seqno(false,
                        bucket_type,
                        0,
                        Vbid(1),
                        vb_uuid,
                        total_persisted,
                        high_seqno);
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    if (isPersistentBucket(h)) {
        total_persisted = get_int_stat(h, "ep_total_persisted");
    } else {
        total_persisted = high_seqno;
    }

    checkeq(cb::engine_errc::success,
            observe_seqno(h, Vbid(1), vb_uuid),
            "Expected success");

    check_observe_seqno(false,
                        bucket_type,
                        0,
                        Vbid(1),
                        vb_uuid,
                        total_persisted,
                        high_seqno);
    return SUCCESS;
}

static enum test_result test_observe_seqno_failover(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        // Set an item
        ItemIface* it = nullptr;
        uint64_t cas1;
        std::string value('x', 100);
        checkeq(cb::engine_errc::success,
                storeCasOut(h,
                            nullptr,
                            Vbid(0),
                            "key" + std::to_string(j),
                            value,
                            PROTOCOL_BINARY_RAW_BYTES,
                            it,
                            cas1),
                "Expected set to succeed");
    }

    wait_for_flusher_to_settle(h);

    uint64_t vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    uint64_t high_seqno = get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    // restart
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);

    uint64_t new_vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");

    checkeq(cb::engine_errc::success,
            observe_seqno(h, Vbid(0), vb_uuid),
            "Expected success");

    const auto bucket_type =
            isPersistentBucket(h) ? EPBucketType::EP : EPBucketType::Ephemeral;
    check_observe_seqno(true,
                        bucket_type,
                        1,
                        Vbid(0),
                        new_vb_uuid,
                        high_seqno,
                        high_seqno,
                        vb_uuid,
                        high_seqno);

    return SUCCESS;
}

static enum test_result test_observe_seqno_error(EngineIface* h) {
    //not my vbucket test
    uint64_t vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    checkeq(cb::engine_errc::not_my_vbucket,
            observe_seqno(h, Vbid(10), vb_uuid),
            "Expected NMVB");

    //invalid uuid for vbucket
    vb_uuid = 0xdeadbeef;
    std::stringstream invalid_data;
    invalid_data.write((char *) &vb_uuid, sizeof(uint64_t));

    auto request = createPacket(cb::mcbp::ClientOpcode::ObserveSeqno,
                                Vbid(0),
                                0,
                                {},
                                {},
                                invalid_data.str());
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::no_such_key,
            h->unknown_command(cookie.get(), *request, add_response),
            "Expected vb uuid not found");

    return SUCCESS;
}

static enum test_result test_observe_single_key(EngineIface* h) {
    stop_persistence(h);

    // Set an item
    std::string value('x', 100);
    ItemIface* it = nullptr;
    uint64_t cas1;
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas1),
            "Set should work");

    // Do an observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key"] = Vbid(0);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check that the key is not persisted
    Vbid vb;
    uint16_t keylen;
    char key[3];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{3}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key", 3),
            std::string_view(key, 3),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 7, sizeof(uint8_t));
    checkeq(uint8_t{OBS_STATE_NOT_PERSISTED},
            persisted,
            "Expected persisted in result");
    memcpy(&cas, last_body.data() + 8, sizeof(uint64_t));
    checkeq(cas1, ntohll(cas), "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_temp_item(EngineIface* h) {
    char const *k1 = "key";

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, k1, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    checkeq(cb::engine_errc::success, del(h, k1, 0, Vbid(0)), "Delete failed");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, k1, errorMetaPair), "Expected to get meta");
    checkeq(DocumentState::Deleted,
            errorMetaPair.second.document_state,
            "Expected deleted flag to be set");
    checkeq(0, get_int_stat(h, "curr_items"), "Expected zero curr_items");

    if (isPersistentBucket(h)) {
        // Persistent: make sure there is one temp_item (as Persistent buckets
        // don't keep deleted items in HashTable, unlike Ephemeral).
        checkeq(1,
                get_int_stat(h, "curr_temp_items"),
                "Expected single temp_items");
    }

    // Do an observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key"] = Vbid(0);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check that the key is not found
    Vbid vb;
    uint16_t keylen;
    char key[3];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    memcpy(&persisted, last_body.data() + 7, sizeof(uint8_t));
    memcpy(&cas, last_body.data() + 8, sizeof(uint64_t));

    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    checkeq(uint16_t{3}, ntohs(keylen), "Wrong keylen in result");
    checkeq(std::string_view("key", 3),
            std::string_view(key, 3),
            "Wrong key in result");
    if (isPersistentBucket(h)) {
        checkeq(OBS_STATE_NOT_FOUND,
                int(persisted),
                "Expected NOT_FOUND in result");
        checkeq(uint64_t(0), ntohll(cas), "Wrong cas in result");
    } else {
        // For ephemeral buckets, deleted items are kept in HT hence we check
        // for LOGICAL_DEL and a valid CAS.
        checkeq(OBS_STATE_LOGICAL_DEL,
                int(persisted),
                "Expected LOGICAL_DEL in result");
        checkne(uint64_t(0), ntohll(cas), "Wrong cas in result");
    }

    return SUCCESS;
}

static enum test_result test_observe_multi_key(EngineIface* h) {
    // Create some vbuckets
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    // Set some keys to observe
    ItemIface* it = nullptr;
    uint64_t cas1, cas2, cas3;
    std::string value('x', 100);
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key1",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas1),
            "Set should work");

    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(1),
                        "key2",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas2),
            "Set should work");

    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(1),
                        "key3",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas3),
            "Set should work");

    if (isPersistentBucket(h)) {
        wait_for_stat_to_be(h, "ep_total_persisted", 3);
    }

    // Do observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key1"] = Vbid(0);
    obskeys["key2"] = Vbid(1);
    obskeys["key3"] = Vbid(1);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check the result
    Vbid vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    const int expected_persisted = isPersistentBucket(h)
                                           ? OBS_STATE_PERSISTED
                                           : OBS_STATE_NOT_PERSISTED;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key1", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    checkeq(cas1, ntohll(cas), "Wrong cas in result");

    memcpy(&vb, last_body.data() + 17, sizeof(Vbid));
    checkeq(Vbid(1), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    checkeq(std::string_view("key2", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 26, sizeof(uint64_t));
    checkeq(cas2, ntohll(cas), "Wrong cas in result");

    memcpy(&vb, last_body.data() + 34, sizeof(Vbid));
    checkeq(Vbid(1), vb.ntoh(),  "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    checkeq(std::string_view("key3", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    checkeq(cas3, ntohll(cas), "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_multiple_observes(EngineIface* h) {
    // Holds the result
    Vbid vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    // Set some keys
    ItemIface* it = nullptr;
    uint64_t cas1, cas2;
    std::string value('x', 100);
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key1",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas1),
            "Set should work");

    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key2",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas2),
            "Set should work");

    if (isPersistentBucket(h)) {
        wait_for_stat_to_be(h, "ep_total_persisted", 2);
    }

    // Do observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key1"] = Vbid(0);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    const int expected_persisted = isPersistentBucket(h)
                                           ? OBS_STATE_PERSISTED
                                           : OBS_STATE_NOT_PERSISTED;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key1", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    checkeq(cas1, ntohll(cas), "Wrong cas in result");
    checkeq(size_t{17}, last_body.size(), "Incorrect body length");

    // Do another observe
    obskeys.clear();
    obskeys["key2"] = Vbid(0);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key2", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    checkeq(cas2, ntohll(cas),"Wrong cas in result");
    checkeq(size_t{17}, last_body.size(), "Incorrect body length");

    return SUCCESS;
}

static enum test_result test_observe_with_not_found(EngineIface* h) {
    // Create some vbuckets
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    // Set some keys
    ItemIface* it = nullptr;
    uint64_t cas1, cas3;
    std::string value('x', 100);
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key1",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas1),
            "Set should work");

    if (isPersistentBucket(h)) {
        wait_for_stat_to_be(h, "ep_total_persisted", 1);
        stop_persistence(h);
    }

    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(1),
                        "key3",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas3),
            "Set should work");

    checkeq(cb::engine_errc::success,
            del(h, "key3", 0, Vbid(1)),
            "Failed to remove a key");

    // Do observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key1"] = Vbid(0);
    obskeys["key2"] = Vbid(0);
    obskeys["key3"] = Vbid(1);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check the result
    Vbid vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    const int expected_persisted = isPersistentBucket(h)
                                           ? OBS_STATE_PERSISTED
                                           : OBS_STATE_NOT_PERSISTED;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key1", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    checkeq(expected_persisted, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    checkeq(ntohll(cas), cas1, "Wrong cas in result");

    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    checkeq(std::string_view("key2", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    checkeq(OBS_STATE_NOT_FOUND, int(persisted), "Expected key_not_found key status");

    memcpy(&vb, last_body.data() + 34, sizeof(Vbid));
    checkeq(Vbid(1), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    checkeq(std::string_view("key3", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    checkeq(OBS_STATE_LOGICAL_DEL, int(persisted), "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    checkne(cas3, ntohll(cas), "Expected cas to be different");

    return SUCCESS;
}

static enum test_result test_observe_errors(EngineIface* h) {
    std::map<std::string, Vbid> obskeys;

    // Check not my vbucket error
    obskeys["key"] = Vbid(1);

    checkeq(cb::engine_errc::not_my_vbucket,
            observe(h, obskeys),
            "Expected not my vbucket");

    // Check invalid packets
    auto pkt = createPacket(
            cb::mcbp::ClientOpcode::Observe, Vbid(0), 0, {}, {}, {"0", 1});
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::invalid_arguments,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Observe failed.");

    pkt = createPacket(
            cb::mcbp::ClientOpcode::Observe, Vbid(0), 0, {}, {}, {"0000", 4});
    checkeq(cb::engine_errc::invalid_arguments,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Observe failed.");

    return SUCCESS;
}

static enum test_result test_control_data_traffic(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value1"),
            "Failed to set key");

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    auto pkt = createPacket(cb::mcbp::ClientOpcode::DisableTraffic);
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Faile to disable data traffic");

    checkeq(cb::engine_errc::temporary_failure,
            store(h, nullptr, StoreSemantics::Set, "key", "value2"),
            "Expected to receive temporary failure");

    pkt = createPacket(cb::mcbp::ClientOpcode::EnableTraffic);
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Faile to enable data traffic");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value2"),
            "Failed to set key");
    return SUCCESS;
}

static enum test_result test_memory_condition(EngineIface* h) {
    std::string data(1024 * 1024, 'x');

    // Disable persistence so this test isn't racy
    stop_persistence(h);
    auto j = 0;

    while (true) {
        std::string key("key-");
        key += std::to_string(j++);

        cb::engine_errc err = store(
                h, nullptr, StoreSemantics::Set, key.c_str(), data.data());

        std::string checkMsg("Failed for reason ");
        checkMsg += to_string(err);
        check(err == cb::engine_errc::success ||
                      err == cb::engine_errc::temporary_failure ||
                      err == cb::engine_errc::no_memory,
              checkMsg.c_str());

        if (err == cb::engine_errc::temporary_failure ||
            err == cb::engine_errc::no_memory) {
            break;
        }
    }
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    return SUCCESS;
}

static enum test_result test_stats_vkey_valid_field(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);

    // Check vkey when a key doesn't exist
    const char* stats_key = "vkey key 0";
    checkeq(cb::engine_errc::no_such_key,
            h->get_stats(
                    *cookie, {stats_key, strlen(stats_key)}, {}, add_stats),
            "Expected not found.");

    stop_persistence(h);

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Failed to set key");

    // Check to make sure a non-persisted item is 'dirty'
    checkeq(cb::engine_errc::success,
            h->get_stats(
                    *cookie, {stats_key, strlen(stats_key)}, {}, add_stats),
            "Failed to get stats.");
    checkeq("dirty"s, vals.find("key_valid")->second, "Expected 'dirty'");

    // Check that a key that is resident and persisted returns valid
    start_persistence(h);
    wait_for_stat_to_be(h, "ep_total_persisted", 1);
    checkeq(cb::engine_errc::success,
            h->get_stats(
                    *cookie, {stats_key, strlen(stats_key)}, {}, add_stats),
            "Failed to get stats.");
    checkeq("valid"s, vals.find("key_valid")->second, "Expected 'valid'");

    // Check that an evicted key still returns valid
    evict_key(h, "key", Vbid(0), "Ejected.");
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, "vkey key 0"sv, {}, add_stats),
            "Failed to get stats.");
    checkeq("valid"s, vals.find("key_valid")->second, "Expected 'valid'");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_multiple_transactions(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");
    for (int j = 0; j < 1000; ++j) {
        std::stringstream s1;
        s1 << "key-0-" << j;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      s1.str().c_str(),
                      s1.str().c_str()),
                "Failed to store a value");
        std::stringstream s2;
        s2 << "key-1-" << j;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      s2.str().c_str(),
                      s2.str().c_str(),
                      nullptr,
                      0,
                      Vbid(1)),
                "Failed to store a value");
    }
    wait_for_stat_to_be(h, "ep_total_persisted", 2000);
    checklt(1, get_int_stat(h, "ep_commit_num"),
          "Expected 20 transaction completions at least");
    return SUCCESS;
}

static enum test_result test_set_ret_meta(EngineIface* h) {
    // Check that set without cas succeeds
    checkeq(cb::engine_errc::success,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 0, 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 1 set rm op");

    checkeq(uint32_t{0}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{1ull}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that set with correct cas succeeds
    checkeq(cb::engine_errc::success,
            set_ret_meta(h,
                         "key",
                         3,
                         "value",
                         5,
                         Vbid(0),
                         last_meta.cas,
                         10,
                         1735689600),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 2 set rm ops");

    checkeq(uint32_t{10}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{1735689600},
            last_meta.exptime,
            "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{2ull}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that updating an item with no cas succeeds
    checkeq(cb::engine_errc::success,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 5, 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 3 set rm ops");

    checkeq(uint32_t{5}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{3ull}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that updating an item with the wrong cas fails
    checkeq(cb::engine_errc::key_already_exists,
            set_ret_meta(
                    h, "key", 3, "value", 5, Vbid(0), last_meta.cas + 1, 5, 0),
            "Expected success");
    checkeq(3,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 3 set rm ops");

    return SUCCESS;
}

static enum test_result test_set_ret_meta_error(EngineIface* h) {
    // Check tmp fail errors
    disable_traffic(h);
    checkeq(cb::engine_errc::temporary_failure,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(0)),
            "Expected success");
    enable_traffic(h);

    // Check not my vbucket errors
    checkeq(cb::engine_errc::not_my_vbucket,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed to set vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            set_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    return SUCCESS;
}

static enum test_result test_add_ret_meta(EngineIface* h) {
    // Check that add with cas fails
    checkeq(cb::engine_errc::not_stored,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 10, 0, 0),
            "Expected success");

    // Check that add without cas succeeds.
    checkeq(cb::engine_errc::success,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 0, 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 1 set rm op");

    checkeq(uint32_t{0}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{1}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that re-adding a key fails
    checkeq(cb::engine_errc::not_stored,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 0, 0),
            "Expected success");

    // Check that adding a key with flags and exptime returns the correct values
    checkeq(cb::engine_errc::success,
            add_ret_meta(h, "key2", 4, "value", 5, Vbid(0), 0, 10, 1735689600),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_set_ret_meta"),
            "Expected 2 set rm ops");

    checkeq(uint32_t{10}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{1735689600},
            last_meta.exptime,
            "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{1}, last_meta.revSeqno, "Invalid result for seqno");

    return SUCCESS;
}

static enum test_result test_add_ret_meta_error(EngineIface* h) {
    // Check tmp fail errors
    disable_traffic(h);
    checkeq(cb::engine_errc::temporary_failure,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0)),
            "Expected success");
    enable_traffic(h);

    // Check not my vbucket errors
    checkeq(cb::engine_errc::not_my_vbucket,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed to add vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(1)),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    return SUCCESS;
}

static enum test_result test_del_ret_meta(EngineIface* h) {
    // Check that deleting a non-existent key fails
    checkeq(cb::engine_errc::no_such_key,
            del_ret_meta(h, "key", 3, Vbid(0), 0),
            "Expected success");

    // Check that deleting a non-existent key with a cas fails
    checkeq(cb::engine_errc::no_such_key,
            del_ret_meta(h, "key", 3, Vbid(0), 10),
            "Expeced success");

    // Check that deleting a key with no cas succeeds
    checkeq(cb::engine_errc::success,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 0, 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");

    checkeq(uint32_t{0}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{1}, last_meta.revSeqno, "Invalid result for seqno");

    checkeq(cb::engine_errc::success,
            del_ret_meta(h, "key", 3, Vbid(0), 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(1,
            get_int_stat(h, "ep_num_ops_del_ret_meta"),
            "Expected 1 del rm op");

    checkeq(uint32_t{0}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{2}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that deleting a key with a cas succeeds.
    checkeq(cb::engine_errc::success,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 10, 1735689600),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");

    checkeq(uint32_t{10}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{1735689600},
            last_meta.exptime,
            "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{3}, last_meta.revSeqno, "Invalid result for seqno");

    checkeq(cb::engine_errc::success,
            del_ret_meta(h, "key", 3, Vbid(0), last_meta.cas),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_del_ret_meta"),
            "Expected 2 del rm ops");

    checkeq(uint32_t{10}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{1735689600},
            last_meta.exptime,
            "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{4}, last_meta.revSeqno, "Invalid result for seqno");

    // Check that deleting a key with the wrong cas fails
    checkeq(cb::engine_errc::success,
            add_ret_meta(h, "key", 3, "value", 5, Vbid(0), 0, 0, 0),
            "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(),
          "Expected set returing meta to succeed");

    checkeq(uint32_t{0}, last_meta.flags, "Invalid result for flags");
    checkeq(time_t{0}, last_meta.exptime, "Invalid result for expiration");
    checkne(uint64_t{0}, last_meta.cas, "Invalid result for cas");
    checkeq(cb::uint48_t{5}, last_meta.revSeqno, "Invalid result for seqno");

    checkeq(cb::engine_errc::key_already_exists,
            del_ret_meta(h, "key", 3, Vbid(0), last_meta.cas + 1),
            "Expected success");
    checkeq(2,
            get_int_stat(h, "ep_num_ops_del_ret_meta"),
            "Expected 2 del rm ops");

    return SUCCESS;
}

static enum test_result test_del_ret_meta_error(EngineIface* h) {
    // Check tmp fail errors
    disable_traffic(h);
    checkeq(cb::engine_errc::temporary_failure,
            del_ret_meta(h, "key", 3, Vbid(0)),
            "Expected success");
    enable_traffic(h);

    // Check not my vbucket errors
    checkeq(cb::engine_errc::not_my_vbucket,
            del_ret_meta(h, "key", 3, Vbid(1), 0, nullptr),
            "Expected NMVB");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            del_ret_meta(h, "key", 3, Vbid(1), 0, nullptr),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_dead),
          "Failed to add vbucket state.");
    checkeq(cb::engine_errc::not_my_vbucket,
            del_ret_meta(h, "key", 3, Vbid(1), 0, nullptr),
            "Expected NMVB");
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");

    return SUCCESS;
}

static enum test_result test_set_with_item_eviction(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "key", Vbid(0), "Ejected.");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "newvalue"),
            "Failed set.");
    check_key_value(h, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_setWithMeta_with_item_eviction(EngineIface* h) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    size_t newValLen = strlen(newVal);

    // create a new key
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, val),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, key, Vbid(0), "Ejected.");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 300;
    itm_meta.flags = 0xdeadbeef;

    // set with meta for a non-resident item should pass.
    checkeq(cb::engine_errc::success,
            set_with_meta(h,
                          key,
                          keylen,
                          newVal,
                          newValLen,
                          Vbid(0),
                          &itm_meta,
                          cas_for_set),
            "Expected to store item");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    return SUCCESS;
}

static enum test_result test_multiple_set_delete_with_metas_full_eviction(
        EngineIface* h) {
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats");
    std::string eviction_policy = vals.find("ep_item_eviction_policy")->second;
    checkeq(std::string{"full_eviction"},
            eviction_policy,
            "Only available for full eviction");

    // init some random metadata
    ItemMetaData itm_meta;
    itm_meta.revSeqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 0;
    itm_meta.flags = 0xdeadbeef;

    for (int ii = 0; ii < 1000; ++ii) {
        std::string key = "key" + std::to_string(ii);
        checkeq(cb::engine_errc::success,
                set_with_meta(h,
                              key.data(),
                              key.size(),
                              "somevalue",
                              9,
                              Vbid(0),
                              &itm_meta,
                              0),
                "Expected to store item");
    }

    wait_for_flusher_to_settle(h);

    int curr_vb_items = get_int_stat(h, "vb_0:num_items", "vbucket-details 0");
    checkeq(1000, curr_vb_items, "Expected all items to be stored");
    const auto num_ops_set_with_meta = get_int_stat(h, "ep_num_ops_set_meta");
    const auto num_ops_del_with_meta = get_int_stat(h, "ep_num_ops_del_meta");
    checkeq(curr_vb_items,
            num_ops_set_with_meta,
            "Expected the number of set_with_meta calls to match the number of "
            "items");

    // If we try to overwrite the item with the same cas it'll return
    // KEY_EEXISTS so let's set a different operation.
    itm_meta.cas = 0xdeadcafe;
    std::thread thread1{[&h, &itm_meta]() {
        for (int ii = 0; ii < 100; ii++) {
            std::string key = "key" + std::to_string(ii);
            checkeq(cb::engine_errc::success,
                    set_with_meta(h,
                                  key.data(),
                                  key.size(),
                                  "somevalueEdited",
                                  15,
                                  Vbid(0),
                                  &itm_meta,
                                  0),
                    "Expected to modify the item");
            checkeq(cb::mcbp::Status::Success,
                    last_status.load(),
                    "Expected to modify the item");
        }
    }};

    std::thread thread2{[&h, &curr_vb_items, &itm_meta]() {
        for (int ii = curr_vb_items - 100; ii < curr_vb_items; ii++) {
            std::string key = "key" + std::to_string(ii);
            checkeq(cb::engine_errc::success,
                    del_with_meta(
                            h, key.data(), key.size(), Vbid(0), &itm_meta, 0),
                    "Expected to delete the item");
            checkeq(cb::mcbp::Status::Success,
                    last_status.load(),
                    "Expected to modify the item");
        }
    }};

    thread1.join();
    thread2.join();

    wait_for_flusher_to_settle(h);

    checkeq(num_ops_set_with_meta + 100,
            get_int_stat(h, "ep_num_ops_set_meta"),
            "Expected 100 set operations");
    checkeq(num_ops_del_with_meta + 100,
            get_int_stat(h, "ep_num_ops_del_meta"),
            "Expected 100 delete operations");

    curr_vb_items = get_int_stat(h, "vb_0:num_items", "vbucket-details 0");
    if (isWarmupEnabled(h)) {
        // Restart, and check data is warmed up correctly.
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   true);

        wait_for_warmup_complete(h);

        checkeq(curr_vb_items,
                get_int_stat(h, "vb_0:num_items", "vbucket-details 0"),
                "Unexpected item count in vbucket");
    }

    return SUCCESS;
}

static enum test_result test_add_with_item_eviction(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue"),
            "Failed to add value.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "key", Vbid(0), "Ejected.");

    checkeq(cb::engine_errc::not_stored,
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue"),
            "Failed to fail to re-add value.");

    // Expiration above was an hour, so let's go to The Future
    testHarness->time_travel(3800);

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "key", "newvalue"),
            "Failed to add value again.");
    check_key_value(h, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_gat_with_item_eviction(EngineIface* h) {
    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "mykey", "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "mykey", Vbid(0), "Ejected.");

    gat(h, "mykey", Vbid(0), 10); // 10 sec as expiration time
    checkeq(cb::mcbp::Status::Success, last_status.load(), "gat mykey");
    checkeq("somevalue"s, last_body, "Invalid data returned");

    // time-travel 9 secs..
    testHarness->time_travel(9);

    // The item should still exist
    check_key_value(h, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness->time_travel(2);

    // The item should have expired now...
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "mykey", Vbid(0)).first,
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_keyStats_with_item_eviction(EngineIface* h) {
    // set (k1,v1) in vbucket 0
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "k1", "v1"),
            "Failed to store an item.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "k1", Vbid(0), "Ejected.");
    auto* cookie = testHarness->create_cookie(h);

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    checkeq(cb::engine_errc::success,
            h->get_stats(*cookie, {statkey1, strlen(statkey1)}, {}, add_stats),
            "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_delWithMeta_with_item_eviction(EngineIface* h) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    // put some random meta data
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, key, Vbid(0), "Ejected.");

    // delete an item with meta data
    checkeq(cb::engine_errc::success,
            del_with_meta(h, key, keylen, Vbid(0), &itemMeta),
            "Expected delete success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    return SUCCESS;
}

static enum test_result test_del_with_item_eviction(EngineIface* h) {
    ItemIface* i = nullptr;
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue", &i),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, "key", Vbid(0), "Ejected.");

    Item *it = reinterpret_cast<Item*>(i);
    uint64_t orig_cas = it->getCas();
    h->release(*i);

    uint64_t cas = 0;
    uint64_t vb_uuid;
    mutation_descr_t mut_info;

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    auto high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");
    checkeq(cb::engine_errc::success,
            del(h, "key", &cas, Vbid(0), nullptr, &mut_info),
            "Failed remove with value.");
    checkne(orig_cas, cas, "Expected CAS to be different on delete");
    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "key"),
            "Expected missing key");
    checkeq(vb_uuid, mut_info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq((high_seqno + 1), mut_info.seqno, "Expected valid sequence number");

    return SUCCESS;
}

static enum test_result test_observe_with_item_eviction(EngineIface* h) {
    // Create some vbuckets
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state.");

    // Set some keys to observe
    ItemIface* it = nullptr;
    uint64_t cas1, cas2, cas3;

    std::string value('x', 100);
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(0),
                        "key1",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas1),
            "Set should work.");
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(1),
                        "key2",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas2),
            "Set should work.");
    checkeq(cb::engine_errc::success,
            storeCasOut(h,
                        nullptr,
                        Vbid(1),
                        "key3",
                        value,
                        PROTOCOL_BINARY_RAW_BYTES,
                        it,
                        cas3),
            "Set should work.");

    wait_for_stat_to_be(h, "ep_total_persisted", 3);

    evict_key(h, "key1", Vbid(0), "Ejected.");
    evict_key(h, "key2", Vbid(1), "Ejected.");

    // Do observe
    std::map<std::string, Vbid> obskeys;
    obskeys["key1"] = Vbid(0);
    obskeys["key2"] = Vbid(1);
    obskeys["key3"] = Vbid(1);
    checkeq(cb::engine_errc::success, observe(h, obskeys), "Expected success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    // Check the result
    Vbid vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body.data(), sizeof(Vbid));
    checkeq(Vbid(0), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 2, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 4, ntohs(keylen));
    checkeq(std::string_view("key1", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 8, sizeof(uint8_t));
    checkeq(uint8_t{OBS_STATE_PERSISTED},
            persisted,
            "Expected persisted in result");
    memcpy(&cas, last_body.data() + 9, sizeof(uint64_t));
    checkeq(ntohll(cas), cas1, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 17, sizeof(Vbid));
    checkeq(Vbid(1), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 19, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 21, ntohs(keylen));
    checkeq(std::string_view("key2", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 25, sizeof(uint8_t));
    checkeq(uint8_t{OBS_STATE_PERSISTED},
            persisted,
            "Expected persisted in result");
    memcpy(&cas, last_body.data() + 26, sizeof(uint64_t));
    checkeq(ntohll(cas), cas2, "Wrong cas in result");

    memcpy(&vb, last_body.data() + 34, sizeof(Vbid));
    checkeq(Vbid(1), vb.ntoh(), "Wrong vbucket in result");
    memcpy(&keylen, last_body.data() + 36, sizeof(uint16_t));
    checkeq(uint16_t{4}, ntohs(keylen), "Wrong keylen in result");
    memcpy(&key, last_body.data() + 38, ntohs(keylen));
    checkeq(std::string_view("key3", 4),
            std::string_view(key, 4),
            "Wrong key in result");
    memcpy(&persisted, last_body.data() + 42, sizeof(uint8_t));
    checkeq(uint8_t{OBS_STATE_PERSISTED},
            persisted,
            "Expected persisted in result");
    memcpy(&cas, last_body.data() + 43, sizeof(uint64_t));
    checkeq(ntohll(cas), cas3, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_expired_item_with_item_eviction(EngineIface* h) {
    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "mykey", "somevalue"),
            "Failed set.");
    gat(h, "mykey", Vbid(0), 10); // 10 sec as expiration time
    checkeq(cb::mcbp::Status::Success, last_status.load(), "gat mykey");
    checkeq("somevalue"s, last_body, "Invalid data returned");

    // Store a dummy item since we do not purge the item with highest seqno
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "dummykey",
                  "dummyvalue",
                  nullptr,
                  0,
                  Vbid(0),
                  0),
            "Error setting.");

    wait_for_flusher_to_settle(h);
    evict_key(h, "mykey", Vbid(0), "Ejected.");

    // time-travel 11 secs..
    testHarness->time_travel(11);

    // Compaction on VBucket 0
    compact_db(h, Vbid(0), 10, 10, 0);

    std::chrono::microseconds sleepTime{128};
    while (get_int_stat(h, "ep_pending_compactions") != 0) {
        decayingSleep(&sleepTime);
    }

    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(1,
            get_int_stat(h, "vb_active_expired"),
            "Expect the compactor to delete an expired item");

    // The item is already expired...
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "mykey", Vbid(0)).first,
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_non_existent_get_and_delete(EngineIface* h) {
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "key1", Vbid(0)).first,
            "Unexpected return status");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Unexpected temp item");
    checkeq(cb::engine_errc::no_such_key,
            del(h, "key3", 0, Vbid(0)),
            "Unexpected return status");
    checkeq(0, get_int_stat(h, "curr_temp_items"), "Unexpected temp item");
    return SUCCESS;
}

static enum test_result test_mb16421(EngineIface* h) {
    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "mykey", "somevalue"),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // Evict Item!
    evict_key(h, "mykey", Vbid(0), "Ejected.");

    // Issue Get Meta
    check(get_meta(h, "mykey"), "Expected to get meta");

    // Issue Get
    checkeq(cb::engine_errc::success,
            get(h, nullptr, "mykey", Vbid(0)).first,
            "Item should be there");

    return SUCCESS;
}

/**
 * Test that if we store an object with xattr the datatype is persisted
 * and read back correctly
 */
static enum test_result test_eviction_with_xattr(EngineIface* h) {
    if (!isPersistentBucket(h)) {
        return SKIPPED;
    }

    const char key[] = "test_eviction_with_xattr";
    cb::xattr::Blob builder;
    builder.set("_ep", "{\foo\":\"bar\"}");
    std::string data{builder.finalize()};

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         key,
                         data.data(),
                         data.size(),
                         0,
                         0,
                         Vbid(0),
                         0,
                         PROTOCOL_BINARY_DATATYPE_XATTR,
                         DocumentState::Alive)
                    .first,
            "Unable to store item");

    wait_for_flusher_to_settle(h);

    // Evict Item!
    evict_key(h, key, Vbid(0), "Ejected.");

    item_info info;
    check(get_item_info(h, &info, key), "Error getting item info");

    checkeq(PROTOCOL_BINARY_DATATYPE_XATTR, info.datatype,
            "Incorrect datatype read back");

    return SUCCESS;
}

static enum test_result test_get_random_key(EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);

    // An empty database should return no key
    auto pkt = createPacket(cb::mcbp::ClientOpcode::GetRandomKey);

    checkeq(cb::engine_errc::no_such_key,
            h->unknown_command(cookie, *pkt, add_response),
            "Database should be empty");

    StoredDocKey key("key", CollectionID::Default);

    // Store a key
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  key.keyData(),
                  "{\"some\":\"value\"}",
                  nullptr,
                  0,
                  Vbid(0),
                  3600,
                  PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed set.");
    checkeq(cb::engine_errc::success,
            get(h, nullptr, key.keyData(), Vbid(0)).first,
            "Item should be there");

    // collections only count after flush
    wait_for_flusher_to_settle(h);

    // We should be able to get one if there is something in there
    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie, *pkt, add_response),
            "get random should work");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");
    checkeq(PROTOCOL_BINARY_DATATYPE_JSON,
            last_datatype.load(),
            "Expected datatype to be JSON");

    // Since it is random we can't really check that we don't get the
    // same value twice...
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_failover_log_behavior(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        // TODO: Ephemeral: We should add a test variant which checks that
        // on restart we generate a new UUID (essentially forcing all clients
        // to rollback if they re-connect; given that all previous data is gone).
        return SKIPPED;
    }

    uint64_t num_entries, top_entry_id;
    // warm up
    wait_for_warmup_complete(h);
    num_entries = get_int_stat(h, "vb_0:num_entries", "failovers");

    checkeq(uint64_t{1},
            num_entries,
            "Failover log should have one entry for new vbucket");
    top_entry_id = get_ull_stat(h, "vb_0:0:id", "failovers");

    // restart
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);
    num_entries = get_int_stat(h, "vb_0:num_entries", "failovers");

    checkeq(uint64_t{2}, num_entries, "Failover log should have grown");
    checkne(top_entry_id, get_ull_stat(h, "vb_0:0:id", "failovers"),
          "Entry at current seq should be overwritten after restart");

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "data"),
                "Failed to store a value");
    }

    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 10);

    // restart
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);
    num_entries = get_int_stat(h, "vb_0:num_entries", "failovers");

    checkeq(uint64_t{3}, num_entries, "Failover log should have grown");
    checkeq(uint64_t{10},
            get_ull_stat(h, "vb_0:0:seq", "failovers"),
            "Latest failover log entry should have correct high sequence "
            "number");

    return SUCCESS;
}

static enum test_result test_hlc_cas(EngineIface* h) {
    const char *key = "key";
    item_info info;
    uint64_t curr_cas = 0, prev_cas = 0;

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, key, "data1"),
            "Failed to store an item");

    check(get_item_info(h, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    checklt(prev_cas, curr_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, "data2"),
            "Failed to store an item");

    check(get_item_info(h, &info, key), "Error getting item info");
    curr_cas = info.cas;
    checklt(prev_cas, curr_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Replace, key, "data3"),
            "Failed to store an item");

    check(get_item_info(h, &info, key), "Error in getting item info");
    curr_cas = info.cas;
    checklt(prev_cas, curr_cas, "CAS is not monotonically increasing");
    prev_cas = curr_cas;

    checkeq(cb::engine_errc::success,
            getl(h, nullptr, key, Vbid(0), 10).first,
            "Expected to be able to getl on first try");
    check(get_item_info(h, &info, key), "Error in getting item info");

    curr_cas = info.cas;
    checklt(prev_cas, curr_cas, "CAS is not monotonically increasing");
    return SUCCESS;
}

/*
    Basic test demonstrating multi-bucket operations.
    Checks that writing the same key to many buckets works as it should.
*/
static enum test_result test_multi_bucket_set_get(engine_test_t* test) {
    const int n_buckets = 20;
    std::vector<BucketHolder> buckets;
    if (create_buckets(test->cfg, n_buckets, buckets) != n_buckets) {
        destroy_buckets(buckets);
        return FAIL;

    }

    for (auto bucket : buckets) {
        // re-use test_setup which will wait for bucket ready
        test_setup(bucket.h);
    }

    int ii = 0;
    for (auto bucket : buckets) {
        std::stringstream val;
        val << "value_" << ii++;
        checkeq(cb::engine_errc::success,
                store(bucket.h,
                      nullptr,
                      StoreSemantics::Set,
                      "key",
                      val.str().c_str()),
                "Error setting.");
    }

    // Read back the values
    ii = 0;
    for (auto bucket : buckets) {
        std::stringstream val;
        val << "value_" << ii++;
        check_key_value(bucket.h, "key", val.str().c_str(), val.str().length());
    }

    destroy_buckets(buckets);

    return SUCCESS;
}

// Write to the vbucket's (couchstore) vbstate (_local/vbstate)
static void write_vb_state(std::string dbdir,
                           Vbid vbucket,
                           const nlohmann::json& json) {
    std::string filename = dbdir + cb::io::DirectorySeparator +
                           std::to_string(vbucket.get()) + ".couch.1";
    Db* handle;
    couchstore_error_t err = couchstore_open_db(
            filename.c_str(), COUCHSTORE_OPEN_FLAG_CREATE, &handle);

    checkeq(COUCHSTORE_SUCCESS, err, "Failed to open " + filename);
    const auto vbStateStr = json.dump();

    LocalDoc vbstate;
    vbstate.id.buf = const_cast<char*>(LocalDocKey::vbstate.data());
    vbstate.id.size = LocalDocKey::vbstate.size();
    vbstate.json.buf = (char*)vbStateStr.c_str();
    vbstate.json.size = vbStateStr.size();
    vbstate.deleted = 0;

    err = couchstore_save_local_document(handle, &vbstate);
    checkeq(COUCHSTORE_SUCCESS, err, "Failed to write local document");
    err = couchstore_commit(handle);
    checkeq(COUCHSTORE_SUCCESS, err, "Failed to commit");
    err = couchstore_close_file(handle);
    checkeq(COUCHSTORE_SUCCESS, err, "Failed to close file");
    err = couchstore_free_db(handle);
    checkeq(COUCHSTORE_SUCCESS, err, "Failed to free db");
}

/**
 * Rewrite the persisted vbucket_state to the oldest version we support upgrade
 * to (v5.0 at time of writing).
 */
static void force_vbstate_to_v5(std::string dbname,
                                Vbid vbucket,
                                bool hlcEpoch = true,
                                bool mightContainXattrs = true) {
    // Create 5.0.0 _local/vbstate
    // Note: unconditionally adding namespaces_supported to keep this test
    // working.
    nlohmann::json vbstate5 = {{"state", "active"},
                               {"checkpoint_id", "1"},
                               {"max_deleted_seqno", "0"},
                               {"snap_start", "0"},
                               {"snap_end", "1"},
                               {"max_cas", "123"},
                               {"namespaces_supported", true}};
    if (hlcEpoch) {
        vbstate5["hlc_epoch"] = std::to_string(HlcCasSeqnoUninitialised);
    }
    if (mightContainXattrs) {
        vbstate5["might_contain_xattrs"] = false;
    }
    write_vb_state(dbname, vbucket, vbstate5);
}

// Regression test for MB-19635
// Check that warming up from a 2.x couchfile doesn't end up with a UUID of 0
// we warmup 2 vbuckets and ensure they get unique IDs.
static enum test_result test_mb19635_upgrade_from_25x(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    std::string backend = get_str_stat(h, "ep_backend");
    if (backend == "rocksdb") {
        // TODO RDB:
        return SKIPPED_UNDER_ROCKSDB;
    }

    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(cb::engine_errc::success,
                store(h,
                      nullptr,
                      StoreSemantics::Set,
                      ss.str().c_str(),
                      "data"),
                "Failed to store a value");
    }

    wait_for_flusher_to_settle(h);

    std::string dbname = get_dbname(testHarness->get_current_testcase()->cfg);

    force_vbstate_to_v5(dbname, Vbid(0));
    force_vbstate_to_v5(dbname, Vbid(1));

    // Now shutdown engine force and restart to warmup from the 2.5.x data.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    uint64_t vb_uuid0 = get_ull_stat(h, "vb_0:uuid", "vbucket-details");
    uint64_t vb_uuid1 = get_ull_stat(h, "vb_1:uuid", "vbucket-details");
    checkne(vb_uuid0, vb_uuid1, "UUID is not unique");

    // Upgrade in this case means the high-seqno is the visible-seqno
    checkeq(get_ull_stat(h, "vb_0:high_seqno", "vbucket-details"),
            get_ull_stat(h, "vb_0:max_visible_seqno", "vbucket-details"),
            "expected max-visible-seqno to be set to high_seqno");

    return SUCCESS;
}

// Regression test for MB-38031
// If upgrade from 4.x via a 5.x 'hop' some vbstate entries that 5.x creates
// won't be present if 5.x is just a stepping stone with no flushes
static enum test_result test_mb38031_upgrade_from_4x_via_5x_hop(
        EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state (vb 1).");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed to set vbucket state (vb 2).");

    int num_items = 10;
    for (int vb = 0; vb < 3; ++vb) {
        for (int j = 0; j < num_items; ++j) {
            std::stringstream ss;
            ss << "key_" << j;
            checkeq(cb::engine_errc::success,
                    store(h,
                          nullptr,
                          StoreSemantics::Set,
                          ss.str().c_str(),
                          "data",
                          nullptr,
                          0,
                          Vbid(vb)),
                    "Failed to store a value");
        }
    }

    wait_for_flusher_to_settle(h);

    std::string dbname = get_dbname(testHarness->get_current_testcase()->cfg);

    // Force vbstate on three vbuckets with each combination of hlc_epoch and
    // might_contain_xattrs
    force_vbstate_to_v5(dbname, Vbid(0), true, false);
    force_vbstate_to_v5(dbname, Vbid(1), false, true);
    force_vbstate_to_v5(dbname, Vbid(2), false, false);

    // Now shutdown engine force and restart to warmup
    testHarness->reload_engine(
            &h, testHarness->get_current_testcase()->cfg, true, false);

    wait_for_warmup_complete(h);

    // Warmup success and all documents loaded
    checkeq(num_items,
            get_int_stat(h, "vb_0:num_items", "vbucket-details 0"),
            "Expected vb:0 to have num_items");
    checkeq(num_items,
            get_int_stat(h, "vb_1:num_items", "vbucket-details 1"),
            "Expected vb:1 to have num_items");
    checkeq(num_items,
            get_int_stat(h, "vb_2:num_items", "vbucket-details 2"),
            "Expected vb:2 to have num_items");
    return SUCCESS;
}

// Regression test for MB-38031
// If upgrade from 4.x via a 5.x 'hop' some vbstate entries that 5.x creates
// won't be present if 5.x is just a stepping stone with no flushes
static enum test_result test_mb38031_illegal_json_throws(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key",
                  "data",
                  nullptr,
                  0,
                  Vbid(0)),
            "Failed to store a value");

    wait_for_flusher_to_settle(h);

    // VBState with a required field missing, max_cas
    const nlohmann::json vbstate = {
            {"state", "active"},
            {"checkpoint_id", "1"},
            {"max_deleted_seqno", "0"},
            {"snap_start", "34344987306"}, // illegal as start > end
            {"snap_end", "1"},
            {"hlc_epoch", std::to_string(HlcCasSeqnoUninitialised)},
            {"might_contain_xattrs", false},
            {"namespaces_supported", true}};

    write_vb_state(get_dbname(testHarness->get_current_testcase()->cfg),
                   Vbid(0),
                   vbstate);

    // reload_engine will throw an engine initialize exception from CouchKVStore
    try {
        testHarness->reload_engine(
                &h, testHarness->get_current_testcase()->cfg, true, false);
    } catch (const std::runtime_error& e) {
        check(std::string(e.what()).find("CouchKVStore::initialize: "
                                         "readVBState error:JsonInvalid") !=
                      std::string::npos,
              "unexpected exception");
        return SUCCESS;
    }

    return FAIL;
}

static void force_vbstate_MB34173_corruption(std::string dbname,
                                             const std::string state,
                                             Vbid vbucket) {
    const nlohmann::json vbstateIllegalSnapshotRange = {
            {"state", state},
            {"checkpoint_id", "1"},
            {"max_deleted_seqno", "0"},
            {"snap_start", "34344987306"}, // illegal as start > end
            {"snap_end", "1"},
            {"max_cas", "123"},
            {"hlc_epoch", std::to_string(HlcCasSeqnoUninitialised)},
            {"might_contain_xattrs", false},
            {"namespaces_supported", true}};

    write_vb_state(dbname, vbucket, vbstateIllegalSnapshotRange);
}

static enum test_result test_MB34173_warmup(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    std::string backend = get_str_stat(h, "ep_backend");
    if (backend == "rocksdb") {
        // TODO RDB:
        return SKIPPED_UNDER_ROCKSDB;
    }

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket state (vb 0).");
    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state (vb 1).");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_pending),
          "Failed to set vbucket state (vb 2).");

    wait_for_flusher_to_settle(h);

    std::string dbname = get_dbname(testHarness->get_current_testcase()->cfg);

    force_vbstate_MB34173_corruption(dbname, "active", Vbid(0));
    force_vbstate_MB34173_corruption(dbname, "replica", Vbid(1));
    force_vbstate_MB34173_corruption(dbname, "pending", Vbid(2));

    // Warmup
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    auto checkRange = [h](int vb) {
        std::string statKey = "vb_" + std::to_string(vb);
        auto key1 = statKey + ":high_seqno";
        auto key2 = statKey + ":last_persisted_snap_start";
        auto key3 = statKey + ":last_persisted_snap_end";
        uint64_t highSeq = get_ull_stat(h, key1.c_str(), "vbucket-seqno");
        uint64_t snapStart = get_ull_stat(h, key2.c_str(), "vbucket-seqno");
        uint64_t snapEnd = get_ull_stat(h, key3.c_str(), "vbucket-seqno");
        check(highSeq <= snapEnd && highSeq >= snapStart,
              "Invalid snapshot range");
    };

    checkRange(0);

    // Expect that vb1/2 have now gone away, warmup skipped them because of the
    // snapshot corruption
    checkeq(cb::engine_errc::not_my_vbucket,
            gat(h, "no-vb", Vbid(1), 0).first,
            "Should have received not my vbucket response for vb1");
    checkeq(cb::engine_errc::not_my_vbucket,
            gat(h, "no-vb", Vbid(2), 0).first,
            "Should have received not my vbucket response for vb2");

    check(set_vbucket_state(h, Vbid(1), vbucket_state_replica),
          "Failed to set vbucket state (vb 1).");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_replica),
          "Failed to set vbucket state (vb 2).");

    checkRange(1);
    checkRange(2);

    return SUCCESS;
}

// Regression test the stats calls that they don't blow the snprintf
// buffers. All of the tests in this batch make sure that all of the stats
// exists (the stats call return a fixed set of stats)
static enum test_result test_mb19687_fixed(EngineIface* h) {
    // Map of stat group names to their content. Not const as we can insert
    // additional stats to check for based on engine config (full / value
    // eviction, persistent / ephemeral ...)
    std::map<std::string_view, std::vector<std::string_view> > statsKeys{
            {"dcp-vbtakeover 0",
             {"status",
              "on_disk_deletes",
              "vb_items",
              "chk_items",
              "estimate"}},
            {"dcp",
             {"ep_dcp_count",
              "ep_dcp_dead_conn_count",
              "ep_dcp_items_remaining",
              "ep_dcp_items_sent",
              "ep_dcp_max_running_backfills",
              "ep_dcp_num_running_backfills",
              "ep_dcp_consumer_count",
              "ep_dcp_producer_count",
              "ep_dcp_queue_fill",
              "ep_dcp_total_bytes",
              "ep_dcp_total_uncompressed_data_size",
              "ep_dcp_total_queue"}},
            {"hash",
             {"vb_0:counted",
              "vb_0:locks",
              "vb_0:max_depth",
              "vb_0:mem_size",
              "vb_0:mem_size_counted",
              "vb_0:min_depth",
              "vb_0:num_system_items",
              "vb_0:reported",
              "vb_0:resized",
              "vb_0:size",
              "vb_0:state"}},
            {"vbucket", {"vb_0"}},
            {"vbucket-details 0",
             {"vb_0",
              "vb_0:bloom_filter",
              "vb_0:bloom_filter_key_count",
              "vb_0:bloom_filter_memory",
              "vb_0:bloom_filter_size",
              "vb_0:drift_ahead_threshold",
              "vb_0:drift_ahead_threshold_exceeded",
              "vb_0:drift_behind_threshold",
              "vb_0:drift_behind_threshold_exceeded",
              "vb_0:high_completed_seqno",
              "vb_0:high_prepared_seqno",
              "vb_0:high_seqno",
              "vb_0:ht_cache_size",
              "vb_0:ht_item_memory",
              "vb_0:ht_item_memory_uncompressed",
              "vb_0:ht_memory",
              "vb_0:ht_num_deleted_items",
              "vb_0:ht_num_in_memory_items",
              "vb_0:ht_num_in_memory_non_resident_items",
              "vb_0:ht_num_items",
              "vb_0:ht_num_temp_items",
              "vb_0:ht_size",
              "vb_0:logical_clock_ticks",
              "vb_0:max_cas",
              "vb_0:max_cas_str",
              "vb_0:max_visible_seqno",
              "vb_0:num_ejects",
              "vb_0:num_items",
              "vb_0:num_non_resident",
              "vb_0:num_prepared_sync_writes",
              "vb_0:num_temp_items",
              "vb_0:ops_create",
              "vb_0:ops_delete",
              "vb_0:ops_get",
              "vb_0:ops_reject",
              "vb_0:ops_update",
              "vb_0:pending_writes",
              "vb_0:persistence_seqno",
              "vb_0:purge_seqno",
              "vb_0:queue_age",
              "vb_0:queue_drain",
              "vb_0:queue_fill",
              "vb_0:queue_memory",
              "vb_0:queue_size",
              "vb_0:rollback_item_count",
              "vb_0:sync_write_accepted_count",
              "vb_0:sync_write_committed_count",
              "vb_0:sync_write_aborted_count",
              "vb_0:hp_vb_req_size",
              "vb_0:topology",
              "vb_0:total_abs_drift",
              "vb_0:total_abs_drift_count",
              "vb_0:uuid",
              "vb_0:might_contain_xattrs",
              "vb_0:max_deleted_revid"}},
            {"vbucket-seqno",
             {"vb_0:abs_high_seqno",
              "vb_0:high_completed_seqno",
              "vb_0:high_prepared_seqno",
              "vb_0:high_seqno",
              "vb_0:last_persisted_seqno",
              "vb_0:last_persisted_snap_end",
              "vb_0:last_persisted_snap_start",
              "vb_0:max_visible_seqno",
              "vb_0:purge_seqno",
              "vb_0:uuid"}},
            {"vbucket-seqno 0",
             {"vb_0:abs_high_seqno",
              "vb_0:high_completed_seqno",
              "vb_0:high_prepared_seqno",
              "vb_0:high_seqno",
              "vb_0:last_persisted_seqno",
              "vb_0:last_persisted_snap_end",
              "vb_0:last_persisted_snap_start",
              "vb_0:max_visible_seqno",
              "vb_0:purge_seqno",
              "vb_0:uuid"}},
            {"prev-vbucket", {"vb_0"}},
            {"prev-vbucket", {"vb_0"}},
            {"checkpoint",
             {"vb_0:last_closed_checkpoint_id",
              "vb_0:mem_usage",
              "vb_0:num_checkpoint_items",
              "vb_0:num_checkpoints",
              "vb_0:num_conn_cursors",
              "vb_0:num_open_checkpoint_items",
              "vb_0:open_checkpoint_id",
              "vb_0:state",
              "vb_0:id_1:key_index_allocator_bytes",
              "vb_0:id_1:key_index_key_allocator_bytes",
              "vb_0:id_1:mem_usage_queued_items",
              "vb_0:id_1:mem_usage_queue_overhead",
              "vb_0:id_1:mem_usage_key_index_overhead",
              "vb_0:id_1:snap_end",
              "vb_0:id_1:snap_start",
              "vb_0:id_1:state",
              "vb_0:id_1:queue_allocator_bytes",
              "vb_0:id_1:type",
              "vb_0:id_1:visible_snap_end"}},
            {"checkpoint 0",
             {"vb_0:last_closed_checkpoint_id",
              "vb_0:mem_usage",
              "vb_0:num_checkpoint_items",
              "vb_0:num_checkpoints",
              "vb_0:num_conn_cursors",
              "vb_0:num_open_checkpoint_items",
              "vb_0:open_checkpoint_id",
              "vb_0:state",
              "vb_0:id_1:key_index_allocator_bytes",
              "vb_0:id_1:key_index_key_allocator_bytes",
              "vb_0:id_1:mem_usage_queued_items",
              "vb_0:id_1:mem_usage_queue_overhead",
              "vb_0:id_1:mem_usage_key_index_overhead",
              "vb_0:id_1:snap_end",
              "vb_0:id_1:snap_start",
              "vb_0:id_1:state",
              "vb_0:id_1:queue_allocator_bytes",
              "vb_0:id_1:type",
              "vb_0:id_1:visible_snap_end"}},
            {"uuid", {"uuid"}},
            {"kvstore",
             {"rw_0:backend_type",
              "rw_0:close",
              "rw_0:failure_del",
              "rw_0:failure_get",
              "rw_0:failure_open",
              "rw_0:failure_set",
              "rw_0:failure_vbset",
              "rw_0:io_flusher_write_amplification",
              "rw_0:io_total_write_amplification",
              "rw_0:io_compaction_read_bytes",
              "rw_0:io_compaction_write_bytes",
              "rw_0:io_bg_fetch_docs_read",
              "rw_0:io_num_write",
              "rw_0:io_bg_fetch_doc_bytes",
              "rw_0:io_total_read_bytes",
              "rw_0:io_total_write_bytes",
              "rw_0:io_document_write_bytes",
              "rw_0:lastCommDocs",
              "rw_0:numLoadedVb",
              "rw_0:open",
              "rw_1:backend_type",
              "rw_1:close",
              "rw_1:failure_del",
              "rw_1:failure_get",
              "rw_1:failure_open",
              "rw_1:failure_set",
              "rw_1:failure_vbset",
              "rw_1:io_flusher_write_amplification",
              "rw_1:io_total_write_amplification",
              "rw_1:io_compaction_read_bytes",
              "rw_1:io_compaction_write_bytes",
              "rw_1:io_bg_fetch_docs_read",
              "rw_1:io_num_write",
              "rw_1:io_bg_fetch_doc_bytes",
              "rw_1:io_total_read_bytes",
              "rw_1:io_total_write_bytes",
              "rw_1:io_document_write_bytes",
              "rw_1:lastCommDocs",
              "rw_1:numLoadedVb",
              "rw_1:open",
              "rw_2:backend_type",
              "rw_2:close",
              "rw_2:failure_del",
              "rw_2:failure_get",
              "rw_2:failure_open",
              "rw_2:failure_set",
              "rw_2:failure_vbset",
              "rw_2:io_flusher_write_amplification",
              "rw_2:io_total_write_amplification",
              "rw_2:io_compaction_read_bytes",
              "rw_2:io_compaction_write_bytes",
              "rw_2:io_bg_fetch_docs_read",
              "rw_2:io_num_write",
              "rw_2:io_bg_fetch_doc_bytes",
              "rw_2:io_total_read_bytes",
              "rw_2:io_total_write_bytes",
              "rw_2:io_document_write_bytes",
              "rw_2:lastCommDocs",
              "rw_2:numLoadedVb",
              "rw_2:open",
              "rw_3:backend_type",
              "rw_3:close",
              "rw_3:failure_del",
              "rw_3:failure_get",
              "rw_3:failure_open",
              "rw_3:failure_set",
              "rw_3:failure_vbset",
              "rw_3:io_flusher_write_amplification",
              "rw_3:io_total_write_amplification",
              "rw_3:io_compaction_read_bytes",
              "rw_3:io_compaction_write_bytes",
              "rw_3:io_bg_fetch_docs_read",
              "rw_3:io_num_write",
              "rw_3:io_bg_fetch_doc_bytes",
              "rw_3:io_total_read_bytes",
              "rw_3:io_total_write_bytes",
              "rw_3:io_document_write_bytes",
              "rw_3:lastCommDocs",
              "rw_3:numLoadedVb",
              "rw_3:open"}},
            {"info", {"info"}},
            {"config",
             {"ep_allow_sanitize_value_in_deletion",
              "ep_backend",
              "ep_backfill_mem_threshold",
              "ep_bfilter_enabled",
              "ep_bfilter_fp_prob",
              "ep_bfilter_key_count",
              "ep_bfilter_residency_threshold",
              "ep_bucket_type",
              "ep_cache_size",
              "ep_chk_expel_enabled",
              "ep_chk_max_items",
              "ep_chk_period",
              "ep_chk_remover_stime",
              "ep_checkpoint_destruction_tasks",
              "ep_checkpoint_memory_ratio",
              "ep_checkpoint_memory_recovery_upper_mark",
              "ep_checkpoint_memory_recovery_lower_mark",
              "ep_checkpoint_max_size",
              "ep_collections_drop_compaction_delay",
              "ep_collections_enabled",
              "ep_compaction_expire_from_start",
              "ep_compaction_write_queue_cap",
              "ep_compaction_max_concurrent_ratio",
              "ep_compression_mode",
              "ep_concurrent_pagers",
              "ep_expiry_pager_concurrency",
              "ep_conflict_resolution_type",
              "ep_connection_manager_interval",
              "ep_couch_bucket",
              "ep_data_traffic_enabled",
              "ep_dbname",
              "ep_dcp_backfill_byte_limit",
              "ep_dcp_conn_buffer_size",
              "ep_dcp_conn_buffer_size_aggr_mem_threshold",
              "ep_dcp_conn_buffer_size_aggressive_perc",
              "ep_dcp_conn_buffer_size_max",
              "ep_dcp_conn_buffer_size_perc",
              "ep_dcp_enable_noop",
              "ep_dcp_flow_control_policy",
              "ep_dcp_min_compression_ratio",
              "ep_dcp_idle_timeout",
              "ep_dcp_noop_mandatory_for_v5_features",
              "ep_dcp_noop_tx_interval",
              "ep_dcp_producer_snapshot_marker_yield_limit",
              "ep_dcp_consumer_control_enabled",
              "ep_dcp_consumer_process_buffered_messages_yield_limit",
              "ep_dcp_consumer_process_buffered_messages_batch_size",
              "ep_dcp_scan_byte_limit",
              "ep_dcp_scan_item_limit",
              "ep_dcp_takeover_max_time",
              "ep_defragmenter_age_threshold",
              "ep_defragmenter_auto_lower_threshold",
              "ep_defragmenter_auto_max_sleep",
              "ep_defragmenter_auto_min_sleep",
              "ep_defragmenter_auto_pid_d",
              "ep_defragmenter_auto_pid_dt",
              "ep_defragmenter_auto_pid_i",
              "ep_defragmenter_auto_pid_p",
              "ep_defragmenter_auto_upper_threshold",
              "ep_defragmenter_chunk_duration",
              "ep_defragmenter_enabled",
              "ep_defragmenter_interval",
              "ep_defragmenter_mode",
              "ep_defragmenter_stored_value_age_threshold",
              "ep_durability_timeout_mode",
              "ep_durability_min_level",
              "ep_executor_pool_backend",
              "ep_exp_pager_enabled",
              "ep_exp_pager_initial_run_time",
              "ep_exp_pager_stime",
              "ep_failpartialwarmup",
              "ep_flusher_total_batch_limit",
              "ep_fsync_after_every_n_bytes_written",
              "ep_couchstore_tracing",
              "ep_couchstore_write_validation",
              "ep_couchstore_mprotect",
              "ep_couchstore_file_cache_max_size",
              "ep_couchstore_midpoint_rollback_optimisation",
              "ep_getl_default_timeout",
              "ep_getl_max_timeout",
              "ep_hlc_drift_ahead_threshold_us",
              "ep_hlc_drift_behind_threshold_us",
              "ep_ht_locks",
              "ep_ht_resize_interval",
              "ep_ht_size",
              "ep_item_compressor_chunk_duration",
              "ep_item_compressor_interval",
              "ep_item_eviction_age_percentage",
              "ep_item_eviction_freq_counter_age_threshold",
              "ep_item_freq_decayer_chunk_duration",
              "ep_item_freq_decayer_percent",
              "ep_item_num_based_new_chk",
              "ep_magma_sync_every_batch",
              "ep_magma_checkpoint_interval",
              "ep_magma_min_checkpoint_interval",
              "ep_magma_checkpoint_threshold",
              "ep_magma_delete_frag_ratio",
              "ep_magma_delete_memtable_writecache",
              "ep_magma_enable_block_cache",
              "ep_magma_enable_direct_io",
              "ep_magma_enable_group_commit",
              "ep_magma_enable_upsert",
              "ep_magma_expiry_frag_threshold",
              "ep_magma_fragmentation_percentage",
              "ep_magma_flusher_thread_percentage",
              "ep_magma_group_commit_max_sync_wait_duration_ms",
              "ep_magma_group_commit_max_transaction_count",
              "ep_magma_heartbeat_interval",
              "ep_magma_max_checkpoints",
              "ep_magma_max_default_storage_threads",
              "ep_magma_max_level_0_ttl",
              "ep_magma_max_recovery_bytes",
              "ep_magma_max_write_cache",
              "ep_magma_mem_quota_ratio",
              "ep_magma_mem_quota_low_watermark_ratio",
              "ep_magma_value_separation_size",
              "ep_magma_initial_wal_buffer_size",
              "ep_magma_write_cache_ratio",
              "ep_magma_expiry_purger_interval",
              "ep_magma_enable_wal",
              "ep_magma_bloom_filter_accuracy",
              "ep_magma_bloom_filter_accuracy_for_bottom_level",
              "ep_max_num_bgfetchers",
              "ep_max_num_flushers",
              "ep_max_checkpoints",
              "ep_max_failover_entries",
              "ep_max_item_privileged_bytes",
              "ep_max_item_size",
              "ep_max_num_shards",
              "ep_max_num_workers",
              "ep_checkpoint_remover_task_count",
              "ep_max_size",
              "ep_max_threads",
              "ep_max_ttl",
              "ep_max_vbuckets",
              "ep_mem_high_wat",
              "ep_mem_low_wat",
              "ep_mem_used_merge_threshold_percent",
              "ep_min_compression_ratio",
              "ep_mutation_mem_threshold",
              "ep_num_auxio_threads",
              "ep_num_nonio_threads",
              "ep_num_reader_threads",
              "ep_num_writer_threads",
              "ep_pager_active_vb_pcnt",
              "ep_pager_sleep_time_ms",
              "ep_pitr_enabled",
              "ep_pitr_granularity",
              "ep_pitr_max_history_age",
              "ep_replication_throttle_threshold",
              "ep_retain_erroneous_tombstones",
              "ep_rocksdb_options",
              "ep_rocksdb_cf_options",
              "ep_rocksdb_bbt_options",
              "ep_rocksdb_low_pri_background_threads",
              "ep_rocksdb_high_pri_background_threads",
              "ep_rocksdb_stats_level",
              "ep_rocksdb_block_cache_ratio",
              "ep_rocksdb_block_cache_high_pri_pool_ratio",
              "ep_rocksdb_memtables_ratio",
              "ep_rocksdb_default_cf_optimize_compaction",
              "ep_rocksdb_seqno_cf_optimize_compaction",
              "ep_rocksdb_write_rate_limit",
              "ep_rocksdb_uc_max_size_amplification_percent",
              "ep_seqno_persistence_timeout",
              "ep_sync_writes_max_allowed_replicas",
              "ep_time_synchronization",
              "ep_uuid",
              "ep_vbucket_mapping_sanity_checking",
              "ep_vbucket_mapping_sanity_checking_error_mode",
              "ep_warmup_batch_size",
              "ep_warmup_min_items_threshold",
              "ep_warmup_min_memory_threshold",
              "ep_xattr_enabled"}},
            {"workload",
             {"ep_workload:num_readers",
              "ep_workload:num_writers",
              "ep_workload:num_auxio",
              "ep_workload:num_nonio",
              "ep_workload:num_shards",
              "ep_workload:ready_tasks",
              "ep_workload:num_sleepers",
              "ep_workload:LowPrioQ_AuxIO:InQsize",
              "ep_workload:LowPrioQ_AuxIO:OutQsize",
              "ep_workload:LowPrioQ_NonIO:InQsize",
              "ep_workload:LowPrioQ_NonIO:OutQsize",
              "ep_workload:LowPrioQ_Reader:InQsize",
              "ep_workload:LowPrioQ_Reader:OutQsize",
              "ep_workload:LowPrioQ_Writer:InQsize",
              "ep_workload:LowPrioQ_Writer:OutQsize"}},
            {"failovers 0",
             {"vb_0:0:id",
              "vb_0:0:seq",
              "vb_0:num_entries",
              "vb_0:num_erroneous_entries_erased"}},
            {"failovers",
             {"vb_0:0:id",
              "vb_0:0:seq",
              "vb_0:num_entries",
              "vb_0:num_erroneous_entries_erased"}},
            {"", // Note: we convert empty to a null to get engine stats
             {"ep_allow_sanitize_value_in_deletion",
              "bytes",
              "curr_items",
              "curr_items_tot",
              "curr_temp_items",
              "ep_access_scanner_last_runtime",
              "ep_access_scanner_num_items",
              "ep_access_scanner_task_time",
              "ep_active_ahead_exceptions",
              "ep_active_behind_exceptions",
              "ep_active_datatype_json",
              "ep_active_datatype_json,xattr",
              "ep_active_datatype_raw",
              "ep_active_datatype_snappy",
              "ep_active_datatype_snappy,json",
              "ep_active_datatype_snappy,json,xattr",
              "ep_active_datatype_snappy,xattr",
              "ep_active_datatype_xattr",
              "ep_active_hlc_drift",
              "ep_active_hlc_drift_count",
              "ep_backend",
              "ep_backfill_mem_threshold",
              "ep_bfilter_enabled",
              "ep_bfilter_fp_prob",
              "ep_bfilter_key_count",
              "ep_bfilter_residency_threshold",
              "ep_bg_fetch_avg_read_amplification",
              "ep_bg_fetched",
              "ep_bg_meta_fetched",
              "ep_bg_remaining_items",
              "ep_bg_remaining_jobs",
              "ep_blob_num",
              "ep_blob_overhead",
              "ep_bucket_priority",
              "ep_bucket_type",
              "ep_cache_size",
              "ep_chk_expel_enabled",
              "ep_chk_max_items",
              "ep_chk_period",
              "ep_chk_persistence_remains",
              "ep_chk_remover_stime",
              "ep_checkpoint_destruction_tasks",
              "ep_checkpoint_memory_pending_destruction",
              "ep_checkpoint_memory_ratio",
              "ep_checkpoint_memory_recovery_upper_mark",
              "ep_checkpoint_memory_recovery_lower_mark",
              "ep_checkpoint_memory_quota",
              "ep_checkpoint_memory_recovery_upper_mark_bytes",
              "ep_checkpoint_memory_recovery_lower_mark_bytes",
              "ep_checkpoint_max_size",
              "ep_checkpoint_computed_max_size",
              "ep_clock_cas_drift_threshold_exceeded",
              "ep_collections_drop_compaction_delay",
              "ep_collections_enabled",
              "ep_compaction_expire_from_start",
              "ep_compaction_write_queue_cap",
              "ep_compaction_max_concurrent_ratio",
              "ep_compression_mode",
              "ep_concurrent_pagers",
              "ep_expiry_pager_concurrency",
              "ep_conflict_resolution_type",
              "ep_connection_manager_interval",
              "ep_couch_bucket",
              "ep_cursors_dropped",
              "ep_mem_freed_by_checkpoint_removal",
              "ep_mem_freed_by_checkpoint_item_expel",
              "ep_num_checkpoints",
              "ep_data_read_failed",
              "ep_data_write_failed",
              "ep_data_traffic_enabled",
              "ep_dbname",
              "ep_dcp_backfill_byte_limit",
              "ep_dcp_conn_buffer_size",
              "ep_dcp_conn_buffer_size_aggr_mem_threshold",
              "ep_dcp_conn_buffer_size_aggressive_perc",
              "ep_dcp_conn_buffer_size_max",
              "ep_dcp_conn_buffer_size_perc",
              "ep_dcp_consumer_control_enabled",
              "ep_dcp_consumer_process_buffered_messages_batch_size",
              "ep_dcp_consumer_process_buffered_messages_yield_limit",
              "ep_dcp_enable_noop",
              "ep_dcp_flow_control_policy",
              "ep_dcp_idle_timeout",
              "ep_dcp_min_compression_ratio",
              "ep_dcp_noop_mandatory_for_v5_features",
              "ep_dcp_noop_tx_interval",
              "ep_dcp_producer_snapshot_marker_yield_limit",
              "ep_dcp_scan_byte_limit",
              "ep_dcp_scan_item_limit",
              "ep_dcp_takeover_max_time",
              "ep_defragmenter_age_threshold",
              "ep_defragmenter_auto_lower_threshold",
              "ep_defragmenter_auto_max_sleep",
              "ep_defragmenter_auto_min_sleep",
              "ep_defragmenter_auto_pid_d",
              "ep_defragmenter_auto_pid_dt",
              "ep_defragmenter_auto_pid_i",
              "ep_defragmenter_auto_pid_p",
              "ep_defragmenter_auto_upper_threshold",
              "ep_defragmenter_chunk_duration",
              "ep_defragmenter_enabled",
              "ep_defragmenter_interval",
              "ep_defragmenter_num_moved",
              "ep_defragmenter_num_visited",
              "ep_defragmenter_mode",
              "ep_defragmenter_stored_value_age_threshold",
              "ep_defragmenter_sv_num_moved",
              "ep_degraded_mode",
              "ep_diskqueue_drain",
              "ep_diskqueue_fill",
              "ep_diskqueue_items",
              "ep_diskqueue_memory",
              "ep_diskqueue_pending",
              "ep_durability_timeout_mode",
              "ep_durability_min_level",
              "ep_executor_pool_backend",
              "ep_exp_pager_enabled",
              "ep_exp_pager_initial_run_time",
              "ep_exp_pager_stime",
              "ep_expired_access",
              "ep_expired_compactor",
              "ep_expired_pager",
              "ep_expiry_pager_task_time",
              "ep_failpartialwarmup",
              "ep_flush_duration_total",
              "ep_flusher_total_batch_limit",
              "ep_fsync_after_every_n_bytes_written",
              "ep_couchstore_tracing",
              "ep_couchstore_write_validation",
              "ep_couchstore_mprotect",
              "ep_couchstore_file_cache_max_size",
              "ep_couchstore_midpoint_rollback_optimisation",
              "ep_getl_default_timeout",
              "ep_getl_max_timeout",
              "ep_hlc_drift_ahead_threshold_us",
              "ep_hlc_drift_behind_threshold_us",
              "ep_ht_locks",
              "ep_ht_resize_interval",
              "ep_ht_size",
              "ep_io_bg_fetch_read_count",
              "ep_io_compaction_read_bytes",
              "ep_io_compaction_write_bytes",
              "ep_io_document_write_bytes",
              "ep_io_flusher_write_amplification",
              "ep_io_total_read_bytes",
              "ep_io_total_write_amplification",
              "ep_io_total_write_bytes",
              "ep_item_compressor_chunk_duration",
              "ep_item_compressor_interval",
              "ep_item_compressor_num_compressed",
              "ep_item_compressor_num_visited",
              "ep_item_eviction_age_percentage",
              "ep_item_eviction_freq_counter_age_threshold",
              "ep_item_freq_decayer_chunk_duration",
              "ep_item_freq_decayer_percent",
              "ep_item_num",
              "ep_item_num_based_new_chk",
              "ep_items_expelled_from_checkpoints",
              "ep_items_rm_from_checkpoints",
              "ep_kv_size",
              "ep_max_num_bgfetchers",
              "ep_max_num_flushers",
              "ep_max_checkpoints",
              "ep_max_failover_entries",
              "ep_max_item_privileged_bytes",
              "ep_max_item_size",
              "ep_max_num_shards",
              "ep_max_num_workers",
              "ep_checkpoint_remover_task_count",
              "ep_max_size",
              "ep_max_threads",
              "ep_max_ttl",
              "ep_max_vbuckets",
              "ep_mem_high_wat",
              "ep_mem_high_wat_percent",
              "ep_mem_low_wat",
              "ep_mem_low_wat_percent",
              "ep_mem_tracker_enabled",
              "ep_mem_used_merge_threshold_percent",
              "ep_checkpoint_memory",
              "ep_checkpoint_memory_overhead_allocator",
              "ep_ht_item_memory",
              "ep_meta_data_memory",
              "ep_meta_data_disk",
              "ep_min_compression_ratio",
              "ep_mutation_mem_threshold",
              "ep_num_access_scanner_runs",
              "ep_num_access_scanner_skips",
              "ep_num_auxio_threads",
              "ep_num_eject_failures",
              "ep_num_expiry_pager_runs",
              "ep_num_freq_decayer_runs",
              "ep_num_non_resident",
              "ep_num_nonio_threads",
              "ep_num_not_my_vbuckets",
              "ep_num_ops_del_meta",
              "ep_num_ops_del_meta_res_fail",
              "ep_num_ops_del_ret_meta",
              "ep_num_ops_get_meta",
              "ep_num_ops_get_meta_on_set_meta",
              "ep_num_ops_set_meta",
              "ep_num_ops_set_meta_res_fail",
              "ep_num_ops_set_ret_meta",
              "ep_num_pager_runs",
              "ep_num_reader_threads",
              "ep_num_value_ejects",
              "ep_num_workers",
              "ep_num_writer_threads",
              "ep_magma_sync_every_batch",
              "ep_magma_checkpoint_interval",
              "ep_magma_min_checkpoint_interval",
              "ep_magma_checkpoint_threshold",
              "ep_magma_delete_frag_ratio",
              "ep_magma_delete_memtable_writecache",
              "ep_magma_enable_block_cache",
              "ep_magma_enable_direct_io",
              "ep_magma_enable_group_commit",
              "ep_magma_enable_upsert",
              "ep_magma_expiry_frag_threshold",
              "ep_magma_fragmentation_percentage",
              "ep_magma_flusher_thread_percentage",
              "ep_magma_group_commit_max_sync_wait_duration_ms",
              "ep_magma_group_commit_max_transaction_count",
              "ep_magma_heartbeat_interval",
              "ep_magma_max_checkpoints",
              "ep_magma_max_default_storage_threads",
              "ep_magma_max_level_0_ttl",
              "ep_magma_max_recovery_bytes",
              "ep_magma_max_write_cache",
              "ep_magma_mem_quota_ratio",
              "ep_magma_mem_quota_low_watermark_ratio",
              "ep_magma_value_separation_size",
              "ep_magma_initial_wal_buffer_size",
              "ep_magma_write_cache_ratio",
              "ep_magma_expiry_purger_interval",
              "ep_magma_enable_wal",
              "ep_magma_bloom_filter_accuracy",
              "ep_magma_bloom_filter_accuracy_for_bottom_level",
              "ep_oom_errors",
              "ep_overhead",
              "ep_pager_active_vb_pcnt",
              "ep_pager_sleep_time_ms",
              "ep_pending_compactions",
              "ep_pending_ops",
              "ep_pending_ops_max",
              "ep_pending_ops_max_duration",
              "ep_pending_ops_total",
              "ep_persist_vbstate_total",
              "ep_pitr_enabled",
              "ep_pitr_granularity",
              "ep_pitr_max_history_age",
              "ep_queue_size",
              "ep_replica_ahead_exceptions",
              "ep_replica_behind_exceptions",
              "ep_replica_datatype_json",
              "ep_replica_datatype_json,xattr",
              "ep_replica_datatype_raw",
              "ep_replica_datatype_snappy",
              "ep_replica_datatype_snappy,json",
              "ep_replica_datatype_snappy,json,xattr",
              "ep_replica_datatype_snappy,xattr",
              "ep_replica_datatype_xattr",
              "ep_replica_hlc_drift",
              "ep_replica_hlc_drift_count",
              "ep_replication_throttle_threshold",
              "ep_retain_erroneous_tombstones",
              "ep_rocksdb_options",
              "ep_rocksdb_cf_options",
              "ep_rocksdb_bbt_options",
              "ep_rocksdb_low_pri_background_threads",
              "ep_rocksdb_high_pri_background_threads",
              "ep_rocksdb_stats_level",
              "ep_rocksdb_block_cache_ratio",
              "ep_rocksdb_block_cache_high_pri_pool_ratio",
              "ep_rocksdb_memtables_ratio",
              "ep_rocksdb_default_cf_optimize_compaction",
              "ep_rocksdb_seqno_cf_optimize_compaction",
              "ep_rocksdb_write_rate_limit",
              "ep_rocksdb_uc_max_size_amplification_percent",
              "ep_rollback_count",
              "ep_seqno_persistence_timeout",
              "ep_startup_time",
              "ep_storedval_num",
              "ep_storedval_overhead",
              "ep_storedval_size",
              "ep_sync_writes_max_allowed_replicas",
              "ep_time_synchronization",
              "ep_tmp_oom_errors",
              "ep_total_cache_size",
              "ep_total_deduplicated",
              "ep_total_del_items",
              "ep_total_enqueued",
              "ep_total_new_items",
              "ep_uuid",
              "ep_value_size",
              "ep_vb_total",
              "ep_vbucket_del",
              "ep_vbucket_del_fail",
              "ep_vbucket_mapping_sanity_checking",
              "ep_vbucket_mapping_sanity_checking_error_mode",
              "ep_warmup_batch_size",
              "ep_warmup_min_items_threshold",
              "ep_warmup_min_memory_threshold",
              "ep_workload_pattern",
              "ep_xattr_enabled",
              "mem_used",
              "mem_used_primary",
              "mem_used_secondary",
              "mem_used_estimate",
              "rollback_item_count",
              "vb_active_bloom_filter_memory",
              "vb_active_curr_items",
              "vb_active_eject",
              "vb_active_expired",
              "vb_active_hp_vb_req_size",
              "vb_active_ht_memory",
              "vb_active_ht_item_memory",
              "vb_active_ht_item_memory_uncompressed",
              "vb_active_meta_data_disk",
              "vb_active_meta_data_memory",
              "vb_active_checkpoint_memory",
              "vb_active_checkpoint_memory_queue",
              "vb_active_checkpoint_memory_overhead_allocator",
              "vb_active_checkpoint_memory_overhead_allocator_index",
              "vb_active_checkpoint_memory_overhead_allocator_index_key",
              "vb_active_checkpoint_memory_overhead_allocator_queue",
              "vb_active_checkpoint_memory_overhead",
              "vb_active_checkpoint_memory_overhead_queue",
              "vb_active_checkpoint_memory_overhead_index",
              "vb_active_mem_freed_by_checkpoint_item_expel",
              "vb_active_mem_freed_by_checkpoint_removal",
              "vb_active_num",
              "vb_active_num_non_resident",
              "vb_active_ops_create",
              "vb_active_ops_delete",
              "vb_active_ops_get",
              "vb_active_ops_reject",
              "vb_active_ops_update",
              "vb_active_perc_mem_resident",
              "vb_active_queue_age",
              "vb_active_queue_drain",
              "vb_active_queue_fill",
              "vb_active_queue_memory",
              "vb_active_queue_pending",
              "vb_active_queue_size",
              "vb_active_rollback_item_count",
              "vb_active_sync_write_accepted_count",
              "vb_active_sync_write_committed_count",
              "vb_active_sync_write_aborted_count",
              "vb_dead_num",
              "vb_pending_bloom_filter_memory",
              "vb_pending_curr_items",
              "vb_pending_eject",
              "vb_pending_expired",
              "vb_pending_hp_vb_req_size",
              "vb_pending_ht_memory",
              "vb_pending_ht_item_memory",
              "vb_pending_ht_item_memory_uncompressed",
              "vb_pending_meta_data_disk",
              "vb_pending_meta_data_memory",
              "vb_pending_checkpoint_memory",
              "vb_pending_checkpoint_memory_queue",
              "vb_pending_checkpoint_memory_overhead_allocator",
              "vb_pending_checkpoint_memory_overhead_allocator_index",
              "vb_pending_checkpoint_memory_overhead_allocator_index_key",
              "vb_pending_checkpoint_memory_overhead_allocator_queue",
              "vb_pending_checkpoint_memory_overhead",
              "vb_pending_checkpoint_memory_overhead_queue",
              "vb_pending_checkpoint_memory_overhead_index",
              "vb_pending_mem_freed_by_checkpoint_item_expel",
              "vb_pending_mem_freed_by_checkpoint_removal",
              "vb_pending_num",
              "vb_pending_num_non_resident",
              "vb_pending_ops_create",
              "vb_pending_ops_delete",
              "vb_pending_ops_get",
              "vb_pending_ops_reject",
              "vb_pending_ops_update",
              "vb_pending_perc_mem_resident",
              "vb_pending_queue_age",
              "vb_pending_queue_drain",
              "vb_pending_queue_fill",
              "vb_pending_queue_memory",
              "vb_pending_queue_pending",
              "vb_pending_queue_size",
              "vb_pending_rollback_item_count",
              "vb_replica_bloom_filter_memory",
              "vb_replica_curr_items",
              "vb_replica_eject",
              "vb_replica_expired",
              "vb_replica_hp_vb_req_size",
              "vb_replica_ht_memory",
              "vb_replica_ht_item_memory",
              "vb_replica_ht_item_memory_uncompressed",
              "vb_replica_meta_data_disk",
              "vb_replica_meta_data_memory",
              "vb_replica_checkpoint_memory",
              "vb_replica_checkpoint_memory_queue",
              "vb_replica_checkpoint_memory_overhead_allocator",
              "vb_replica_checkpoint_memory_overhead_allocator_index",
              "vb_replica_checkpoint_memory_overhead_allocator_index_key",
              "vb_replica_checkpoint_memory_overhead_allocator_queue",
              "vb_replica_checkpoint_memory_overhead",
              "vb_replica_checkpoint_memory_overhead_queue",
              "vb_replica_checkpoint_memory_overhead_index",
              "vb_replica_mem_freed_by_checkpoint_item_expel",
              "vb_replica_mem_freed_by_checkpoint_removal",
              "vb_replica_num",
              "vb_replica_num_non_resident",
              "vb_replica_ops_create",
              "vb_replica_ops_delete",
              "vb_replica_ops_get",
              "vb_replica_ops_reject",
              "vb_replica_ops_update",
              "vb_replica_perc_mem_resident",
              "vb_replica_queue_age",
              "vb_replica_queue_drain",
              "vb_replica_queue_fill",
              "vb_replica_queue_memory",
              "vb_replica_queue_pending",
              "vb_replica_queue_size",
              "vb_replica_rollback_item_count",
              "vb_replica_sync_write_accepted_count",
              "vb_replica_sync_write_committed_count",
              "vb_replica_sync_write_aborted_count"}},
            {"memory",
             {"bytes",
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || \
        !defined(HAVE_JEMALLOC)
              "ep_arena:allocated",
              "ep_arena_global:allocated",
              "ep_arena_global_missing_some_keys",
              "ep_arena_missing_some_keys",
#else
              "ep_arena:allocated",
              "ep_arena:arena",
              "ep_arena:base",
              "ep_arena:fragmentation_size",
              "ep_arena:internal",
              "ep_arena:large.allocated",
              "ep_arena:mapped",
              "ep_arena:resident",
              "ep_arena:retained",
              "ep_arena:small.allocated",
              "ep_arena_global:allocated",
              "ep_arena_global:arena",
              "ep_arena_global:base",
              "ep_arena_global:fragmentation_size",
              "ep_arena_global:internal",
              "ep_arena_global:large.allocated",
              "ep_arena_global:mapped",
              "ep_arena_global:resident",
              "ep_arena_global:retained",
              "ep_arena_global:small.allocated",
#endif
              "ep_blob_num",
              "ep_blob_overhead",
              "ep_item_num",
              "ep_kv_size",
              "ep_max_size",
              "ep_mem_high_wat",
              "ep_mem_high_wat_percent",
              "ep_mem_low_wat",
              "ep_mem_low_wat_percent",
              "ep_mem_used_primary",
              "ep_mem_used_secondary",
              "ep_oom_errors",
              "ep_overhead",
              "ep_storedval_num",
              "ep_storedval_overhead",
              "ep_storedval_size",
              "ep_tmp_oom_errors",
              "ep_value_size",
              "ht_mem_used_replica",
              "mem_used",
              "mem_used_estimate",
              "mem_used_merge_threshold",
              "replica_checkpoint_memory_overhead"}}};

    if (isWarmupEnabled(h)) {
        // Add stats which are only available if warmup is enabled:
        auto& eng_stats = statsKeys.at("");
        eng_stats.insert(eng_stats.end(), {"ep_warmup_dups",
                                           "ep_warmup_oom",
                                           "ep_warmup_time",
                                           "ep_warmup_thread"});
    }

    if (isCompressionEnabled(h)) {
        auto& vb0_hash_stats = statsKeys.at("hash");
        vb0_hash_stats.insert(vb0_hash_stats.end(),
                              {"vb_0:mem_size_uncompressed"});
    }

    if (isPersistentBucket(h)) {
        // Add data_size and file_size stats to toplevel group.
        auto& eng_stats = statsKeys.at("");

        eng_stats.insert(
                eng_stats.end(),
                {"ep_db_data_size", "ep_db_file_size", "ep_db_prepare_size"});
        // Using explicit initializer lists due to http://stackoverflow
        // .com/questions/36557969/invalid-iterator-range-while-inserting
        // -initializer-list-to-an-stdvector
        eng_stats.insert(eng_stats.end(),
                         std::initializer_list<std::string_view>{
                                 "ep_flusher_state", "ep_flusher_todo"});
        eng_stats.insert(eng_stats.end(),
                         {"ep_commit_num",
                          "ep_commit_time",
                          "ep_commit_time_total",
                          "ep_item_begin_failed",
                          "ep_item_commit_failed",
                          "ep_item_flush_expired",
                          "ep_item_flush_failed",
                          "ep_total_persisted",
                          "ep_uncommitted_items",
                          "ep_compaction_failed",
                          "ep_compaction_aborted"});

        // Config variables only valid for persistent
        std::initializer_list<std::string_view> persistentConfig = {
                "ep_access_scanner_enabled",
                "ep_alog_block_size",
                "ep_alog_max_stored_items",
                "ep_alog_path",
                "ep_alog_resident_ratio_threshold",
                "ep_alog_sleep_time",
                "ep_alog_task_time",
                "ep_item_eviction_policy",
                "ep_persistent_metadata_purge_age",
                "ep_warmup"};
        eng_stats.insert(eng_stats.end(), persistentConfig);

        // 'diskinfo and 'diskinfo detail' keys should be present now.
        statsKeys["diskinfo"] = {
                "ep_db_data_size", "ep_db_file_size", "ep_db_prepare_size"};
        statsKeys["diskinfo detail"] = {
                "vb_0:data_size", "vb_0:file_size", "vb_0:prepare_size"};

        // Add stats which are only available for persistent buckets:
        std::initializer_list<std::string_view> persistence_stats = {
                "vb_0:persistence:cursor_checkpoint_id",
                "vb_0:persistence:cursor_seqno",
                "vb_0:persistence:num_visits",
                "vb_0:num_items_for_persistence"};
        auto& ckpt_stats = statsKeys.at("checkpoint");
        ckpt_stats.insert(ckpt_stats.end(), persistence_stats);
        auto& ckpt0_stats = statsKeys.at("checkpoint 0");
        ckpt0_stats.insert(ckpt0_stats.end(), persistence_stats);

        auto& vb_details = statsKeys.at("vbucket-details 0");
        vb_details.insert(vb_details.end(),
                          {"vb_0:db_data_size",
                           "vb_0:db_file_size",
                           "vb_0:db_prepare_size"});

        // Config variables only valid for persistent
        auto& config_stats = statsKeys.at("config");
        config_stats.insert(config_stats.end(), persistentConfig);
    }

    if (isEphemeralBucket(h)) {
        auto& eng_stats = statsKeys.at("");
        eng_stats.insert(eng_stats.end(),
                         {"ep_ephemeral_full_policy",
                          "ep_ephemeral_metadata_mark_stale_chunk_duration",
                          "ep_ephemeral_metadata_purge_age",
                          "ep_ephemeral_metadata_purge_interval",
                          "ep_ephemeral_metadata_purge_stale_chunk_duration",

                          "vb_active_auto_delete_count",
                          "vb_active_ht_tombstone_purged_count",
                          "vb_active_seqlist_count",
                          "vb_active_seqlist_deleted_count",
                          "vb_active_seqlist_purged_count",
                          "vb_active_seqlist_read_range_count",
                          "vb_active_seqlist_stale_count",
                          "vb_active_seqlist_stale_value_bytes",
                          "vb_active_seqlist_stale_metadata_bytes",

                          "vb_replica_auto_delete_count",
                          "vb_replica_ht_tombstone_purged_count",
                          "vb_replica_seqlist_count",
                          "vb_replica_seqlist_deleted_count",
                          "vb_replica_seqlist_purged_count",
                          "vb_replica_seqlist_read_range_count",
                          "vb_replica_seqlist_stale_count",
                          "vb_replica_seqlist_stale_value_bytes",
                          "vb_replica_seqlist_stale_metadata_bytes",

                          "vb_pending_auto_delete_count",
                          "vb_pending_ht_tombstone_purged_count",
                          "vb_pending_seqlist_count",
                          "vb_pending_seqlist_deleted_count",
                          "vb_pending_seqlist_purged_count",
                          "vb_pending_seqlist_read_range_count",
                          "vb_pending_seqlist_stale_count",
                          "vb_pending_seqlist_stale_value_bytes",
                          "vb_pending_seqlist_stale_metadata_bytes"});

        auto& vb_details = statsKeys.at("vbucket-details 0");
        vb_details.insert(vb_details.end(),
                          {"vb_0:auto_delete_count",
                           "vb_0:ht_tombstone_purged_count",
                           "vb_0:seqlist_count",
                           "vb_0:seqlist_deleted_count",
                           "vb_0:seqlist_high_seqno",
                           "vb_0:seqlist_purged_count",
                           "vb_0:seqlist_range_read_begin",
                           "vb_0:seqlist_range_read_count",
                           "vb_0:seqlist_range_read_end",
                           "vb_0:seqlist_stale_count",
                           "vb_0:seqlist_stale_metadata_bytes",
                           "vb_0:seqlist_stale_value_bytes"});

        auto& config_stats = statsKeys.at("config");
        config_stats.insert(
                config_stats.end(),
                {"ep_ephemeral_full_policy",
                 "ep_ephemeral_metadata_mark_stale_chunk_duration",
                 "ep_ephemeral_metadata_purge_age",
                 "ep_ephemeral_metadata_purge_interval",
                 "ep_ephemeral_metadata_purge_stale_chunk_duration"});
    }

    // In addition to the exact stat keys above, we also use regex patterns
    // for variable keys:
    std::map<std::string_view, std::vector<std::regex> > statsPatterns{
            {"hash", {std::regex{"vb_0:histo_\\d+,\\d+"}}},
            {"kvtimings",
             {std::regex{"ro_[0-3]:readTime_\\d+,\\d+"},
              std::regex{"ro_[0-3]:readSize_\\d+,\\d+"},
              std::regex{"rw_[0-3]:readTime_\\d+,\\d+"},
              std::regex{"rw_[0-3]:readSize_\\d+,\\d+"}}}};

    bool error = false;
    for (auto& entry : statsKeys) {
        // Fetch the statistics for each group. Note we need an owning
        // type (std::string) here as the stats callback requires callers
        // to take a copy of key/value if they require it after the callback
        // returns.
        std::vector<std::string> actual;
        checkeq(cb::engine_errc::success,
                get_stats(h,
                          {entry.first.empty() ? nullptr : entry.first.data(),
                           entry.first.size()},
                          {},
                          [&actual](auto key, auto value, auto ctx) {
                              actual.emplace_back(key);
                          }),
                ("Failed to get stats: "s + std::string{entry.first}).c_str());

        // Sort the keys from the fetched stats and expected keys (required
        // for set_difference).
        std::sort(actual.begin(), actual.end());

        auto& expected = entry.second;
        std::sort(expected.begin(), expected.end());

        // (A) Find any missing stats - those expected (in statsKeys) but not
        // found in actual.
        std::vector<std::string_view> missing;
        std::set_difference(expected.begin(),
                            expected.end(),
                            actual.begin(),
                            actual.end(),
                            std::inserter(missing, missing.begin()));

        for (const auto& key : missing) {
            error = true;
            fprintf(stderr,
                    "Missing stat:  %s from stat group %s\n",
                    std::string{key}.c_str(),
                    std::string{entry.first}.c_str());
        }

        // (B) Find any extra stats - those in actual which are not in expected.
        std::vector<std::string_view> extra;
        std::set_difference(actual.begin(),
                            actual.end(),
                            expected.begin(),
                            expected.end(),
                            std::inserter(extra, extra.begin()));

        for (const auto& key : extra) {
            // We have extra key(s) which don't exactly match `expected`; see if
            // there's a regex which matches before
            // reporting an error.
            bool matched = false;
            const auto& group = entry.first;
            const auto patterns = statsPatterns.find(group);
            if (patterns != statsPatterns.end()) {
                // We have regex(s), see if any match.
                for (const auto& pattern : patterns->second) {
                    if (std::regex_match(std::string{key}, pattern)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                error = true;
                fprintf(stderr,
                        "Unexpected stat: %s from stat group %s\n",
                        std::string{key}.c_str(),
                        std::string(entry.first).c_str());
            }
        }
    }

    if (error) {
        abort_msg("missing stats", "stats error", __FILE__, __LINE__);
    }

    return SUCCESS;
}

// Regression test the stats calls that they don't blow the snprintf
// buffers. All of the tests in this batch make sure that some of the stats
// exists (the server may return more)
static enum test_result test_mb19687_variable(EngineIface* h) {
    // all of these should be const, but g++ seems to have problems with that
    std::map<std::string, std::vector<std::string> > statsKeys{
            {"dispatcher", {}}, // Depends on how how long the dispatcher ran..
            {"key mykey 0",
             {"key_cas",
              "key_exptime",
              "key_flags",
              "key_is_dirty",
              "key_vb_state"}},

            // These stat groups return histograms so we can't guess the
            // key names...
            {"timings",
             // ... apart from the means of hdrhistogram-derived metrics
             {"get_stats_cmd_mean",
              "item_alloc_sizes_mean",
              "set_vb_cmd_mean",
              "store_cmd_mean"}},
            {"scheduler", {}},
            {"runtimes", {}},
            {"kvtimings", {}},
    };

    if (isWarmupEnabled(h)) {
        statsKeys.insert( { "warmup", { "ep_warmup",
                                        "ep_warmup_state",
                                        "ep_warmup_thread",
                                        "ep_warmup_key_count",
                                        "ep_warmup_value_count",
                                        "ep_warmup_dups",
                                        "ep_warmup_oom",
                                        "ep_warmup_min_memory_threshold",
                                        "ep_warmup_min_item_threshold",
                                        "ep_warmup_estimated_key_count",
                                        "ep_warmup_estimated_value_count" } });
    }

    if (isPersistentBucket(h)) {
        statsKeys.insert({"vkey mykey 0",
                          {"key_cas",
                           "key_exptime",
                           "key_flags",
                           "key_is_dirty",
                           "key_valid",
                           "key_vb_state"}});
    }

    item_info info;

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "mykey", "data1"),
            "Failed to store an item");

    bool error = false;
    for (const auto& entry : statsKeys) {
        vals.clear();
        checkeq(cb::engine_errc::success,
                get_stats(h,
                          {entry.first.data(), entry.first.size()},
                          {},
                          add_stats),
                ("Failed to get stats: "s + entry.first).c_str());

        // Verify that the stats we expected is there..
        for (const auto& key : entry.second) {
            auto iter = vals.find(key);
            if (iter == vals.end()) {
                error = true;
                fprintf(stderr, "Missing stat:  %s from stat group %s\n",
                        key.c_str(),
                        entry.first.c_str());
            }
        }
    }

    if (error) {
        abort_msg("missing stats", "stats error", __FILE__, __LINE__);
    }

    return SUCCESS;
}

static enum test_result test_mb20697(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    std::string dbname = vals["ep_dbname"];

    // Make the couchstore files in the db directory unwritable.
    CouchstoreFileAccessGuard makeCouchstoreFileReadOnly(dbname);

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "store should have succeeded");

    /* Ensure that this results in commit failure and the stat gets incremented */
    wait_for_stat_change(h, "ep_item_commit_failed", 0);

    return SUCCESS;
}

/* Check if vbucket reject ops are incremented on persistence failure */
static enum test_result test_mb20744_check_incr_reject_ops(EngineIface* h) {
    std::string dbname = get_dbname(testHarness->get_current_testcase()->cfg);
    std::string filename = dbname + cb::io::DirectorySeparator + "0.couch.1";

    /* corrupt the couchstore file */
    FILE* fp = fopen(filename.c_str(), "wb");

    if (fp == nullptr) {
        return FAIL;
    }

    char buf[2048];
    memset(buf, 'x', sizeof(buf));

    size_t numBytes = fwrite(buf, sizeof(char), sizeof(buf), fp);

    fflush(fp);

    checkeq(size_t{2048}, numBytes, "Bytes written should be equal to 2048");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "store should have succeeded");

    wait_for_stat_change(h, "vb_active_ops_reject", 0);

    checkne(0,
            get_int_stat(h, "vb_0:ops_reject", "vbucket-details 0"),
            "Expected rejected ops to not be 0");

    fclose(fp);

    cb::io::rmrf(filename.c_str());

    cb::io::mkdirp(dbname);

    return SUCCESS;
}

static enum test_result test_mb20943_complete_pending_ops_on_vbucket_delete(
        EngineIface* h) {
    auto* cookie = testHarness->create_cookie(h);
    bool  ready = false;
    std::mutex m;
    std::condition_variable cv;

    check(set_vbucket_state(h, Vbid(1), vbucket_state_pending),
          "Failed to set vbucket state.");
    testHarness->set_ewouldblock_handling(cookie, false);

    checkeq(cb::engine_errc::would_block,
            get(h, cookie, "key", Vbid(1)).first,
            "Expected EWOULDBLOCK.");

    // Create a thread that will wait for the cookie notify.
    std::thread notify_waiter{[&cv, &ready, &m, &cookie](){
        {
            std::lock_guard<std::mutex> lk(m);
            testHarness->lock_cookie(cookie);
            ready = true;
        }
        // Once we have locked the cookie we can allow the main thread to
        // continue.
        cv.notify_one();
        testHarness->waitfor_cookie(cookie);
        testHarness->unlock_cookie(cookie);

    }};

    std::unique_lock<std::mutex> lk(m);
    // Wait until spawned thread has locked the cookie.
    cv.wait(lk, [&ready]{return ready;});
    lk.unlock();
    checkeq(cb::engine_errc::success,
            vbucketDelete(h, Vbid(1)),
            "Expected success");
    // Wait for the thread to finish, which will occur when the thread has been
    // notified.
    notify_waiter.join();

    // vbucket no longer exists and therefore should return not my vbucket.
    checkeq(cb::engine_errc::not_my_vbucket,
            get(h, cookie, "key", Vbid(1)).first,
            "Expected NOT MY VBUCKET.");
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

/* This test case checks the purge seqno validity when no items are actually
   purged in a compaction call */
static enum test_result test_vbucket_compact_no_purge(EngineIface* h) {
    const int num_items = 2;
    const char* key[num_items] = {"k1", "k2"};
    const char* value = "somevalue";

    /* Write 2 keys */
    for (const auto& count : key) {
        checkeq(cb::engine_errc::success,
                store(h, nullptr, StoreSemantics::Set, count, value),
                "Error setting.");
    }

    /* Delete one key */
    checkeq(cb::engine_errc::success,
            del(h, key[0], 0, Vbid(0)),
            "Failed remove with value.");

    /* Store a dummy item since we do not purge the item with highest seqno */
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "dummy_key", value),
            "Error setting.");
    wait_for_flusher_to_settle(h);

    /* Compact once */
    int exp_purge_seqno =
            get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno") - 1;
    compact_db(h,
               Vbid(0),
               get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno"),
               1,
               1);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(exp_purge_seqno,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    /* Compact again, this time we don't expect to purge any items */
    compact_db(h,
               Vbid(0),
               get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno"),
               1,
               1);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(exp_purge_seqno,
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value after another compaction");

    if (isWarmupEnabled(h)) {
        /* Reload the engine */
        testHarness->reload_engine(&h,
                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   false);

        wait_for_warmup_complete(h);

        /* Purge seqno should not change after reload */
        checkeq(exp_purge_seqno,
                get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
                "purge_seqno didn't match expected value after reload");
    }
    return SUCCESS;
}

/**
 * Test that the DocumentState passed in get is properly handled
 */
static enum test_result test_mb23640(EngineIface* h) {
    const std::string key{"mb-23640"};
    const std::string value{"my value"};
    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         key.c_str(),
                         value.data(),
                         value.size(),
                         0,
                         0,
                         Vbid(0),
                         0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         DocumentState::Alive)
                    .first,
            "Unable to store item");

    // I should be able to get the key if I ask for anything which
    // includes Alive
    checkeq(cb::engine_errc::success,
            get(h, nullptr, key, Vbid(0), DocStateFilter::Alive).first,
            "Failed to get the document when specifying Alive");

    checkeq(cb::engine_errc::success,
            get(h, nullptr, key, Vbid(0), DocStateFilter::AliveOrDeleted).first,
            "Failed to get the document when specifying dead or alive");

    // ep-engine don't support fetching deleted only
    checkeq(cb::engine_errc::not_supported,
            get(h, nullptr, key, Vbid(0), DocStateFilter::Deleted).first,
            "AFAIK ep-engine don't support fetching only deleted items");

    // Delete the document
    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         key.c_str(),
                         value.data(),
                         value.size(),
                         0,
                         0,
                         Vbid(0),
                         0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         DocumentState::Deleted)
                    .first,
            "Unable to delete item");

    // I should be able to get the key if I ask for anything which
    // includes Deleted
    checkeq(cb::engine_errc::not_supported,
            get(h, nullptr, key, Vbid(0), DocStateFilter::Deleted).first,
            "AFAIK ep-engine don't support fetching only deleted items");

    checkeq(cb::engine_errc::success,
            get(h, nullptr, key, Vbid(0), DocStateFilter::AliveOrDeleted).first,
            "Failed to get the deleted document when specifying dead or alive");

    // It should _not_ be found if I ask for a deleted document
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, key, Vbid(0), DocStateFilter::Alive).first,
            "Expected the document to be gone");
    return SUCCESS;
}

static enum test_result test_replace_at_pending_insert(EngineIface* h) {
    auto* cookie1 = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie1, false);
    auto* cookie2 = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie2, false);

    const auto vbid = Vbid(0);

    // Add a replica in topology. By doing that, no SW will ever be Committed
    const char meta[] = R"({"topology":[["active", "replica"]]})";
    check(set_vbucket_state(h, vbid, vbucket_state_active, {meta}),
          "set_vbucket_state failed");

    const auto statSet =
            "vbucket-durability-state " + std::to_string(vbid.get());
    const auto vbPrefix = "vb_" + std::to_string(vbid.get()) + ":";

    const auto checkHPSAndHighSeqno =
            [h, &statSet, &vbPrefix](int expectedHPS, int expectedHS) -> void {
        const auto durStats = get_all_stats(h, statSet.c_str());
        checkeq(expectedHPS,
                std::stoi(durStats.at(vbPrefix + "high_prepared_seqno")),
                "HPS must be 0");
        checkeq(expectedHS,
                std::stoi(durStats.at(vbPrefix + "high_seqno")),
                "high_seqno must be 0");
    };

    // No doc around
    checkHPSAndHighSeqno(0, 0);

    // The insert blocks as durability requirements cannot be satisfied with
    // a single node, so the write will stay pending. Note that we provide an
    // infinite timeout, so the write will never abort either.
    using namespace cb::durability;
    const char key[] = "key";
    checkeq(cb::engine_errc::would_block,
            store(h,
                  cookie1,
                  StoreSemantics::Add,
                  key,
                  "add-value",
                  nullptr,
                  0 /*cas*/,
                  vbid,
                  3600 /*exp*/,
                  0 /*datatype*/,
                  DocumentState::Alive,
                  {{Level::Majority, Timeout::Infinity()}}),
            "durable add failed");

    // Prepare pending
    checkHPSAndHighSeqno(1, 1);

    // Simulate replace on a second connection
    checkeq(cb::engine_errc::no_such_key,
            replace(h, cookie2, key, "replace-value", 0 /*flags*/, vbid),
            "replace failed");

    // Prepare still pending, high-seqno not increased as replace rejected
    checkHPSAndHighSeqno(1, 1);

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie2);

    return SUCCESS;
}

/**
 * Test to ensure that larger buckets don't starve smaller buckets of run time
 * on the reader threads during warmup, as this can cause artificially long
 * warmup times.
 */
static test_result test_reader_thread_starvation_warmup(EngineIface* h) {
    const size_t keysPerVbucket = 1500;
    const size_t numberOfKeyVbucketSmall = 1;

    // 1. Set up second bucket to be which will be smaller than the default
    std::string smallBucketName("smallBucket");
    auto smallBucketDir = cb::io::mkdtemp(smallBucketName + "XXXXXX");
    auto smallBucketConf = testHarness->get_current_testcase()->cfg +
                           "couch_bucket=" + smallBucketName +
                           ";dbname=" + smallBucketDir + ".db;";
    auto* smallBucket = testHarness->create_bucket(true, smallBucketConf);
    test_setup(smallBucket);
    auto* slowBucket = h;

    // 2. Write keys to
    const std::string keyBase("key-");
    Vbid vb(0);
    check(set_vbucket_state(slowBucket, vb, vbucket_state_active),
          "Failed to set vbucket state for vb");
    write_items(slowBucket, keysPerVbucket, 0, keyBase.c_str(), "value", 0, vb);

    check(set_vbucket_state(smallBucket, vb, vbucket_state_active),
          "Failed to set vbucket state for vb");
    write_items(smallBucket,
                numberOfKeyVbucketSmall,
                0,
                keyBase.c_str(),
                "value",
                0,
                vb);
    // 3. Ensure all documents have been written to disk
    wait_for_flusher_to_settle(smallBucket);
    wait_for_flusher_to_settle(slowBucket);

    // 4. Destroy the buckets in memory so we can perform warmup
    testHarness->destroy_bucket(slowBucket, false);
    testHarness->destroy_bucket(smallBucket, false);

    // 5. Start warming up the slow bucket first
    ThreadGate tg(2);
    {
        // Create the in memory engine but don't kick of initialization
        slowBucket = testHarness->create_bucket(false, "");
        auto* me = dynamic_cast<MockEngine*>(slowBucket);
        // add a test hook which will slow down the warmup of the bucket
        dynamic_cast<EventuallyPersistentEngine*>(me->the_engine.get())
                ->hangWarmupHook = [&tg]() -> void {
            using namespace std::chrono_literals;
            // Block the backfill until we can read the stats of the
            // small bucket
            tg.threadUp();
        };
        // start warmup
        checkeq(me->the_engine->initialize(
                        testHarness->get_current_testcase()->cfg),
                cb::engine_errc::success,
                "Init of bucket did not succeed");
    }
    // 6. Create and warmup the small bucket
    smallBucket = testHarness->create_bucket(true, smallBucketConf);

    // 7. Ensure we can get stats of the vbucket state during warmup
    checkeq(get_str_stat(smallBucket, "vb_0", "vbucket"),
            std::string("active"),
            "vbucket state isn't active");
    // 8. Ensure of the slow bucket is still warming up
    checkne(get_str_stat(slowBucket, "ep_warmup_thread", "warmup"),
            std::string("complete"),
            "Slow bucket completed before the fast bucket");
    // Small bucket stats are available, unblock the slow bucket warmup
    tg.threadUp();
    // 9. Wait for the small bucket is warmup
    wait_for_warmup_complete(smallBucket);
    // 10. Wait for the slow bucket to warmup
    wait_for_warmup_complete(slowBucket);
    // 11. Ensure all buckets have the correct count
    verify_curr_items(smallBucket, numberOfKeyVbucketSmall, "after warmup");
    verify_curr_items(slowBucket, keysPerVbucket, "after warmup");
    // 12. Ensure the buckets are destroyed and shutdown at the end of the test
    testHarness->destroy_bucket(smallBucket, true);
    cb::io::rmrf(smallBucketDir);
    testHarness->destroy_bucket(slowBucket, true);
    return SUCCESS;
}

/**
 * Verify that a SyncWrite which is not committed within its durability timeout
 * is aborted.
 * Tests at the engine level, ensuring the background task runs and identifies
 * the SyncWrite has exceeded timeout.
 */
static enum test_result test_sync_write_timeout(EngineIface* h) {
    auto* cookie1 = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie1, false);

    const auto vbid = Vbid(0);

    // Add a replica in topology. By doing that, no SW will ever be Committed
    check(set_vbucket_state(h,
                            vbid,
                            vbucket_state_active,
                            {R"({"topology":[["active", "replica"]]})"}),
          "set_vbucket_state failed");

    // Perform a sequence of SyncWrites with timeouts. Each should wait
    // (return would_block) until at least as long as the specified durability
    // timeout before failing with "sync_write_ambiguous".
    // Issuing >1 to verify that timeout task is correctly re-scheduled after
    // each timeout.
    // The insert should return would_block as durability requirements cannot
    // be satisfied with a single node, so the Sync Write will stay pending
    // until it times out.
    using namespace cb::durability;
    const uint16_t timeous_ms = 10;
    for (int i = 0; i < 3; i++) {
        auto key = std::string("key_") + std::to_string(i);
        const auto start = std::chrono::steady_clock::now();

        checkeq(cb::engine_errc::would_block,
                store(h,
                      cookie1,
                      StoreSemantics::Set,
                      key.c_str(),
                      "add-value",
                      nullptr,
                      0,
                      vbid,
                      0,
                      0,
                      DocumentState::Alive,
                      {{Level::Majority, Timeout{timeous_ms}}}),
                "durable add failed");

        // Wait for the cookie to be notified - should occur no sooner than
        // timeout.
        auto status = mock_waitfor_cookie(cookie1);
        const auto end = std::chrono::steady_clock::now();

        checkeq(cb::engine_errc::sync_write_ambiguous,
                status,
                "SyncWrite with timeout did not return ambiguous");

        checkge(std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                      start)
                        .count(),
                std::chrono::milliseconds{timeous_ms}.count(),
                "SyncWrite was aborted before its durability timeout");
    }

    testHarness->destroy_cookie(cookie1);
    return SUCCESS;
}

// Test that seqnoPersistence times out whilst idle
static enum test_result test_seqno_persistence_timeout(EngineIface* h) {
    auto* cookie1 = testHarness->create_cookie(h);
    const int num_items = 2;
    write_items(h, num_items, 0, "key", "somevalue");
    wait_for_flusher_to_settle(h);

    // Should timeout
    checkeq(cb::engine_errc::temporary_failure,
            seqnoPersistence(h, nullptr, Vbid(0), /*seqno*/ num_items * 2),
            "Expected success for seqno persistence request");
    testHarness->destroy_cookie(cookie1);
    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char* default_dbname = "./ep_testsuite.db";

BaseTestCase testsuite_testcases[] = {
        // ep-engine specific functionality
        TestCase("expiry pager settings",
                 test_expiry_pager_settings,
                 test_setup,
                 teardown,
                 "exp_pager_enabled=false",
                 prepare,
                 cleanup),
        TestCase("expiry",
                 test_expiry,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("expiry with xattr",
                 test_expiry_with_xattr,
                 test_setup,
                 teardown,
                 "exp_pager_enabled=false",
                 prepare,
                 cleanup),
        TestCase("expiry_loader",
                 test_expiry_loader,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("expiration on compaction",
                 test_expiration_on_compaction,
                 test_setup,
                 teardown,
                 "exp_pager_enabled=false",
                 /* TODO RDB: RocksDB doesn't expire items yet */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("expiration on warmup",
                 test_expiration_on_warmup,
                 test_setup,
                 teardown,
                 "exp_pager_stime=1",
                 // TODO RDB: Needs the 'ep_expired_pager' stat
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("expiry_duplicate_warmup",
                 test_bug3454,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: ep_warmup_value_count is wrong */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("expiry_no_items_warmup",
                 test_bug3522,
                 test_setup,
                 teardown,
                 "exp_pager_stime=3",
                 prepare,
                 cleanup),
        TestCase("replica read",
                 test_get_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("replica read: invalid state - active",
                 test_get_replica_active_state,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("replica read: invalid state - pending",
                 test_get_replica_pending_state,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("replica read: invalid state - dead",
                 test_get_replica_dead_state,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("replica read: invalid key",
                 test_get_replica_invalid_key,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test getr with evicted key",
                 test_get_replica_non_resident,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("test observe no data",
                 test_observe_no_data,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe single key",
                 test_observe_single_key,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe on temp item",
                 test_observe_temp_item,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test observe multi key",
                 test_observe_multi_key,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test multiple observes",
                 test_multiple_observes,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe with not found",
                 test_observe_with_not_found,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe not my vbucket",
                 test_observe_errors,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe seqno basic tests",
                 test_observe_seqno_basic_tests,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe seqno failover",
                 test_observe_seqno_failover,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test observe seqno error",
                 test_observe_seqno_error,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test memory condition",
                 test_memory_condition,
                 test_setup,
                 teardown,
                 "max_size=2621440",
                 // TODO RDB: Depending on the configuration, RocksDB
                 // pre-allocates memory in its internal Arena before a DB is
                 // opened, in a way we do not fully control yet.
                 // That makes this test to fail depending on the size of the
                 // pre-allocation.
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("warmup conf",
                 test_warmup_conf,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("itempager conf",
                 test_itempager_conf,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("PiTR conf",
                 test_pitr_conf,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        // magma turns off bloom filters
        TestCase("bloomfilter conf",
                 test_bloomfilter_conf,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_skip_broken_under_magma,
                 cleanup),
        // magma turns off bloom filters
        TestCase("test bloomfilters",
                 test_bloomfilters,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: Fails in full eviction. Rockdb does not report
                 // the correct 'ep_bg_num_samples' stat
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),
        // magma turns off bloom filters
        TestCase("test bloomfilters with store apis",
                 test_bloomfilters_with_store_apis,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket_skip_broken_under_magma,
                 cleanup),
        // magma turns off bloom filters
        TestCase("test bloomfilters's in a delete+set scenario",
                 test_bloomfilter_delete_plus_set_scenario,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket_skip_broken_under_magma,
                 cleanup),
        TestCase("test datatype",
                 test_datatype,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test datatype with unknown command",
                 test_datatype_with_unknown_command,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test access scanner settings",
                 test_access_scanner_settings,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test access scanner",
                 test_access_scanner,
                 test_setup,
                 teardown,
                 // Need to cap at <number of items written, so we create
                 // >1 checkpoint. Also given bucket quota is being
                 // constrained, also limit shards to 4 so amount of memory
                 // overhead is more or less constant.
                 "chk_max_items=500;"
                 "chk_remover_stime=1;"
                 "max_num_shards=4;"
                 "max_size=10000000;checkpoint_memory_recovery_upper_mark=0;"
                 "checkpoint_memory_recovery_lower_mark=0",
                 // TODO RDB: This test requires full control and accurate
                 // tracking on how memory is allocated by the underlying
                 // store. We do not have that yet for RocksDB. Depending
                 // on the configuration, RocksDB pre-allocates default-size
                 // blocks of memory in the internal Arena.
                 // For this specific test, the problem is that we cannot store
                 // all the item we need (cb::engine_errc::no_memory), and the
                 // 'vb_active_perc_mem_resident' stat never goes below the
                 // threshold we expect. Needs to resize 'max_size' to consider
                 // RocksDB pre-allocations.
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test set_param message",
                 test_set_param_message,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_size=6291456",
                 prepare,
                 cleanup),

        TestCase("test warmup oom",
                 test_warmup_oom,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),

        // Stats tests
        TestCase("item stats",
                 test_item_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: vBucket delete stat not correct */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("stats",
                 test_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("io stats",
                 test_io_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("file stats",
                 test_vb_file_stats,
                 test_setup,
                 teardown,
                 "magma_checkpoint_interval=0;"
                 "magma_min_checkpoint_interval=0;"
                 "magma_sync_every_batch=true",
                 /* TODO RDB: Needs stat:ep_db_data_size */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("file stats post warmup",
                 test_vb_file_stats_after_warmup,
                 test_setup,
                 teardown,
                 "magma_checkpoint_interval=0;"
                 "magma_min_checkpoint_interval=0;"
                 "magma_sync_every_batch=true",
                 prepare,
                 cleanup),
        TestCase("bg stats",
                 test_bg_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("bg meta stats",
                 test_bg_meta_stats,
                 test_setup,
                 teardown,
                 "nullptr",
                 prepare_ep_bucket,
                 cleanup),
        TestCase("mem stats",
                 test_mem_stats,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_period=60;checkpoint_memory_recovery_"
                 "upper_mark=0;checkpoint_memory_recovery_lower_mark=0",
                 prepare,
                 cleanup),
        TestCase("stats key",
                 test_key_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("stats key EACCESS",
                 test_key_stats_eaccess,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("stats vkey",
                 test_vkey_stats,
                 test_setup,
                 teardown,
                 "max_vbuckets=5;max_num_shards=4",
                 prepare_ep_bucket,
                 cleanup),
        TestCase("stats vkey callback tests",
                 test_stats_vkey_valid_field,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("warmup stats",
                 test_warmup_stats,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: RocksDB does not report the currect
                 // 'vb_X:num_items' stat
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("warmup with threshold",
                 test_warmup_with_threshold,
                 test_setup,
                 teardown,
                 "warmup_min_items_threshold=1",
                 prepare,
                 cleanup),
        TestCase("seqno stats",
                 test_stats_seqno,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("diskinfo stats",
                 test_stats_diskinfo,
                 test_setup,
                 teardown,
                 "magma_checkpoint_interval=0;"
                 "magma_min_checkpoint_interval=0;"
                 "magma_sync_every_batch=true",
                 /* TODO RDB: DB file size is not reported correctly */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("stats curr_items ADD SET",
                 test_curr_items_add_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("stats curr_items DELETE",
                 test_curr_items_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under Rocks */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("stats curr_items vbucket_state_dead",
                 test_curr_items_dead,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("startup token stat",
                 test_cbd_225,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("ep workload stats",
                 test_workload_stats,
                 test_setup,
                 teardown,
                 "max_num_shards=5;max_threads=10",
                 prepare,
                 cleanup),
        TestCase("ep max workload stats",
                 test_max_workload_stats,
                 test_setup,
                 teardown,
                 "max_num_shards=5;max_threads=14;num_auxio_threads=1;num_"
                 "nonio_threads=4",
                 prepare,
                 cleanup),
        TestCase("test ALL_KEYS api",
                 test_all_keys_api,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: implement RocksDBKVStore::getAllKeys */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test ALL_KEYS api during bucket creation",
                 test_all_keys_api_during_bucket_creation,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("ep worker stats",
                 test_worker_stats,
                 test_setup,
                 teardown,
                 "max_num_workers=8;max_threads=8",
                 prepare,
                 cleanup),

        // eviction
        TestCase("value eviction",
                 test_value_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        // duplicate items on disk
        TestCase("duplicate items on disk",
                 test_duplicate_items_disk,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: evict_key expects "Evicted" but gets
                  * "Already evicted" - possibly caused by stats
                  */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        // special non-Ascii keys
        TestCase("test special char keys",
                 test_specialKeys,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test binary keys",
                 test_binKeys,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        // restart tests
        TestCase("test restart",
                 test_restart,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set+get+restart+hit (bin)",
                 test_restart_bin_val,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test kill -9 bucket",
                 test_kill9_bucket,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test shutdown with force",
                 test_flush_shutdown_force,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test shutdown without force",
                 test_flush_shutdown_noforce,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: implement getItemCount
                 // (needs the 'curr_items' stat)
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test shutdown snapshot range",
                 test_shutdown_snapshot_range,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare,
                 cleanup),

        // it takes 61+ second to finish the following test.
        // TestCase("continue warmup after loading access log",
        //         test_warmup_accesslog,
        //         test_setup, teardown,
        //         "warmup_min_items_threshold=75;alog_path=/tmp/epaccess.log;"
        //         "alog_task_time=0;alog_sleep_time=1",
        //         prepare, cleanup),

        // disk>RAM tests
        TestCase("disk>RAM golden path",
                 test_disk_gt_ram_golden,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_period=60;checkpoint_memory_recovery_"
                 "upper_mark=0;checkpoint_memory_recovery_lower_mark=0",
                 /* TODO RDB: ep_total_persisted not correct under Rocks */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("disk>RAM paged-out rm",
                 test_disk_gt_ram_paged_rm,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_period=60;checkpoint_memory_recovery_"
                 "upper_mark=0;checkpoint_memory_recovery_lower_mark=0",
                 /* TODO RDB: ep_total_persisted not correct under Rocks */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("disk>RAM update paged-out",
                 test_disk_gt_ram_update_paged_out,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("disk>RAM delete paged-out",
                 test_disk_gt_ram_delete_paged_out,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("disk>RAM set bgfetch race",
                 test_disk_gt_ram_set_race,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup,
                 true),
        TestCase("disk>RAM delete bgfetch race",
                 test_disk_gt_ram_rm_race,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup,
                 true),

        // vbucket negative tests
        TestCase("vbucket get (dead)",
                 test_wrong_vb_get,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket get (pending)",
                 test_vb_get_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket get (replica)",
                 test_vb_get_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket set (dead)",
                 test_wrong_vb_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket set (pending)",
                 test_vb_set_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket set (replica)",
                 test_vb_set_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket replace (dead)",
                 test_wrong_vb_replace,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket replace (pending)",
                 test_vb_replace_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket replace (replica)",
                 test_vb_replace_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket add (dead)",
                 test_wrong_vb_add,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket add (pending)",
                 test_vb_add_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket add (replica)",
                 test_vb_add_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket cas (dead)",
                 test_wrong_vb_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket cas (pending)",
                 test_vb_cas_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket cas (replica)",
                 test_vb_cas_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket del (dead)",
                 test_wrong_vb_del,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket del (pending)",
                 test_vb_del_pending,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("vbucket del (replica)",
                 test_vb_del_replica,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test vbucket get",
                 test_vbucket_get,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test vbucket get missing",
                 test_vbucket_get_miss,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test vbucket create",
                 test_vbucket_create,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test vbucket compact",
                 test_vbucket_compact,
                 test_setup,
                 teardown,
                 nullptr,
                 /* In ephemeral buckets we don't do compaction. We have
                    module test 'EphTombstoneTest' to test tombstone purging */
                 // TODO RDB: Needs RocksDBKVStore to implement manual
                 // compaction and item expiration on compaction.
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("test compaction config",
                 test_compaction_config,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test multiple vb compactions",
                 test_multiple_vb_compactions,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test multiple vb compactions with workload",
                 test_multi_vb_compactions_with_workload,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test async vbucket destroy",
                 test_async_vbucket_destroy,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test sync vbucket destroy",
                 test_sync_vbucket_destroy,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test async vbucket destroy (multitable)",
                 test_async_vbucket_destroy,
                 test_setup,
                 teardown,
                 "max_vbuckets=16;max_num_shards=4;ht_size=7;ht_locks=3",
                 prepare,
                 cleanup),
        TestCase("test sync vbucket destroy (multitable)",
                 test_sync_vbucket_destroy,
                 test_setup,
                 teardown,
                 "max_vbuckets=16;max_num_shards=4;ht_size=7;ht_locks=3",
                 prepare,
                 cleanup),
        TestCase(
                "test vbucket destroy stats",
                test_vbucket_destroy_stats,
                test_setup,
                teardown,
                "chk_remover_stime=1;"
                "chk_period=60;"
                "chk_expel_enabled=false;chk_period=60;checkpoint_memory_"
                "recovery_upper_mark=0;checkpoint_memory_recovery_lower_mark=0",
                /* Checkpoint expelling needs to be disabled for this test
                 * because the test checks for items being removed by
                 * monitoring the ep_items_rm_from_checkpoints stat.  If the
                 * items have already been expelled the stat will not change.
                 */
                prepare_ep_bucket,
                cleanup),
        TestCase("test async vbucket destroy restart",
                 test_async_vbucket_destroy_restart,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test sync vbucket destroy restart",
                 test_sync_vbucket_destroy_restart,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test takeover stats race with vbucket create (DCP)",
                 test_takeover_stats_race_with_vb_create_DCP,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test num persisted deletes (takeover stats)",
                 test_takeover_stats_num_persisted_deletes,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: Implement RocksDBKVStore::getNumPersistedDeletes
                 // Magma: no plans to support persisted deletes
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),

        // stats uuid
        TestCase("test stats uuid",
                 test_uuid_stats,
                 test_setup,
                 teardown,
                 "uuid=foobar",
                 prepare,
                 cleanup),

        // revision id's
        TestCase("revision sequence numbers",
                 test_revid,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("mb-4314",
                 test_regression_mb4314,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("mb-3466",
                 test_mb3466,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        // Data traffic control tests
        TestCase("control data traffic",
                 test_control_data_traffic,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        // Transaction tests
        TestCase("multiple transactions",
                 test_multiple_transactions,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),

        // Returning meta tests
        TestCase("test set ret meta",
                 test_set_ret_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test set ret meta error",
                 test_set_ret_meta_error,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test add ret meta",
                 test_add_ret_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test add ret meta error",
                 test_add_ret_meta_error,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test del ret meta",
                 test_del_ret_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test del ret meta error",
                 test_del_ret_meta_error,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test set with item_eviction",
                 test_set_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test set_with_meta with item_eviction",
                 test_setWithMeta_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test multiple set and del with meta with item_eviction",
                 test_multiple_set_delete_with_metas_full_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 // Skipping for MB-29182
                 prepare_full_eviction_skip_under_rocks,
                 cleanup),
        TestCase("test add with item_eviction",
                 test_add_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test replace with eviction",
                 test_replace_with_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("test replace with eviction (full)",
                 test_replace_with_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test get_and_touch with item_eviction",
                 test_gat_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test key_stats with item_eviction",
                 test_keyStats_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test del with item_eviction",
                 test_del_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test del_with_meta with item_eviction",
                 test_delWithMeta_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test observe with item_eviction",
                 test_observe_with_item_eviction,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase(
                "test expired item with item_eviction",
                test_expired_item_with_item_eviction,
                test_setup,
                teardown,
                nullptr,
                prepare_full_eviction_skip_under_rocks, // Skipping for MB-29182
                cleanup),
        TestCase("test get & delete on non existent items",
                 test_non_existent_get_and_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test MB-16421",
                 test_mb16421,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_full_eviction,
                 cleanup),
        TestCase("test eviction with xattr",
                 test_eviction_with_xattr,
                 test_setup,
                 teardown,
                 "item_eviction_policy=full_eviction",
                 prepare,
                 cleanup),

        TestCase("test get random key",
                 test_get_random_key,
                 test_setup,
                 teardown,
                 nullptr,
                 // no collection item counts on rocks
                 prepare_skip_broken_under_rocks,
                 cleanup),

        TestCase("test failover log behavior",
                 test_failover_log_behavior,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test hlc cas",
                 test_hlc_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        //@TODO RDB: Broken because rocksDB threads stick around after
        // bucket is destroyed and have persistent thread locals
        TestCaseV2("multi_bucket set/get ",
                   test_multi_bucket_set_get,
                   nullptr,
                   teardown_v2,
                   nullptr,
                   prepare_ep_bucket_skip_broken_under_rocks,
                   cleanup),

        TestCase("test_mb19635_upgrade_from_25x",
                 test_mb19635_upgrade_from_25x,
                 test_setup,
                 teardown,
                 nullptr,
                 // magma has no support for upgrades at this time
                 prepare_skip_broken_under_magma,
                 cleanup),

        TestCase("test_MB-19687_fixed",
                 test_mb19687_fixed,
                 test_setup,
                 teardown,
                 // Set a fixed number of shards for stats checking.
                 "max_num_shards=4",
                 // TODO RDB: Needs to fix some missing/unexpected stats
                 // Magma has no support for upgrades at this time
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),

        TestCase("test_MB-19687_variable",
                 test_mb19687_variable,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test vbucket compact no purge",
                 test_vbucket_compact_no_purge,
                 test_setup,
                 teardown,
                 nullptr,
                 /* In ephemeral buckets we don't do compaction. We have
                    module test 'EphTombstoneTest' to test tombstone purging */
                 // TODO RDB: Needs RocksDBKVStore to implement manual
                 // compaction and item expiration on compaction.
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),

        TestCase("test_MB-20697",
                 test_mb20697,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: Needs the 'ep_item_commit_failed' stat
                 // Magma: This test does not apply to magma because magma
                 // does not close/reopen files while an instance is active.
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test_MB-test_mb20943_remove_pending_ops_on_vbucket_delete",
                 test_mb20943_complete_pending_ops_on_vbucket_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test_mb20744_check_incr_reject_ops",
                 test_mb20744_check_incr_reject_ops,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: Needs the 'vb_active_ops_reject' stat
                 // Magma: Error injection for magma is done as part of
                 // the magma unit tests.
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),

        TestCase("test_MB-23640_get_document_of_any_state",
                 test_mb23640,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test MB-33919 past tombstone",
                 test_MB_33919,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test_MB34173_warmup",
                 test_MB34173_warmup,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO Magma: Magma does not support updating the
                 // data store without going through magma instance.
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),

        TestCase("test_mb38031_upgrade_from_4x_via_5x_hop",
                 test_mb38031_upgrade_from_4x_via_5x_hop,
                 test_setup,
                 teardown,
                 nullptr,
                 // couchstore only issue - so skip magma and rocks
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),

        TestCase("test_mb38031_illegal_json_throws",
                 test_mb38031_illegal_json_throws,
                 test_setup,
                 teardown,
                 nullptr,
                 // couchstore only issue - so skip magma and rocks
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),

        TestCase("test_replace_at_pending_insert",
                 test_replace_at_pending_insert,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO: The test logic assumes that the KV BloomFilter is
                 // enabled,
                 // but it is currently disabled for Magma
                 prepare_ep_bucket_skip_broken_under_magma,
                 cleanup),

        TestCase("test reader thread starvation during warmup due to low "
                 "reader threads",
                 test_reader_thread_starvation_warmup,
                 test_setup,
                 teardown,
                 "num_reader_threads=1;",
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),

        TestCase("test sync write timeout",
                 test_sync_write_timeout,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test seqno persistence timeout",
                 test_seqno_persistence_timeout,
                 test_setup,
                 teardown,
                 "seqno_persistence_timeout=0;",
                 prepare,
                 cleanup),

        TestCase(
                nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
