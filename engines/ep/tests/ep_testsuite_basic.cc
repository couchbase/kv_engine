/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Testsuite for 'basic' key-value functionality in ep-engine.
 */

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/types.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/platform_thread.h>
#include <array>

#define WHITESPACE_DB "whitespace sucks.db"

// Types //////////////////////////////////////////////////////////////////////


// Helper functions ///////////////////////////////////////////////////////////

static bool epsilon(int val, int target, int ep=5) {
    return abs(val - target) < ep;
}


// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_alloc_limit(EngineIface* h) {
    auto rv = allocate(h,
                       nullptr,
                       "key",
                       20 * 1024 * 1024,
                       0,
                       0,
                       PROTOCOL_BINARY_RAW_BYTES,
                       Vbid(0));
    checkeq(cb::engine_errc::success, rv.first, "Allocated 20MB item");

    rv = allocate(h,
                  nullptr,
                  "key",
                  (20 * 1024 * 1024) + 1,
                  0,
                  0,
                  PROTOCOL_BINARY_RAW_BYTES,
                  Vbid(0));
    checkeq(cb::engine_errc::too_big, rv.first, "Object too big");

    return SUCCESS;
}

static enum test_result test_memory_tracking(EngineIface* h) {
    // Need memory tracker to be able to check our memory usage.
    std::string tracker = get_str_stat(h, "ep_mem_tracker_enabled");
    if (tracker == "true") {
        return SUCCESS;
    } else {
        std::cerr << "Memory tracker not enabled ...";
        return SKIPPED;
    }
}

static enum test_result test_max_size_and_water_marks_settings(EngineIface* h) {
    checkeq(1000, get_int_stat(h, "ep_max_size"), "Incorrect initial size.");
    check(epsilon(get_int_stat(h, "ep_mem_low_wat"), 750),
          "Incorrect initial low wat.");
    check(epsilon(get_int_stat(h, "ep_mem_high_wat"), 850),
          "Incorrect initial high wat.");
    checkeq(0.75f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect initial low wat. percent");
    checkeq(0.85f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect initial high wat. percent");

    setAndWaitForQuotaChange(h, 10000000);

    check(epsilon(get_int_stat(h, "ep_mem_low_wat"), 7500000),
          "Incorrect larger low wat.");
    check(epsilon(get_int_stat(h, "ep_mem_high_wat"), 8500000),
          "Incorrect larger high wat.");
    checkeq(0.75f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect larger low wat. percent");
    checkeq(0.85f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect larger high wat. percent");

    set_param(h, EngineParamCategory::Flush, "mem_low_wat", "7000000");
    set_param(h, EngineParamCategory::Flush, "mem_high_wat", "8000000");

    checkeq(7000000,
            get_int_stat(h, "ep_mem_low_wat"),
            "Incorrect even larger low wat.");
    checkeq(8000000,
            get_int_stat(h, "ep_mem_high_wat"),
            "Incorrect even larger high wat.");
    checkeq(0.7f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect even larger low wat. percent");
    checkeq(0.8f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect even larger high wat. percent");
    setAndWaitForQuotaChange(h, 5000000);

    check(epsilon(get_int_stat(h, "ep_mem_low_wat"), 3500000),
          "Incorrect smaller low wat.");
    check(epsilon(get_int_stat(h, "ep_mem_high_wat"), 4000000),
          "Incorrect smaller high wat.");
    checkeq(0.7f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect smaller low wat. percent");
    checkeq(0.8f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect smaller high wat. percent");

    set_param(h, EngineParamCategory::Flush, "mem_low_wat", "2500000");
    set_param(h, EngineParamCategory::Flush, "mem_high_wat", "3500000");

    checkeq(2500000,
            get_int_stat(h, "ep_mem_low_wat"),
            "Incorrect even smaller low wat.");
    checkeq(3500000,
            get_int_stat(h, "ep_mem_high_wat"),
            "Incorrect even smaller high wat.");
    checkeq(0.5f, get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect even smaller low wat. percent");
    checkeq(0.7f, get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect even smaller high wat. percent");

    // Once you've changed the ratios by setting explicit water marks, changes
    // to the max_size will calculate watermarks based on the current ratio.
    // Example here is that we get 50% and 70% watermarks because of the
    // prior configuration tweaks that resulted in changes to the percentages.
    setAndWaitForQuotaChange(h, 5000000);
    checkeq(2500000,
            get_int_stat(h, "ep_mem_low_wat"),
            "Incorrect low wat."); // Now engine computes 50 %
    checkeq(3500000,
            get_int_stat(h, "ep_mem_high_wat"),
            "Incorrect high wat"
            ".");
    checkeq(0.5f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect even smaller low wat. percent");
    checkeq(0.7f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect even smaller high wat. percent");

    testHarness->reload_engine(&h,
                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);

    checkeq(1000, get_int_stat(h, "ep_max_size"), "Incorrect initial size.");
    check(epsilon(get_int_stat(h, "ep_mem_low_wat"), 750),
          "Incorrect intial low wat.");
    check(epsilon(get_int_stat(h, "ep_mem_high_wat"), 850),
          "Incorrect initial high wat.");
    checkeq(0.75f, get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect initial low wat. percent");
    checkeq(0.85f, get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect initial high wat. percent");

    // Finally check a new engine with explicit water marks results in the
    // expected percentages
    std::string newConfig(testHarness->get_current_testcase()->cfg);
    newConfig += "mem_low_wat=550;mem_high_wat=660";

    testHarness->reload_engine(&h, newConfig.c_str(), true, true);

    wait_for_warmup_complete(h);

    checkeq(1000, get_int_stat(h, "ep_max_size"), "Incorrect initial size.");
    check(epsilon(get_int_stat(h, "ep_mem_low_wat"), 550),
          "Incorrect intial low wat.");
    check(epsilon(get_int_stat(h, "ep_mem_high_wat"), 660),
          "Incorrect initial high wat.");
    checkeq(0.55f,
            get_float_stat(h, "ep_mem_low_wat_percent"),
            "Incorrect initial low wat. percent");
    checkeq(0.66f,
            get_float_stat(h, "ep_mem_high_wat_percent"),
            "Incorrect initial high wat. percent");

    return SUCCESS;
}

static enum test_result test_whitespace_db(EngineIface* h) {
    vals.clear();

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    // We append the whitespace portion of the name to the current one as this
    // should be unique when running under ctest (it gets set via our command
    // line args)
    std::string dbname;
    dbname.assign(vals["ep_dbname"] + std::string(WHITESPACE_DB));
    rmdb(dbname);

    std::string oldparam("dbname=" + vals["ep_dbname"]);
    std::string newparam("dbname=" + dbname);
    std::string config = testHarness->get_current_testcase()->cfg;
    std::string::size_type found = config.find(oldparam);
    if (found != config.npos) {
        config.replace(found, oldparam.size(), newparam);
    }
    testHarness->reload_engine(&h, config.c_str(), true, false);
    wait_for_warmup_complete(h);

    vals.clear();
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    if (vals["ep_dbname"] != dbname) {
        std::cerr << "Expected dbname = '" << dbname << "'"
                  << ", got '" << vals["ep_dbname"] << "'" << std::endl;
        return FAIL;
    }

    check(cb::io::isDirectory(dbname), "I expected the whitespace db to exist");
    return SUCCESS;
}

void test_whitespace_db_cleanup(engine_test_t* test, enum test_result result) {
    // Cleanup the whitespace db if it exists
    std::string dbname;
    dbname.assign(vals["ep_dbname"] + std::string(WHITESPACE_DB));
    rmdb(dbname);

    // Chain to the normal cleanup to cleanup the original db
    cleanup(test, result);
}

static enum test_result test_get_miss(EngineIface* h) {
    checkeq(cb::engine_errc::no_such_key, verify_key(h, "k"), "Expected miss.");
    return SUCCESS;
}

static enum test_result test_set(EngineIface* h) {

    uint64_t vb_uuid = 0, high_seqno = 0;
    const int num_sets = 5, num_keys = 4;

    std::string key_arr[num_keys] = { "dummy_key",
                                      "checkpoint_start",
                                      "checkpoint_end",
                                      "key" };

    for (auto& k : key_arr) {
        for (int j = 0; j < num_sets; j++) {
            vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
            high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

            std::string err_str_store("Error setting " + k);
            checkeq(cb::engine_errc::success,
                    store(h, nullptr, StoreSemantics::Set, k, "somevalue"),
                    err_str_store.c_str());

            std::string err_str_get_item_info("Error getting " + k);
            item_info info;
            checkeq(true,
                    get_item_info(h, &info, k.c_str()),
                    err_str_get_item_info.c_str());

            std::string err_str_vb_uuid("Expected valid vbucket uuid for " + k);
            checkeq(vb_uuid, info.vbucket_uuid, err_str_vb_uuid.c_str());

            std::string err_str_seqno("Expected valid sequence number for " +
                                      k);
            checkeq(high_seqno + 1, info.seqno, err_str_seqno.c_str());
        }
    }

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);

        std::stringstream error1, error2;
        error1 << "Expected ep_total_persisted >= num_keys (" << num_keys << ")";
        error2 << "Expected ep_total_persisted <= num_sets*num_keys ("
               << num_sets*num_keys << ")";

        // The flusher could of ran > 1 times. We can only assert
        // that we persisted between num_keys and upto num_keys*num_sets
        checkle(num_keys, get_int_stat(h, "ep_total_persisted"),
                error1.str().c_str());
        checkge((num_sets * num_keys), get_int_stat(h, "ep_total_persisted"),
                error2.str().c_str());
    }
    return SUCCESS;
}

extern "C" {
    static void conc_del_set_thread(void *arg) {
        auto* h = static_cast<EngineIface*>(arg);

        for (int i = 0; i < 5000; ++i) {
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue");
            checkeq(cb::engine_errc::success,
                    store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
                    "Error setting.");
            // Ignoring the result here -- we're racing.
            del(h, "key", 0, Vbid(0));
        }
    }
}

static enum test_result test_conc_set(EngineIface* h) {
    const int n_threads = 8;
    std::array<std::thread, n_threads> threads;

    wait_for_persisted_value(h, "key", "value1");
    int ii = 0;
    for (auto& thread : threads) {
        thread = create_thread([h]() { conc_del_set_thread(h); },
                               "t:" + std::to_string(ii++));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    if (isWarmupEnabled(h)) {
        wait_for_flusher_to_settle(h);

        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   false);
        wait_for_warmup_complete(h);

        cb_assert(0 == get_int_stat(h, "ep_warmup_dups"));
    }

    return SUCCESS;
}

struct multi_set_args {
    EngineIface* h;
    std::string prefix;
    int count;
};

extern "C" {
    static void multi_set_thread(void *arg) {
        auto *msa = static_cast<multi_set_args *>(arg);

        for (int i = 0; i < msa->count; i++) {
            std::stringstream s;
            s << msa->prefix << i;
            std::string key(s.str());
            checkeq(cb::engine_errc::success,
                    store(msa->h,
                          nullptr,
                          StoreSemantics::Set,
                          key,
                          "somevalue"),
                    "Set failure!");
        }
    }
}

static enum test_result test_multi_set(EngineIface* h) {
    struct multi_set_args msa1, msa2;
    msa1.h = h;
    msa1.prefix = "ONE_";
    msa1.count = 10000;
    auto thread1 =
            create_thread([&msa1]() { multi_set_thread(&msa1); }, "thread1");

    msa2.h = h;
    msa2.prefix = "TWO_";
    msa2.count = 10000;
    auto thread2 =
            create_thread([&msa2]() { multi_set_thread(&msa2); }, "thread2");
    thread1.join();
    thread2.join();

    wait_for_flusher_to_settle(h);

    checkeq(20000,
            get_int_stat(h, "curr_items"),
            "Mismatch in number of items inserted");
    checkeq(20000,
            get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno"),
            "Unexpected high sequence number");

    return SUCCESS;
}

static enum test_result test_set_get_hit(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "store failure");
    check_key_value(h, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_getl_delete_with_cas(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Failed to set key");

    auto ret = getl(h, nullptr, "key", Vbid(0), 15);
    checkeq(cb::engine_errc::success,
            ret.first,
            "Expected getl to succeed on key");
    item_info info;
    check(h->get_item_info(*ret.second.get(), info), "Failed to get item info");

    checkeq(cb::engine_errc::success,
            del(h, "key", info.cas, Vbid(0)),
            "Expected SUCCESS");

    return SUCCESS;
}

static enum test_result test_getl_delete_with_bad_cas(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Failed to set key");

    uint64_t cas = last_cas;
    checkeq(cb::engine_errc::success,
            getl(h, nullptr, "key", Vbid(0), 15).first,
            "Expected getl to succeed on key");

    checkeq(cb::engine_errc::locked_tmpfail,
            del(h, "key", cas, Vbid(0)),
            "Expected TMPFAIL");

    return SUCCESS;
}

static enum test_result test_getl_set_del_with_meta(EngineIface* h) {
    const char *key = "key";
    const char *val = "value";
    const char *newval = "newvalue";
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, val),
            "Failed to set key");

    checkeq(cb::engine_errc::success,
            getl(h, nullptr, key, Vbid(0), 15).first,
            "Expected getl to succeed on key");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, key, errorMetaPair), "Expected to get meta");

    //init some random metadata
    ItemMetaData itm_meta(0xdeadbeef, 10, 0xdeadbeef, time(nullptr) + 300);

    //do a set with meta
    checkeq(cb::engine_errc::locked,
            set_with_meta(h,
                          key,
                          newval,
                          Vbid(0),
                          &itm_meta,
                          errorMetaPair.second.cas),
            "Expected item to be locked");

    //do a del with meta
    checkeq(cb::engine_errc::locked_tmpfail,
            del_with_meta(h, key, Vbid(0), &itm_meta, last_cas),
            "Expected item to be locked");
    return SUCCESS;
}

static enum test_result test_getl(EngineIface* h) {
    const char *key = "k1";
    Vbid vbucketId = Vbid(0);
    uint32_t expiration = 25;

    auto* cookie = testHarness->create_cookie(h);

    checkeq(cb::engine_errc::no_such_key,
            getl(h, cookie, key, vbucketId, expiration).first,
            "expected the key to be missing...");

    checkeq(cb::engine_errc::success,
            store(h,
                  cookie,
                  StoreSemantics::Set,
                  key,
                  "{\"lock\":\"data\"}",
                  nullptr,
                  0,
                  vbucketId,
                  3600,
                  PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed to store an item.");

    /* retry getl, should succeed */
    auto ret = getl(h, cookie, key, vbucketId, expiration);
    checkeq(cb::engine_errc::success,
            ret.first,
            "Expected to be able to getl on first try");

    item_info info;
    check(h->get_item_info(*ret.second.get(), info), "Failed to get item info");

    checkeq(std::string{"{\"lock\":\"data\"}"},
            std::string((const char*)info.value[0].iov_base,
                        info.value[0].iov_len),
            "Body was malformed.");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype,
            "Expected datatype to be JSON");

    /* wait 16 seconds */
    testHarness->time_travel(16);

    /* lock's taken so this should fail */
    checkeq(cb::engine_errc::locked_tmpfail,
            getl(h, cookie, key, vbucketId, expiration).first,
            "Expected to fail getl on second try");

    checkne(cb::engine_errc::success,
            store(h,
                  cookie,
                  StoreSemantics::Set,
                  key,
                  "lockdata2",
                  nullptr,
                  0,
                  vbucketId),
            "Should have failed to store an item.");

    /* wait another 10 seconds */
    testHarness->time_travel(10);

    /* retry set, should succeed */
    checkeq(cb::engine_errc::success,
            store(h,
                  cookie,
                  StoreSemantics::Set,
                  key,
                  "lockdata",
                  nullptr,
                  0,
                  vbucketId),
            "Failed to store an item.");

    /* point to wrong vbucket, to test NOT_MY_VB response */
    checkeq(cb::engine_errc::not_my_vbucket,
            getl(h, cookie, key, Vbid(10), expiration).first,
            "Should have received not my vbucket response");

    /* acquire lock, should succeed */
    ret = getl(h, cookie, key, vbucketId, expiration);
    checkeq(cb::engine_errc::success,
            ret.first,
            "Acquire lock should have succeeded");
    check(h->get_item_info(*ret.second.get(), info), "Failed to get item info");
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_RAW_BYTES), info.datatype,
            "Expected datatype to be RAW BYTES");

    /* try an delete operation which should fail */
    uint64_t cas = 0;

    checkeq(cb::engine_errc::locked_tmpfail,
            del(h, key, 0, Vbid(0)),
            "Delete failed");

    /* bug MB 2699 append after getl should fail with
     * cb::engine_errc::temporary_failure */

    testHarness->time_travel(26);

    char binaryData1[] = "abcdefg\0gfedcba";

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         cookie,
                         StoreSemantics::Set,
                         key,
                         {binaryData1, sizeof(binaryData1) - 1},
                         82758,
                         0,
                         Vbid(0))
                    .first,
            "Failed set.");

    /* acquire lock, should succeed */
    checkeq(cb::engine_errc::success,
            getl(h, cookie, key, vbucketId, expiration).first,
            "Acquire lock should have succeeded");

    /* bug MB 3252 & MB 3354.
     * 1. Set a key with an expiry value.
     * 2. Take a lock on the item before it expires
     * 3. Wait for the item to expire
     * 4. Perform a CAS operation, should fail
     * 5. Perform a set operation, should succeed
     */
    const char *ekey = "test_expiry";
    const char *edata = "some test data here.";

    ret = allocate(h,
                   cookie,
                   ekey,
                   strlen(edata),
                   0,
                   2,
                   PROTOCOL_BINARY_RAW_BYTES,
                   Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation Failed");

    check(h->get_item_info(*ret.second.get(), info), "Failed to get item info");

    memcpy(info.value[0].iov_base, edata, strlen(edata));

    checkeq(cb::engine_errc::success,
            h->store(*cookie,
                     *ret.second.get(),
                     cas,
                     StoreSemantics::Set,
                     {},
                     DocumentState::Alive,
                     false),
            "Failed to Store item");
    check_key_value(h, ekey, edata, strlen(edata));

    testHarness->time_travel(3);
    cas = last_cas;

    /* cas should fail */
    ret = storeCasVb11(h,
                       cookie,
                       StoreSemantics::CAS,
                       ekey,
                       {binaryData1, sizeof(binaryData1) - 1},
                       82758,
                       cas,
                       Vbid(0));
    checkne(cb::engine_errc::success, ret.first, "CAS succeeded.");

    /* but a simple store should succeed */
    checkeq(cb::engine_errc::success,
            store(h,
                  cookie,
                  StoreSemantics::Set,
                  ekey,
                  edata,
                  nullptr,
                  0,
                  vbucketId),
            "Failed to store an item.");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_unl(EngineIface* h) {
    const char *key = "k2";
    Vbid vbucketId = Vbid(0);

    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    std::string eviction_policy;
    auto itr = vals.find("ep_item_eviction_policy");
    if (itr != vals.end()) {
        eviction_policy = itr->second;
    } else {
        eviction_policy = "value_only";
    }

    if (eviction_policy == "full_eviction") {
        checkeq(cb::engine_errc::temporary_failure,
                unl(h, nullptr, key, vbucketId),
                "expected a TMPFAIL");
    } else {
        checkeq(cb::engine_errc::no_such_key,
                unl(h, nullptr, key, vbucketId),
                "expected the key to be missing...");
    }

    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  key,
                  "lockdata",
                  nullptr,
                  0,
                  vbucketId),
            "Failed to store an item.");

    /* getl, should succeed */
    auto ret = getl(h, nullptr, key, vbucketId, 0);
    checkeq(cb::engine_errc::success,
            ret.first,
            "Expected to be able to getl on first try");
    item_info info;
    checkeq(true,
            h->get_item_info(*ret.second.get(), info),
            "failed to get item info");
    uint64_t cas = info.cas;

    /* lock's taken unlocking with a random cas value should fail */
    checkeq(cb::engine_errc::locked_tmpfail,
            unl(h, nullptr, key, vbucketId),
            "Expected to fail getl on second try");

    checkeq(cb::engine_errc::success,
            unl(h, nullptr, key, vbucketId, cas),
            "Expected to succed unl with correct cas");

    /* acquire lock, should succeed */
    checkeq(cb::engine_errc::success,
            getl(h, nullptr, key, vbucketId, 0).first,
            "Lock should work after unlock");

    /* wait 16 seconds */
    testHarness->time_travel(16);

    /* lock has expired, unl should fail */
    checkeq(cb::engine_errc::temporary_failure,
            unl(h, nullptr, key, vbucketId, last_cas),
            "Expected to fail unl on lock timeout");

    return SUCCESS;
}

static enum test_result test_unl_nmvb(EngineIface* h) {
    const char *key = "k2";
    Vbid vbucketId = Vbid(10);

    checkeq(cb::engine_errc::not_my_vbucket,
            unl(h, nullptr, key, vbucketId),
            "expected NOT_MY_VBUCKET to unlocking a key in a vbucket we don't "
            "own");

    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(EngineIface* h) {
    char binaryData[] = "abcdefg\0gfedcba";
    cb_assert(sizeof(binaryData) != strlen(binaryData));

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         "key",
                         {binaryData, sizeof(binaryData)},
                         82758,
                         0,
                         Vbid(0))
                    .first,
            "Failed to set.");
    check_key_value(h, "key", binaryData, sizeof(binaryData));
    return SUCCESS;
}

static enum test_result test_set_with_cas_non_existent(EngineIface* h) {
    const char *key = "test_expiry_flush";
    auto* cookie = testHarness->create_cookie(h);
    auto ret = allocate(
            h, cookie, key, 10, 0, 0, PROTOCOL_BINARY_RAW_BYTES, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");

    Item* it = reinterpret_cast<Item*>(ret.second.get());
    it->setCas(1234);

    uint64_t cas = 0;
    checkeq(cb::engine_errc::no_such_key,
            h->store(*cookie,
                     *ret.second.get(),
                     cas,
                     StoreSemantics::Set,
                     {},
                     DocumentState::Alive,
                     false),
            "Expected not found");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_set_change_flags(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed to set.");

    item_info info;
    uint32_t flags = 828258;
    check(get_item_info(h, &info, "key"), "Failed to get value.");
    cb_assert(info.flags != flags);

    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         StoreSemantics::Set,
                         "key",
                         "newvalue",
                         flags,
                         0,
                         Vbid(0))
                    .first,
            "Failed to set again.");

    check(get_item_info(h, &info, "key"), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_add(EngineIface* h) {
    item_info info;
    uint64_t vb_uuid = 0;
    uint64_t high_seqno = 0;

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue"),
            "Failed to add value.");

    check(get_item_info(h, &info, "key"), "Error getting item info");
    checkeq(vb_uuid, info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, info.seqno, "Expected valid sequence number");

    checkeq(cb::engine_errc::not_stored,
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue"),
            "Failed to fail to re-add value.");

    // This aborts on failure.
    check_key_value(h, "key", "somevalue", 9);

    // Expiration above was an hour, so let's go to The Future
    testHarness->time_travel(3800);

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "key", "newvalue"),
            "Failed to add value again.");

    check_key_value(h, "key", "newvalue", 8);
    return SUCCESS;
}

static enum test_result test_add_add_with_cas(EngineIface* h) {
    ItemIface* i = nullptr;
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Add, "key", "somevalue", &i),
            "Failed set.");
    check_key_value(h, "key", "somevalue", 9);
    item_info info;
    check(h->get_item_info(*i, info), "Should be able to get info");

    checkeq(cb::engine_errc::key_already_exists,
            store(h,
                  nullptr,
                  StoreSemantics::Add,
                  "key",
                  "somevalue",
                  nullptr,
                  info.cas),
            "Should not be able to add the key two times");

    h->release(*i);
    return SUCCESS;
}

static enum test_result test_cas(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed to do initial set.");
    checkne(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::CAS, "key", "failcas"),
            "Failed to fail initial CAS.");
    check_key_value(h, "key", "somevalue", 9);

    auto ret = get(h, nullptr, "key", Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "Failed to get value.");

    item_info info;
    check(h->get_item_info(*ret.second.get(), info),
          "Failed to get item info.");

    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::CAS,
                  "key",
                  "winCas",
                  nullptr,
                  info.cas),
            "Failed to store CAS");
    check_key_value(h, "key", "winCas", 6);

    uint64_t cval = 99999;
    checkeq(cb::engine_errc::no_such_key,
            store(h,
                  nullptr,
                  StoreSemantics::CAS,
                  "non-existing",
                  "winCas",
                  nullptr,
                  cval),
            "CAS for non-existing key returned the wrong error code");
    return SUCCESS;
}

static enum test_result test_replace(EngineIface* h) {
    item_info info;
    uint64_t vb_uuid = 0;
    uint64_t high_seqno = 0;

    checkne(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Replace, "key", "somevalue"),
            "Failed to fail to replace non-existing value.");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed to set value.");

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Replace, "key", "somevalue"),
            "Failed to replace existing value.");

    check(get_item_info(h, &info, "key"), "Error getting item info");

    checkeq(vb_uuid, info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, info.seqno, "Expected valid sequence number");

    check_key_value(h, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_touch(EngineIface* h) {
    // Try to touch an unknown item...
    checkeq(cb::engine_errc::no_such_key,
            touch(h, "mykey", Vbid(0), 0),
            "Testing unknown key");

    // illegal vbucket
    checkeq(cb::engine_errc::not_my_vbucket,
            touch(h, "mykey", Vbid(5), 0),
            "Testing illegal vbucket");

    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "mykey", "somevalue"),
            "Failed set.");

    check_key_value(h, "mykey", "somevalue", strlen("somevalue"));

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, "mykey", errorMetaPair), "Get meta failed");

    item_info currMeta = errorMetaPair.second;

    checkeq(cb::engine_errc::success,
            touch(h, "mykey", Vbid(0), uint32_t(time(nullptr) + 10)),
            "touch mykey");
    checkne(last_cas.load(),
            currMeta.cas,
            "touch should have returned an updated CAS");

    check(get_meta(h, "mykey", errorMetaPair), "Get meta failed");

    checkne(errorMetaPair.second.cas, currMeta.cas,
          "touch should have updated the CAS");
    checkne(errorMetaPair.second.exptime, currMeta.exptime,
          "touch should have updated the expiry time");
    checkeq(errorMetaPair.second.seqno, (currMeta.seqno + 1),
          "touch should have incremented rev seqno");

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

static enum test_result test_touch_mb7342(EngineIface* h) {
    const char *key = "MB-7342";
    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, "v"),
            "Failed set.");

    checkeq(cb::engine_errc::success, touch(h, key, Vbid(0), 0), "touch key");

    check_key_value(h, key, "v", 1);

    // Travel a loong time to see if the object is still there (the default
    // store sets an exp time of 3600
    testHarness->time_travel(3700);

    check_key_value(h, key, "v", 1);

    return SUCCESS;
}

static enum test_result test_touch_mb10277(EngineIface* h) {
    const char *key = "MB-10277";
    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, "v"),
            "Failed set.");
    wait_for_flusher_to_settle(h);
    evict_key(h, key, Vbid(0), "Ejected.");

    checkeq(cb::engine_errc::success,
            touch(h,
                  key,
                  Vbid(0),
                  3600), // A new expiration time remains in the same.
            "touch key");

    return SUCCESS;
}

static enum test_result test_gat(EngineIface* h) {
    // Try to gat an unknown item...
    auto ret = gat(h, "mykey", Vbid(0), 10);
    checkeq(cb::engine_errc::no_such_key,
            cb::engine_errc(ret.first),
            "Testing unknown key");

    // illegal vbucket
    ret = gat(h, "mykey", Vbid(5), 10);
    checkeq(cb::engine_errc::not_my_vbucket,
            cb::engine_errc(ret.first),
            "Testing illegal vbucket");

    // Store the item!
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "mykey",
                  "{\"some\":\"value\"}",
                  nullptr,
                  0,
                  Vbid(0),
                  3600,
                  PROTOCOL_BINARY_DATATYPE_JSON),
            "Failed set.");

    check_key_value(
            h, "mykey", R"({"some":"value"})", strlen(R"({"some":"value"})"));

    ret = gat(h, "mykey", Vbid(0), 10);
    checkeq(cb::engine_errc::success, cb::engine_errc(ret.first), "gat mykey");

    item_info info;
    check(h->get_item_info(*ret.second.get(), info),
          "Getting item info failed");

    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_DATATYPE_JSON),
            info.datatype, "Expected datatype to be JSON");

    std::string body{static_cast<char*>(info.value[0].iov_base),
    info.value[0].iov_len};
    check(body.compare(0, sizeof("{\"some\":\"value\"}"),
                       "{\"some\":\"value\"}") == 0,
          "Invalid data returned");

    // time-travel 9 secs..
    testHarness->time_travel(9);

    // The item should still exist
    check_key_value(
            h, "mykey", R"({"some":"value"})", strlen(R"({"some":"value"})"));

    // time-travel 2 secs..
    testHarness->time_travel(2);

    // The item should have expired now...
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "mykey", Vbid(0)).first,
            "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gat_locked(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value"),
            "Failed to set key");

    checkeq(cb::engine_errc::success,
            getl(h, nullptr, "key", Vbid(0), 15).first,
            "Expected getl to succeed on key");

    auto ret = gat(h, "key", Vbid(0), 10);
    checkeq(cb::engine_errc::locked,
            cb::engine_errc(ret.first),
            "Expected LOCKED");

    testHarness->time_travel(16);
    ret = gat(h, "key", Vbid(0), 10);
    checkeq(cb::engine_errc::success,
            cb::engine_errc(ret.first),
            "Expected success");

    testHarness->time_travel(11);
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "key", Vbid(0)).first,
            "Expected value to be expired");
    return SUCCESS;
}

static enum test_result test_touch_locked(EngineIface* h) {
    ItemIface* itm = nullptr;
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "value", &itm),
            "Failed to set key");
    h->release(*itm);

    checkeq(cb::engine_errc::success,
            getl(h, nullptr, "key", Vbid(0), 15).first,
            "Expected getl to succeed on key");

    checkeq(cb::engine_errc::locked,
            touch(h, "key", Vbid(0), 10),
            "Expected tmp fail");

    testHarness->time_travel(16);
    checkeq(cb::engine_errc::success,
            touch(h, "key", Vbid(0), 10),
            "Expected success");

    testHarness->time_travel(11);
    checkeq(cb::engine_errc::no_such_key,
            get(h, nullptr, "key", Vbid(0)).first,
            "Expected value to be expired");

    return SUCCESS;
}

static enum test_result test_mb5215(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "coolkey", "cooler"),
            "Failed set.");

    check_key_value(h, "coolkey", "cooler", strlen("cooler"));

    // set new exptime to 111
    int expTime = time(nullptr) + 111;

    checkeq(cb::engine_errc::success,
            touch(h, "coolkey", Vbid(0), expTime),
            "touch coolkey");

    //reload engine
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);
    wait_for_warmup_complete(h);

    //verify persisted expiration time
    const char *statkey = "key coolkey 0";
    int newExpTime;
    checkeq(cb::engine_errc::success,
            get(h, nullptr, "coolkey", Vbid(0)).first,
            "Missing key");
    newExpTime = get_int_stat(h, "key_exptime", statkey);
    checkeq(expTime, newExpTime, "Failed to persist new exptime");

    // evict key, touch expiration time, and verify
    evict_key(h, "coolkey", Vbid(0), "Ejected.");

    expTime = time(nullptr) + 222;
    checkeq(cb::engine_errc::success,
            touch(h, "coolkey", Vbid(0), expTime),
            "touch coolkey");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    checkeq(cb::engine_errc::success,
            get(h, nullptr, "coolkey", Vbid(0)).first,
            "Missing key");
    newExpTime = get_int_stat(h, "key_exptime", statkey);
    checkeq(expTime, newExpTime, "Failed to persist new exptime");

    return SUCCESS;
}

/* Testing functionality to store a value for a deleted item
 * and also retrieve the value of a deleted item.
 * Need to check:
 *
 * - Each possible state transition between Alive, Deleted-with-value and
 *   Deleted-no-value.
 */
static enum test_result test_delete_with_value(EngineIface* h) {
    const uint64_t cas_0 = 0;
    const Vbid vbid = Vbid(0);
    auto* cookie = testHarness->create_cookie(h);

    // Store an initial (not-deleted) value.
    checkeq(cb::engine_errc::success,
            store(h, cookie, StoreSemantics::Set, "key", "somevalue"),
            "Failed set");
    wait_for_flusher_to_settle(h);

    checkeq(uint64_t(1),
            get_stat<uint64_t>(h, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected initial item count");

    /* Alive -> Deleted-with-value */
    checkeq(cb::engine_errc::success,
            delete_with_value(h, cookie, cas_0, "key", "deleted"),
            "Failed Alive -> Delete-with-value");

    checkeq(uint64_t(0),
            get_stat<uint64_t>(h, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected num_items after Alive -> Delete-with-value");

    auto res = get_value(h, cookie, "key", vbid, DocStateFilter::Alive);
    checkeq(cb::engine_errc::no_such_key,
            res.first,
            "Unexpectedly accessed Deleted-with-value via DocState::Alive");

    res = get_value(h, cookie, "key", vbid, DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success,
            res.first,
            "Failed to fetch Alive -> Delete-with-value");
    checkeq(std::string("deleted"), res.second, "Unexpected value (deleted)");

    /* Deleted-with-value -> Deleted-with-value (different value). */
    checkeq(cb::engine_errc::success,
            delete_with_value(h, cookie, cas_0, "key", "deleted 2"),
            "Failed Deleted-with-value -> Deleted-with-value");

    checkeq(uint64_t(0),
            get_stat<uint64_t>(h, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected num_items after Delete-with-value -> "
            "Delete-with-value");

    res = get_value(h, cookie, "key", vbid, DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success,
            res.first,
            "Failed to fetch key (deleted 2)");
    checkeq(std::string("deleted 2"),
            res.second,
            "Unexpected value (deleted 2)");

    /* Delete-with-value -> Alive */
    checkeq(cb::engine_errc::success,
            store(h, cookie, StoreSemantics::Set, "key", "alive 2", nullptr),
            "Failed Delete-with-value -> Alive");
    wait_for_flusher_to_settle(h);

    checkeq(uint64_t(1),
            get_stat<uint64_t>(h, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected num_items after Delete-with-value -> Alive");

    res = get_value(h, cookie, "key", vbid, DocStateFilter::Alive);
    checkeq(cb::engine_errc::success,
            res.first,
            "Failed to fetch Delete-with-value -> Alive via DocState::Alive");
    checkeq(std::string("alive 2"), res.second, "Unexpected value (alive 2)");

    // Also check via DocState::Deleted
    res = get_value(h, cookie, "key", vbid, DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success,
            res.first,
            "Failed to fetch Delete-with-value -> Alive via DocState::Deleted");
    checkeq(std::string("alive 2"),
            res.second,
            "Unexpected value (alive 2) via DocState::Deleted");

    /* Alive -> Deleted-no-value */
    checkeq(cb::engine_errc::success,
            del(h, "key", cas_0, vbid, cookie),
            "Failed Alive -> Deleted-no-value");
    wait_for_flusher_to_settle(h);

    checkeq(uint64_t(0),
            get_stat<uint64_t>(h, "vb_0:num_items", "vbucket-details 0"),
            "Unexpected num_items after Alive -> Delete-no-value");

    res = get_value(h, cookie, "key", vbid, DocStateFilter::Alive);
    checkeq(cb::engine_errc::no_such_key,
            res.first,
            "Unexpectedly accessed Deleted-no-value via DocState::Alive");

    /* Deleted-no-value -> Delete-with-value */
    checkeq(cb::engine_errc::success,
            delete_with_value(h, cookie, cas_0, "key", "deleted 3"),
            "Failed delete with value (deleted 2)");

    res = get_value(h, cookie, "key", vbid, DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success,
            res.first,
            "Failed to fetch key (deleted 3)");
    checkeq(std::string("deleted 3"),
            res.second,
            "Unexpected value (deleted 3)");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/* Similar to test_delete_with_value, except also checks that CAS values
 */
static enum test_result test_delete_with_value_cas(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key1", "somevalue"),
            "Failed set");

    cb::EngineErrorMetadataPair errorMetaPair;

    check(get_meta(h, "key1", errorMetaPair), "Get meta failed");

    uint64_t curr_revseqno = errorMetaPair.second.seqno;

    /* Store a deleted item first with CAS 0 */
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key1",
                  "deletevalue",
                  nullptr,
                  0,
                  Vbid(0),
                  3600,
                  0x00,
                  DocumentState::Deleted),
            "Failed delete with value");

    check(get_meta(h, "key1", errorMetaPair), "Get meta failed");

    checkeq(errorMetaPair.second.seqno,
            curr_revseqno + 1,
            "rev seqno should have incremented");

    ItemIface* i = nullptr;
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key2", "somevalue", &i),
            "Failed set");

    item_info info;
    check(h->get_item_info(*i, info), "Getting item info failed");

    h->release(*i);

    check(get_meta(h, "key2", errorMetaPair), "Get meta failed");

    curr_revseqno = errorMetaPair.second.seqno;

    /* Store a deleted item with the existing CAS value */
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key2",
                  "deletevaluewithcas",
                  nullptr,
                  info.cas,
                  Vbid(0),
                  3600,
                  0x00,
                  DocumentState::Deleted),
            "Failed delete value with cas");

    wait_for_flusher_to_settle(h);

    check(get_meta(h, "key2", errorMetaPair), "Get meta failed");

    checkeq(errorMetaPair.second.seqno,
            curr_revseqno + 1,
            "rev seqno should have incremented");

    curr_revseqno = errorMetaPair.second.seqno;

    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key2",
                  "newdeletevalue",
                  &i,
                  0,
                  Vbid(0),
                  3600,
                  0x00,
                  DocumentState::Deleted),
            "Failed delete value with cas");

    wait_for_flusher_to_settle(h);

    check(h->get_item_info(*i, info), "Getting item info failed");
    checkeq(int(DocumentState::Deleted),
            int(info.document_state),
            "Incorrect DocState for deleted item");
    checkne(uint64_t(0), info.cas, "Expected non-zero CAS for deleted item");

    h->release(*i);

    check(get_meta(h, "key2", errorMetaPair), "Get meta failed");

    checkeq(errorMetaPair.second.seqno,
            curr_revseqno + 1,
            "rev seqno should have incremented");

    curr_revseqno = errorMetaPair.second.seqno;

    // Attempt to Delete-with-value using incorrect CAS (should fail)
    const uint64_t incorrect_CAS = info.cas + 1;
    checkeq(cb::engine_errc::key_already_exists,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key2",
                  "newdeletevaluewithcas",
                  nullptr,
                  incorrect_CAS,
                  Vbid(0),
                  3600,
                  0x00,
                  DocumentState::Deleted),
            "Expected KEY_EEXISTS with incorrect CAS");

    // Attempt with correct CAS.
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key2",
                  "newdeletevaluewithcas",
                  nullptr,
                  info.cas,
                  Vbid(0),
                  3600,
                  0x00,
                  DocumentState::Deleted),
            "Failed delete value with cas");

    wait_for_flusher_to_settle(h);

    auto ret = get(h, nullptr, "key2", Vbid(0), DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success, ret.first, "Failed to get value");

    check(get_meta(h, "key2", errorMetaPair), "Get meta failed");

    checkeq(errorMetaPair.second.seqno,
            curr_revseqno + 1,
            "rev seqno should have incremented");

    check(h->get_item_info(*ret.second.get(), info),
          "Getting item info failed");
    checkeq(int(DocumentState::Deleted),
            int(info.document_state),
            "Incorrect DocState for deleted item");

    checkeq(static_cast<uint8_t>(DocumentState::Deleted),
            static_cast<uint8_t>(info.document_state),
            "document must be in deleted state");

    std::string buf(static_cast<char*>(info.value[0].iov_base),
                    info.value[0].iov_len);

    checkeq(0, buf.compare("newdeletevaluewithcas"), "Data mismatch");

    ret = get(h, nullptr, "key", Vbid(0), DocStateFilter::Alive);
    checkeq(cb::engine_errc::no_such_key,
            ret.first,
            "Getting value should have failed");

    return SUCCESS;
}

static enum test_result test_delete(EngineIface* h) {
    ItemIface* i = nullptr;
    // First try to delete something we know to not be there.
    checkeq(cb::engine_errc::no_such_key,
            del(h, "key", 0, Vbid(0)),
            "Failed to fail initial delete.");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue", &i),
            "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    uint64_t orig_cas = it->getCas();
    h->release(*i);
    check_key_value(h, "key", "somevalue", 9);

    uint64_t cas = 0;
    uint64_t vb_uuid = 0;
    mutation_descr_t mut_info;
    uint64_t high_seqno = 0;

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");
    checkeq(cb::engine_errc::success,
            del(h, "key", &cas, Vbid(0), nullptr, &mut_info),
            "Failed remove with value.");
    checkne(orig_cas, cas, "Expected CAS to be updated on delete");
    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "key"),
            "Expected missing key");
    checkeq(vb_uuid, mut_info.vbucket_uuid, "Expected valid vbucket uuid");
    checkeq(high_seqno + 1, mut_info.seqno, "Expected valid sequence number");

    // Can I time travel to an expired object and delete it?
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue", &i),
            "Failed set.");
    h->release(*i);
    testHarness->time_travel(3617);
    checkeq(cb::engine_errc::no_such_key,
            del(h, "key", 0, Vbid(0)),
            "Did not get ENOENT removing an expired object.");
    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "key"),
            "Expected missing key");

    return SUCCESS;
}

static enum test_result test_set_delete(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed set.");
    check_key_value(h, "key", "somevalue", 9);
    checkeq(cb::engine_errc::success,
            del(h, "key", 0, Vbid(0)),
            "Failed remove with value.");
    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "key"),
            "Expected missing key");
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", 0);
    return SUCCESS;
}

static enum test_result test_set_delete_invalid_cas(EngineIface* h) {
    ItemIface* i = nullptr;
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue", &i),
            "Failed set.");
    check_key_value(h, "key", "somevalue", 9);
    item_info info;
    check(h->get_item_info(*i, info), "Should be able to get info");
    h->release(*i);

    checkeq(cb::engine_errc::key_already_exists,
            del(h, "key", info.cas + 1, Vbid(0)),
            "Didn't expect to be able to remove the item with wrong cas");

    checkeq(cb::engine_errc::success,
            del(h, "key", info.cas, Vbid(0)),
            "Subsequent delete with correct CAS did not succeed");

    return SUCCESS;
}

static enum test_result test_delete_set(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    wait_for_persisted_value(h, "key", "value1");

    checkeq(cb::engine_errc::success,
            del(h, "key", 0, Vbid(0)),
            "Failed remove with value.");

    wait_for_persisted_value(h, "key", "value2");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    check_key_value(h, "key", "value2", 6);
    checkeq(cb::engine_errc::success,
            del(h, "key", 0, Vbid(0)),
            "Failed remove with value.");
    wait_for_flusher_to_settle(h);

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);

    checkeq(cb::engine_errc::no_such_key,
            verify_key(h, "key"),
            "Expected missing key");

    return SUCCESS;
}

static enum test_result test_get_delete_missing_file(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            get_stats(h, {}, {}, add_stats),
            "Failed to get stats.");

    const char *key = "key";
    wait_for_persisted_value(h, key, "value2delete");

    // Make the couchstore files in the db directory totally inaccessible.
    std::string dbname = vals["ep_dbname"];
    CouchstoreFileAccessGuard makeCouchstoreFileInaccessible(
            dbname, CouchstoreFileAccessGuard::Mode::DenyAll);

    auto ret = get(h, nullptr, key, Vbid(0));

    // ep engine must be unaware of well-being of the db file as long as
    // the item is still in the memory
    checkeq(cb::engine_errc::success, ret.first, "Expected success for get");

    evict_key(h, key, Vbid(0), "Ejected." /*expected_msg*/);
    ret = get(h, nullptr, key, Vbid(0));

    // ep engine must be now aware of the ill-fated db file where
    // the item is supposedly stored
    checkeq(cb::engine_errc::temporary_failure,
            ret.first,
            "Expected tmp fail for get");

    return SUCCESS;
}

static enum test_result test_bug7023(EngineIface* h) {
    std::vector<std::string> keys;
    // Make a vbucket mess.
    const int nitems = 10000;
    const int iterations = 5;
    for (int j = 0; j < nitems; ++j) {
        keys.push_back("key" + std::to_string(j));
    }

    std::vector<std::string>::iterator it;
    for (int j = 0; j < iterations; ++j) {
        check(set_vbucket_state(h, Vbid(0), vbucket_state_dead),
              "Failed set set vbucket 0 dead.");
        checkeq(cb::engine_errc::success,
                vbucketDelete(h, Vbid(0)),
                "Expected vbucket deletion to work.");
        check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
              "Failed set set vbucket 0 active.");
        for (it = keys.begin(); it != keys.end(); ++it) {
            checkeq(cb::engine_errc::success,
                    store(h,
                          nullptr,
                          StoreSemantics::Set,
                          it->c_str(),
                          it->c_str()),
                    "Failed to store a value");
        }
    }
    wait_for_flusher_to_settle(h);

    if (isWarmupEnabled(h)) {
        // Restart again, to verify no data loss.
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   false);

        wait_for_warmup_complete(h);
        checkeq(nitems,
                get_int_stat(h, "ep_warmup_value_count", "warmup"),
                "Incorrect items following warmup");
    }
    return SUCCESS;
}

static enum test_result test_mb3169(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "set", "value"),
            "Failed to store a value");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "delete", "0"),
            "Failed to store a value");
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "get", "getvalue"),
            "Failed to store a value");

    wait_for_stat_to_be(h, "ep_total_persisted", 3);

    evict_key(h, "set", Vbid(0), "Ejected.");
    evict_key(h, "delete", Vbid(0), "Ejected.");
    evict_key(h, "get", Vbid(0), "Ejected.");

    checkeq(3, get_int_stat(h, "curr_items"), "Expected 3 items");
    checkeq(3,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected all items to be resident");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "set", "value2"),
            "Failed to store a value");
    wait_for_flusher_to_settle(h);

    checkeq(3, get_int_stat(h, "curr_items"), "Expected 3 items");
    checkeq(2,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected mutation to mark item resident");

    checkeq(cb::engine_errc::success,
            del(h, "delete", 0, Vbid(0)),
            "Delete failed");

    wait_for_flusher_to_settle(h);

    checkeq(2, get_int_stat(h, "curr_items"), "Expected 2 items after del");
    checkeq(1,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected delete to remove non-resident item");

    check_key_value(h, "get", "getvalue", 8);

    checkeq(2, get_int_stat(h, "curr_items"), "Expected 2 items after get");
    checkeq(0,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_mb5172(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key-1", "value-1"),
            "Failed to store a value");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key-2", "value-2"),
            "Failed to store a value");

    wait_for_flusher_to_settle(h);

    checkeq(0,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected all items to be resident");

    // restart the server.
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    checkeq(0,
            get_int_stat(h, "ep_num_non_resident"),
            "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_set_vbucket_out_of_range(EngineIface* h) {
    check(!set_vbucket_state(h, Vbid(10000), vbucket_state_active),
          "Shouldn't have been able to set vbucket 10000");
    return SUCCESS;
}

static enum test_result set_max_cas_mb21190(EngineIface* h) {
    uint64_t max_cas = get_ull_stat(h, "vb_0:max_cas", "vbucket-details 0");
    std::string max_cas_str = std::to_string(max_cas+1);
    set_param(h,
              EngineParamCategory::Vbucket,
              "max_cas",
              max_cas_str.data(),
              Vbid(0));
    checkeq(cb::mcbp::Status::Success, last_status.load(),
            "Failed to set_param max_cas");
    checkeq(max_cas + 1,
            get_ull_stat(h, "vb_0:max_cas", "vbucket-details 0"),
            "max_cas didn't change");
    checkeq(cb::engine_errc::not_my_vbucket,
            set_param(h,
                      EngineParamCategory::Vbucket,
                      "max_cas",
                      max_cas_str.data(),
                      Vbid(1)),
            "Expected not my vbucket for vb 1");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h,
                      EngineParamCategory::Vbucket,
                      "max_cas",
                      "JUNK",
                      Vbid(0)),
            "Expected EINVAL");
    return SUCCESS;
}

static enum test_result warmup_mb21769(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    // Validate some VB data post warmup
    // VB 0 will be empty
    // VB 1 will not be empty
    // VB 2 will not be empty and will have had set_state as the final ops

    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed to set vbucket state for vb1");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed to set vbucket state for vb2");

    const int num_items = 10;
    write_items(h, num_items, 0, "vb1", "value", 0 /*expiry*/, Vbid(1));
    write_items(h, num_items, 0, "vb2", "value", 0 /*expiry*/, Vbid(2));
    wait_for_flusher_to_settle(h);

    // flip replica to active to drive more _local writes
    check(set_vbucket_state(h, Vbid(2), vbucket_state_replica),
          "Failed to set vbucket state (replica) for vb2");
    wait_for_flusher_to_settle(h);

    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed to set vbucket state (replica) for vb2");
    wait_for_flusher_to_settle(h);

    // Force a shutdown so the warmup will create failover entries
    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);

    wait_for_warmup_complete(h);

    // values of interested stats for each VB
    std::array<uint64_t, 3> high_seqnos = {{0, num_items, num_items}};
    std::array<uint64_t, 3> snap_starts = {{0, num_items, num_items}};
    std::array<uint64_t, 3> snap_ends = {{0, num_items, num_items}};
    // we will check the seqno of the 0th entry of each vbucket's failover table
    std::array<uint64_t, 3> failover_entry0 = {{0, num_items, num_items}};

    for (uint64_t vb = 0; vb <= 2; vb++) {
        std::string vb_prefix = "vb_" + std::to_string(vb) + ":";
        std::string high_seqno = vb_prefix + "high_seqno";
        std::string snap_start = vb_prefix + "last_persisted_snap_start";
        std::string snap_end = vb_prefix + "last_persisted_snap_end";
        std::string fail0 = vb_prefix + "0:seq";
        std::string vb_group_key = "vbucket-seqno " + std::to_string(vb);
        std::string failovers_key = "failovers " + std::to_string(vb);

        checkeq(high_seqnos[vb],
                get_ull_stat(h, high_seqno.c_str(), vb_group_key.c_str()),
                std::string("high_seqno incorrect vb:" + std::to_string(vb))
                        .c_str());
        checkeq(snap_starts[vb],
                get_ull_stat(h, snap_start.c_str(), vb_group_key.c_str()),
                std::string("snap_start incorrect vb:" + std::to_string(vb))
                        .c_str());
        checkeq(snap_ends[vb],
                get_ull_stat(h, snap_end.c_str(), vb_group_key.c_str()),
                std::string("snap_end incorrect vb:" + std::to_string(vb))
                        .c_str());
        auto failoverTable = get_all_stats(h, failovers_key.c_str());
        if (failoverTable[fail0] != std::to_string(failover_entry0[vb])) {
            std::cerr << "failover table entry 0 is incorrect for vb:" << vb
                      << " expected:" << failover_entry0[vb]
                      << " got:" << failoverTable[fail0]
                      << " dumping failover table\n";
            for (const auto& stat : failoverTable) {
                std::cerr << stat.first << ":" << stat.second << std::endl;
            }
            std::string detail = "vbucket-details " + std::to_string(vb);
            auto details = get_all_stats(h, detail.c_str());
            std::cerr << detail << std::endl;
            for (const auto& stat : details) {
                std::cerr << stat.first << ":" << stat.second << std::endl;
            }
            return FAIL;
        }
    }

    return SUCCESS;
}

/**
 * Callback from the document API being called after the CAS was assigned
 * to the object. We're allowed to modify the content, so let's just change
 * the string.
 *
 * @param info info about the document
 */
static uint64_t pre_link_seqno(0);
static void pre_link_doc_callback(item_info& info) {
    checkne(uint64_t(0), info.cas, "CAS value should be set");
    // mock the actual value so we can see it was changed
    memcpy(info.value[0].iov_base, "valuesome", 9);
    pre_link_seqno = info.seqno;
}

/**
 * Verify that we've hooked into the checkpoint and that the pre-link
 * document api method is called.
 */
static test_result pre_link_document(EngineIface* h) {
    item_info info;

    PreLinkFunction function = pre_link_doc_callback;
    testHarness->set_pre_link_function(function);
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed set.");
    testHarness->set_pre_link_function({});

    // Fetch the value and verify that the callback was called!
    auto ret = get(h, nullptr, "key", Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "get failed");
    check(h->get_item_info(*ret.second.get(), info),
          "Failed to get item info.");
    checkeq(0, memcmp(info.value[0].iov_base, "valuesome", 9),
           "Expected value to be modified");
    checkeq(pre_link_seqno, info.seqno, "Sequence numbers should match");

    return SUCCESS;
}

/**
 * verify that get_if works as expected
 */
static test_result get_if(EngineIface* h) {
    const std::string key("get_if");

    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, key, "somevalue"),
            "Failed set.");

    if (isPersistentBucket(h)) {
        wait_for_flusher_to_settle(h);
        evict_key(h, key.c_str(), Vbid(0), "Ejected.");
    }

    auto* cookie = testHarness->create_cookie(h);
    auto doc = h->get_if(*cookie,
                         DocKey(key, DocKeyEncodesCollectionId::No),
                         Vbid(0),
                         [](const item_info&) { return true; });
    check(doc.second, "document should be found");

    doc = h->get_if(*cookie,
                    DocKey(key, DocKeyEncodesCollectionId::No),
                    Vbid(0),
                    [](const item_info&) { return false; });
    check(!doc.second, "document should not be found");

    doc = h->get_if(*cookie,
                    DocKey("no", DocKeyEncodesCollectionId::No),
                    Vbid(0),
                    [](const item_info&) { return true; });
    check(!doc.second, "non-existing document should not be found");

    checkeq(cb::engine_errc::success,
            del(h, key.c_str(), 0, Vbid(0)),
            "Failed remove with value");

    doc = h->get_if(*cookie,
                    DocKey(key, DocKeyEncodesCollectionId::No),
                    Vbid(0),
                    [](const item_info&) { return true; });
    check(!doc.second, "deleted document should not be found");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static test_result max_ttl_out_of_range(EngineIface* h) {
    // Test absolute first as this is the bigger time travel
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Flush, "max_ttl", "-1"),
            "Should not be allowed to set a negative value");
    checkeq(cb::engine_errc::invalid_arguments,
            set_param(h, EngineParamCategory::Flush, "max_ttl", "2147483648"),
            "Should not be allowed to set > int32::max");

    return SUCCESS;
}

static test_result max_ttl(EngineIface* h) {
    // Make limit be greater than 30 days in seconds so that ep-engine must
    // create a absolute expiry time internally.
    const int absoluteExpiry = (60 * 60 * 24 * 31);
    auto absoluteExpiryStr = std::to_string(absoluteExpiry);

    const int relativeExpiry = 100;
    auto relativeExpiryStr = std::to_string(relativeExpiry);

    checkeq(0, get_int_stat(h, "ep_max_ttl"), "max_ttl should be 0");

    // Test absolute first as this is the bigger time travel
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "max_ttl",
                      absoluteExpiryStr.c_str()),
            "Failed to set max_ttl");
    checkeq(absoluteExpiry,
            get_int_stat(h, "ep_max_ttl"),
            "max_ttl didn't change");

    // Store will set 0 expiry, which results in 100 seconds of ttl
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key-abs",
                  "somevalue",
                  nullptr,
                  0,
                  Vbid(0),
                  0 /*exp*/),
            "Failed set.");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, "key-abs", errorMetaPair), "Get meta failed");
    checkne(time_t(0),
            errorMetaPair.second.exptime,
            "expiry should not be zero");

    // Force expiry
    testHarness->time_travel(absoluteExpiry + 1);

    auto ret = get(h, nullptr, "key-abs", Vbid(0));
    checkeq(cb::engine_errc::no_such_key,
            ret.first,
            "Failed, expected no_such_key.");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "max_ttl",
                      relativeExpiryStr.c_str()),
            "Failed to set max_ttl");
    checkeq(relativeExpiry,
            get_int_stat(h, "ep_max_ttl"),
            "max_ttl didn't change");

    // Store will set 0 expiry, which results in 100 seconds of ttl
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  "key-rel",
                  "somevalue",
                  nullptr,
                  0,
                  Vbid(0),
                  0 /*exp*/),
            "Failed set.");

    check(get_meta(h, "key-rel", errorMetaPair), "Get meta failed");
    checkne(time_t(0),
            errorMetaPair.second.exptime,
            "expiry should not be zero");

    // Force expiry
    testHarness->time_travel(relativeExpiry + 1);

    ret = get(h, nullptr, "key-rel", Vbid(0));
    checkeq(cb::engine_errc::no_such_key,
            ret.first,
            "Failed, expected no_such_key.");

    return SUCCESS;
}

static test_result max_ttl_setWithMeta(EngineIface* h) {
    // Make limit be greater than 30 days in seconds so that ep-engine must
    // create a absolute expiry time internally.
    const int absoluteExpiry = (60 * 60 * 24 * 31);
    auto absoluteExpiryStr = std::to_string(absoluteExpiry);
    std::string keyAbs = "key-abs";

    const int relativeExpiry = 100;
    auto relativeExpiryStr = std::to_string(relativeExpiry);
    std::string keyRel = "key-rel";

    checkeq(0, get_int_stat(h, "ep_max_ttl"), "max_ttl should be 0");

    // Test absolute first as this is the bigger time travel
    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "max_ttl",
                      absoluteExpiryStr.c_str()),
            "Failed to set max_ttl");
    checkeq(absoluteExpiry,
            get_int_stat(h, "ep_max_ttl"),
            "max_ttl didn't change");

    // SWM with 0 expiry which results in an expiry being set
    ItemMetaData itemMeta(0xdeadbeef, 10, 0xf1a95, 0 /*expiry*/);
    checkeq(cb::engine_errc::success,
            set_with_meta(h, keyAbs, keyAbs, Vbid(0), &itemMeta, 0 /*cas*/),
            "Expected to store item");

    cb::EngineErrorMetadataPair errorMetaPair;
    check(get_meta(h, keyAbs.c_str(), errorMetaPair), "Get meta failed");
    checkne(time_t(0),
            errorMetaPair.second.exptime,
            "expiry should not be zero");

    // Force expiry
    testHarness->time_travel(absoluteExpiry + 1);

    auto ret = get(h, nullptr, keyAbs.c_str(), Vbid(0));
    checkeq(cb::engine_errc::no_such_key,
            ret.first,
            "Failed, expected no_such_key.");

    checkeq(cb::engine_errc::success,
            set_param(h,
                      EngineParamCategory::Flush,
                      "max_ttl",
                      relativeExpiryStr.c_str()),
            "Failed to set max_ttl");
    checkeq(relativeExpiry,
            get_int_stat(h, "ep_max_ttl"),
            "max_ttl didn't change");

    checkeq(cb::engine_errc::success,
            set_with_meta(h, keyRel, keyRel, Vbid(0), &itemMeta, 0 /*cas*/),
            "Expected to store item");

    check(get_meta(h, keyRel.c_str(), errorMetaPair), "Get meta failed");
    checkne(time_t(0),
            errorMetaPair.second.exptime,
            "expiry should not be zero");

    // Force expiry
    testHarness->time_travel(relativeExpiry + 1);

    ret = get(h, nullptr, keyRel.c_str(), Vbid(0));
    checkeq(cb::engine_errc::no_such_key,
            ret.first,
            "Failed, expected no_such_key.");

    // Final test, exceed the maxTTL and check we got capped!
#if 0
    // TN: This piece of the test is broken as the set call fails with
    //     KeyEExists.. I haven't looked in details on what we're actually
    //     trying to test.. Disable for now
    itemMeta.exptime = errorMetaPair.second.exptime + 1000;
    checkeq(cb::engine_errc::success,
            set_with_meta(h,
                          keyRel.c_str(),
                          keyRel.size(),
                          keyRel.c_str(),
                          keyRel.size(),
                          Vbid(0),
                          &itemMeta,
                          0 /*cas*/),
            "Expected to store item");

    check(get_meta(h, keyRel.c_str(), errorMetaPair), "Get meta failed");
    checkne(itemMeta.exptime,
            errorMetaPair.second.exptime,
            "expiry should have been changed/capped");
#endif
    return SUCCESS;
}

// MB-53953: Ensure bucket shutdowns if a scan is created yet never completed
static enum test_result test_range_scan_no_cancel(EngineIface* h) {
    checkeq(cb::engine_errc::success,
            store(h, nullptr, StoreSemantics::Set, "key", "somevalue"),
            "Failed set.");

    wait_for_flusher_to_settle(h);

    auto* cookie = testHarness->create_cookie(h);

    // Create a scan and do nothing - it should not block shutdown
    checkeq(cb::engine_errc::success,
            h->createRangeScan(*cookie,
                               cb::rangescan::CreateParameters{
                                       Vbid(0),
                                       CollectionID::Default,
                                       {"a"},
                                       {"z"},
                                       cb::rangescan::KeyOnly::Yes,
                                       {},
                                       {}})
                    .first,
            "createRangeScan failed");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////
// Test manifest //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

const char* default_dbname = "./ep_testsuite_basic.db";

BaseTestCase testsuite_testcases[] = {
        TestCase("test alloc limit",
                 test_alloc_limit,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test_memory_tracking",
                 test_memory_tracking,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test max_size - water_mark changes",
                 test_max_size_and_water_marks_settings,
                 test_setup,
                 teardown,
                 "max_size=1000;ht_locks=1;ht_size=3",
                 prepare,
                 cleanup),
        TestCase("test whitespace dbname",
                 test_whitespace_db,
                 test_setup,
                 teardown,
                 "ht_locks=1;ht_size=3",
                 prepare,
                 test_whitespace_db_cleanup),
        TestCase("get miss",
                 test_get_miss,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set", test_set, test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("concurrent set",
                 test_conc_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("multi set",
                 test_multi_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("set+get hit",
                 test_set_get_hit,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test getl then del with cas",
                 test_getl_delete_with_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test getl then del with bad cas",
                 test_getl_delete_with_bad_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test getl then set with meta",
                 test_getl_set_del_with_meta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("getl",
                 test_getl,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("unl", test_unl, test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("unl not my vbucket",
                 test_unl_nmvb,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set+get hit (bin)",
                 test_set_get_hit_bin,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set with cas non-existent",
                 test_set_with_cas_non_existent,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set+change flags",
                 test_set_change_flags,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("add", test_add, test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("add+add(same cas)",
                 test_add_add_with_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("cas", test_cas, test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("replace",
                 test_replace,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test touch",
                 test_touch,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test touch (MB-7342)",
                 test_touch_mb7342,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test touch (MB-10277)",
                 test_touch_mb10277,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_ep_bucket,
                 cleanup),
        TestCase("test gat",
                 test_gat,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test locked gat",
                 test_gat_locked,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test locked touch",
                 test_touch_locked,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test mb5215",
                 test_mb5215,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: implement getItemCount. Needs the
                 // 'ep_num_non_resident' stat.
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("delete",
                 test_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete with value",
                 test_delete_with_value,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: vBucket num_items not correct under Rocks when
                  * full eviction */
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("delete with value CAS",
                 test_delete_with_value_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set/delete",
                 test_set_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_skip_broken_under_rocks_full_eviction,
                 cleanup),
        TestCase("set/delete (invalid cas)",
                 test_set_delete_invalid_cas,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("delete/set/delete",
                 test_delete_set,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("get/delete with missing db file",
                 test_get_delete_missing_file,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: This test fails because under RocksDB we can
                 // still find a key after deleting the DB file and evicting
                 // the key in the internal MemTable (which is also used as
                 // read-cache).
                 // TODO magma: uses couchstore specific functions
                 prepare_ep_bucket_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("vbucket deletion doesn't affect new data",
                 test_bug7023,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: implement getItemCount. Needs the
                 // 'ep_warmup_value_count' stat.
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("non-resident decrementers",
                 test_mb3169,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: implement getItemCount. Needs the 'curr_items'
                 // stat.
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("resident ratio after warmup",
                 test_mb5172,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("set vb 10000",
                 test_set_vbucket_out_of_range,
                 test_setup,
                 teardown,
                 "max_vbuckets=1024",
                 prepare,
                 cleanup),
        TestCase("set max_cas MB21190",
                 set_max_cas_mb21190,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("warmup_mb21769",
                 warmup_mb21769,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("pre_link_document",
                 pre_link_document,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("engine get_if",
                 get_if,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test max_ttl range",
                 max_ttl_out_of_range,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test max_ttl",
                 max_ttl,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test max_ttl_setWithMeta",
                 max_ttl_setWithMeta,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test_range_scan_no_cancel",
                 test_range_scan_no_cancel,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare_skip_broken_under_ephemeral_and_rocks,
                 cleanup),

        // sentinel
        TestCase(nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
