/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// mock_cookie.h must be included before ep_test_apis.h as ep_test_apis.h
// define a macro named check and some of the folly headers also use the
// name check
#include <programs/engine_testapp/mock_cookie.h>

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include <platform/cb_malloc.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <iostream>
#include <string>

const char *dbname_env = nullptr;

static enum test_result skipped_test_function(EngineIface* h);

BaseTestCase::BaseTestCase(const char *_name, const char *_cfg, bool _skip)
  : name(_name),
    cfg(_cfg),
    skip(_skip) {
}

TestCase::TestCase(const char* _name,
                   enum test_result (*_tfun)(EngineIface*),
                   bool (*_test_setup)(EngineIface*),
                   bool (*_test_teardown)(EngineIface*),
                   const char* _cfg,
                   enum test_result (*_prepare)(engine_test_t* test),
                   void (*_cleanup)(engine_test_t* test,
                                    enum test_result result),
                   bool _skip)
    : BaseTestCase(_name, _cfg, _skip) {
    test.tfun = _tfun;
    test.test_setup = _test_setup;
    test.test_teardown = _test_teardown;
    test.prepare = _prepare;
    test.cleanup = _cleanup;
}

TestCaseV2::TestCaseV2(const char *_name,
                       enum test_result(*_tfun)(engine_test_t *),
                       bool(*_test_setup)(engine_test_t *),
                       bool(*_test_teardown)(engine_test_t *),
                       const char *_cfg,
                       enum test_result (*_prepare)(engine_test_t *test),
                       void (*_cleanup)(engine_test_t *test, enum test_result result),
                       bool _skip)
  : BaseTestCase(_name, _cfg, _skip) {
    test.api_v2.tfun = _tfun;
    test.api_v2.test_setup = _test_setup;
    test.api_v2.test_teardown = _test_teardown;
    test.prepare = _prepare;
    test.cleanup = _cleanup;
}

engine_test_t* BaseTestCase::getTest() {
    engine_test_t *ret = &test;

    std::string nm(name);
    std::stringstream ss;

    if (cfg != nullptr) {
        ss << cfg << ";";
    }

    // Default to the suite's dbname if the test config didn't already
    // specify it.
    if ((cfg == nullptr) ||
        (std::string(cfg).find("dbname=") == std::string::npos)) {
        ss << "dbname=" << default_dbname << ";";
    }

    // Default to 4 vBuckets (faster to setup/teardown) if the test config
    // didn't already specify it or shards.
    if ((cfg == nullptr) ||
        ((std::string(cfg).find("max_vbuckets=") == std::string::npos) &&
        (std::string(cfg).find("max_num_shards=") == std::string::npos))) {
        ss << "max_vbuckets=4;max_num_shards=4;";
    }

    if (skip) {
        nm.append(" (skipped)");
        ret->tfun = skipped_test_function;
    }

    ret->name = std::move(nm);
    ret->cfg = ss.str();

    return ret;
}

static enum test_result skipped_test_function(EngineIface*) {
    return SKIPPED;
}

enum test_result rmdb(const char* path) {
    if (cb::io::isDirectory(path)) {
        cb::io::rmrf(path);
    }
    if (cb::io::isDirectory(path) || cb::io::isFile(path)) {
        std::cerr << "Failed to remove: " << path << " " << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

bool test_setup(EngineIface* h) {
    wait_for_warmup_complete(h);

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set VB0 state.");

    const auto bucket_type = get_str_stat(h, "ep_bucket_type");
    if (bucket_type == "persistent") {
        // Wait for vb0's state (active) to be persisted to disk, that way
        // we know the KVStore files exist on disk.
        wait_for_stat_to_be_gte(h, "ep_persist_vbstate_total", 1);
    } else if (bucket_type == "ephemeral") {
        // No persistence to wait for here.
    } else {
        check(false,
              (std::string("test_setup: unknown bucket_type '") + bucket_type +
               "' - cannot continue.")
                      .c_str());
        return false;
    }

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    // warmup is complete, notify ep engine that it must now enable
    // data traffic
    auto request = createPacket(cb::mcbp::ClientOpcode::EnableTraffic);

    checkeq(cb::engine_errc::success,
            h->unknown_command(cookie.get(), *request, add_response),
            "Failed to enable data traffic");

    return true;
}

bool teardown(EngineIface*) {
    vals.clear();
    return true;
}

bool teardown_v2(engine_test_t* test) {
    (void)test;
    vals.clear();
    return true;
}

std::string get_dbname(const std::string& test_cfg) {
    if (test_cfg.empty()) {
        return dbname_env;
    }

    auto idx = test_cfg.find("dbname=");
    if (idx == std::string::npos) {
        return dbname_env;
    }

    auto dbname = test_cfg.substr(idx + 7);
    std::string::size_type end = dbname.find(';');
    if (end != dbname.npos) {
        dbname.resize(end);
    }

    return dbname;
}

enum test_result prepare(engine_test_t *test) {
    std::string dbname = get_dbname(test->cfg);
    /* Remove if the same DB directory already exists */
    try {
        rmdb(dbname.c_str());
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }
    cb::io::mkdirp(dbname);
    return SUCCESS;
}

/// Prepare a test which is currently broken (i.e. under investigation).
enum test_result prepare_broken_test(engine_test_t* test) {
    return SKIPPED;
}

enum test_result prepare_ep_bucket(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_rocks(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_magma(
        engine_test_t* test) {
    if (std::string(test->cfg).find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_rocks_and_magma(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }
    if (cfg.find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_rocks_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    if (cfg.find("backend=rocksdb") != std::string::npos &&
        cfg.find("item_eviction_policy=full_eviction")) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    return prepare_ep_bucket(test);
}

enum test_result prepare_skip_broken_under_rocks(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_magma(engine_test_t* test) {
    if (std::string(test->cfg).find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_rocks_and_magma(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }
    if (cfg.find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_ephemeral_and_rocks(
        engine_test_t* test) {
    return prepare_ep_bucket_skip_broken_under_rocks(test);
}

enum test_result prepare_ephemeral_bucket(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_full_eviction(engine_test_t *test) {

    // If we cannot find FE in the config, skip the test
    if (std::string(test->cfg).find("item_eviction_policy=full_eviction") ==
        std::string::npos) {
        return SKIPPED;
    }

    // Ephemeral buckets don't support full eviction.
    if (std::string(test->cfg).find("bucket_type=ephemeral")
            != std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_full_eviction_skip_under_rocks(engine_test_t *test) {

    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);

}

enum test_result prepare_skip_broken_under_rocks_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        if (cfg.find("backend=rocksdb") != std::string::npos &&
            cfg.find("item_eviction_policy=full_eviction")) {
            return SKIPPED_UNDER_ROCKSDB;
        }
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_skip_broken_under_ephemeral(engine_test_t *test) {
    return prepare_ep_bucket(test);
}

void cleanup(engine_test_t *test, enum test_result result) {
    (void)result;
    // Nuke the database files we created
    std::string dbname = get_dbname(test->cfg);
    /* Remove only the db file this test created */
    try {
        rmdb(dbname.c_str());
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }
}

// Should only one test be run, and if so which number? If -1 then all tests
// are run.
static int oneTestIdx;

struct test_harness* testHarness;

// Array of testcases. Provided by the specific testsuite.
extern BaseTestCase testsuite_testcases[];

// Examines the list of tests provided by the specific testsuite
// via the testsuite_testcases[] array, populates `testcases` and returns it.
std::vector<engine_test_t> get_tests() {
    std::vector<engine_test_t> testcases;

    // Calculate the size of the tests..
    int num = 0;
    while (testsuite_testcases[num].getName() != nullptr) {
        ++num;
    }

    oneTestIdx = -1;
    char *testNum = getenv("EP_TEST_NUM");
    if (testNum) {
        sscanf(testNum, "%d", &oneTestIdx);
        if (oneTestIdx < 0 || oneTestIdx > num) {
            oneTestIdx = -1;
        }
    }
    dbname_env = getenv("EP_TEST_DIR");
    if (!dbname_env) {
        dbname_env = default_dbname;
    }

    if (oneTestIdx == -1) {
        for (int jj = 0; jj < num; ++jj) {
            auto* r = testsuite_testcases[jj].getTest();
            if (r) {
                testcases.emplace_back(*r);
            }
        }
    } else {
        auto* r = testsuite_testcases[oneTestIdx].getTest();
        if (r) {
            testcases.emplace_back(*r);
        }
    }

    return testcases;
}

BucketType get_bucket_type() {
    return BucketType::Couchbase;
}

bool setup_suite(struct test_harness *th) {
    testHarness = th;
    return true;
}


bool teardown_suite() {
    return true;
}

/*
 * Create n_buckets and return how many were actually created.
 */
int create_buckets(const std::string& cfg,
                   int n_buckets,
                   std::vector<BucketHolder>& buckets) {
    std::string dbname = get_dbname(cfg);

    for (int ii = 0; ii < n_buckets; ii++) {
        std::stringstream config, dbpath;
        dbpath << dbname.c_str() << ii;
        std::string str_cfg(cfg);
        /* Find the position of "dbname=" in str_cfg */
        size_t pos = str_cfg.find("dbname=");
        if (pos != std::string::npos) {
            /* Move till end of the dbname */
            size_t new_pos = str_cfg.find(';', pos);
            str_cfg.insert(new_pos, std::to_string(ii));
            config << str_cfg;
        } else {
            config << str_cfg << "dbname=" << dbpath.str();
        }

        try {
            rmdb(dbpath.str().c_str());
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        auto* handle = testHarness->create_bucket(true, config.str());
        if (handle) {
            buckets.emplace_back(BucketHolder(handle, dbpath.str()));
        } else {
            return ii;
        }
    }
    return n_buckets;
}

void destroy_buckets(std::vector<BucketHolder> &buckets) {
    for(auto bucket : buckets) {
        testHarness->destroy_bucket(bucket.h, false);
        rmdb(bucket.dbpath.c_str());
    }
}

void check_key_value(EngineIface* h,
                     const char* key,
                     const char* val,
                     size_t vlen,
                     Vbid vbucket) {
    // Fetch item itself, to ensure we maintain a ref-count on the underlying
    // Blob while comparing the key.
    auto getResult = get(h, nullptr, key, vbucket);
    checkeq(cb::engine_errc::success,
            getResult.first,
            "Failed to fetch document");
    item_info info;
    check(h->get_item_info(*getResult.second.get(), info),
          "Failed to get_item_info");

    std::string_view payload;
    cb::compression::Buffer inflated;
    if (isCompressionEnabled(h) &&
        (info.datatype & PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                 {static_cast<const char *>(info.value[0].iov_base),
                                  info.value[0].iov_len},
                                 inflated);
        payload = inflated;
    } else {
        payload = {static_cast<const char *>(info.value[0].iov_base),
                                             info.value[0].iov_len};
    }

    checkeq(std::string_view(val, vlen), payload, "Data mismatch");
}

bool isCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() != BucketCompressionMode::Off;
}

bool isActiveCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() == BucketCompressionMode::Active;
}

bool isPassiveCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() == BucketCompressionMode::Passive;
}

bool isWarmupEnabled(EngineIface* h) {
    return isPersistentBucket(h) && get_bool_stat(h, "ep_warmup");
}

bool isPersistentBucket(EngineIface* h) {
    return get_str_stat(h, "ep_bucket_type") == "persistent";
}

bool isEphemeralBucket(EngineIface* h) {
    return get_str_stat(h, "ep_bucket_type") == "ephemeral";
}

bool isFollyExecutorPool(EngineIface* h) {
    return get_str_stat(h, "ep_executor_pool_backend", "config") == "folly";
}

void checkPersistentBucketTempItems(EngineIface* h, int exp) {
    if (isPersistentBucket(h)) {
        checkeq(exp,
                get_int_stat(h, "curr_temp_items"),
                "CheckPersistentBucketTempItems(): Num temp items not as "
                "expected");
    }
}
