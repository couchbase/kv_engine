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

#pragma once

#include "ep_test_apis.h"
#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <sstream>
#include <vector>

std::ostream& operator<<(std::ostream& os, const ObserveKeyState& oks);

template <typename T>
static void checkeqfn(T exp,
                      T got,
                      const std::string& msg,
                      const char* file,
                      const int linenum) {
    if (exp != got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "', got `" << got << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

template <typename T>
static void checknefn(T exp,
                      T got,
                      const std::string& msg,
                      const char* file,
                      const int linenum) {
    if (exp == got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "' to not equal `" << got << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

template <typename T>
static void checklefn(T exp,
                      T got,
                      const std::string& msg,
                      const char* file,
                      const int linenum) {
    if (exp > got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "' to be less than or equal to `" << got
           << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

template <typename T>
static void checkltfn(T exp, T got, const char *msg, const char *file,
                      const int linenum) {
    if (exp >= got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "' to be less than `" << got
           << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

template <typename T>
static void checkgefn(T exp, T got, const char *msg, const char *file,
                      const int linenum) {
    if (exp < got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "' to be greater than or equal to `" << got
           << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

template <typename T>
static void checkgtfn(T exp, T got, const char *msg, const char *file,
                      const int linenum) {
    if (exp <= got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "' to be greater than `" << got
           << "' - " << msg;
        abort_msg(ss.str().c_str(), "", file, linenum);
    }
}

#define checkeq(expected, actual, msg) \
    checkeqfn(expected, actual, msg, __FILE__, __LINE__)
#define checkne(expected, actual, msg) \
    checknefn(expected, actual, msg, __FILE__, __LINE__)
#define checkle(expected, actual, msg) \
    checklefn(expected, actual, msg, __FILE__, __LINE__)
#define checklt(expected, actual, msg) \
    checkltfn(expected, actual, msg, __FILE__, __LINE__)
#define checkge(expected, actual, msg) \
    checkgefn(expected, actual, msg, __FILE__, __LINE__)
#define checkgt(expected, actual, msg) \
    checkgtfn(expected, actual, msg, __FILE__, __LINE__)

class BaseTestCase {
public:
    BaseTestCase(const char *_name,  const char *_cfg, bool _skip = false);

    BaseTestCase(const BaseTestCase& o) = default;

    engine_test_t *getTest();

    const char *getName() {
        return name;
    }

protected:
    engine_test_t test = {};

private:
    const char *name;
    const char *cfg;
    bool skip;
};

class TestCase : public BaseTestCase {
public:
    TestCase(const char* _name,
             enum test_result (*_tfun)(EngineIface*),
             bool (*_test_setup)(EngineIface*),
             bool (*_test_teardown)(EngineIface*),
             const char* _cfg,
             enum test_result (*_prepare)(engine_test_t* test),
             void (*_cleanup)(engine_test_t* test, enum test_result result),
             bool _skip = false);
};

class TestCaseV2 : public BaseTestCase {
public:
    TestCaseV2(const char *_name,
               enum test_result(*_tfun)(engine_test_t *),
               bool(*_test_setup)(engine_test_t *),
               bool(*_test_teardown)(engine_test_t *),
               const char *_cfg,
               enum test_result (*_prepare)(engine_test_t *test),
               void (*_cleanup)(engine_test_t *test, enum test_result result),
               bool _skip = false);
};

// Name to use for database directory
extern const char *dbname_env;

// Handle of the test_harness, provided by engine_testapp.
extern struct test_harness* testHarness;

// Default DB name. Provided by the specific testsuite.
extern const char* default_dbname;

enum test_result rmdb(std::string_view path);

// Default testcase setup function
bool test_setup(EngineIface* h);

// Default testcase teardown function
bool teardown(EngineIface* h);
bool teardown_v2(engine_test_t* test);


// Default testcase prepare function.
enum test_result prepare(engine_test_t *test);

/// Prepare a test which is currently broken (i.e. under investigation).
enum test_result prepare_broken_test(engine_test_t* test);

/**
 * Prepare a test which is only applicable for persistent buckets (EPBucket) -
 * for other types it should be skipped.
 */
enum test_result prepare_ep_bucket(engine_test_t* test);

/**
 * Prepare a test which is currently expected to fail when using
 * Magma and skip the test.
 * As Magma mature, these tests should be revisited.
 */
enum test_result prepare_skip_broken_under_magma(engine_test_t* test);

/**
 * Prepare a test which is only applicable to a persistent bucket, but
 * is currently expected to fail when using Magma and so should
 * be skipped.
 */
enum test_result prepare_ep_bucket_skip_broken_under_magma(engine_test_t* test);

/**
 * Prepare a test which is only applicable for ephemeral buckets
 * (EphemeralBucket) - for other types it should be skipped.
 */
enum test_result prepare_ephemeral_bucket(engine_test_t* test);

/**
 * Prepare a test which is only applicable to full eviction mode - for
 * for other eviction types it should be skipped.
 */
enum test_result prepare_full_eviction(engine_test_t *test);

/**
 * TODO TEMPORARY:
 * Prepare a test which currently is broken for Ephemeral buckets and so
 * should be skipped for them for now.
 *
 * Any test using this *should* eventually pass, so these should be fixed.
 */
enum test_result prepare_skip_broken_under_ephemeral(engine_test_t *test);


// Default testcase cleanup function.
void cleanup(engine_test_t *test, enum test_result result);

struct BucketHolder {
    BucketHolder(EngineIface* _h, std::string _dbpath)
        : h(_h), dbpath(std::move(_dbpath)) {
    }

    EngineIface* h;
    const std::string dbpath;
};

/*
  Create n_buckets and add to the buckets vector.
  Returns the number of buckets actually created.
*/
int create_buckets(const std::string& cfg,
                   int n_buckets,
                   std::vector<BucketHolder>& buckets);

/*
  Destroy all of the buckets in the vector and delete the DB path.
*/
void destroy_buckets(std::vector<BucketHolder> &buckets);

// Verifies that the given key and value exist in the store.
void check_key_value(EngineIface* h,
                     const char* key,
                     const char* val,
                     size_t vlen,
                     Vbid vbucket = Vbid(0));

std::string get_dbname(const std::string& test_cfg);

// Returns true if Compression is enabled for the given engine.
bool isCompressionEnabled(EngineIface* h);

// Returns true if passive compression is enabled for the given engine.
bool isPassiveCompressionEnabled(EngineIface* h);

// Returns true if active compression is enabled for the given engine.
bool isActiveCompressionEnabled(EngineIface* h);

// Returns true if Warmup is enabled for the given engine.
bool isWarmupEnabled(EngineIface* h);

bool isSecondaryWarmupEnabled(EngineIface* h);

// Returns true if the given engine is a persistent bucket (EPBucket).
bool isPersistentBucket(EngineIface* h);

// Returns true if the given engine is a magma bucket.
bool isMagmaBucket(EngineIface* h);

// Returns true if the given engine is an ephemeral bucket (EphemeralBucket).
bool isEphemeralBucket(EngineIface* h);

// Returns true if the the given engine is using the FollyExecutorPool backend.
bool isFollyExecutorPool(EngineIface* h);

// Checks number of temp items in a persistent bucket (EPBucket).
void checkPersistentBucketTempItems(EngineIface* h, int exp);

// Set the Bucket quota to the given value and wait for it to take effect
void setAndWaitForQuotaChange(EngineIface* h, uint64_t newQuota);
