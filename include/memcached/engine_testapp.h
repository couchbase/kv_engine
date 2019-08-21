/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <daemon/bucket_type.h>
#include <memcached/engine.h>
#include <functional>

enum class OutputFormat {
    Text,
    XML,
};

enum test_result {
    SUCCESS = 11,
    SKIPPED = 12,
    FAIL = 13,
    DIED = 14,
    CORE = 15,
    PENDING = 19,
    TIMEOUT = 23,
    SUCCESS_AFTER_RETRY = 24,
    SKIPPED_UNDER_ROCKSDB = 25,
    SKIPPED_UNDER_MAGMA = 26
};

/**
 * Exception thrown by the checkXX() functions if a test expecation fails.
 *
 * Note this deliberately doesn't inherit from std::exception; so it shouldn't
 * be caught by any catch() statements in the engine and is propogated
 * back to the test harness.
 */
struct MEMCACHED_PUBLIC_CLASS TestExpectationFailed {};

typedef struct test engine_test_t;

using PreLinkFunction = std::function<void(item_info&)>;

/**
 * The test_harness structure provides an API for the various test
 * cases to manipulate the test framework
 */
struct test_harness {
    virtual ~test_harness() = default;

    BucketType bucketType = BucketType::Unknown;
    const char* default_engine_cfg = nullptr;
    OutputFormat output_format = OutputFormat::Text;
    const char* output_file_prefix = "output.";
    std::string bucket_type;

    /**
     * The method to call notify_io_complete if one don't have a copy
     * of the server API
     *
     * @param cookie the cookie to notify
     * @param status the status code to set for the cookie
     */
    virtual void notify_io_complete(const void* cookie,
                                    ENGINE_ERROR_CODE status) = 0;

    /**
     * Create a new cookie instance
     */
    virtual const void* create_cookie() = 0;

    /**
     * Destroy a cookie (and invalidate the allocated memory)
     */
    virtual void destroy_cookie(const void* cookie) = 0;

    /**
     * Set the ewouldblock mode for the specified cookie
     */
    virtual void set_ewouldblock_handling(const void* cookie, bool enable) = 0;

    /**
     * Set if mutations_extra's should be handled for the specified cookie
     */
    virtual void set_mutation_extras_handling(const void* cookie,
                                              bool enable) = 0;
    /**
     * Set the datatypes the cookie should support
     */
    virtual void set_datatype_support(const void* cookie,
                                      protocol_binary_datatype_t datatypes) = 0;
    /**
     * Set if collections is enabled for the specified cookie
     */
    virtual void set_collections_support(const void* cookie, bool enable) = 0;

    /**
     * Lock the specified cookie
     */
    virtual void lock_cookie(const void* cookie) = 0;

    /**
     * Unlock the specified cookie
     */
    virtual void unlock_cookie(const void* cookie) = 0;

    /**
     * Wait for a cookie to be notified
     */
    virtual void waitfor_cookie(const void* cookie) = 0;

    /**
     * Store the specified pointer in the specified cookie
     */
    virtual void store_engine_specific(const void* cookie,
                                       void* engine_data) = 0;

    /**
     * Get the number of references for the specified cookie
     */
    virtual int get_number_of_mock_cookie_references(const void* cookie) = 0;

    /**
     * Add the specified offset (in seconds) to the internal clock. (Adding
     * a negative value is supported)
     */
    virtual void time_travel(int offset) = 0;

    /**
     * Get the handle to the current test case
     */
    virtual const engine_test_t* get_current_testcase() = 0;

    /**
     * Try to release memory back to the operating system
     */
    virtual void release_free_memory() = 0;

    /**
     * Create a new bucket instance
     * @param initialize set to true if the initialize method should be called
     *                   on the newly created instance
     * @param cfg The configuration to pass to initialize (if enabled)
     * @return The newly created engine or nullptr if create failed
     */
    virtual EngineIface* create_bucket(bool initialize, const char* cfg) = 0;

    /**
     * Destroy (and invalidate) the specified bucket
     *
     * @param bucket the bucket to destroy
     * @param force if the bucket should be allwed a graceful shutdown or not
     */
    virtual void destroy_bucket(EngineIface* bucket, bool force) = 0;

    /**
     * Try to reload the current engine
     *
     * @param handle a pointer to the old handle (and the new one is returned)
     * @param cfg the configuration to use
     * @param init it initialize should be called or not
     * @param force should the old one be shut down with force or not
     */
    virtual void reload_engine(EngineIface** h,
                               const char* cfg,
                               bool init,
                               bool force) = 0;

    /**
     * Set the method which should be called as part of the pre-link step
     * for a document
     */
    virtual void set_pre_link_function(PreLinkFunction function) = 0;
};

/*
    API v2 gives access to the test struct and delegates bucket create/destroy
    to the test.
    test cases can now interleave bucket(s) creation and I/O
*/
struct test_api_v2 {
    enum test_result(*tfun)(engine_test_t *test);
    bool(*test_setup)(engine_test_t *test);
    bool(*test_teardown)(engine_test_t *test);
};

struct test {
    const char *name;
    enum test_result (*tfun)(EngineIface*);
    bool (*test_setup)(EngineIface*);
    bool (*test_teardown)(EngineIface*);

    const char *cfg;
    /**
     * You might want to prepare the environment for running
     * the test <em>before</em> the engine is loaded.
     * @param test the test about to be started
     * @return An appropriate "status" code
     */
    enum test_result (*prepare)(engine_test_t *test);

    /**
     * You might want to clean up after the test
     * @param test the test that just finished
     * @param th result of the test
     */
    void (*cleanup)(engine_test_t *test, enum test_result result);

    struct test_api_v2 api_v2;
};

#define TEST_CASE(name, test, setup, teardown, cfg, prepare, cleanup) \
    {name, test, setup, teardown, cfg, prepare, cleanup,\
     {NULL, NULL, NULL}}

#define TEST_CASE_V2(name, test, setup, teardown, cfg, prepare, cleanup) \
    {name, NULL, NULL, NULL, cfg, prepare, cleanup,\
     {test, setup, teardown}}

typedef engine_test_t* (*GET_TESTS)(void);

typedef bool (*SETUP_SUITE)(struct test_harness *);

typedef bool (*TEARDOWN_SUITE)(void);
