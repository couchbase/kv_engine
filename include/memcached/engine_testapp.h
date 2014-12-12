/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_ENGINE_TESTAPP_H
#define MEMCACHED_ENGINE_TESTAPP_H

#include <memcached/engine.h>

#ifdef    __cplusplus
extern "C" {
#endif

enum test_result {
    SUCCESS = 11,
    SKIPPED = 12,
    FAIL = 13,
    DIED = 14,
    CORE = 15,
    PENDING = 19,
    TIMEOUT = 23
};

typedef struct test engine_test_t;

struct test_harness {
    const char *engine_path;
    const char *default_engine_cfg;
    void(*reload_engine)(ENGINE_HANDLE **, ENGINE_HANDLE_V1 **,
                         const char *, const char *, bool, bool);
    const void *(*create_cookie)(void);
    void (*destroy_cookie)(const void *cookie);
    void (*set_ewouldblock_handling)(const void *cookie, bool enable);
    void (*set_mutation_extras_handling)(const void *cookie, bool enable);
    void (*set_datatype_support)(const void *cookie, bool enable);
    void (*lock_cookie)(const void *cookie);
    void (*unlock_cookie)(const void *cookie);
    void (*waitfor_cookie)(const void *cookie);
    void (*time_travel)(int offset);
    const engine_test_t* (*get_current_testcase)(void);
    size_t (*get_mapped_bytes)(void);
    void (*release_free_memory)(void);

    ENGINE_HANDLE_V1* (*create_bucket)(bool initialize, const char* cfg);
    void (*destroy_bucket)(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1, bool force);
    void(*reload_bucket)(ENGINE_HANDLE **, ENGINE_HANDLE_V1 **,
                         const char *, bool, bool);
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
    enum test_result(*tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
    bool(*test_setup)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
    bool(*test_teardown)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);


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

#ifdef    __cplusplus
}
#endif

#endif    /* MEMCACHED_ENGINE_TESTAPP_H */
