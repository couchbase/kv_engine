#ifndef EP_TESTSUITE_H
#define EP_TESTSUITE_H 1

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#ifdef __cplusplus
extern "C" {
#endif

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th);

MEMCACHED_PUBLIC_API
bool teardown_suite();

enum test_result prepare(engine_test_t *test);

void cleanup(engine_test_t *test, enum test_result result);

#ifdef __cplusplus
}
#endif

#endif
