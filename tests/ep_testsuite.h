#ifndef TESTS_EP_TESTSUITE_H_
#define TESTS_EP_TESTSUITE_H_ 1

#include "config.h"

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
bool teardown_suite(void);

#ifdef __cplusplus
}
#endif

#endif  // TESTS_EP_TESTSUITE_H_
