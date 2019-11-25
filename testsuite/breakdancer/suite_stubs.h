/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include "disable_optimize.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <platform/cbassert.h>

#ifdef __cplusplus
extern "C" {
#endif

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th);

MEMCACHED_PUBLIC_API
bool teardown_suite(void);

bool test_setup(EngineIface* h);
bool teardown(EngineIface* h);
void delay(int amt);
void add(EngineIface* h);
void flush(EngineIface* h);
void del(EngineIface* h);
void set(EngineIface* h);

void checkValue(EngineIface* h, const char* exp);
void assertNotExists(EngineIface* h);

#define assertHasError() cb_assert(hasError)
#define assertHasNoError() cb_assert(!hasError)

extern int expiry;
extern bool hasError;
extern struct test_harness* testHarness;

#ifdef __cplusplus
}
#endif
