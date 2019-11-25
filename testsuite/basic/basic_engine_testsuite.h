/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <memcached/engine_testapp.h>

extern "C" {
    MEMCACHED_PUBLIC_API
    engine_test_t* get_tests(void);

    MEMCACHED_PUBLIC_API
    bool setup_suite(struct test_harness *th);

    MEMCACHED_PUBLIC_API
    bool teardown_suite();
}
