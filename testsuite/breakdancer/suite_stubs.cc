/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>

#include <memcached/engine.h>
#include <platform/cb_malloc.h>

#include "suite_stubs.h"

int expiry = 3600;
bool hasError = false;
struct test_harness testHarness;

static DocKey key("key", DocNamespace::DefaultCollection);

bool test_setup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    delay(2);
    return true;
}

bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    return true;
}

void delay(int amt) {
    testHarness.time_travel(amt);
    hasError = false;
}

static void storeItem(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      ENGINE_STORE_OPERATION op) {
    uint64_t cas = 0;
    const char *value = "0";
    const int flags = 0;
    const void* cookie = testHarness.create_cookie();
    size_t vlen;
    item_info info;

    vlen = strlen(value);
    auto ret = h1->allocate(
            cookie, key, vlen, flags, expiry, PROTOCOL_BINARY_RAW_BYTES, 0);
    cb_assert(ret.first == cb::engine_errc::success);

    if (!h1->get_item_info(h, ret.second.get(), &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, ret.second.get(), 0);

    auto rv =
            h1->store(cookie, ret.second.get(), cas, op, DocumentState::Alive);

    testHarness.destroy_cookie(cookie);
    hasError = rv != ENGINE_SUCCESS;
}

void add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_ADD);
}

void flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto* cookie = testHarness.create_cookie();
    hasError = h1->flush(h, cookie);
    testHarness.destroy_cookie(cookie);
}

void del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
	uint64_t cas = 0;
	mutation_descr_t mut_info;
        const auto* cookie = testHarness.create_cookie();
        hasError = h1->remove(cookie, key, cas, 0, mut_info) != ENGINE_SUCCESS;
        testHarness.destroy_cookie(cookie);
}

void set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_SET);
}

void checkValue(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* exp) {
    const auto* cookie = testHarness.create_cookie();
    auto rv = h1->get(cookie, key, 0, DocStateFilter::Alive);
    testHarness.destroy_cookie(cookie);
    cb_assert(rv.first == cb::engine_errc::success);

    item_info info;
    h1->get_item_info(h, rv.second.get(), &info);

    char* buf = new char[info.value[0].iov_len + 1];
    memcpy(buf, info.value[0].iov_base, info.value[0].iov_len);
    buf[info.value[0].iov_len] = 0x00;
    if (strlen(exp) > info.value[0].iov_len) {
        fprintf(stderr, "Expected at least %d bytes for ``%s'', got %d as ``%s''\n",
                (int)strlen(exp), exp, (int)info.value[0].iov_len, buf);
        abort();
    }

    if (memcmp(info.value[0].iov_base, exp, strlen(exp)) != 0) {
        fprintf(stderr, "Expected ``%s'', got ``%s''\n", exp, buf);
        abort();
    }

    delete []buf;
}

void assertNotExists(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto* cookie = testHarness.create_cookie();
    auto rv = h1->get(cookie, key, 0, DocStateFilter::Alive);
    testHarness.destroy_cookie(cookie);
    cb_assert(rv.first == cb::engine_errc::no_such_key);
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}

#define NSEGS 10
