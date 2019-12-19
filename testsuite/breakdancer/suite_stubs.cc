/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdlib.h>
#include <string.h>

#include <memcached/durability_spec.h>
#include <memcached/engine.h>
#include <platform/cb_malloc.h>

#include "suite_stubs.h"

int expiry = 3600;
bool hasError = false;
struct test_harness* testHarness;

static DocKey key("key", DocKeyEncodesCollectionId::No);

bool test_setup(EngineIface*) {
    delay(2);
    return true;
}

bool teardown(EngineIface*) {
    return true;
}

void delay(int amt) {
    testHarness->time_travel(amt);
    hasError = false;
}

static void storeItem(EngineIface* h, ENGINE_STORE_OPERATION op) {
    uint64_t cas = 0;
    const char *value = "0";
    const int flags = 0;
    const void* cookie = testHarness->create_cookie(h);
    size_t vlen;
    item_info info;

    vlen = strlen(value);
    auto ret = h->allocate(cookie,
                           key,
                           vlen,
                           flags,
                           expiry,
                           PROTOCOL_BINARY_RAW_BYTES,
                           Vbid(0));
    cb_assert(ret.first == cb::engine_errc::success);

    if (!h->get_item_info(ret.second.get(), &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h->item_set_cas(ret.second.get(), 0);

    auto rv = h->store(
            cookie, ret.second.get(), cas, op, {}, DocumentState::Alive);

    testHarness->destroy_cookie(cookie);
    hasError = rv != ENGINE_SUCCESS;
}

void add(EngineIface* h) {
    storeItem(h, OPERATION_ADD);
}

void flush(EngineIface* h) {
    const auto* cookie = testHarness->create_cookie(h);
    hasError = h->flush(cookie);
    testHarness->destroy_cookie(cookie);
}

void del(EngineIface* h) {
    uint64_t cas = 0;
    mutation_descr_t mut_info;
    const auto* cookie = testHarness->create_cookie(h);
    hasError = h->remove(cookie, key, cas, Vbid(0), {}, mut_info) !=
               ENGINE_SUCCESS;
    testHarness->destroy_cookie(cookie);
}

void set(EngineIface* h) {
    storeItem(h, OPERATION_SET);
}

void checkValue(EngineIface* h, const char* exp) {
    const auto* cookie = testHarness->create_cookie(h);
    auto rv = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    testHarness->destroy_cookie(cookie);
    cb_assert(rv.first == cb::engine_errc::success);

    item_info info;
    h->get_item_info(rv.second.get(), &info);

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

void assertNotExists(EngineIface* h) {
    const auto* cookie = testHarness->create_cookie(h);
    auto rv = h->get(cookie, key, Vbid(0), DocStateFilter::Alive);
    testHarness->destroy_cookie(cookie);
    cb_assert(rv.first == cb::engine_errc::no_such_key);
}

bool setup_suite(struct test_harness *th) {
    testHarness = th;
    return true;
}

bool teardown_suite() {
    return true;
}

#define NSEGS 10
