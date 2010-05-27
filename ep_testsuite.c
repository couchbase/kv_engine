/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include "command_ids.h"

bool abort_msg(const char *expr, const char *msg, int line);

#define check(expr, msg) \
    (expr) ? 0 : abort_msg(#expr, msg, __LINE__)

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *);

struct test_harness testHarness;

bool abort_msg(const char *expr, const char *msg, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            __FILE__, line, msg, expr);
    abort();
    // UNREACHABLE
    return false;
}

static bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    unlink("/tmp/test.db");
    unlink("/tmp/test.db-0.sqlite");
    unlink("/tmp/test.db-1.sqlite");
    unlink("/tmp/test.db-2.sqlite");
    unlink("/tmp/test.db-3.sqlite");
    return true;
}

static ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                      const void *cookie,
                                      ENGINE_STORE_OPERATION op,
                                      const char *key,
                                      const char *value, size_t vlen,
                                      uint32_t flags,
                                      item **outitem, uint64_t casIn,
                                      uint16_t vb) {
    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &it,
                      key, strlen(key),
                      vlen, flags, 3600);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info = { .nvalue = 1 };
    if (!h1->get_item_info(h, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, vb);

    if (outitem) {
        *outitem = it;
    }

    return rv;
}

static ENGINE_ERROR_CODE storeCasVb(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    const void *cookie,
                                    ENGINE_STORE_OPERATION op,
                                    const char *key, const char *value,
                                    item **outitem, uint64_t casIn,
                                    uint16_t vb) {
    return storeCasVb11(h, h1, cookie, op, key, value, strlen(value), 9258,
                        outitem, casIn, vb);

}

static ENGINE_ERROR_CODE storeCas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  const void *cookie,
                                  ENGINE_STORE_OPERATION op,
                                  const char *key, const char *value,
                                  item **outitem, uint64_t casIn) {
    return storeCasVb(h, h1, cookie, op, key, value, outitem, casIn, 0);
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie,
                               ENGINE_STORE_OPERATION op,
                               const char *key, const char *value,
                               item **outitem) {
    return storeCas(h, h1, cookie, op, key, value, outitem, 0);
}


static ENGINE_ERROR_CODE verify_vb_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                       const char* key, uint16_t vbucket) {
    item *i = NULL;
    return h1->get(h, NULL, &i, key, strlen(key), vbucket);
}

static ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    const char* key) {
    return verify_vb_key(h, h1, key, 0);
}

static bool get_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const char* key, item_info *info) {
    item *i = NULL;
    if (h1->get(h, NULL, &i, key, strlen(key), 0) != ENGINE_SUCCESS) {
        return false;
    }
    info->nvalue = 1;
    if (!h1->get_item_info(h, i, info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }
    return true;
}

static enum test_result check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const char* key,
                                        const char* val, size_t vlen) {
    item *i = NULL;
    check(h1->get(h, NULL, &i, key, strlen(key), 0) == ENGINE_SUCCESS,
          "Failed to get in check_key_value");

    item_info info = { .nvalue = 1 };
    check(h1->get_item_info(h, i, &info), "check_key_value");

    assert(info.nvalue == 1);
    if (vlen != info.value[0].iov_len) {
        fprintf(stderr, "Expected length %zd, got %zd\n",
                vlen, info.value[0].iov_len);
        check(vlen == info.value[0].iov_len, "Length mismatch.");
    }

    check(memcmp(info.value[0].iov_base, val, vlen) == 0, "Data mismatch");

    return SUCCESS;
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(storeCasVb(h, h1, "cookie", op,
                     "key", "somevalue", &i, 11, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not_my_vbucket");
    return SUCCESS;
}

protocol_binary_response_status last_status = 0;
char *last_key = NULL;
char *last_body = NULL;

static bool add_response(const void *key, uint16_t keylen,
                         const void *ext, uint8_t extlen,
                         const void *body, uint32_t bodylen,
                         uint8_t datatype, uint16_t status,
                         uint64_t cas, const void *cookie) {
    (void)ext;
    (void)extlen;
    (void)datatype;
    (void)cas;
    (void)cookie;
    last_status = status;
    if (last_body) {
        free(last_body);
        last_body = NULL;
    }
    if (bodylen > 0) {
        last_body = malloc(bodylen);
        assert(last_body);
        memcpy(last_body, body, bodylen);
    }
    if (last_key) {
        free(last_key);
        last_key = NULL;
    }
    if (keylen > 0) {
        last_key = malloc(keylen);
        assert(last_key);
        memcpy(last_key, key, keylen);
    }
    return true;
}

static void* create_packet(uint8_t opcode, const char *key, const char *val) {
    char *pkt_raw = calloc(1,
                           sizeof(protocol_binary_request_header)
                           + strlen(key)
                           + strlen(val));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.bodylen = htonl(strlen(key) + strlen(val));
    req->request.keylen = htons(strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header),
           key, strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header) + strlen(key),
           val, strlen(val));
    return pkt_raw;
}

//
// ----------------------------------------------------------------------
// The actual tests are below.
// ----------------------------------------------------------------------
//

static enum test_result test_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_key(h, h1, "k") == ENGINE_KEY_ENOENT, "Expected miss.");
    return SUCCESS;
}

static enum test_result test_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(ENGINE_SUCCESS ==
          store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i),
          "Error setting.");
    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(ENGINE_SUCCESS ==
          storeCasVb11(h, h1, "cookie", OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0),
          "Failed to set.");
    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set.");

    item_info info;
    uint32_t flags = 828258;
    check(get_value(h, h1, "key", &info), "Failed to get value.");
    assert(info.flags != flags);

    check(storeCasVb11(h, h1, "cookie", OPERATION_SET, "key",
                       "newvalue", strlen("newvalue"), flags, &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to set again.");

    check(get_value(h, h1, "key", &info), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to do initial set.");
    check(store(h, h1, "cookie", OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    check_key_value(h, h1, "key", "somevalue", 9);

    check(h1->get(h, NULL, &i, "key", 3, 0) == ENGINE_SUCCESS,
          "Failed to get value.");

    item_info info = { .nvalue = 1 };
    check(h1->get_item_info(h, i, &info), "Failed to get item info.");

    check(storeCas(h, h1, "cookie", OPERATION_CAS, "key", "winCas", &i,
                   info.cas) == ENGINE_SUCCESS,
          "Failed to store CAS");
    check_key_value(h, h1, "key", "winCas", 6);
    return SUCCESS;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, "cookie", OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    check(store(h, h1, "cookie", OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");
    return check_key_value(h, h1, "key", "somevalue", 9);
}

static enum test_result test_incr_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    h1->arithmetic(h, "cookie", "key", 3, true, false, 1, 0, 0,
                   &cas, &result,
                   0);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected to not find key");
    return SUCCESS;
}

static enum test_result test_incr_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    check(h1->arithmetic(h, "cookie", "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed first arith");
    check(result == 1, "Failed result verification.");

    check(h1->arithmetic(h, "cookie", "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed second arith.");
    check(result == 2, "Failed second result verification.");

    check(h1->arithmetic(h, "cookie", "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed third arith.");
    check(result == 3, "Failed third result verification.");

    return check_key_value(h, h1, "key", "3\r\n", 3);
}

static enum test_result test_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, "cookie", OPERATION_ADD,"key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");

    check(h1->arithmetic(h, "cookie", "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    return check_key_value(h, h1, "key", "2\r\n", 3);
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, "cookie", "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->flush(h, "cookie", 0) == ENGINE_SUCCESS,
          "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_flush_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, "cookie", "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    check(h1->flush(h, "cookie", 0) == ENGINE_SUCCESS,
          "Failed to flush");

    check(store(h, h1, "cookie", OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key2", "somevalue", 9);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    check(store(h, h1, "cookie", OPERATION_SET, "key3", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush, post-restart set.");
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, "cookie", "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->remove(h, "cookie", "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    check(store(h, h1, "cookie", OPERATION_SET, "key", val, &i) == ENGINE_SUCCESS,
          "Failed set.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);
    return check_key_value(h, h1, "key", val, strlen(val));
}

static enum test_result test_restart_bin_val(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {



    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(storeCasVb11(h, h1, "cookie", OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected wrong bucket.");
    return SUCCESS;
}

static enum test_result test_wrong_vb_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_wrong_vb_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_wrong_vb_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_wrong_vb_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_wrong_vb_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_wrong_vb_del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, "cookie", "key", 3, 0, 1),
          "Expected wrong bucket.");
    return SUCCESS;
}

static enum test_result test_alloc_limit(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    ENGINE_ERROR_CODE rv;

    rv = h1->allocate(h, NULL, &it, "key", 3, 20 * 1024 * 1024, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocated 20MB item");
    h1->release(h, NULL, it);

    rv = h1->allocate(h, NULL, &it, "key", 3, (20 * 1024 * 1024) + 1, 0, 0);
    check(rv == ENGINE_E2BIG, "Object too big");

    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    void *pkt = create_packet(CMD_GET_VBUCKET, "1", "");
    if (h1->unknown_command(h, "cookie", pkt, add_response) != ENGINE_SUCCESS) {
        return FAIL;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    void *pkt = create_packet(CMD_GET_VBUCKET, "0", "");
    if (h1->unknown_command(h, "cookie", pkt, add_response) != ENGINE_SUCCESS) {
        return FAIL;
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return FAIL;
    }

    return strcmp("active", last_key) == 0 ? SUCCESS : FAIL;
}

engine_test_t* get_tests(void) {
    static engine_test_t tests[]  = {
        // basic tests
        {"test alloc limit", test_alloc_limit, NULL, teardown, NULL},
        {"get miss", test_get_miss, NULL, teardown, NULL},
        {"set", test_set, NULL, teardown, NULL},
        {"set+get hit", test_set_get_hit, NULL, teardown, NULL},
        {"set+get hit (bin)", test_set_get_hit_bin, NULL, teardown, NULL},
        {"set+change flags", test_set_change_flags, NULL, teardown, NULL},
        {"add", test_add, NULL, teardown, NULL},
        {"cas", test_cas, NULL, teardown, NULL},
        {"incr miss", test_incr_miss, NULL, teardown, NULL},
        {"incr", test_incr, NULL, teardown, NULL},
        {"incr with default", test_incr_default, NULL, teardown, NULL},
        {"delete", test_delete, NULL, teardown, NULL},
        {"flush", test_flush, NULL, teardown, NULL},
        // Stats tests
        {"stats", NULL, NULL, teardown, NULL},
        {"stats key", NULL, NULL, teardown, NULL},
        {"stats vkey", NULL, NULL, teardown, NULL},
        // restart tests
        {"test restart", test_restart, NULL, teardown, NULL},
        {"set+get+restart+hit (bin)", test_restart_bin_val, NULL, teardown, NULL},
        {"flush+restart", test_flush_restart, NULL, teardown, NULL},
        // vbucket negative tests
        {"test wrong vbucket get", test_wrong_vb_get, NULL, teardown, NULL},
        {"test wrong vbucket set", test_wrong_vb_set, NULL, teardown, NULL},
        {"test wrong vbucket add", test_wrong_vb_add, NULL, teardown, NULL},
        {"test wrong vbucket cas", test_wrong_vb_cas, NULL, teardown, NULL},
        {"test wrong vbucket append", test_wrong_vb_append, NULL, teardown, NULL},
        {"test wrong vbucket prepend", test_wrong_vb_prepend, NULL, teardown, NULL},
        {"test wrong vbucket del", test_wrong_vb_del, NULL, teardown, NULL},
        // Vbucket management tests
        {"test vbucket get", test_vbucket_get, NULL, teardown, NULL},
        {"test vbucket get missing", test_vbucket_get_miss, NULL, teardown, NULL},
        {"test vbucket create", NULL, NULL, teardown, NULL},
        {"test vbucket destroy", NULL, NULL, teardown, NULL},
        {NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}

bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}
