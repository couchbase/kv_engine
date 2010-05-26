/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *);

struct test_harness testHarness;

static bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    unlink("/tmp/test.db");
    unlink("/tmp/test.db-0.sqlite");
    unlink("/tmp/test.db-1.sqlite");
    unlink("/tmp/test.db-2.sqlite");
    unlink("/tmp/test.db-3.sqlite");
    return true;
}

static ENGINE_ERROR_CODE storeCasVb(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    const void *cookie,
                                    ENGINE_STORE_OPERATION op,
                                    const char *key, const char *value,
                                    item **outitem, uint64_t casIn,
                                    uint16_t vb) {

    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &it,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    item_info info = { .nvalue = 1 };
    if (!h1->get_item_info(h, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, strlen(value));
    h1->item_set_cas(h, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, vb);

    if (outitem) {
        *outitem = it;
    }

    return rv;
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

static enum test_result check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const char* key,
                                        const char* val, size_t vlen) {
    item *i = NULL;
    if (h1->get(h, NULL, &i, key, strlen(key), 0) != ENGINE_SUCCESS) {
        return FAIL;
    }

    item_info info = { .nvalue = 1 };
    if (!h1->get_item_info(h, i, &info)) {
        fprintf(stderr, "get_item_info failed\n");
        return FAIL;
    }

    assert(info.nvalue == 1);
    if (!vlen == info.value[0].iov_len) {
        fprintf(stderr, "vlen=%d, info.value[0].iov_len=%d\n", (int)vlen, (int)info.value[0].iov_len);
        return FAIL;
    }

    return memcmp(info.value[0].iov_base, val, vlen) == 0 ? SUCCESS : FAIL;
}

static enum test_result check_status(ENGINE_ERROR_CODE wanted,
                                     ENGINE_ERROR_CODE got) {
    if (wanted != got) {
        fprintf(stderr, "Wanted %d, got %d\n", wanted, got);
    }
    return wanted == got ? SUCCESS : FAIL;
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = storeCasVb(h, h1, "cookie", op,
                                      "key", "somevalue", &i, 11, 1);
    return rv == ENGINE_NOT_MY_VBUCKET ? SUCCESS : FAIL;
}

//
// ----------------------------------------------------------------------
// The actual tests are below.
// ----------------------------------------------------------------------
//

static enum test_result test_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return check_status(ENGINE_KEY_ENOENT, verify_key(h, h1, "k"));
}

static enum test_result test_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    return check_status(ENGINE_SUCCESS,
                        store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i));
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    if (store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) != ENGINE_SUCCESS) {
        return FAIL;
    }
    return check_status(ENGINE_SUCCESS, verify_key(h, h1, "key"));
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    if (store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) != ENGINE_SUCCESS) {
        return FAIL;
    }
    if (store(h, h1, "cookie", OPERATION_CAS, "key", "failcas", &i) == ENGINE_SUCCESS) {
        return FAIL;
    }
    if (check_key_value(h, h1, "key", "somevalue", 9) == FAIL) {
        return FAIL;
    }
    if (h1->get(h, NULL, &i, "key", 3, 0) != ENGINE_SUCCESS) {
        return FAIL;
    }
    item_info info = { .nvalue = 1 };
    if (!h1->get_item_info(h, i, &info)) {
        abort();
    }
    if (storeCas(h, h1, "cookie", OPERATION_CAS, "key", "winCas", &i,
                 info.cas) != ENGINE_SUCCESS) {
        return FAIL;
    }
    return check_key_value(h, h1, "key", "winCas", 6);
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    if (store(h, h1, "cookie", OPERATION_ADD,"key", "somevalue", &i) != ENGINE_SUCCESS) {
        return FAIL;
    }
    if (store(h, h1, "cookie", OPERATION_ADD,"key", "somevalue", &i) != ENGINE_KEY_EEXISTS) {
        return FAIL;
    }
    return check_key_value(h, h1, "key", "somevalue", 9);
}

static enum test_result test_incr_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    h1->arithmetic(h, "cookie", "key", 3, true, false, 1, 0, 0,
                   &cas, &result,
                   0);
    return check_status(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"));
}

static enum test_result test_incr_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    h1->arithmetic(h, "cookie", "key", 3, true, true, 1, 1, 0,
                   &cas, &result,
                   0);
    if (result != 1) {
        return FAIL;
    }
    h1->arithmetic(h, "cookie", "key", 3, true, false, 1, 1, 0,
                   &cas, &result,
                   0);
    if (result != 2) {
        return FAIL;
    }
    h1->arithmetic(h, "cookie", "key", 3, true, true, 1, 1, 0,
                   &cas, &result,
                   0);
    if (result != 3) {
        return FAIL;
    }
    return check_key_value(h, h1, "key", "3", 1);
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    if (store(h, h1, "cookie", OPERATION_SET, "key", "somevalue", &i) != ENGINE_SUCCESS) {
        return FAIL;
    }
    if (check_key_value(h, h1, "key", "somevalue", 9) != SUCCESS) {
        return FAIL;
    }
    if (h1->remove(h, "cookie", "key", 3, 0, 0) != ENGINE_SUCCESS) {
        return FAIL;
    }
    return check_status(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"));
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    if (store(h, h1, "cookie", OPERATION_SET, "key", val, &i) != ENGINE_SUCCESS) {
        return FAIL;
    }

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);
    return check_key_value(h, h1, "key", val, strlen(val));
}

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return check_status(ENGINE_NOT_MY_VBUCKET, verify_vb_key(h, h1, "key", 1));
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
    return check_status(ENGINE_NOT_MY_VBUCKET,
                        h1->remove(h, "cookie", "key", 3, 0, 1));
}

engine_test_t* get_tests(void) {
    static engine_test_t tests[]  = {
        // basic tests
        {"get miss", test_get_miss, NULL, teardown, NULL},
        {"set", test_set, NULL, teardown, NULL},
        {"set+get hit", test_set_get_hit, NULL, teardown, NULL},
        {"add", test_add, NULL, teardown, NULL},
        {"cas", test_cas, NULL, teardown, NULL},
        {"incr miss", test_incr_miss, NULL, teardown, NULL},
        {"incr with default", test_incr_default, NULL, teardown, NULL},
        {"delete", test_delete, NULL, teardown, NULL},
        {"flush", NULL, NULL, teardown, NULL},
        // Stats tests
        {"stats", NULL, NULL, teardown, NULL},
        {"stats key", NULL, NULL, teardown, NULL},
        {"stats vkey", NULL, NULL, teardown, NULL},
        // restart tests
        {"test restart", test_restart, NULL, teardown, NULL},
        // vbucket negative tests
        {"test wrong vbucket get", test_wrong_vb_get, NULL, teardown, NULL},
        {"test wrong vbucket set", test_wrong_vb_set, NULL, teardown, NULL},
        {"test wrong vbucket add", test_wrong_vb_add, NULL, teardown, NULL},
        {"test wrong vbucket cas", test_wrong_vb_cas, NULL, teardown, NULL},
        {"test wrong vbucket append", test_wrong_vb_append, NULL, teardown, NULL},
        {"test wrong vbucket prepend", test_wrong_vb_prepend, NULL, teardown, NULL},
        {"test wrong vbucket del", test_wrong_vb_del, NULL, teardown, NULL},
        {NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}

bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}
