#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>

#include <memcached/engine.h>

#include "suite_stubs.h"
#include "command_ids.h"

int locktime = 30;
int expiry = 3600;
bool hasError = false;
uint64_t cas = (((uint64_t)1) << 31);
struct test_harness testHarness;
protocol_binary_response_status last_status = 0;

static const char *key = "key";

static void clearCAS(void) {
   cas = (((uint64_t)1) << 31);
}

bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    clearCAS();
    return true;
}

void delay(int amt) {
    testHarness.time_travel(amt);
    hasError = false;
}

static bool add_response(const void *k, uint16_t keylen,
                         const void *ext, uint8_t extlen,
                         const void *body, uint32_t bodylen,
                         uint8_t datatype, uint16_t status,
                         uint64_t pcas, const void *cookie) {
    (void)k;
    (void)keylen;
    (void)ext;
    (void)extlen;
    (void)body;
    (void)bodylen;
    (void)datatype;
    (void)pcas;
    (void)cookie;

    last_status = status;

    return true;
}

static void storeItem(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      ENGINE_STORE_OPERATION op, bool rememberCAS,
                      uint64_t usingCASID) {
    item *it = NULL;
    uint64_t mycas = 0;
    char *value = "0";
    const int flags = 0;
    const void *cookie = NULL;

    if (op == OPERATION_APPEND) {
        value = "-suffix";
    } else if (op == OPERATION_PREPEND) {
        value = "prefix-";
    }

    size_t vlen = strlen(value);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &it,
                      key, strlen(key),
                      vlen, flags, expiry);
    assert(rv == ENGINE_SUCCESS);

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, usingCASID);

    rv = h1->store(h, cookie, it, (rememberCAS ? &cas : &mycas), op, 0);

    hasError = rv != ENGINE_SUCCESS;

    // If we changed the CAS, make sure we don't know it.
    if (!hasError && !rememberCAS) {
        clearCAS();
    }
    assert(cas != 0);
}

static uint8_t  sync_event_id;

static bool handle_sync_response(const void *k, uint16_t keylen,
                                 const void *ext, uint8_t extlen,
                                 const void *body, uint32_t bodylen,
                                 uint8_t datatype, uint16_t status,
                                 uint64_t pcas, const void *cookie) {
    (void)k;
    (void)keylen;
    (void)ext;
    (void)extlen;
    (void)body;
    (void)bodylen;
    (void)datatype;
    (void)pcas;
    (void)cookie;

    last_status = status;
    sync_event_id = 0;

    if (status == ENGINE_SUCCESS) {
        size_t offset = 0;
        char *response = (char*)body;

        uint16_t nkeys;
        memcpy(&nkeys, response + offset, sizeof(nkeys));
        nkeys = ntohs(nkeys);
        offset += sizeof(nkeys);
        assert(nkeys == 1);

        offset += sizeof(uint64_t); // cas
        offset += sizeof(uint16_t); // vbid
        offset += sizeof(uint16_t); // keylen

        memcpy(&sync_event_id, response + offset, sizeof(sync_event_id));
    }

    return true;
}

static protocol_binary_request_header*
create_sync_packet(uint32_t flags) {
    protocol_binary_request_header *req = calloc(1,
                                                 sizeof(protocol_binary_request_header)
                                                 + 256);
    assert(req);

    req->request.opcode = CMD_SYNC;
    req->request.bodylen = htonl(strlen(key));

    char *p = (char*)(req) + sizeof(protocol_binary_request_header);

    uint32_t options = htonl(flags);
    memcpy(p, &options, sizeof(options));
    p += sizeof(options);

    uint16_t keyCount = htons(1);
    memcpy(p, &keyCount, sizeof(keyCount));
    p += sizeof(keyCount);

    uint64_t scas = 0;
    memcpy(p, &scas, sizeof(scas));
    p += sizeof(scas);

    uint16_t vbucketid = 0;
    memcpy(p, &vbucketid, sizeof(vbucketid));
    p += sizeof(vbucketid);

    uint16_t keylen = htons(strlen(key));
    memcpy(p, &keylen, sizeof(keylen));
    p += sizeof(keylen);

    memcpy(p, key, strlen(key));

    return req;
}

void sync(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = create_sync_packet(0x8);
    assert(pkt);
    ENGINE_ERROR_CODE res = h1->unknown_command(h, NULL, pkt, handle_sync_response);
    assert(res == ENGINE_SUCCESS);
    hasError = res != ENGINE_SUCCESS
        || last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS
        || sync_event_id != 0x1;
}

void add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_ADD, false, 0);
}

void append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_APPEND, false, 0);
}

void appendUsingCAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_APPEND, false, cas);
}

void decr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t mycas;
    uint64_t result;
    clearCAS();
    hasError = h1->arithmetic(h, NULL, key, strlen(key), false, false, 1, 0, expiry,
                              &mycas, &result,
                              0) != ENGINE_SUCCESS;
}

void decrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t mycas;
    uint64_t result;
    clearCAS();
    hasError = h1->arithmetic(h, NULL, key, strlen(key), false, true, 1, 0, expiry,
                              &mycas, &result,
                              0) != ENGINE_SUCCESS;
}

void prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_PREPEND, false, 0);
}

void prependUsingCAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_PREPEND, false, cas);
}

void flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    hasError = h1->flush(h, NULL, 0);
}

void del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    hasError = h1->remove(h, NULL, key, strlen(key), 0, 0) != ENGINE_SUCCESS;
}

void deleteUsingCAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    hasError = h1->remove(h, NULL, key, strlen(key), cas, 0) != ENGINE_SUCCESS;
}

void set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_SET, false, 0);
}

void setUsingCAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_SET, false, cas);
}

void setRetainCAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_SET, true, 0);
}

void incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t mycas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, strlen(key), true, false, 1, 0, expiry,
                              &mycas, &result,
                              0) != ENGINE_SUCCESS;
}

void incrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t mycas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, strlen(key), true, true, 1, 0, expiry,
                              &mycas, &result,
                              0) != ENGINE_SUCCESS;
}

void get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), 0);
    hasError = rv != ENGINE_SUCCESS;
}

void checkValue(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* exp) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);

    item_info info;
    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);

    char buf[info.value[0].iov_len + 1];
    memcpy(buf, info.value[0].iov_base, info.value[0].iov_len);
    buf[sizeof(buf) - 1] = 0x00;
    if (buf[strlen(buf) - 1] == '\n') {
        buf[strlen(buf) - 1] = 0x00;
        if (buf[strlen(buf) - 1] == '\r') {
            buf[strlen(buf) - 1] = 0x00;
        }
    }

    assert(info.nvalue == 1);
    if (strlen(exp) > info.value[0].iov_len) {
        fprintf(stderr, "Expected at least %d bytes for ``%s'', got %d as ``%s''\n",
                (int)strlen(exp), exp, (int)info.value[0].iov_len, buf);
        abort();
    }

    if (memcmp(info.value[0].iov_base, exp, strlen(exp)) != 0) {
        fprintf(stderr, "Expected ``%s'', got ``%s''\n", exp, buf);
        abort();
    }
}

static protocol_binary_request_header* create_packet(uint8_t opcode,
                                                     const char *val) {
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
    return req;
}

void getLock(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint16_t vbucketId = 0;

    protocol_binary_request_header *pkt = create_packet(CMD_GET_LOCKED, "");
    pkt->request.vbucket = htons(vbucketId);

    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        fprintf(stderr, "Failed to issue getl request.\n");
        abort();
    }

    hasError = last_status != 0;
}


void assertNotExists(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), 0);
    assert(rv == ENGINE_KEY_ENOENT);
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}

static int test_compare(const void *av, const void *bv) {
    const engine_test_t *a = av;
    const engine_test_t *b = bv;
    return strcmp(a->name, b->name);
}

#define NSEGS 10

// This is basically a really late linker, but it makes separating the
// test thing into multiple compilation units possible.
MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {
    engine_test_t* testsegs[NSEGS];
    engine_test_t* rv = NULL;
    int i = 0, j = 0;
    size_t num_tests = 0, pos = 0;

    testsegs[i++] = get_tests_0();
    testsegs[i++] = get_tests_1();
    testsegs[i++] = get_tests_2();
    testsegs[i++] = get_tests_3();
    testsegs[i++] = get_tests_4();
    testsegs[i++] = get_tests_5();
    testsegs[i++] = get_tests_6();
    testsegs[i++] = get_tests_7();
    testsegs[i++] = get_tests_8();
    testsegs[i++] = get_tests_9();
    assert(i == NSEGS);

    for (i = 0; i < NSEGS; ++i) {
        for (j = 0; testsegs[i][j].name; ++j) {
            ++num_tests;
        }
    }

    rv = calloc(num_tests+1, sizeof(engine_test_t));
    assert(rv);

    for (i = 0; i < NSEGS; ++i) {
        for (j = 0; testsegs[i][j].name; ++j) {
            rv[pos++] = testsegs[i][j];
        }
    }

    qsort(rv, num_tests, sizeof(engine_test_t), test_compare);

    return rv;
}
