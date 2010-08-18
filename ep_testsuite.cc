/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <vector>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>

#ifdef HAS_ARPA_INET_H
#include <arpa/inet.h>
#endif

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include "command_ids.h"

#ifdef linux
/* /usr/include/netinet/in.h defines macros from ntohs() to _bswap_nn to
 * optimize the conversion functions, but the prototypes generate warnings
 * from gcc. The conversion methods isn't the bottleneck for my app, so
 * just remove the warnings by undef'ing the optimization ..
 */
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

bool abort_msg(const char *expr, const char *msg, int line);

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

extern "C" {
    MEMCACHED_PUBLIC_API
    engine_test_t* get_tests(void);

    MEMCACHED_PUBLIC_API
    bool setup_suite(struct test_harness *);
}

protocol_binary_response_status last_status(static_cast<protocol_binary_response_status>(0));
char *last_key = NULL;
char *last_body = NULL;
std::map<std::string, std::string> vals;

struct test_harness testHarness;

class ThreadData {
public:
    ThreadData(ENGINE_HANDLE *eh, ENGINE_HANDLE_V1 *ehv1) : h(eh), h1(ehv1) {}
    ENGINE_HANDLE    *h;
    ENGINE_HANDLE_V1 *h1;
};

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
    vals.clear();
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

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, vb);

    if (outitem) {
        *outitem = it;
    }

    return rv;
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie,
                               ENGINE_STORE_OPERATION op,
                               const char *key, const char *value,
                               item **outitem, uint64_t casIn = 0,
                               uint16_t vb = 0) {
    return storeCasVb11(h, h1, cookie, op, key, value, strlen(value),
                        9258, outitem, casIn, vb);
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
    if (!h1->get_item_info(h, NULL, i, info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }
    return true;
}

static enum test_result check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const char* key,
                                        const char* val, size_t vlen,
                                        bool checkcrlf = false, uint16_t vbucket = 0) {
    int crlfOffset = checkcrlf ? 2 : 0;
    item *i = NULL;
    check(h1->get(h, NULL, &i, key, strlen(key), vbucket) == ENGINE_SUCCESS,
          "Failed to get in check_key_value");

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "check_key_value");

    assert(info.nvalue == 1);
    if (vlen + crlfOffset != info.value[0].iov_len) {
        std::cerr << "Expected length " << vlen + crlfOffset
                  << " got " << info.value[0].iov_len << std::endl;
        check(vlen + crlfOffset == info.value[0].iov_len, "Length mismatch.");
    }

    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");

    if (checkcrlf) {
        const char *s = static_cast<const char*>(info.value[0].iov_base);
        check(memcmp(s + vlen, "\r\n", 2) == 0, "missing crlf");
    }

    return SUCCESS;
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not_my_vbucket");
    return SUCCESS;
}

static void add_stats(const char *key, const uint16_t klen,
                      const char *val, const uint32_t vlen,
                      const void *cookie) {
    (void)cookie;
    std::string k(key, klen);
    std::string v(val, vlen);
    vals[k] = v;
}

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
    last_status = static_cast<protocol_binary_response_status>(status);
    if (last_body) {
        free(last_body);
        last_body = NULL;
    }
    if (bodylen > 0) {
        last_body = static_cast<char*>(malloc(bodylen + 1));
        assert(last_body);
        memcpy(last_body, body, bodylen);
        last_body[bodylen] = '\0';
    }
    if (last_key) {
        free(last_key);
        last_key = NULL;
    }
    if (keylen > 0) {
        last_key = static_cast<char*>(malloc(keylen + 1));
        assert(last_key);
        memcpy(last_key, key, keylen);
        last_key[keylen] = '\0';
    }
    return true;
}

static protocol_binary_request_header* create_packet(uint8_t opcode,
                                                     const char *key,
                                                     const char *val) {
    char *pkt_raw = static_cast<char*>(calloc(1,
                                              sizeof(protocol_binary_request_header)
                                              + strlen(key)
                                              + strlen(val)));
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

static void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const char *key, uint16_t vbucketId=0,
                      const char *msg = NULL) {
    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        key, "");
    pkt->request.vbucket = htons(vbucketId);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to evict key.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success evicting key.");

    if (msg != NULL && strcmp(last_key, msg) != 0) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_key);
        abort();
    }
}

static bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              uint16_t vb, const char *state) {
    char vbid[8];
    snprintf(vbid, sizeof(vbid), "%d", vb);

    protocol_binary_request_header *pkt = create_packet(CMD_SET_VBUCKET, vbid, state);
    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool set_flush_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            const char *param, const char *val) {
    protocol_binary_request_header *pkt = create_packet(CMD_SET_FLUSH_PARAM,
                                                        param, val);
    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 uint16_t vb, const char *expected,
                                 bool mute = false) {
    char vbid[8];
    snprintf(vbid, sizeof(vbid), "%d", vb);

    protocol_binary_request_header *pkt = create_packet(CMD_GET_VBUCKET, vbid, "");
    ENGINE_ERROR_CODE errcode = h1->unknown_command(h, NULL, pkt, add_response);
    if (errcode != ENGINE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Error code when getting vbucket %d\n", errcode);
        }
        return false;
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Last protocol status was %d (%s)\n",
                    last_status, last_key ? last_key : "unknown");
        }
        return false;
    }

    return strncmp(expected, last_key, strlen(expected)) == 0;
}

static void waitfor_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  uint16_t vb, const char *state) {
    bool dead = strcmp(state, "dead") == 0;
    while (!verify_vbucket_state(h, h1, vb, state, true)) {
        if (dead && strcmp(last_key, "That's not my bucket.") == 0) {
            break;
        }
        usleep(100);
    }
}

static bool verify_vbucket_missing(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                   uint16_t vb) {
    char vbid[8];
    snprintf(vbid, sizeof(vbid), "vb_%d", vb);
    std::string vbstr(vbid);

    // Try up to three times to verify the bucket is missing.  Bucket
    // state changes are async.
    for (int i = 0; i < 3; i++) {
        vals.clear();
        check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
              "Failed to get stats.");

        if (vals.find(vbstr) == vals.end()) {
            return true;
        }

        usleep(500000);
    }

    std::cerr << "Expected bucket missing, got " << vals[vbstr] << std::endl;

    return false;
}

static enum test_result test_pending_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, "pending"), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, "pending"), "Bucket state was not set to pending.");
    check(store(h, h1, cookie, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_EWOULDBLOCK,
        "Expected woodblock");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, "replica"), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, "replica"), "Bucket state was not set to replica.");
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not my vbucket");
    return SUCCESS;
}

static int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname) {
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string s = vals[statname];
    return atoi(s.c_str());
}

static void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int exp, const char *msg) {
    vals.clear();
    int curr_items = get_int_stat(h, h1, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

static void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                     const char *key, const char *val,
                                     uint16_t vbucketId=0) {

    item *i = NULL;
    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i, vbucketId) == ENGINE_SUCCESS,
          "Failed to store an item.");

    // Wait for persistence...
    while (get_int_stat(h, h1, "ep_total_persisted") == numStored) {
        usleep(100);
    }
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
          store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
          "Error setting.");
    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(ENGINE_SUCCESS ==
          storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0),
          "Failed to set.");
    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set.");

    item_info info;
    uint32_t flags = 828258;
    check(get_value(h, h1, "key", &info), "Failed to get value.");
    assert(info.flags != flags);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "newvalue", strlen("newvalue"), flags, &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to set again.");

    check(get_value(h, h1, "key", &info), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to do initial set.");
    check(store(h, h1, NULL, OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    check_key_value(h, h1, "key", "somevalue", 9);

    check(h1->get(h, NULL, &i, "key", 3, 0) == ENGINE_SUCCESS,
          "Failed to get value.");

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "Failed to get item info.");

    check(store(h, h1, NULL, OPERATION_CAS, "key", "winCas", &i,
                info.cas) == ENGINE_SUCCESS,
          "Failed to store CAS");
    check_key_value(h, h1, "key", "winCas", 6);

    uint64_t cval = 99999;
    check(store(h, h1, NULL, OPERATION_CAS, "non-existing", "winCas", &i,
                cval) == ENGINE_KEY_ENOENT,
          "CAS for non-existing key returned the wrong error code");
    return SUCCESS;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");
    return check_key_value(h, h1, "key", "somevalue", 9);
}

static enum test_result test_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to replace non-existing value.");
    check(store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set value.");
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to replace existing value.");
    return check_key_value(h, h1, "key", "somevalue", 9);
}

static enum test_result test_incr_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    h1->arithmetic(h, NULL, "key", 3, true, false, 1, 0, 0,
                   &cas, &result,
                   0);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected to not find key");
    return SUCCESS;
}

static enum test_result test_incr_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    check(h1->arithmetic(h, NULL, "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed first arith");
    check(result == 1, "Failed result verification.");

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed second arith.");
    check(result == 2, "Failed second result verification.");

    check(h1->arithmetic(h, NULL, "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed third arith.");
    check(result == 3, "Failed third result verification.");

    return check_key_value(h, h1, "key", "3\r\n", 3);
}

static enum test_result test_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    return check_key_value(h, h1, "key", "2\r\n", 3);
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_flush_multiv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, "active"), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_vb_key(h, h1, "key2", 2), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9, false, 2);

    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_vb_key(h, h1, "key2", 2), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_flush_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
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
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key2", "somevalue", 9);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    check(store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush, post-restart set.");
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush_multiv_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, "active"), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(verify_vbucket_missing(h, h1, 2), "Bucket 2 came back.");
    return SUCCESS;
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    check(store(h, h1, NULL, OPERATION_SET, "key", val, &i) == ENGINE_SUCCESS,
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
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
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

static enum test_result test_vb_get_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "pending"), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    item *i = NULL;
    check(ENGINE_EWOULDBLOCK == h1->get(h, cookie, &i, "key", strlen("key"), 1),
          "Expected woodblock.");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "replica"), "Failed to set vbucket state.");
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected not my bucket.");
    return SUCCESS;
}

static enum test_result test_wrong_vb_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_vb_incr_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, "pending"), "Failed to set vbucket state.");
    check(h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_EWOULDBLOCK,
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_incr_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, "replica"), "Failed to set vbucket state.");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my bucket.");
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

static enum test_result test_wrong_vb_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_wrong_vb_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_wrong_vb_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_wrong_vb_del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected wrong bucket.");
    return SUCCESS;
}

static enum test_result test_expiry(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), 10, 0, 1);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");

    h1->release(h, NULL, it);
    sleep(2);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    return SUCCESS;
}

static enum test_result test_vb_del_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, h1, 1, "pending"), "Failed to set vbucket state.");
    check(ENGINE_EWOULDBLOCK == h1->remove(h, cookie, "key", 3, 0, 1),
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "replica"), "Failed to set vbucket state.");
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected not my vbucket.");
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

static enum test_result test_memory_limit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int used = get_int_stat(h, h1, "mem_used");
    int max = get_int_stat(h, h1, "ep_max_data_size");
    check(get_int_stat(h, h1, "ep_oom_errors") == 0,
          "Expected no OOM errors.");
    assert(used < max);

    char data[8192];
    memset(data, 'x', sizeof(data));
    size_t vlen = max - used - 128;
    data[vlen] = 0x00;

    item *i = NULL;
    // So if we add an item,
    check(store(h, h1, NULL, OPERATION_SET, "key", data, &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", data, vlen);

    // There should be no room for another.
    check(store(h, h1, NULL, OPERATION_SET, "key2", data, &i) == ENGINE_ENOMEM,
          "should have failed second set");
    check(get_int_stat(h, h1, "ep_oom_errors") == 1,
          "Expected an OOM error.");
    check_key_value(h, h1, "key", data, vlen);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    // Until we remove that item
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue2", &i) == ENGINE_SUCCESS,
          "should have succeded on the last set");
    check_key_value(h, h1, "key2", "somevalue2", 10);

    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_missing(h, h1, 1) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_state(h, h1, 0, "active") ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!verify_vbucket_missing(h, h1, 1)) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, h1, 1, "active")) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    return verify_vbucket_state(h, h1, 1, "active") ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "active"), "Failed to set vbucket state.");
    waitfor_vbucket_state(h, h1, 1, "active");

    protocol_binary_request_header *pkt = create_packet(CMD_DEL_VBUCKET, "1", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    pkt = create_packet(CMD_DEL_VBUCKET, "2", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected failure deleting non-existent bucket.");

    check(set_vbucket_state(h, h1, 1, "dead"), "Failed set set vbucket 1 state.");
    waitfor_vbucket_state(h, h1, 1, "dead");

    pkt = create_packet(CMD_DEL_VBUCKET, "1", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 0 was not missing after deleting it.");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "active"), "Failed to set vbucket state.");
    waitfor_vbucket_state(h, h1, 1, "active");

    protocol_binary_request_header *pkt = create_packet(CMD_DEL_VBUCKET, "1", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    // Store a value so the restart will try to resurrect it.
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 1)
          == ENGINE_SUCCESS, "Failed to set a value");
    check_key_value(h, h1, "key", "somevalue", 9, false, 1);

    // Reload to get a flush forced.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    check(verify_vbucket_state(h, h1, 1, "pending"),
          "Bucket state was not pending after restart.");
    check(set_vbucket_state(h, h1, 1, "active"), "Failed to set vbucket state.");
    waitfor_vbucket_state(h, h1, 1, "active");
    check_key_value(h, h1, "key", "somevalue", 9, false, 1);

    check(set_vbucket_state(h, h1, 1, "dead"), "Failed set set vbucket 1 state.");
    waitfor_vbucket_state(h, h1, 1, "dead");

    pkt = create_packet(CMD_DEL_VBUCKET, "1", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true);

    if (verify_vbucket_state(h, h1, 1, "pending", true)) {
        std::cerr << "Bucket came up in pending state after delete." << std::endl;
        abort();
    }

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after restart.");

    return SUCCESS;
}

static enum test_result test_vb_set_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_add_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_vb_set_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_replace_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_replace_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_add_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_tap_rcvr_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    for (size_t i = 0; i < 8192; ++i) {
        char *data = static_cast<char *>(malloc(i));
        memset(data, 'x', i);
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                             data, i, 0) == ENGINE_SUCCESS,
              "Failed tap notify.");
        std::stringstream ss;
        ss << "failed key at " << i;
        check(check_key_value(h, h1, "key", data, i, true) == SUCCESS,
              ss.str().c_str());
        free(data);
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[1];
    check(h1->tap_notify(h, NULL, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         "data", 4, 1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "pending"), "Failed to set vbucket state.");
    char eng_specific[1];
    check(h1->tap_notify(h, NULL, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, "replica"), "Failed to set vbucket state.");
    char eng_specific[1];
    check(h1->tap_notify(h, NULL, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}


static enum test_result verify_item(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    item *i,
                                    const char* key, size_t klen,
                                    const char* val, size_t vlen)
{
    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "get item info failed");
    check(info.nvalue == 1, "iovectors not supported");
    // We can pass in a NULL key to avoid the key check (for tap streams)
    if (key) {
        check(klen == info.nkey, "Incorrect key length");
        check(memcmp(info.key, key, klen) == 0, "Incorrect key value");
    }
    check(vlen == info.value[0].iov_len, "Incorrect value length");
    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");

    return SUCCESS;
}

static enum test_result test_tap_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    for (int i = 0; i < 29; ++i) {
        std::stringstream ss;
        ss << "key" << i;
        keys.push_back(ss.str());
    }
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");
    // set_flush_param(h, h1, "bg_fetch_delay", "1");

    std::vector<std::string>::iterator keyit;
    for (keyit = keys.begin(); keyit != keys.end(); ++keyit) {
        item *i = NULL;
        check(store(h, h1, NULL, OPERATION_SET, keyit->c_str(),
                    "value", &i, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + static_cast<int>(keys.size())) {
        usleep(100);
    }

    for (keyit = keys.begin(); keyit != keys.end(); ++keyit) {
        evict_key(h, h1, keyit->c_str(), 0, "Ejected.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;

    tap_event_t event;
    int found = 0;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            ++found;
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            break;
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    if (found != static_cast<int>(keys.size())) {
        std::cerr << "Expected " << keys.size()
                  << " items in stream, got " << found << std::endl;
        return FAIL;
    }

    testHarness.unlock_cookie(cookie);
    h1->release(h, cookie, it);
    return SUCCESS;
}

static enum test_result test_novb0(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_vbucket_missing(h, h1, 0), "vb0 existed and shouldn't have.");
    return SUCCESS;
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.size() > 10, "Kind of expected more stats than that.");
    check(vals.find("ep_version") != vals.end(), "Found no ep_version.");

    return SUCCESS;
}

static enum test_result test_mem_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char value[2048];
    memset(value, 'b', sizeof(value));
    strcpy(value + sizeof(value) - 4, "\r\n");
    wait_for_persisted_value(h, h1, "key", value);
    int mem_used = get_int_stat(h, h1, "mem_used");
    int tot_used = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    check(mem_used == tot_used + overhead,
          "mem_used seems wrong");
    evict_key(h, h1, "key", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_total_cache_size") == tot_used,
          "Evict a value shouldn't increase the total cache size");
    check(get_int_stat(h, h1, "mem_used") < mem_used,
          "Expected mem_used to decrease when an item is evicted");
    check_key_value(h, h1, "key", value, strlen(value), false, 0);
    check(get_int_stat(h, h1, "mem_used") == mem_used,
          "Expected mem_used to decrease when an item is evicted");

    return SUCCESS;
}

static enum test_result test_io_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_io_num_read") == 0,
          "Expected reset stats to set io_num_read to zero");
    check(get_int_stat(h, h1, "ep_io_num_write") == 0,
          "Expected reset stats to set io_num_write to zero");
    check(get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected reset stats to set io_read_bytes to zero");
    check(get_int_stat(h, h1, "ep_io_write_bytes") == 0,
          "Expected reset stats to set io_write_bytes to zero");
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    check(get_int_stat(h, h1, "ep_io_num_read") == 0 &&
          get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected storing one value to not change the read counter");

    check(get_int_stat(h, h1, "ep_io_num_write") == 1 &&
          get_int_stat(h, h1, "ep_io_write_bytes") == 4,
          "Expected storing the key to update the write counter");
    evict_key(h, h1, "a", 0, "Ejected.");

    check_key_value(h, h1, "a", "b\r\n", 3, false, 0);

    check(get_int_stat(h, h1, "ep_io_num_read") == 1 &&
          get_int_stat(h, h1, "ep_io_read_bytes") == 4,
          "Expected reading the value back in to update the read counter");
    check(get_int_stat(h, h1, "ep_io_num_write") == 1 &&
          get_int_stat(h, h1, "ep_io_write_bytes") == 4,
          "Expected reading the value back in to not update the write counter");

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_io_num_read") == 0,
          "Expected reset stats to set io_num_read to zero");
    check(get_int_stat(h, h1, "ep_io_num_write") == 0,
          "Expected reset stats to set io_num_write to zero");
    check(get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected reset stats to set io_read_bytes to zero");
    check(get_int_stat(h, h1, "ep_io_write_bytes") == 0,
          "Expected reset stats to set io_write_bytes to zero");


    return SUCCESS;
}

static enum test_result test_bg_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, false, 0);

    check(get_int_stat(h, h1, "ep_bg_num_samples") == 1,
          "Expected one sample");

    check(vals.find("ep_bg_min_wait") != vals.end(), "Found no ep_bg_min_wait.");
    check(vals.find("ep_bg_max_wait") != vals.end(), "Found no ep_bg_max_wait.");
    check(vals.find("ep_bg_wait_avg") != vals.end(), "Found no ep_bg_wait_avg.");
    check(vals.find("ep_bg_min_load") != vals.end(), "Found no ep_bg_min_load.");
    check(vals.find("ep_bg_max_load") != vals.end(), "Found no ep_bg_max_load.");
    check(vals.find("ep_bg_load_avg") != vals.end(), "Found no ep_bg_load_avg.");

    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, false, 0);
    check(get_int_stat(h, h1, "ep_bg_num_samples") == 2,
          "Expected one sample");

    return SUCCESS;
}

static enum test_result test_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // Verify initial case.
    verify_curr_items(h, h1, 0, "init");

    // Verify set and add case
    check(store(h, h1, NULL, OPERATION_ADD,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    verify_curr_items(h, h1, 3, "three items stored");

    // Verify delete case.
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    verify_curr_items(h, h1, 2, "one item deleted");

    // Verify flush case (remove the two remaining from above)
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    verify_curr_items(h, h1, 0, "flush");

    // Verify delete vbucket case.
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(set_vbucket_state(h, h1, 0, "dead"), "Failed set set vbucket 0 state.");
    waitfor_vbucket_state(h, h1, 0, "dead");
    protocol_binary_request_header *pkt = create_packet(CMD_DEL_VBUCKET, "0", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success deleting vbucket.");
    verify_curr_items(h, h1, 0, "del vbucket");

    return SUCCESS;
}


static enum test_result test_value_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k1", 0, "Can't eject: Dirty or a small object.");
    wait_for_persisted_value(h, h1, "k1", "some value");
    evict_key(h, h1, "k1", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected one non-resident item");

    evict_key(h, h1, "k1", 0, "Already ejected.");

    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        "missing-key", "");
    pkt->request.vbucket = htons(0);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to evict key.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");

    check(get_int_stat(h, h1, "ep_num_value_ejects") == 1,
          "Expected only one value to be ejected");

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");

    check_key_value(h, h1, "k1", "some value", 10);
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_golden(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");
    check(get_int_stat(h, h1, "ep_kv_size") == 0,
          "Initial kv_size was not zero.");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    int kv_size = get_int_stat(h, h1, "ep_kv_size");
    int mem_used = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    int kv_size2 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used2 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    assert(kv_size2 < kv_size);
    assert(mem_used2 < mem_used);

    // Reload the data.
    check_key_value(h, h1, "k1", "some value", 10);

    int kv_size3 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used3 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));
    // Should not have marked the thing dirty.
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    assert(kv_size == kv_size3);
    assert(mem_used == mem_used3);

    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    while (get_int_stat(h, h1, "ep_total_persisted") == numStored) {
        usleep(100);
    }

    check(get_int_stat(h, h1, "ep_kv_size") == 0,
          "Initial kv_size was not zero.");
    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") == overhead,
          "Fell below initial overhead.");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_paged_rm(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");
    check(get_int_stat(h, h1, "ep_kv_size") == 0,
          "Initial kv_size was not zero.");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    while (get_int_stat(h, h1, "ep_total_persisted") == numStored) {
        usleep(100);
    }

    check(get_int_stat(h, h1, "ep_kv_size") == 0,
          "Initial kv_size was not zero.");
    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") == overhead,
          "Fell below initial overhead.");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    wait_for_persisted_value(h, h1, "k1", "13");

    evict_key(h, h1, "k1");

    check(h1->arithmetic(h, NULL, "k1", 2, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    check_key_value(h, h1, "k1", "14\r\n", 4);

    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_update_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "k1", "new value", &i) == ENGINE_SUCCESS,
          "Failed to update an item.");

    check_key_value(h, h1, "k1", "new value", 9);

    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_delete_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed to delete.");

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

extern "C" {
    static void* bg_set_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        item *i = NULL;
        check(store(td->h, td->h1, NULL, OPERATION_SET,
                    "k1", "new value", &i) == ENGINE_SUCCESS,
              "Failed to update an item.");

        delete td;
        return NULL;
    }

    static void* bg_del_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        check(td->h1->remove(td->h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
              "Failed to delete.");

        delete td;
        return NULL;
    }

    static void* bg_incr_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        uint64_t cas = 0, result = 0;
        check(td->h1->arithmetic(td->h, NULL, "k1", 2, true, false, 1, 1, 0,
                                 &cas, &result,
                                 0) == ENGINE_SUCCESS,
              "Failed to incr value.");

        delete td;
        return NULL;
    }

}

static enum test_result test_disk_gt_ram_set_race(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_flush_param(h, h1, "bg_fetch_delay", "1");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_set_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    check_key_value(h, h1, "k1", "new value", 9);

    // Should have bg_fetched, but discarded the old value.
    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr_race(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "13");
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    set_flush_param(h, h1, "bg_fetch_delay", "1");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_incr_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    // Value is as it was before.
    check_key_value(h, h1, "k1", "13", 2);
    // Should have bg_fetched to retrieve it even with a concurrent incr.
    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    // Give incr time to finish (it's doing another background fetch)
    while (1 == get_int_stat(h, h1, "ep_bg_fetched")) { usleep(10); }
    while (1 == get_int_stat(h, h1, "ep_total_enqueued")) { usleep(10); }

    // The incr mutated the value.
    check_key_value(h, h1, "k1", "14\r\n", 4);

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_rm_race(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_flush_param(h, h1, "bg_fetch_delay", "1");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_del_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    // Should have bg_fetched, but discarded the old value.
    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

engine_test_t* get_tests(void) {

    static engine_test_t tests[]  = {
        // basic tests
        {"test alloc limit", test_alloc_limit, NULL, teardown, NULL},
        {"test total memory limit", test_memory_limit, NULL, teardown,
         "max_size=4096;ht_locks=1;ht_size=3"},
        {"get miss", test_get_miss, NULL, teardown, NULL},
        {"set", test_set, NULL, teardown, NULL},
        {"set+get hit", test_set_get_hit, NULL, teardown, NULL},
        {"getl", NULL, NULL, teardown, NULL},
        {"set+get hit (bin)", test_set_get_hit_bin, NULL, teardown, NULL},
        {"set+change flags", test_set_change_flags, NULL, teardown, NULL},
        {"add", test_add, NULL, teardown, NULL},
        {"cas", test_cas, NULL, teardown, NULL},
        {"replace", test_replace, NULL, teardown, NULL},
        {"incr miss", test_incr_miss, NULL, teardown, NULL},
        {"incr", test_incr, NULL, teardown, NULL},
        {"incr with default", test_incr_default, NULL, teardown, NULL},
        {"delete", test_delete, NULL, teardown, NULL},
        {"flush", test_flush, NULL, teardown, NULL},
        {"flush multi vbuckets", test_flush_multiv, NULL, teardown, NULL},
        {"expiry", test_expiry, NULL, teardown, NULL},
        // Stats tests
        {"stats", test_stats, NULL, teardown, NULL},
        {"io stats", test_io_stats, NULL, teardown, NULL},
        {"bg stats", test_bg_stats, NULL, teardown, NULL},
        {"mem stats", test_mem_stats, NULL, teardown, NULL},
        {"stats key", NULL, NULL, teardown, NULL},
        {"stats vkey", NULL, NULL, teardown, NULL},
        {"stats curr_items", test_curr_items, NULL, teardown, NULL},
        // eviction
        {"value eviction", test_value_eviction, NULL, teardown, NULL},
        // tap tests
        {"tap receiver mutation", test_tap_rcvr_mutate, NULL, teardown, NULL},
        {"tap receiver mutation (dead)", test_tap_rcvr_mutate_dead,
         NULL, teardown, NULL},
        {"tap receiver mutation (pending)", test_tap_rcvr_mutate_pending,
         NULL, teardown, NULL},
        {"tap receiver mutation (replica)", test_tap_rcvr_mutate_replica,
         NULL, teardown, NULL},
        {"tap stream", test_tap_stream, NULL, teardown, NULL},
        // restart tests
        {"test restart", test_restart, NULL, teardown, NULL},
        {"set+get+restart+hit (bin)", test_restart_bin_val, NULL, teardown, NULL},
        {"flush+restart", test_flush_restart, NULL, teardown, NULL},
        {"flush multiv+restart", test_flush_multiv_restart, NULL, teardown, NULL},
        // disk>RAM tests
        {"disk>RAM golden path", test_disk_gt_ram_golden, NULL, teardown, NULL},
        {"disk>RAM paged-out rm", test_disk_gt_ram_paged_rm, NULL, teardown, NULL},
        {"disk>RAM update paged-out", test_disk_gt_ram_update_paged_out, NULL,
         teardown, NULL},
        {"disk>RAM delete paged-out", test_disk_gt_ram_delete_paged_out, NULL,
         teardown, NULL},
        {"disk>RAM paged-out incr", test_disk_gt_ram_incr, NULL,
         teardown, NULL},
        {"disk>RAM set bgfetch race", test_disk_gt_ram_set_race, NULL,
         teardown, NULL},
        {"disk>RAM incr bgfetch race", test_disk_gt_ram_incr_race, NULL,
         teardown, NULL},
        {"disk>RAM delete bgfetch race", test_disk_gt_ram_rm_race, NULL,
         teardown, NULL},
        // vbucket negative tests
        {"vbucket incr (dead)", test_wrong_vb_incr, NULL, teardown, NULL},
        {"vbucket incr (pending)", test_vb_incr_pending, NULL, teardown, NULL},
        {"vbucket incr (replica)", test_vb_incr_replica, NULL, teardown, NULL},
        {"vbucket get (dead)", test_wrong_vb_get, NULL, teardown, NULL},
        {"vbucket get (pending)", test_vb_get_pending, NULL, teardown, NULL},
        {"vbucket get (replica)", test_vb_get_replica, NULL, teardown, NULL},
        {"vbucket getl (dead)", NULL, NULL, teardown, NULL},
        {"vbucket getl (pending)", NULL, NULL, teardown, NULL},
        {"vbucket getl (replica)", NULL, NULL, teardown, NULL},
        {"vbucket set (dead)", test_wrong_vb_set, NULL, teardown, NULL},
        {"vbucket set (pending)", test_vb_set_pending, NULL, teardown, NULL},
        {"vbucket set (replica)", test_vb_set_replica, NULL, teardown, NULL},
        {"vbucket replace (dead)", test_wrong_vb_replace, NULL, teardown, NULL},
        {"vbucket replace (pending)", test_vb_replace_pending, NULL, teardown, NULL},
        {"vbucket replace (replica)", test_vb_replace_replica, NULL, teardown, NULL},
        {"vbucket add (dead)", test_wrong_vb_add, NULL, teardown, NULL},
        {"vbucket add (pending)", test_vb_add_pending, NULL, teardown, NULL},
        {"vbucket add (replica)", test_vb_add_replica, NULL, teardown, NULL},
        {"vbucket cas (dead)", test_wrong_vb_cas, NULL, teardown, NULL},
        {"vbucket cas (pending)", test_vb_cas_pending, NULL, teardown, NULL},
        {"vbucket cas (replica)", test_vb_cas_replica, NULL, teardown, NULL},
        {"vbucket append (dead)", test_wrong_vb_append, NULL, teardown, NULL},
        {"vbucket append (pending)", test_vb_append_pending, NULL, teardown, NULL},
        {"vbucket append (replica)", test_vb_append_replica, NULL, teardown, NULL},
        {"vbucket prepend (dead)", test_wrong_vb_prepend, NULL, teardown, NULL},
        {"vbucket prepend (pending)", test_vb_prepend_pending, NULL, teardown, NULL},
        {"vbucket prepend (replica)", test_vb_prepend_replica, NULL, teardown, NULL},
        {"vbucket del (dead)", test_wrong_vb_del, NULL, teardown, NULL},
        {"vbucket del (pending)", test_vb_del_pending, NULL, teardown, NULL},
        {"vbucket del (replica)", test_vb_del_replica, NULL, teardown, NULL},
        // Vbucket management tests
        {"no vb0 at startup", test_novb0, NULL, teardown, "vb0=false"},
        {"test vbucket get", test_vbucket_get, NULL, teardown, NULL},
        {"test vbucket get missing", test_vbucket_get_miss, NULL, teardown, NULL},
        {"test vbucket create", test_vbucket_create, NULL, teardown, NULL},
        {"test vbucket destroy", test_vbucket_destroy, NULL, teardown, NULL},
        {"test vbucket destroy restart", test_vbucket_destroy_restart,
         NULL, teardown, NULL},
        {NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}

bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}
