/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include "basic_engine_testsuite.h"

#include <iostream>
#include <vector>
#include <sstream>

struct test_harness test_harness;


// Checks that a and b are equal; if not then assert.
#define assert_equal(a, b) assert_equal_impl((a), (b), #a, #b, __FILE__, __LINE__)

// Checkt that a >= b; if not then assert.
#define assert_ge(a, b) assert_ge_impl((a), (b), #a, #b, __FILE__, __LINE__)

template<typename T>
static void assert_equal_impl(const T& a_value, const T& b_value,
                              const char* a_name, const char* b_name,
                              const char* file, int line) {
    if (a_value != b_value) {
        std::stringstream ss;
        ss << "Check '" << a_name << " == " << b_name << "' failed - '"
           << a_value << " == " << b_value << "' at " << file << ":" << line;
        std::cerr << ss.str() << std::endl;
        abort();
    }
}

template<typename T>
static void assert_ge_impl(const T& a_value, const T& b_value,
                           const char* a_name, const char* b_name,
                           const char* file, int line) {
    if (a_value < b_value) {
        std::stringstream ss;
        ss << "Check '" << a_name << " >= " << b_name << "' failed - '"
           << a_value << " >= " << b_value << "' at " << file << ":" << line;
        std::cerr << ss.str() << std::endl;
        abort();
    }
}

/*
 * Make sure that get_info returns something and that repeated calls to it
 * return the same something.
 */
static enum test_result get_info_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    cb_assert(info != NULL);
    cb_assert(info == h1->get_info(h));
    return SUCCESS;
}

/*
 * Make sure that the structure returned by get_info has a non-null description.
 */
static enum test_result get_info_description_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    cb_assert(info->description != NULL);
    return SUCCESS;
}

/*
 * Make sure that the structure returned by get_info has a valid number of
 * features and that the size of the feautes array equals that value
 */
static enum test_result get_info_features_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info *info = h1->get_info(h);
    uint32_t nfeats = info->num_features;
    const feature_info *fi = info->features;
    cb_assert (nfeats > 0);
    while (nfeats-- > 0) {
        cb_assert(fi++ != NULL);
    }

    return SUCCESS;
}

/*
 * Make sure we can successfully allocate an item, allocate op returns success
 * and that item struct is populated
 */
static enum test_result allocate_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("akey", test_harness.doc_namespace);
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1,1,1,
                        PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(test_item != NULL);
    h1->release(h,NULL,test_item);
    return SUCCESS;
}

/*
 * Verify set behavior
 */
static enum test_result set_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it;
    DocKey key("key", test_harness.doc_namespace);
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    cb_assert(h1->allocate(h, NULL, &it, key, 1, 1, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);

    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        cb_assert(h1->store(h, NULL, it, &cas, OPERATION_SET,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }
    h1->release(h, NULL, it);
    return SUCCESS;
}

/*
 * Verify add behavior
 */
static enum test_result add_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it;
    DocKey key("key", test_harness.doc_namespace);
    uint64_t cas;
    int ii;
    cb_assert(h1->allocate(h, NULL, &it, key, 1, 1, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);

    for (ii = 0; ii < 10; ++ii) {
        ENGINE_ERROR_CODE ret = h1->store(h, NULL, it, &cas, OPERATION_ADD,
                                          DocumentState::Alive);
        if (ii == 0) {
            cb_assert(ret == ENGINE_SUCCESS);
            cb_assert(cas != 0);
        } else {
            cb_assert(ret == ENGINE_NOT_STORED);
        }
    }
    h1->release(h, NULL, it);
    return SUCCESS;
}

/*
 * Verify replace behavior
 */
static enum test_result replace_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it;
    uint64_t prev_cas;
    uint64_t cas = 0;
    int ii;
    item_info item_info;
    item_info.nvalue = 1;

    cb_assert(set_test(h, h1) == SUCCESS);
    DocKey key("key", test_harness.doc_namespace);
    cb_assert(h1->allocate(h, NULL, &it, key, sizeof(int), 1, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->get_item_info(h, NULL, it, &item_info) == true);


    for (ii = 0; ii < 10; ++ii) {
        prev_cas = cas;
        *(int*)(item_info.value[0].iov_base) = ii;
        cb_assert(h1->store(h, NULL, it, &cas, OPERATION_REPLACE,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        cb_assert(cas != prev_cas);
    }
    h1->release(h, NULL, it);

    cb_assert(h1->get(h, NULL, &it, key, 0,
                      DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h1->get_item_info(h, NULL, it, &item_info) == true);
    cb_assert(item_info.value[0].iov_len == sizeof(int));
    cb_assert(*(int*)(item_info.value[0].iov_base) == 9);
    h1->release(h, NULL, it);

    return SUCCESS;
}

/*
 * Make sure when we can successfully store an item after it has been allocated
 * and that the cas for the stored item has been generated.
 */
static enum test_result store_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("bkey", test_harness.doc_namespace);
    uint64_t cas = 0;
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1,1,1,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(cas != 0);
    h1->release(h,NULL,test_item);
    return SUCCESS;
}

/*
 * Make sure when we can successfully retrieve an item that has been stored in
 * the engine
 */
static enum test_result get_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    item *test_item_get = NULL;
    DocKey key("get_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h1->get(h,NULL,&test_item_get, key, 0,
                      DocumentState::Alive) == ENGINE_SUCCESS);
    h1->release(h,NULL,test_item);
    h1->release(h,NULL,test_item_get);
    return SUCCESS;
}

static enum test_result expiry_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    item *test_item_get = NULL;
    DocKey key("get_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0, 10,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    test_harness.time_travel(11);
    cb_assert(h1->get(h, NULL, &test_item_get, key, 0,
                      DocumentState::Alive) == ENGINE_KEY_ENOENT);
    h1->release(h,NULL,test_item);
    return SUCCESS;
}

/*
 * Make sure that we can release an item. For the most part all this test does
 * is ensure that thinds dont go splat when we call release. It does nothing to
 * ensure that release did much of anything.
 */
static enum test_result release_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("release_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    h1->release(h, NULL, test_item);
    return SUCCESS;
}

/*
 * Make sure that we can remove an item and that after the item has been
 * removed it can not be retrieved.
 */
static enum test_result remove_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("remove_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    mutation_descr_t mut_info;
    item *check_item;

    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h1->remove(h, NULL, key, &cas, 0, &mut_info) == ENGINE_SUCCESS);
    check_item = test_item;
    cb_assert(h1->get(h, NULL, &check_item, key,
                      0, DocumentState::Alive) ==  ENGINE_KEY_ENOENT);
    cb_assert(check_item == NULL);
    h1->release(h, NULL, test_item);
    return SUCCESS;
}

/*
 * Make sure we can successfully perform a flush operation and that any item
 * stored before the flush can not be retrieved
 */
static enum test_result flush_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("flush_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    item *check_item;

    test_harness.time_travel(3);
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    cb_assert(h1->flush(h, NULL, 0) == ENGINE_SUCCESS);
    check_item = test_item;
    cb_assert(h1->get(h, NULL, &check_item, key,
                      0, DocumentState::Alive) ==  ENGINE_KEY_ENOENT);
    cb_assert(check_item == NULL);
    h1->release(h, NULL, test_item);
    return SUCCESS;
}

/*
 * Make sure we can successfully retrieve the item info struct for an item and
 * that the contents of the item_info are as expected.
 */
static enum test_result get_item_info_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("get_item_info_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    const rel_time_t exp = 1;
    item_info ii;
    ii.nvalue = 1;

    cb_assert(h1->allocate(h, NULL, &test_item, key, 1,0, exp,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    /* Had this been actual code, there'd be a connection here */
    cb_assert(h1->get_item_info(h, NULL, test_item, &ii) == true);
    assert_equal(cas, ii.cas);
    assert_equal(0u, ii.flags);
    cb_assert(strcmp(reinterpret_cast<const char*>(key.data()), static_cast<const char*>(ii.key)) == 0);
    assert_equal(uint16_t(key.size()), ii.nkey);
    assert_equal(1u, ii.nbytes);
    // exptime is a rel_time_t; i.e. seconds since server started. Therefore can only
    // check that the returned value is at least as large as the value
    // we requested (i.e. not in the past).
    assert_ge(ii.exptime, exp);
    h1->release(h, NULL, test_item);
    return SUCCESS;
}

static enum test_result item_set_cas_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("item_set_cas_test_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    const rel_time_t exp = 1;
    uint64_t newcas;
    item_info ii;

    ii.nvalue = 1;
    cb_assert(h1->allocate(h, NULL, &test_item, key, 1,0, exp,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    newcas = cas + 1;
    h1->item_set_cas(h, NULL, test_item, newcas);
    cb_assert(h1->get_item_info(h, NULL, test_item, &ii) == true);
    cb_assert(ii.cas == newcas);
    h1->release(h, NULL, test_item);
    return SUCCESS;
}

uint32_t evictions;
static void eviction_stats_handler(const char *key, const uint16_t klen,
                                   const char *val, const uint32_t vlen,
                                   const void *cookie) {

    if (strncmp(key, "evictions", klen) == 0) {
        char buffer[1024];
        memcpy(buffer, val, vlen);
        buffer[vlen] = '\0';
        evictions = atoi(buffer);
    }
}

static enum test_result lru_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey hot_key("hot_key", test_harness.doc_namespace);
    uint64_t cas = 0;
    int ii;
    int jj;

    cb_assert(h1->allocate(h, NULL, &test_item,
                           hot_key, 4096, 0, 0,
                           PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item,
                        &cas, OPERATION_SET,
                        DocumentState::Alive) == ENGINE_SUCCESS);
    h1->release(h, NULL, test_item);

    for (ii = 0; ii < 250; ++ii) {
        uint8_t key[1024];

        cb_assert(h1->get(h, NULL, &test_item,
                          hot_key, 0, DocumentState::Alive) == ENGINE_SUCCESS);
        h1->release(h, NULL, test_item);
        DocKey allocate_key(key,
                            snprintf(reinterpret_cast<char*>(key), sizeof(key),
                                     "lru_test_key_%08d", ii),
                            test_harness.doc_namespace);
        cb_assert(h1->allocate(h, NULL, &test_item,
                               allocate_key, 4096, 0, 0,
                               PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
        cb_assert(h1->store(h, NULL, test_item,
                            &cas, OPERATION_SET,
                            DocumentState::Alive) == ENGINE_SUCCESS);
        h1->release(h, NULL, test_item);
        cb_assert(h1->get_stats(h, NULL, NULL, 0,
                             eviction_stats_handler) == ENGINE_SUCCESS);
        if (evictions == 2) {
            break;
        }
    }

    cb_assert(ii < 250);
    for (jj = 0; jj <= ii; ++jj) {
        uint8_t key[1024];
        DocKey get_key(key,
                       snprintf(reinterpret_cast<char*>(key), sizeof(key),
                                "lru_test_key_%08d", jj),
                       test_harness.doc_namespace);
        if (jj == 0 || jj == 1) {
            cb_assert(h1->get(h, NULL, &test_item, get_key,
                              0, DocumentState::Alive) == ENGINE_KEY_ENOENT);
        } else {
            cb_assert(h1->get(h, NULL, &test_item, get_key,
                              0, DocumentState::Alive) == ENGINE_SUCCESS);
            cb_assert(test_item != NULL);
            h1->release(h, NULL, test_item);
        }
    }
    return SUCCESS;
}

static enum test_result get_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result reset_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result get_stats_struct_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static enum test_result aggregate_stats_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return PENDING;
}

static protocol_binary_response_header *last_response;

static void release_last_response(void) {
    cb_free(last_response);
    last_response = NULL;
}

static bool response_handler(const void* key, uint16_t keylen,
                             const void *ext, uint8_t extlen,
                             const void *body, uint32_t bodylen,
                             uint8_t datatype, uint16_t status,
                             uint64_t cas, const void *cookie)
{
    protocol_binary_response_header *r;
    char *ptr;

    cb_assert(last_response == NULL);
    last_response = static_cast<protocol_binary_response_header*>
                    (cb_malloc(sizeof(*last_response) + keylen + extlen + bodylen));
    if (last_response == NULL) {
        return false;
    }

    r = last_response;
    r->response.magic = PROTOCOL_BINARY_RES;
    r->response.opcode = 0xff; /* we don't know this! */
    r->response.keylen = htons(keylen);
    r->response.extlen = extlen;
    r->response.datatype = PROTOCOL_BINARY_RAW_BYTES;
    r->response.status = htons(status);
    r->response.bodylen = htonl(keylen + extlen + bodylen);
    r->response.opaque = 0xffffff; /* we don't know this */
    r->response.cas = cas;
    ptr = (char*)(r + 1);
    memcpy(ptr, ext, extlen);
    ptr += extlen;
    memcpy(ptr, key, keylen);
    ptr += keylen;
    memcpy(ptr, body, bodylen);

    return true;
}

static enum test_result touch_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE ret;
    union request {
        protocol_binary_request_touch touch;
        char buffer[512];
    };
    DocKey key("get_test_key", test_harness.doc_namespace);
    union request r;
    item *item = NULL;

    memset(r.buffer, 0, sizeof(r));
    r.touch.message.header.request.magic = PROTOCOL_BINARY_REQ;
    r.touch.message.header.request.opcode = PROTOCOL_BINARY_CMD_TOUCH;
    r.touch.message.header.request.keylen = htons((uint16_t)key.size());
    r.touch.message.header.request.extlen = 4;
    r.touch.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    r.touch.message.header.request.vbucket = 0;
    r.touch.message.header.request.bodylen = htonl((uint32_t)key.size() + 4);
    r.touch.message.header.request.opaque = 0xdeadbeef;
    r.touch.message.header.request.cas = 0;
    r.touch.message.body.expiration = htonl(10);

    memcpy(r.buffer + sizeof(r.touch.bytes), key.data(), key.size());
    ret = h1->unknown_command(h, NULL, &r.touch.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    cb_assert(last_response->response.keylen == 0);
    cb_assert(last_response->response.extlen == 0);
    cb_assert(last_response->response.bodylen == 0);
    release_last_response();

    /* store and get a key */
    cb_assert(get_test(h, h1) == SUCCESS);

    /* Set expiry time to 10 secs.. */
    ret = h1->unknown_command(h, NULL, &r.touch.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(last_response->response.keylen == 0);
    cb_assert(last_response->response.extlen == 0);
    cb_assert(last_response->response.bodylen == 0);
    release_last_response();

    /* time-travel 11 secs.. */
    test_harness.time_travel(11);

    /* The item should have expired now... */
    cb_assert(h1->get(h, NULL, &item, key,
                      0, DocumentState::Alive) == ENGINE_KEY_ENOENT);

    /* Verify that it doesn't accept bogus packets. extlen is mandatory */
    r.touch.message.header.request.extlen = 0;
    r.touch.message.header.request.bodylen = htonl((uint32_t)key.size());
    ret = h1->unknown_command(h, NULL, &r.touch.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    /* key is mandatory! */
    r.touch.message.header.request.extlen = 4;
    r.touch.message.header.request.keylen = 0;
    r.touch.message.header.request.bodylen = htonl(4);
    ret = h1->unknown_command(h, NULL, &r.touch.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    return SUCCESS;
}

static enum test_result gat_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    union request {
        protocol_binary_request_gat gat;
        char buffer[512];
    };

    DocKey key("get_test_key", test_harness.doc_namespace);
    ENGINE_ERROR_CODE ret;
    item *item = NULL;
    union request r;
    memset(r.buffer, 0, sizeof(r));
    r.gat.message.header.request.magic = PROTOCOL_BINARY_REQ;
    r.gat.message.header.request.opcode = PROTOCOL_BINARY_CMD_GAT;
    r.gat.message.header.request.keylen = htons((uint16_t)key.size());
    r.gat.message.header.request.extlen = 4;
    r.gat.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    r.gat.message.header.request.vbucket = 0;
    r.gat.message.header.request.bodylen = htonl((uint32_t)key.size() + 4);
    r.gat.message.header.request.opaque = 0xdeadbeef;
    r.gat.message.header.request.cas = 0;
    r.gat.message.body.expiration = htonl(10);

    memcpy(r.buffer + sizeof(r.gat.bytes), key.data(), key.size());
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    cb_assert(last_response->response.keylen == 0);
    cb_assert(last_response->response.extlen == 0);
    cb_assert(last_response->response.bodylen == 0);
    release_last_response();

    /* store and get a key */
    cb_assert(get_test(h, h1) == SUCCESS);

    /* Set expiry time to 10 secs.. */
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(last_response->response.keylen == 0);
    cb_assert(last_response->response.extlen == 4);
    cb_assert(ntohl(last_response->response.bodylen) == 5); /* get_test sets 1 byte datalen */
    release_last_response();

    /* time-travel 11 secs.. */
    test_harness.time_travel(11);

    /* The item should have expired now... */
    cb_assert(h1->get(h, NULL, &item, key,
                      0, DocumentState::Alive) == ENGINE_KEY_ENOENT);

    /* Verify that it doesn't accept bogus packets. extlen is mandatory */
    r.gat.message.header.request.extlen = 0;
    r.gat.message.header.request.bodylen = htonl((uint32_t)key.size());
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    /* key is mandatory! */
    r.gat.message.header.request.extlen = 4;
    r.gat.message.header.request.keylen = 0;
    r.gat.message.header.request.bodylen = htonl(4);
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    return SUCCESS;
}

static enum test_result gatq_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    union request {
        protocol_binary_request_gat gat;
        char buffer[512];
    };

    ENGINE_ERROR_CODE ret;
    item *item = NULL;
    DocKey key("get_test_key", test_harness.doc_namespace);
    union request r;
    memset(r.buffer, 0, sizeof(r));
    r.gat.message.header.request.magic = PROTOCOL_BINARY_REQ;
    r.gat.message.header.request.opcode = PROTOCOL_BINARY_CMD_GATQ;
    r.gat.message.header.request.keylen = htons((uint16_t)key.size());
    r.gat.message.header.request.extlen = 4;
    r.gat.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    r.gat.message.header.request.vbucket = 0;
    r.gat.message.header.request.bodylen = htonl((uint32_t)key.size() + 4);
    r.gat.message.header.request.opaque = 0xdeadbeef;
    r.gat.message.header.request.cas = 0;
    r.gat.message.body.expiration = htonl(10);

    memcpy(r.buffer + sizeof(r.gat.bytes), key.data(), key.size());
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);

    /* GATQ is quiet and should not produce any result */
    cb_assert(last_response == NULL);

    /* store and get a key */
    cb_assert(get_test(h, h1) == SUCCESS);

    /* Set expiry time to 10 secs.. */
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(last_response->response.keylen == 0);
    cb_assert(last_response->response.extlen == 4);
    cb_assert(ntohl(last_response->response.bodylen) == 5); /* get_test sets 1 byte datalen */
    release_last_response();

    /* time-travel 11 secs.. */
    test_harness.time_travel(11);

    /* The item should have expired now... */
    cb_assert(h1->get(h, NULL, &item, key,
                      0, DocumentState::Alive) == ENGINE_KEY_ENOENT);

    /* Verify that it doesn't accept bogus packets. extlen is mandatory */
    r.gat.message.header.request.extlen = 0;
    r.gat.message.header.request.bodylen = htonl((uint32_t)key.size());
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    /* key is mandatory! */
    r.gat.message.header.request.extlen = 4;
    r.gat.message.header.request.keylen = 0;
    r.gat.message.header.request.bodylen = htonl(4);
    ret = h1->unknown_command(h, NULL, &r.gat.message.header, response_handler, test_harness.doc_namespace);
    cb_assert(ret == ENGINE_SUCCESS);
    cb_assert(last_response != NULL);
    cb_assert(ntohs(last_response->response.status) == PROTOCOL_BINARY_RESPONSE_EINVAL);
    release_last_response();

    return SUCCESS;
}

static enum test_result test_datatype(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *test_item = NULL;
    DocKey key("{foo:1}", test_harness.doc_namespace);
    uint64_t cas = 0;
    item_info ii;
    memset(&ii, 0, sizeof(ii));
    ii.nvalue = 1;

    cb_assert(h1->allocate(h, NULL, &test_item, key, 1, 0,
                           0, 1, 0/*vb*/) == ENGINE_SUCCESS);
    cb_assert(h1->store(h, NULL, test_item, &cas,
                        OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
    h1->release(h, NULL, test_item);

    cb_assert(h1->get(h, NULL, &test_item, key,
                      0, DocumentState::Alive) == ENGINE_SUCCESS);

    cb_assert(h1->get_item_info(h, NULL, test_item, &ii) == true);
    cb_assert(ii.datatype == 1);
    h1->release(h, NULL, test_item);

    return SUCCESS;
}

/*
 * Destroy many buckets - this test is really more interesting with valgrind
 *  destroy should invoke a background cleaner thread and at exit time there
 *  shall be no items left behind.
 */
static enum test_result test_n_bucket_destroy(engine_test_t *test) {
    const int n_buckets = 20;
    const int n_keys = 256;
    std::vector<std::pair<ENGINE_HANDLE*, ENGINE_HANDLE_V1*> > buckets;
    for (int ii = 0; ii < n_buckets; ii++) {
        ENGINE_HANDLE_V1* handle = test_harness.create_bucket(true, test->cfg);
        if (handle) {
            buckets.push_back(std::make_pair(reinterpret_cast<ENGINE_HANDLE*>(handle), handle));
        } else {
            return FAIL;
        }
    }

    for (auto bucket : buckets) {
        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            item *test_item = NULL;
            uint64_t cas = 0;
            DocKey allocate_key(ss, test_harness.doc_namespace);
            cb_assert(bucket.second->allocate(bucket.first, NULL, &test_item,
                                              allocate_key, 256, 1, 1,
                                              PROTOCOL_BINARY_RAW_BYTES, 0) ==
                                                    ENGINE_SUCCESS);
            cb_assert(bucket.second->store(bucket.first, NULL, test_item, &cas,
                                           OPERATION_SET,
                                           DocumentState::Alive) == ENGINE_SUCCESS);
        }
    }

    for (auto itr : buckets) {
        test_harness.destroy_bucket(itr.first, itr.second, false);
    }

    return SUCCESS;
}

/*
 * create and delete buckets, the idea being that the background deletion
 * is running whilst we're creating more buckets.
 */
static enum test_result test_bucket_destroy_interleaved(engine_test_t *test) {
    const int n_keys = 20;
    const int buckets = 5;

    for (int b = 0; b < buckets; b++) {
        ENGINE_HANDLE_V1* h1 = test_harness.create_bucket(true, test->cfg);
        ENGINE_HANDLE* h = reinterpret_cast<ENGINE_HANDLE*>(h1);

        for (int ii = 0; ii < n_keys; ii++) {
            std::string ss = "KEY" + std::to_string(ii);
            item *test_item = NULL;
            uint64_t cas = 0;
            DocKey allocate_key(ss, test_harness.doc_namespace);
            cb_assert(h1->allocate(h, NULL, &test_item, allocate_key, 111256, 1,
                                   1, PROTOCOL_BINARY_RAW_BYTES, 0) == ENGINE_SUCCESS);
            cb_assert(h1->store(h, NULL, test_item, &cas,
                                OPERATION_SET, DocumentState::Alive) == ENGINE_SUCCESS);
            h1->release(h, NULL, test_item);
        }

        test_harness.destroy_bucket(h, h1, false);
    }

    return SUCCESS;
}

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {
    static engine_test_t tests[]  = {
        TEST_CASE("get info test", get_info_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get info description test", get_info_description_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get info features test", get_info_features_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("allocate test", allocate_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("set test", set_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("add test", add_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("replace test", replace_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("store test", store_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get test", get_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("expiry test", expiry_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("remove test", remove_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("release test", release_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("flush test", flush_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get item info test", get_item_info_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("set cas test", item_set_cas_test, NULL, NULL, NULL, NULL, NULL),
#ifndef VALGRIND
        // this test is disabled for VALGRIND because cache_size=48 and using malloc don't work.
        TEST_CASE("LRU test", lru_test, NULL, NULL, "cache_size=48", NULL, NULL),
#endif
        TEST_CASE("get stats test", get_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("reset stats test", reset_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("get stats struct test", get_stats_struct_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("aggregate stats test", aggregate_stats_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("touch", touch_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("Get And Touch", gat_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("Get And Touch Quiet", gatq_test, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE("Test datatype", test_datatype, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE_V2("Bucket destroy", test_n_bucket_destroy, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE_V2("Bucket destroy interleaved", test_bucket_destroy_interleaved, NULL, NULL, NULL, NULL, NULL),
        TEST_CASE(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    };
    return tests;
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    test_harness = *th;
    return true;
}
