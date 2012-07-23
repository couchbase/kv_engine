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
#include "config.h"

#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <vector>
#include <sstream>
#include <cstdlib>
#include <sys/stat.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <pthread.h>
#include <netinet/in.h>
#include <dirent.h>

#ifdef HAS_ARPA_INET_H
#include <arpa/inet.h>
#endif

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#include "atomic.hh"
#include "sqlite-pst.hh"
#include "mutex.hh"
#include "locks.hh"

#include "ep_testsuite.h"
#include "command_ids.h"
#include "mock/mccouch.hh"


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

// ptr_fun don't like the extern "C" thing for unlock cookie.. cast it
// away ;)
typedef void (*UNLOCK_COOKIE_T)(const void *cookie);

extern "C" bool abort_msg(const char *expr, const char *msg, int line);


template <typename T>
static void checkeqfn(T exp, T got, const char *msg, const char *file, const int linenum) {
    if (exp != got) {
        std::stringstream ss;
        ss << "Expected `" << exp << "', got `" << got << "' - " << msg;
        abort_msg(ss.str().c_str(), file, linenum);
    }
}

#define checkeq(a, b, c) checkeqfn(a, b, c, __FILE__, __LINE__)

extern "C" {

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

#define WHITESPACE_DB "whitespace sucks.db"
#define MULTI_DISPATCHER_CONFIG \
    "initfile=t/wal.sql;ht_size=129;ht_locks=3;chk_remover_stime=1;chk_period=60;db_strategy=multiMTVBDB"

protocol_binary_response_status last_status(static_cast<protocol_binary_response_status>(0));
char *last_key = NULL;
char *last_body = NULL;
// set dump_stats to true if you like to dump the stats as we go along...
static bool dump_stats = false;
std::map<std::string, std::string> vals;
uint32_t last_bodylen = 0;
uint64_t last_cas = 0;
bool last_deleted_flag = false;

struct test_harness testHarness;

class ThreadData {
public:
    ThreadData(ENGINE_HANDLE *eh, ENGINE_HANDLE_V1 *ehv1,
               int e=0) : h(eh), h1(ehv1), extra(e) {}
    ENGINE_HANDLE    *h;
    ENGINE_HANDLE_V1 *h1;
    int               extra;
};

bool abort_msg(const char *expr, const char *msg, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            __FILE__, line, msg, expr);
    abort();
    // UNREACHABLE
    return false;
}

static void rmrf(const char *fname)
{
    struct stat st;
    if (stat(fname, &st) == 0) {
        if ((st.st_mode & S_IFDIR) == S_IFDIR) {
            DIR *dp = opendir(fname);
            if (dp != NULL) {
                struct dirent *de;
                char buffer[PATH_MAX + sizeof(*de)];

                while (true) {
                    // readdir_r doesn't work the same way on solaris/mac/linux
                    de = NULL;
                    readdir_r(dp, (struct dirent*)buffer, &de);
                    if (de == NULL) {
                        break;
                    }
                    if (!(de->d_name[0] == '.' &&
                          (de->d_name[1] == '.' || de->d_name[1] == '\0'))) {
                        char path[PATH_MAX];
                        snprintf(path, sizeof(path), "%s/%s",
                                 fname, de->d_name);
                        rmrf(path);
                    }
                }
                closedir(dp);
            }
        }

        check(remove(fname) == 0, "Failed to remove file");
    }
}

static enum test_result rmdb(void)
{
    const char *files[] = { WHITESPACE_DB,
                            WHITESPACE_DB,
                            WHITESPACE_DB "-0.sqlite",
                            WHITESPACE_DB "-1.sqlite",
                            WHITESPACE_DB "-2.sqlite",
                            WHITESPACE_DB "-3.sqlite",
                            "/tmp/test.db",
                            "/tmp/mutation.log",
                            "/tmp/test.db-0.sqlite",
                            "/tmp/test.db-1.sqlite",
                            "/tmp/test.db-2.sqlite",
                            "/tmp/test.db-3.sqlite",
                            "/tmp/test.db-wal",
                            "/tmp/test.db-0.sqlite-wal",
                            "/tmp/test.db-1.sqlite-wal",
                            "/tmp/test.db-2.sqlite-wal",
                            "/tmp/test.db-3.sqlite-wal",
                            "/tmp/test.db-shm",
                            "/tmp/test.db-0.sqlite-shm",
                            "/tmp/test.db-1.sqlite-shm",
                            "/tmp/test.db-2.sqlite-shm",
                            "/tmp/test.db-3.sqlite-shm",
                            NULL };
    int ii = 0;
    while (files[ii] != NULL) {
        rmrf(files[ii]);
        if (access(files[ii], F_OK) != -1) {
            std::cerr << "Failed to remove: " << files[ii] << " ";
            return FAIL;
        }
        ++ii;
    }

    return SUCCESS;
}

static enum test_result skipped_test_function(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void) h;
    (void) h1;
    return SKIPPED;
}

static bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    vals.clear();
    return true;
}

static inline void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    usleep(*sleepTime);
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

static void add_stats(const char *key, const uint16_t klen,
                      const char *val, const uint32_t vlen,
                      const void *cookie) {
    (void)cookie;
    std::string k(key, klen);
    std::string v(val, vlen);

    if (dump_stats) {
        std::cout << "stat[" << k << "] = " << v << std::endl;
    }

    vals[k] = v;
}

static int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey = NULL) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    std::string s = vals[statname];
    return atoi(s.c_str());
}

static std::string get_str_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey = NULL) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    std::string s = vals[statname];
    return s;
}

static void waitfor_restore_state(ENGINE_HANDLE *h,
                                  ENGINE_HANDLE_V1 *h1,
                                  const char *state,
                                  uint32_t sampletime = 250)
{
    do {
        usleep(sampletime);
        vals.clear();
        check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
              "Failed to get stats.");
    } while (vals["ep_restore:state"] != state);
    vals.clear();
}

static bool wait_for_warmup_complete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    while (h1->get_stats(h, NULL, "warmup", 6, add_stats) == ENGINE_SUCCESS) {
        useconds_t sleepTime = 128;
        std::string s = vals["ep_warmup_thread"];
        if (strcmp(s.c_str(), "complete") == 0) {
            break;
        }
        decayingSleep(&sleepTime);
        vals.clear();
    }
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
    } else {
        h1->release(h, NULL, it);
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
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), vbucket);
    if (rv == ENGINE_SUCCESS) {
        h1->release(h, NULL, i);
    }
    return rv;
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
        h1->release(h, NULL, i);
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    h1->release(h, NULL, i);
    return true;
}

static bool get_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item *i,
                    std::string &key) {

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, i, &info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    key.assign((const char*)info.key, info.nkey);
    return true;
}



static enum test_result check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const char* key,
                                        const char* val, size_t vlen,
                                        uint16_t vbucket = 0) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv;
    if ((rv = h1->get(h, NULL, &i, key, strlen(key), vbucket)) != ENGINE_SUCCESS) {
        fprintf(stderr, "Expected ENGINE_SUCCESS on get of %s (vb=%d), got %d\n",
                key, vbucket, rv);
        abort();
    }

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "check_key_value");

    assert(info.nvalue == 1);
    if (vlen != info.value[0].iov_len) {
        std::cerr << "Expected length " << vlen
                  << " got " << info.value[0].iov_len << std::endl;
        checkeq(vlen, (size_t)info.value[0].iov_len, "Length mismatch.");
    }

    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static bool add_response(const void *key, uint16_t keylen,
                         const void *ext, uint8_t extlen,
                         const void *body, uint32_t bodylen,
                         uint8_t datatype, uint16_t status,
                         uint64_t cas, const void *cookie) {
    (void)ext;
    (void)extlen;
    (void)datatype;
    (void)cookie;
    last_bodylen = bodylen;
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
    last_cas = cas;
    return true;
}

static bool add_response_get_meta(const void *key, uint16_t keylen,
                                  const void *ext, uint8_t extlen,
                                  const void *body, uint32_t bodylen,
                                  uint8_t datatype, uint16_t status,
                                  uint64_t cas, const void *cookie) {
    (void)datatype;
    (void)cookie;
    if (ext && extlen > 0) {
        uint32_t flags;
        memcpy(&flags, ext, extlen);
        last_deleted_flag = ntohl(flags) & GET_META_ITEM_DELETED_FLAG;
    }
    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

static bool test_setup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    wait_for_warmup_complete(h, h1);

    // warmup is complete, notify ep engine that it must now enable
    // data traffic
    protocol_binary_request_no_extras req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header *>(&req);
    memset (&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = CMD_ENABLE_TRAFFIC;

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to enable data traffic");

    int restore_mode = get_int_stat(h, h1, "ep_restore_mode");
    if (!restore_mode) {
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "Expected to be able to enable data traffic at engine level");
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

static protocol_binary_request_header* createPacket(uint8_t opcode,
                                                    uint16_t vbid,
                                                    const char *key = NULL,
                                                    const char *val = NULL) {
    char *pkt_raw;
    uint32_t keylen = key != NULL ? strlen(key) : 0;
    uint32_t vallen = val != NULL ? strlen(val) : 0;
    pkt_raw = static_cast<char*>(calloc(1,
                                        sizeof(protocol_binary_request_header)
                                        + keylen
                                        + vallen));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.vbucket = ntohs(vbid);
    req->request.bodylen = htonl(keylen + vallen);
    req->request.keylen = htons(keylen);
    if (keylen > 0) {
        memcpy(pkt_raw + sizeof(protocol_binary_request_header),
               key, keylen);
    }

    if (vallen > 0) {
        memcpy(pkt_raw + sizeof(protocol_binary_request_header) + keylen,
               val, vallen);
    }

    return req;
}

static protocol_binary_request_header* create_set_param_packet(uint8_t opcode,
                                                               engine_param_t paramtype,
                                                               const char *key,
                                                               const char *val) {
    size_t keylen = strlen(key);
    size_t vallen = strlen(val);
    size_t header_size = sizeof(protocol_binary_request_header) + sizeof(engine_param_t);

    char *pkt_raw = static_cast<char*>(calloc(1, header_size + keylen + vallen));
    assert(pkt_raw);
    protocol_binary_request_set_param *req = (protocol_binary_request_set_param*) pkt_raw;
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = opcode;
    req->message.header.request.extlen = sizeof(engine_param_t);
    req->message.header.request.bodylen = htonl(keylen + vallen + sizeof(engine_param_t));
    req->message.header.request.keylen = htons(keylen);
    req->message.body.param_type = static_cast<engine_param_t>(htonl(paramtype));
    memcpy(pkt_raw + header_size, key, keylen);
    memcpy(pkt_raw + header_size + keylen, val, vallen);

    return (protocol_binary_request_header *)req;
}

static void stop_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    while (true) {
        useconds_t sleepTime = 128;
        if (get_str_stat(h, h1, "ep_flusher_state", 0) == "running") {
            break;
        }
        decayingSleep(&sleepTime);
    }

    protocol_binary_request_header *pkt = create_packet(CMD_STOP_PERSISTENCE, "", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error stopping persistence.");
}

static void start_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = create_packet(CMD_START_PERSISTENCE, "", "");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error starting persistence.");
}

static protocol_binary_request_header*
       createObservePacket(std::map<std::string, uint16_t> obskeys) {
    std::stringstream value;
    std::map<std::string, uint16_t>::iterator it;
    for (it = obskeys.begin(); it != obskeys.end(); it++) {
        uint16_t vb = htons(it->second);
        uint16_t keylen = htons(it->first.length());
        value.write((char*) &vb, sizeof(uint16_t));
        value.write((char*) &keylen, sizeof(uint16_t));
        value.write(it->first.c_str(), it->first.length());
    }

    uint16_t val_len = value.str().length();

    char *pkt_raw;
    pkt_raw = static_cast<char*>(calloc(1, sizeof(protocol_binary_request_header)
                                           + val_len));
    assert(pkt_raw);
    protocol_binary_request_header *req = (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = CMD_OBSERVE;
    req->request.vbucket = ntohs(0);
    req->request.bodylen = htonl(val_len);
    req->request.keylen = htons(0);

    if (val_len > 0) {
        memcpy(pkt_raw + sizeof(protocol_binary_request_header),
            value.str().data(), val_len);
    }
    return req;
}

static void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const char *key, uint16_t vbucketId=0,
                      const char *msg = NULL, bool expectError = false) {
    int nonResidentItems = get_int_stat(h, h1, "ep_num_non_resident");
    int numEjectedItems = get_int_stat(h, h1, "ep_num_value_ejects");
    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        key, "");
    pkt->request.vbucket = htons(vbucketId);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to evict key.");

    if (expectError) {
        check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
              "Expected exists when evicting key.");
    } else {
        if (strcmp(last_body, "Already ejected.") != 0) {
            nonResidentItems++;
            numEjectedItems++;
        }
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "Expected success evicting key.");
    }

    checkeq(nonResidentItems, get_int_stat(h, h1, "ep_num_non_resident"),
            "Incorrect number of non-resident items");
    checkeq(numEjectedItems, get_int_stat(h, h1, "ep_num_value_ejects"),
            "Incorrect number of ejected items");

    if (msg != NULL && strcmp(last_body, msg) != 0) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_body);
        abort();
    }
}

static bool get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
                     ItemMetaData &itm_meta) {
    uint16_t nkey = strlen(key);

    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_get req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_GET_META;
    msg.req.message.header.request.keylen = ntohs(nkey);
    msg.req.message.header.request.vbucket = htons(0);
    msg.req.message.header.request.bodylen = htonl(nkey);
    memcpy(msg.buffer + sizeof(msg.req.bytes), key, nkey);

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                add_response_get_meta);
    check(ret == ENGINE_SUCCESS, "Expected get_meta call to be successful");
    if (last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return itm_meta.decode((uint8_t *)last_body);
    } else {
        return false;
    }
}

static enum test_result test_getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "k1";
    uint16_t keylen = (uint16_t)strlen(key);
    char *pkt_raw = static_cast<char*>(calloc(1,sizeof(protocol_binary_request_getl)
                                                 + keylen));
    memcpy(pkt_raw + sizeof(protocol_binary_request_getl), key, keylen);
    uint16_t vbucketId = 0;
    uint32_t expiration = 25;

    protocol_binary_request_getl *gl = (protocol_binary_request_getl*)pkt_raw;
    protocol_binary_request_header *pkt = (protocol_binary_request_header *)pkt_raw;

    gl->message.header.request.magic = PROTOCOL_BINARY_REQ;
    gl->message.header.request.opcode = CMD_GET_LOCKED;
    gl->message.header.request.extlen = 4;
    gl->message.header.request.bodylen = htonl(keylen + 4);
    gl->message.header.request.keylen = htons(keylen);
    gl->message.header.request.vbucket = htons(vbucketId);
    gl->message.body.expiration = htonl(expiration);
    memcpy(gl->bytes + sizeof(gl->bytes), key, keylen);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Getl Failed");

    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");
    if (last_body != NULL && (strcmp(last_body, "NOT_FOUND") != 0)) {
        fprintf(stderr, "Should have returned NOT_FOUND. Getl Failed");
        abort();
    }

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* retry getl, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");
    check(strcmp("lockdata", last_body) == 0, "Body was malformed.");

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock's taken so this should fail */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (last_body != NULL && (strcmp(last_body, "LOCK_ERROR") != 0)) {
        fprintf(stderr, "Should have returned LOCK_ERROR. Getl Failed");
        abort();
    }

    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata2", &i, 0, vbucketId)
          != ENGINE_SUCCESS, "Should have failed to store an item.");
    h1->release(h, NULL, i);

    /* wait another 10 seconds */
    testHarness.time_travel(10);

    /* retry set, should succeed */
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");

    /* try an incr operation followed by a delete, both of which should fail */
    uint64_t cas = 0;
    uint64_t result = 0;

    check(h1->arithmetic(h, NULL, key, 2, true, false, 1, 1, 0,
                         &cas, &result,
                         0)  == ENGINE_TMPFAIL, "Incr failed");


    check(h1->remove(h, NULL, key, 2, 0, 0) == ENGINE_TMPFAIL,
          "Delete failed");


    /* bug MB 2699 append after getl should fail with ENGINE_TMPFAIL */

    testHarness.time_travel(26);

    char binaryData1[] = "abcdefg\0gfedcba";
    char binaryData2[] = "abzdefg\0gfedcba";

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, key,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");


    /* append should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key,
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_TMPFAIL,
          "Append should fail.");
    h1->release(h, NULL, i);

    /* bug MB 3252 & MB 3354.
     * 1. Set a key with an expiry value.
     * 2. Take a lock on the item before it expires
     * 3. Wait for the item to expire
     * 4. Perform a CAS operation, should fail
     * 5. Perform a set operation, should succeed
     */
    const char *ekey = "test_expiry";
    const char *edata = "some test data here.";

    item *it = NULL;

    check(h1->allocate(h, NULL, &it, ekey, strlen(ekey), strlen(edata), 0, 2)
        == ENGINE_SUCCESS, "Allocation Failed");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, edata, strlen(edata));

    check(h1->store(h, NULL, it, &cas, OPERATION_SET, 0) ==
        ENGINE_SUCCESS, "Failed to Store item");
    check_key_value(h, h1, ekey, edata, strlen(edata));
    h1->release(h, NULL, it);

    /* item created. lock it and wait for the object to expire */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");

    testHarness.time_travel(3);
    cas = last_cas;

    /* cas should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_CAS, ekey,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, cas, 0)
          != ENGINE_SUCCESS,
          "CAS succeeded.");
    h1->release(h, NULL, i);

    /* but a simple store should succeed */
    check(store(h, h1, NULL, OPERATION_SET, ekey, edata, &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "k2";
    uint16_t vbucketId = 0;

    protocol_binary_request_header *pkt = create_packet(CMD_GET_LOCKED,
                                                        key, "");
    pkt->request.vbucket = htons(vbucketId);

    protocol_binary_request_header *pkt_ul = create_packet(CMD_UNLOCK_KEY,
                                                        key, "");
    pkt_ul->request.vbucket = htons(vbucketId);

    check(h1->unknown_command(h, NULL, pkt_ul, add_response) == ENGINE_SUCCESS,
          "Getl Failed");

    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");
    if (last_body != NULL && (strcmp(last_body, "NOT_FOUND") != 0)) {
        fprintf(stderr, "Should have returned NOT_FOUND. Unl Failed");
        abort();
    }

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);

    /* getl, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");

    /* save the returned cas value for later */
    uint64_t cas = last_cas;

    /* lock's taken unlocking with a random cas value should fail */
    check(h1->unknown_command(h, NULL, pkt_ul, add_response) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (last_body != NULL && (strcmp(last_body, "UNLOCK_ERROR") != 0)) {
        fprintf(stderr, "Should have returned UNLOCK_ERROR. Unl Failed");
        abort();
    }

    /* set the correct cas value in the outgoing request */
    pkt_ul->request.cas = htonll(cas);

    check(h1->unknown_command(h, NULL, pkt_ul, add_response) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to succed unl with correct cas");

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Lock failed");

    pkt_ul->request.cas = last_cas;

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock has expired, unl should fail */
    check(h1->unknown_command(h, NULL, pkt_ul, add_response) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail unl on lock timeout");

    return SUCCESS;
}

static bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              uint16_t vb, vbucket_state_t state) {

    protocol_binary_request_set_vbucket req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_VBUCKET;
    req.message.header.request.vbucket = htons(vb);
    req.message.body.state = static_cast<vbucket_state_t>(htonl(state));

    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool set_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      engine_param_t paramtype, const char *param, const char *val) {
    protocol_binary_request_header *pkt = create_set_param_packet(CMD_SET_PARAM,
                                                                  paramtype,
                                                                  param, val);
    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 uint16_t vb, vbucket_state_t expected,
                                 bool mute = false) {

    protocol_binary_request_get_vbucket req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_VBUCKET;
    req.message.header.request.vbucket = htons(vb);

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
                    last_status, last_body ? last_body : "unknown");
        }
        return false;
    }

    vbucket_state_t state;
    memcpy(&state, last_body, sizeof(state));
    state = static_cast<vbucket_state_t>(ntohl(state));
    return state == expected;
}

static bool verify_vbucket_missing(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                   uint16_t vb) {
    char vbid[8];
    snprintf(vbid, sizeof(vbid), "vb_%d", vb);
    std::string vbstr(vbid);

    // Try up to three times to verify the bucket is missing.  Bucket
    // state changes are async.
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    if (vals.find(vbstr) == vals.end()) {
        return true;
    }

    std::cerr << "Expected bucket missing, got " << vals[vbstr] << std::endl;

    return false;
}

static void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int exp, const char *msg) {
    int curr_items = get_int_stat(h, h1, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

static void wait_for_stat_change(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *stat, int initial) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, stat) == initial) {
        decayingSleep(&sleepTime);
    }
}

static void wait_for_stat_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                const char *stat, int final, const char* stat_key=NULL) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, stat, stat_key) != final) {
        decayingSleep(&sleepTime);
    }
}

static void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_flusher_todo")
           + get_int_stat(h, h1, "ep_queue_size")
           + get_int_stat(h, h1, "ep_uncommitted_items") > 0) {
        decayingSleep(&sleepTime);
    }
    wait_for_stat_change(h, h1, "ep_commit_num", 0);
}

static void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                     const char *key, const char *val,
                                     uint16_t vbucketId=0) {

    item *i = NULL;
    int commitNum = get_int_stat(h, h1, "ep_commit_num");
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, key, val, &i, 0, vbucketId),
            "Failed to store an item.");

    // Wait for persistence...
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_commit_num", commitNum);
    h1->release(h, NULL, i);
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // Add operation with cas != 0 doesn't make sense
        cas = 0;
    }
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not_my_vbucket");
    h1->release(h, NULL, i);
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_pending_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_pending), "Bucket state was not set to pending.");
    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // Add operation with cas != 0 doesn't make sense..
        cas = 0;
    }
    check(store(h, h1, cookie, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_EWOULDBLOCK,
        "Expected woodblock");
    h1->release(h, NULL, i);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_replica), "Bucket state was not set to replica.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");

    uint64_t cas = 11;
    if (op == OPERATION_ADD) {
        // performing add with a CAS != 0 doesn't make sense...
        cas = 0;
    }
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, cas, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not my vbucket");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    h1->release(h, NULL, i);
    return SUCCESS;
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

static enum test_result test_init_fail(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              "dbname=/non/existent/path/dbname",
                              false, false);

    check(h1->initialize(h, "dbname=/non/existent/path/dbname")
          == ENGINE_FAILED, "Failed to fail to initialize");

    // This test will crash *after* this if it can't successfully
    // destroy the engine.
    return SUCCESS;
}

static enum test_result test_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(ENGINE_SUCCESS ==
          store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
          "Error setting.");
    h1->release(h, NULL, i);
    return SUCCESS;
}

struct handle_pair {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
};

extern "C" {
    static void* conc_del_set_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);
        item *it = NULL;

        for (int i = 0; i < 5000; ++i) {
            store(hp->h, hp->h1, NULL, OPERATION_ADD,
                  "key", "somevalue", &it);
            hp->h1->release(hp->h, NULL, it);
            usleep(10);
            checkeq(ENGINE_SUCCESS,
                    store(hp->h, hp->h1, NULL, OPERATION_SET,
                          "key", "somevalue", &it),
                    "Error setting.");
            hp->h1->release(hp->h, NULL, it);
            usleep(10);
            // Ignoring the result here -- we're racing.
            hp->h1->remove(hp->h, NULL, "key", 3, 0, 0);
            usleep(10);
        }
        return NULL;
    }
}

static enum test_result test_conc_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const int n_threads = 8;
    pthread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    wait_for_persisted_value(h, h1, "key", "value1");

    for (int i = 0; i < n_threads; i++) {
        int r = pthread_create(&threads[i], NULL, conc_del_set_thread, &hp);
        assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    wait_for_flusher_to_settle(h, h1);

    std::string backend = get_str_stat(h, h1, "ep_backend");

    // There should be no more newer items than deleted items (sqlite only).
    if (backend.compare("sqlite") == 0 &&
        std::abs(get_int_stat(h, h1, "ep_total_new_items") -
        get_int_stat(h, h1, "ep_total_del_items")) > 1) {
        std::cout << "new:       " << get_int_stat(h, h1, "ep_total_new_items") << std::endl
                  << "rm:        " << get_int_stat(h, h1, "ep_total_del_items") << std::endl
                  << "persisted: " << get_int_stat(h, h1, "ep_total_persisted") << std::endl
                  << "commits:   " << get_int_stat(h, h1, "ep_commit_num") << std::endl;
        abort();
    }

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    assert(0 == get_int_stat(h, h1, "ep_warmed_dups"));

    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    h1->release(h, NULL, i);
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
    h1->release(h, NULL, i);
    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set.");
    h1->release(h, NULL, i);

    item_info info;
    uint32_t flags = 828258;
    check(get_value(h, h1, "key", &info), "Failed to get value.");
    assert(info.flags != flags);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "newvalue", strlen("newvalue"), flags, &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to set again.");
    h1->release(h, NULL, i);

    check(get_value(h, h1, "key", &info), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to do initial set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    check(h1->get(h, NULL, &i, "key", 3, 0) == ENGINE_SUCCESS,
          "Failed to get value.");

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "Failed to get item info.");
    h1->release(h, NULL, i);

    check(store(h, h1, NULL, OPERATION_CAS, "key", "winCas", &i,
                info.cas) == ENGINE_SUCCESS,
          "Failed to store CAS");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "winCas", 6);

    uint64_t cval = 99999;
    check(store(h, h1, NULL, OPERATION_CAS, "non-existing", "winCas", &i,
                cval) == ENGINE_KEY_ENOENT,
          "CAS for non-existing key returned the wrong error code");
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");
    h1->release(h, NULL, i);

    // This aborts on failure.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    check(store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i) == ENGINE_SUCCESS,
          "Failed to add value again.");
    h1->release(h, NULL, i);

    return check_key_value(h, h1, "key", "newvalue", 8);
}

static enum test_result test_add_add_with_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD, "key",
                "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    info.nvalue = 1;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info) == true,
          "Should be able to get info");

    item *i2 = NULL;
    ENGINE_ERROR_CODE ret;
    check((ret = store(h, h1, NULL, OPERATION_ADD, "key",
                       "somevalue", &i2, info.cas)) == ENGINE_KEY_EEXISTS,
          "Should not be able to add the key two times");

    h1->release(h, NULL, i);
    h1->release(h, NULL, i2);
    return SUCCESS;
}

static enum test_result test_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to replace non-existing value.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set value.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to replace existing value.");
    h1->release(h, NULL, i);
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

    return check_key_value(h, h1, "key", "3", 1);
}

static enum test_result test_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "\r\nfoo\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected append failure.");
    h1->release(h, NULL, i);
    delete bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    std::string expected;
    expected.append(binaryData1, sizeof(binaryData1) - 1);
    expected.append(binaryData2, sizeof(binaryData2) - 1);

    return check_key_value(h, h1, "key", expected.data(), expected.length());
}

static enum test_result test_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "foo\r\n\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected prepend failure.");
    h1->release(h, NULL, i);
    delete bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");
    h1->release(h, NULL, i);

    std::string expected;
    expected.append(binaryData2, sizeof(binaryData2) - 1);
    expected.append(binaryData1, sizeof(binaryData1) - 1);

    return check_key_value(h, h1, "key", expected.data(), expected.length());
}

static enum test_result test_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    return check_key_value(h, h1, "key", "2", 1);
}

static enum test_result test_bug2799(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD, "key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    h1->release(h, NULL, i);

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    check_key_value(h, h1, "key", "2", 1);

    testHarness.time_travel(3617);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_flush_disabled(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // start an engine with disabled flush, the flush() should be noop and
    // we expect to see the key after flush()

    // store a key and check its existence
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP, "Flush should be disabled");
    //check the key
    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");

    // restart engine with flush enabled and redo the test, we expect flush to succeed
    std::string param = "flushall_enabled=false";
    std::string config = testHarness.get_current_testcase()->cfg;
    size_t found = config.find(param);
    if(found != config.npos) {
        config.replace(found, param.size(), "flushall_enabled=true");
    }
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              config.c_str(),
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Flush should be enabled");

    //expect missing key
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_flush_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    int mem_used = get_int_stat(h, h1, "mem_used");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_key(h, h1, "key2"), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9);

    int mem_used2 = get_int_stat(h, h1, "mem_used");
    int overhead2 = get_int_stat(h, h1, "ep_overhead");
    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    assert(mem_used2 > mem_used);
    // "mem_used2 - overhead2" (i.e., ep_kv_size) should be greater than the hashtable cache size
    // due to the checkpoint overhead
    assert(mem_used2 - overhead2 > cacheSize2);

    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    wait_for_flusher_to_settle(h, h1);

    mem_used2 = get_int_stat(h, h1, "mem_used");
    overhead2 = get_int_stat(h, h1, "ep_overhead");
    cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    assert(mem_used2 == mem_used);
    assert(overhead2 == overhead);
    assert(nonResident2 == nonResident);
    assert(cacheSize2 == cacheSize);

    return SUCCESS;
}

static enum test_result test_flush_multiv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");
    h1->release(h, NULL, i);

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_vb_key(h, h1, "key2", 2), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9, 2);

    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");

    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_flush_all") != vals.end(), "Failed to get the status of flush_all");

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
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key2", "somevalue", 9);

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush, post-restart set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush_multiv_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");
    h1->release(h, NULL, i);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Flush
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

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
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    // Can I time travel to an expired object and delete it?
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(3617);
    checkeq(ENGINE_KEY_ENOENT, h1->remove(h, NULL, "key", 3, 0, 0),
            "Did not get ENOENT removing an expired object.");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_set_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Failed set.");
    h1->release(h, NULL, i);
    check_key_value(h, h1, "key", "somevalue", 9);
    checkeq(ENGINE_SUCCESS, h1->remove(h, NULL, "key", 3, 0, 0),
            "Failed remove with value.");
    checkeq(ENGINE_KEY_ENOENT, verify_key(h, h1, "key"), "Expected missing key");
    wait_for_flusher_to_settle(h, h1);
    checkeq(0, get_int_stat(h, h1, "curr_items"), "Deleting left tombstone.");
    return SUCCESS;
}

static enum test_result test_set_delete_invalid_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key",
                "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    item_info info;
    info.nvalue = 1;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info) == true,
          "Should be able to get info");
    h1->release(h, NULL, i);

    check(h1->remove(h, NULL, "key", 3, info.cas + 1, 0) == ENGINE_KEY_EEXISTS,
          "Didn't expect to be able to remove the item with wrong cas");
    return SUCCESS;
}

static enum test_result test_bug2509(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (int j = 0; j < 10000; ++j) {
        item *itm = NULL;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &itm),
                "Failed set.");
        h1->release(h, NULL, itm);
        usleep(10);
        checkeq(ENGINE_SUCCESS, h1->remove(h, NULL, "key", 3, 0, 0),
                "Failed remove with value.");
        usleep(10);
    }

    // Restart again, to verify we don't have any duplicates.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    return get_int_stat(h, h1, "ep_warmup_dups") == 0 ? SUCCESS : FAIL;
}

static enum test_result test_bug2761(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    // Make a vbucket mess.
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    std::vector<std::string>::iterator it;
    for (int j = 0; j < 1000; ++j) {
        check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set set vbucket 0 dead.");
        protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 0);
        check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
              "Failed to request delete bucket");
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "Expected vbucket deletion to work.");
        check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set set vbucket 0 active.");
        for (it = keys.begin(); it != keys.end(); ++it) {
            item *i;
            check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i)
                  == ENGINE_SUCCESS, "Failed to store a value");
            h1->release(h, NULL, i);
        }
    }
    wait_for_flusher_to_settle(h, h1);

    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, h1, it->c_str(), 0, "Ejected.");
    }
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set set vbucket 0 dead.");
    sleep(1);
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set set vbucket 0 active.");
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }
    return SUCCESS;
}

// bug 2830 related items

static void bug_2830_child(int reader, int writer) {
    alarm(60);

    sqlite3 *db;

    const char * fn = "/tmp/test.db-0.sqlite";
    if(sqlite3_open(fn, &db) !=  SQLITE_OK) {
        throw std::runtime_error("Error initializing sqlite3");
    }

    // This will immediately lock the database
    PreparedStatement pst(db, "begin immediate");
    assert(pst.execute() >= 0);

    // Signal we've got the txn so the parent can start trying to fail.
    char buf[1];
    buf[0] = 'x';
    assert(write(writer, buf, 1) == 1);

    // Wait for the signal that we've broken something
    assert(read(reader, buf, 1) == 1);

    // Let's go ahead and rollback before we close the DB.  Just to be nice.
    PreparedStatement pstrollback(db, "rollback");
    assert(pstrollback.execute() >= 0);
    sqlite3_close(db);
}

extern "C" {
    // This thread will watch for failures to begin a transaction, and
    // then signal the child that it's done enough damage so it can
    // exit.
    static void* bug2830_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        const char *key = "key";
        const char *val = "value";
        int initial = get_int_stat(td->h, td->h1, "ep_item_begin_failed");

        item *i = NULL;
        check(store(td->h, td->h1, NULL, OPERATION_SET, key, val, &i, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
        td->h1->release(td->h, NULL, i);
        wait_for_stat_change(td->h, td->h1, "ep_item_begin_failed", initial);
        char buf[1];
        assert(write(td->extra, buf, 1) == 1);
        return NULL;
    }
}

static enum test_result test_bug2830(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // XXX:  Need to be able to detect the vb snapshot has run.  We can
    // do that once MB-2663 is done.  Until then, 100ms should do.
    usleep(100 * 1000);

    pid_t child;
    int p2c[2]; // parent to child
    int c2p[2]; // child to parent

    assert(pipe(p2c) == 0);
    assert(pipe(c2p) == 0);

    int childreader = p2c[0];
    int childwriter = c2p[1];

    int parentreader = c2p[0];
    int parentwriter = p2c[1];

    switch (child = fork()) {
    case 0:
        bug_2830_child(childreader, childwriter);
        exit(0);
        abort(); // not reached
    case -1:
        perror("fork");
        abort();
        break;
    }

    // Wait for the child to let us know we can start or work.
    char buf[1];
    assert(read(parentreader, buf, 1) == 1);

    // Start a thread to monitor stats and let us know when we've had
    // enough.
    ThreadData *td = new ThreadData(h, h1, parentwriter);
    pthread_t tid;
    if (pthread_create(&tid, NULL, bug2830_thread, td) != 0) {
        abort();
    }

    // Wait for the thread to indicate stuff's done.
    assert(pthread_join(tid, NULL) == 0);

    // And let us write out our data.
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key");
    check_key_value(h, h1, "key", "value", 5);

    // The child will die and we'll verify it does so safely.
    int status;
    assert(child == waitpid(child, &status, 0));
    assert(WIFEXITED(status));
    assert(WEXITSTATUS(status) == 0);

    // Verify we had a failure.
    assert(get_int_stat(h, h1, "ep_item_begin_failed") > 0);

    return SUCCESS;
}

// end of bug 2830 related items

static enum test_result test_delete_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "key", "value1");

    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");

    wait_for_persisted_value(h, h1, "key", "value2");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    check_key_value(h, h1, "key", "value2", 6);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    static const char val[] = "somevalue";
    ENGINE_ERROR_CODE ret;
    check((ret = store(h, h1, NULL, OPERATION_SET, "key", val, &i)) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    return check_key_value(h, h1, "key", val, strlen(val));
}

static enum test_result test_specialKeys(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    ENGINE_ERROR_CODE ret;

    // Simplified Chinese "Couchbase"
    static const char key0[] = "沙发数据库";
    static const char val0[] = "some Chinese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key0, val0, &i)) == ENGINE_SUCCESS,
          "Failed set Chinese key");
    check_key_value(h, h1, key0, val0, strlen(val0));
    h1->release(h, NULL, i);
    // Traditional Chinese "Couchbase"
    static const char key1[] = "沙發數據庫";
    static const char val1[] = "some Traditional Chinese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key1, val1, &i)) == ENGINE_SUCCESS,
          "Failed set Traditional Chinese key");
    h1->release(h, NULL, i);
    // Korean "couch potato"
    static const char key2[] = "쇼파감자";
    static const char val2[] = "some Korean value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key2, val2, &i)) == ENGINE_SUCCESS,
          "Failed set Korean key");
    h1->release(h, NULL, i);
    // Russian "couch potato"
    static const char key3[] = "лодырь, лентяй";
    static const char val3[] = "some Russian value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key3, val3, &i)) == ENGINE_SUCCESS,
          "Failed set Russian key");
    h1->release(h, NULL, i);
    // Japanese "couch potato"
    static const char key4[] = "カウチポテト";
    static const char val4[] = "some Japanese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key4, val4, &i)) == ENGINE_SUCCESS,
          "Failed set Japanese key");
    h1->release(h, NULL, i);
    // Indian char key, and no idea what it is
    static const char key5[] = "हरियानवी";
    static const char val5[] = "some Indian value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key5, val5, &i)) == ENGINE_SUCCESS,
          "Failed set Indian key");
    h1->release(h, NULL, i);
    // Portuguese translation "couch potato"
    static const char key6[] = "sedentário";
    static const char val6[] = "some Portuguese value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key6, val6, &i)) == ENGINE_SUCCESS,
          "Failed set Portuguese key");
    h1->release(h, NULL, i);
    // Arabic translation "couch potato"
    static const char key7[] = "الحافلةالبطاطة";
    static const char val7[] = "some Arabic value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key7, val7, &i)) == ENGINE_SUCCESS,
          "Failed set Arabic key");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    check_key_value(h, h1, key0, val0, strlen(val0));
    check_key_value(h, h1, key1, val1, strlen(val1));
    check_key_value(h, h1, key2, val2, strlen(val2));
    check_key_value(h, h1, key3, val3, strlen(val3));
    check_key_value(h, h1, key4, val4, strlen(val4));
    check_key_value(h, h1, key5, val5, strlen(val5));
    check_key_value(h, h1, key6, val6, strlen(val6));
    check_key_value(h, h1, key7, val7, strlen(val7));
    return SUCCESS;
}

static enum test_result test_binKeys(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    ENGINE_ERROR_CODE ret;

    // binary key with char values beyond 0x7F
    static const char key0[] = "\xe0\xed\xf1\x6f\x7f\xf8\xfa";
    static const char val0[] = "some value val8";
    check((ret = store(h, h1, NULL, OPERATION_SET, key0, val0, &i)) == ENGINE_SUCCESS,
          "Failed set binary key0");
    check_key_value(h, h1, key0, val0, strlen(val0));
    h1->release(h, NULL, i);
    // binary keys with char values beyond 0x7F
    static const char key1[] = "\xf1\xfd\xfe\xff\xf0\xf8\xef";
    static const char val1[] = "some value val9";
    check((ret = store(h, h1, NULL, OPERATION_SET, key1, val1, &i)) == ENGINE_SUCCESS,
          "Failed set binary key1");
    check_key_value(h, h1, key1, val1, strlen(val1));
    h1->release(h, NULL, i);
    // binary keys with special utf-8 BOM (Byte Order Mark) values 0xBB 0xBF 0xEF
    static const char key2[] = "\xff\xfe\xbb\xbf\xef";
    static const char val2[] = "some utf-8 bom value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key2, val2, &i)) == ENGINE_SUCCESS,
          "Failed set binary utf-8 bom key");
    check_key_value(h, h1, key2, val2, strlen(val2));
    h1->release(h, NULL, i);
    // binary keys with special utf-16BE BOM values "U+FEFF"
    static const char key3[] = "U+\xfe\xff\xefU+\xff\xfe";
    static const char val3[] = "some utf-16 bom value";
    check((ret = store(h, h1, NULL, OPERATION_SET, key3, val3, &i)) == ENGINE_SUCCESS,
          "Failed set binary utf-16 bom key");
    check_key_value(h, h1, key3, val3, strlen(val3));
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    check_key_value(h, h1, key0, val0, strlen(val0));
    check_key_value(h, h1, key1, val1, strlen(val1));
    check_key_value(h, h1, key2, val2, strlen(val2));
    check_key_value(h, h1, key3, val3, strlen(val3));
    return SUCCESS;
}

static enum test_result test_mb4898(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    for (int j = 0; j < 10; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);

    std::stringstream cfg;
    cfg << testHarness.get_current_testcase()->cfg << ";klog_path=/tmp/mutation.log";
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              cfg.str().c_str(), // Enable a mutation log
                              true, false);
    assert(get_int_stat(h, h1, "curr_items") == 10);
    check(get_int_stat(h, h1, "count_new", "klog") == 10,
          "Number of new log entries should be 10");

    return SUCCESS;
}


static enum test_result test_restart_bin_val(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {



    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_get_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    item *i = NULL;
    check(ENGINE_EWOULDBLOCK == h1->get(h, cookie, &i, "key", strlen("key"), 1),
          "Expected woodblock.");
    h1->release(h, NULL, i);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_incr_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_EWOULDBLOCK,
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_incr_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
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
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_expiry(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(5);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    int expired_access = get_int_stat(h, h1, "ep_expired_access");
    int expired_pager = get_int_stat(h, h1, "ep_expired_pager");
    int active_expired = get_int_stat(h, h1, "vb_active_expired");
    check(expired_pager == 0, "Expected zero expired item by pager");
    check(expired_access == 1, "Expected an expired item on access");
    check(active_expired == 1, "Expected an expired active item");
    checkeq(ENGINE_SUCCESS, store(h, h1, NULL, OPERATION_SET, key, data, &it),
            "Failed set.");
    h1->release(h, NULL, it);

    std::stringstream ss;
    ss << "curr_items stat should be still 1 after ";
    ss << "overwriting the key that was expired, but not purged yet";
    checkeq(1, get_int_stat(h, h1, "curr_items"), ss.str().c_str());

    return SUCCESS;
}

static enum test_result test_expiry_loader(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_loader";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    // Restart the engine to ensure the above expired item is not loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    assert(0 == get_int_stat(h, h1, "ep_warmup_value_count", "warmup"));

    return SUCCESS;
}

static enum test_result test_expiry_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_flush";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    // Expiry time set to 2 seconds from now
    // ep_engine has 3 seconds of expiry window which means that
    // an item expired in 3 seconds won't be persisted
    rv = h1->allocate(h, NULL, &it, key, strlen(key), 10, 0, 2);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    int item_flush_expired = get_int_stat(h, h1, "ep_item_flush_expired");
    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");

    h1->release(h, NULL, it);

    wait_for_stat_change(h, h1, "ep_item_flush_expired", item_flush_expired);

    return SUCCESS;
}

static enum test_result test_bug3454(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_duplicate_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 5);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Advance the ep_engine time by 10 sec for the above item to be expired.
    testHarness.time_travel(10);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    cas = 0;
    // Add a new item with the same key.
    rv = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(rv == ENGINE_SUCCESS, "Add failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_SUCCESS,
          "Item shouldn't expire");
    h1->release(h, NULL, it);

    // Restart the engine to ensure the above unexpired new item is loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    assert(1 == get_int_stat(h, h1, "ep_warmup_value_count", "warmup"));
    assert(0 == get_int_stat(h, h1, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static enum test_result test_bug3522(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_no_items_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Add a new item with the same key and 2 sec of expiration.
    const char *new_data = "new data here.";
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(new_data), 0, 2);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, new_data, strlen(new_data));

    cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, new_data, strlen(new_data));
    h1->release(h, NULL, it);
    // As the expiry window is 3 sec by default, the flusher won't persist the new item, but will
    // delete the old item from database before the shutdown.

    // Restart the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    assert(0 == get_int_stat(h, h1, "ep_warmup_value_count", "warmup"));
    assert(0 == get_int_stat(h, h1, "ep_warmup_dups", "warmup"));

    return SUCCESS;
}

static protocol_binary_request_header *
           prepare_get_replica(ENGINE_HANDLE *h,
                               ENGINE_HANDLE_V1 *h1,
                               vbucket_state_t state,
                               bool makeinvalidkey = false) {
    const char *key = "k0";
    size_t key_size = strlen(key);

    size_t hdr_size = sizeof(protocol_binary_request_no_extras);
    char *pkt_raw = static_cast<char*>(calloc(1, strlen(key) + hdr_size));

    uint16_t id = 0;
    uint8_t  extlen = 8;

    protocol_binary_request_no_extras *gr;
    protocol_binary_request_header *pkt;

    memcpy(pkt_raw + hdr_size, key, key_size);
    gr = (protocol_binary_request_no_extras *)pkt_raw;
    gr->message.header.request.opcode = CMD_GET_REPLICA;
    gr->message.header.request.extlen = extlen;
    gr->message.header.request.bodylen = htonl(key_size + extlen);
    gr->message.header.request.keylen = htons(key_size);
    gr->message.header.request.vbucket = htons(id);

    pkt = &gr->message.header;

    if (!makeinvalidkey) {
        item *i = NULL;
        check(store(h, h1, NULL, OPERATION_SET, key, "replicadata", &i, 0, id)
              == ENGINE_SUCCESS, "Get Replica Failed");
        h1->release(h, NULL, i);

        check(set_vbucket_state(h, h1, id, state),
              "Failed to set vbucket active state, Get Replica Failed");
    }

    return pkt;
}

static enum test_result test_get_replica_active_state(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_active);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");

    return SUCCESS;
}

static enum test_result test_get_replica_pending_state(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;

    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    pkt = prepare_get_replica(h, h1, vbucket_state_pending);
    check(h1->unknown_command(h, cookie, pkt, add_response) ==
          ENGINE_EWOULDBLOCK, "Should have returned error for pending state");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_get_replica_dead_state(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_dead);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");

    return SUCCESS;
}

static enum test_result test_get_replica(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
                              "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected PROTOCOL_BINARY_RESPONSE_SUCCESS response.");
    check(strcmp("replicadata", last_body) == 0,
                 "Should have returned identical value");

    return SUCCESS;
}

static enum test_result test_get_replica_invalid_key(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt;
    bool makeinvalidkey = true;
    pkt = prepare_get_replica(h, h1, vbucket_state_replica, makeinvalidkey);
    check(h1->unknown_command(h, NULL, pkt, add_response) ==
          ENGINE_SUCCESS, "Get Replica Failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET response.");
    return SUCCESS;
}

static enum test_result test_vb_del_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(ENGINE_EWOULDBLOCK == h1->remove(h, cookie, "key", 3, 0, 1),
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_touch *req = reinterpret_cast<protocol_binary_request_touch *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TOUCH;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(time(NULL) + 10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to touch an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch mykey");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_gat *req = reinterpret_cast<protocol_binary_request_gat *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GAT;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to touch an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");

    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(memcmp(last_body, "somevalue", sizeof("somevalue")) == 0,
          "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gatq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_gat *req = reinterpret_cast<protocol_binary_request_gat *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GATQ;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to gat an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    last_status = static_cast<protocol_binary_response_status>(0xffff);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");

    // We should not have sent any response!
    check(last_status == 0xffff, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(memcmp(last_body, "somevalue", sizeof("somevalue")) == 0,
          "Invalid data returned");
    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_mb5215(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "coolkey", "cooler", &itm)
          == ENGINE_SUCCESS, "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "coolkey", "cooler", strlen("cooler"))
          == SUCCESS, "Failed to retrieve data");

    // set new exptime to 111
    int expTime = time(NULL) + 111;
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_touch *req =
        reinterpret_cast<protocol_binary_request_touch *>(buffer);
    protocol_binary_request_header *request =
        reinterpret_cast<protocol_binary_request_header*>(req);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TOUCH;
    req->message.header.request.vbucket = 0;
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(7);
    req->message.header.request.bodylen = htonl(17);
    req->message.body.expiration = ntohl(expTime);
    memcpy(buffer + sizeof(req->bytes), "coolkey", 7);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch coolkey");

    //reload engine
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    //verify persisted expiration time
    const char *statkey = "key coolkey 0";
    int newExpTime;
    check(h1->get(h, NULL, &itm, "coolkey", 7, 0) == ENGINE_SUCCESS,
          "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    check(newExpTime == expTime, "Failed to persist new exptime");

    // evict key, touch expiration time, and verify
    evict_key(h, h1, "coolkey", 0, "Ejected.");

    expTime = time(NULL) + 222;
    req->message.body.expiration = ntohl(expTime);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch coolkey");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    check(h1->get(h, NULL, &itm, "coolkey", 7, 0) == ENGINE_SUCCESS,
          "Missing key");
    h1->release(h, NULL, itm);
    newExpTime = get_int_stat(h, h1, "key_exptime", statkey);
    check(newExpTime == expTime, "Failed to persist new exptime");

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

static enum test_result test_whitespace_db(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals["ep_dbname"] != std::string(WHITESPACE_DB)) {
        std::cerr << "Expected dbname = ``" << WHITESPACE_DB << "''"
                  << ", got ``" << vals["ep_dbname"] << "''" << std::endl;
        return FAIL;
    }

    check(access(WHITESPACE_DB, F_OK) != -1, "I expected the whitespace db to exist");
    return SUCCESS;
}

static enum test_result test_db_shards(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_dbname") != vals.end(), "Found no db name");
    std::string db_name = vals["ep_dbname"];
    int dbShards = get_int_stat(h, h1, "ep_db_shards");
    check(dbShards == 5, "Expected five shards for db store");

    check(remove(db_name.c_str()) == 0,
          "Error removing db file.");
    for (int i = 0; i < dbShards; ++i) {
        std::stringstream shard_name;
        shard_name << db_name << "-" << i << ".sqlite";
        std::string s_name = shard_name.str();
        std::string error_msg("Error removing ");
        error_msg.append(s_name);
        check(remove(s_name.c_str()) == 0, error_msg.c_str());
    }

    return SUCCESS;
}

static enum test_result test_single_db_strategy(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_dbname") != vals.end(), "Found no db name");
    check(vals.find("ep_db_strategy") != vals.end(), "Found no db strategy");
    std::string db_strategy = vals["ep_db_strategy"];
    assert(strcmp(db_strategy.c_str(), "singleDB") == 0);

    wait_for_persisted_value(h, h1, "key", "somevalue");
    evict_key(h, h1, "key", 0, "Ejected.");
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_memory_limit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int used = get_int_stat(h, h1, "mem_used");
    int max = static_cast<int>(get_int_stat(h, h1, "ep_max_data_size") * 0.9);
    check(get_int_stat(h, h1, "ep_oom_errors") == 0 &&
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 0, "Expected no OOM errors.");
    assert(used < max);

    char data[8192];
    memset(data, 'x', sizeof(data));
    size_t vlen = max - used - 192;
    data[vlen] = 0x00;

    item *i = NULL;
    // So if we add an item,
    check(store(h, h1, NULL, OPERATION_SET, "key", data, &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", data, vlen);
    h1->release(h, NULL, i);

    // There should be no room for another.
    ENGINE_ERROR_CODE second = store(h, h1, NULL, OPERATION_SET, "key2", data, &i);
    check(second == ENGINE_ENOMEM || second == ENGINE_TMPFAIL,
          "should have failed second set");
    h1->release(h, NULL, i);
    check(get_int_stat(h, h1, "ep_oom_errors") == 1 ||
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 1, "Expected an OOM error.");

    ENGINE_ERROR_CODE overwrite = store(h, h1, NULL, OPERATION_SET, "key", data, &i);
    check(overwrite == ENGINE_ENOMEM || overwrite == ENGINE_TMPFAIL,
          "should have failed second override");
    h1->release(h, NULL, i);
    check(get_int_stat(h, h1, "ep_oom_errors") == 2 ||
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 2, "Expected another OOM error.");
    check_key_value(h, h1, "key", data, vlen);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Until we remove that item
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue2", &i) == ENGINE_SUCCESS,
          "should have succeded on the last set");
    check_key_value(h, h1, "key2", "somevalue2", 10);
    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_missing(h, h1, 1) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_state(h, h1, 0, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!verify_vbucket_missing(h, h1, 1)) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, h1, 1, vbucket_state_active)) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    return verify_vbucket_state(h, h1, 1, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                             const char* value = NULL) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    protocol_binary_request_header *pkt =
        createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1, NULL, value);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");

    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 2, NULL, value);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected failure deleting non-existent bucket.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1, NULL, value);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 0 was not missing after deleting it.");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_stats(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    int mem_used = get_int_stat(h, h1, "mem_used");
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    int vbucketDel = get_int_stat(h, h1, "ep_vbucket_del");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    wait_for_stat_change(h, h1, "ep_vbucket_del", vbucketDel);

    int mem_used2 = get_int_stat(h, h1, "mem_used");
    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead2 = get_int_stat(h, h1, "ep_overhead");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    check(mem_used2 == mem_used, "memory should be the same");
    check(cacheSize2 == cacheSize, "cache size should be the same");
    check(overhead2 == overhead, "overhead should be the same");
    check(nonResident2 == nonResident, "non resident count should be the same");

    return SUCCESS;
}

static enum test_result vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                const char* value = NULL) {
    protocol_binary_request_header *pkt =
        createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1, NULL, value);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    // Store a value so the restart will try to resurrect it.
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 1)
          == ENGINE_SUCCESS, "Failed to set a value");
    check_key_value(h, h1, "key", "somevalue", 9, 1);
    h1->release(h, NULL, i);

    // Reload to get a flush forced.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    check(verify_vbucket_state(h, h1, 1, vbucket_state_dead),
          "Bucket state was not dead after restart.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");
    check_key_value(h, h1, "key", "somevalue", 9, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    if (verify_vbucket_state(h, h1, 1, vbucket_state_pending, true)) {
        std::cerr << "Bucket came up in pending state after delete." << std::endl;
        abort();
    }

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after restart.");

    return SUCCESS;
}

static enum test_result test_async_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy(h, h1);
}

static enum test_result test_sync_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy(h, h1, "async=0");
}

static enum test_result test_async_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy_restart(h, h1);
}

static enum test_result test_sync_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return vbucket_destroy_restart(h, h1, "async=0");
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
    char eng_specific[3];
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
        check(check_key_value(h, h1, "key", data, i) == SUCCESS,
              ss.str().c_str());
        free(data);
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    for (size_t i = 1; i < 10; ++i) {
        std::stringstream ss;
        ss << i;
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 0,
                             ss.str().c_str(), ss.str().length(), 1) == ENGINE_SUCCESS,
              "Failed tap notify.");
        check(h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 0,
                             ss.str().c_str(), ss.str().length(), 1) == ENGINE_SUCCESS,
              "Failed tap notify.");
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
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    char eng_specific[1];
    check(h1->tap_notify(h, NULL, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    char eng_specific[1];
    check(h1->tap_notify(h, NULL, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, 0, 0,
                         "data", 4, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->tap_notify(h, NULL, NULL, 0,
                         1, 0, TAP_DELETION, 0, "key", 3, 0, 0, 0,
                         0, 0, 0) == ENGINE_SUCCESS,
          "Failed tap notify.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->tap_notify(h, NULL, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         NULL, 0, 1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(h1->tap_notify(h, NULL, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         NULL, 0, 1) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    check(h1->tap_notify(h, NULL, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, 0, 0,
                         NULL, 0, 1) == ENGINE_SUCCESS,
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

static const void* createTapConn(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *name) {
    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name,
                                             strlen(name),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");
    return cookie;
}

static enum test_result test_tap_agg_stats(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    std::vector<const void*> cookies;

    cookies.push_back(createTapConn(h, h1, "replica_a"));
    cookies.push_back(createTapConn(h, h1, "replica_b"));
    cookies.push_back(createTapConn(h, h1, "replica_c"));
    cookies.push_back(createTapConn(h, h1, "rebalance_a"));
    cookies.push_back(createTapConn(h, h1, "userconnn"));

    check(get_int_stat(h, h1, "replica:count", "tapagg _") == 3,
          "Incorrect replica count on tap agg");
    check(get_int_stat(h, h1, "rebalance:count", "tapagg _") == 1,
          "Incorrect rebalance count on tap agg");
    check(get_int_stat(h, h1, "_total:count", "tapagg _") == 5,
          "Incorrect total count on tap agg");

    std::for_each(cookies.begin(), cookies.end(),
                  std::ptr_fun((UNLOCK_COOKIE_T)testHarness.unlock_cookie));

    return SUCCESS;
}

static enum test_result test_tap_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + num_keys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
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
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            testHarness.unlock_cookie(cookie);
            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_sends_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 5;
    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }
    wait_for_flusher_to_settle(h, h1);

    for (int ii = 0; ii < num_keys - 2; ++ii) {
        std::stringstream ss;
        ss << "key" << ii;
        checkeq(ENGINE_SUCCESS, h1->remove(h, NULL, ss.str().c_str(),
                ss.str().length(), 0, 0), "Delete failed");
    }
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    int num_mutations = 0;
    int num_deletes = 0;

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
        case TAP_DISCONNECT:
            break;
        case TAP_MUTATION:
            num_mutations++;
            break;
        case TAP_DELETION:
            num_deletes++;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    check(num_mutations == 2, "Incorrect number of remaining mutations");
    check(num_deletes == (num_keys - 2), "Incorrect number of deletes");

    testHarness.unlock_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_tap_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    int initializedKeys = 0;

    memset(keys, 0, sizeof(keys));

    for (; initializedKeys < num_keys / 2; ++initializedKeys) {
        keys[initializedKeys] = false;
        std::stringstream ss;
        ss << initializedKeys;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + initializedKeys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < initializedKeys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    uint16_t vbucketfilter[2];
    vbucketfilter[0] = ntohs(1);
    vbucketfilter[1] = ntohs(0);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS |
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;
    bool allows_more_mutations(true);

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (initializedKeys < num_keys) {
            keys[initializedKeys] = false;
            std::stringstream ss;
            ss << initializedKeys;
            check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                        "value", NULL, 0, 0) == ENGINE_SUCCESS,
                  "Failed to store an item.");
            ++initializedKeys;
        }

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            // This will be false if we've seen a vbucket set state.
            assert(allows_more_mutations);

            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            break;
        case TAP_DISCONNECT:
            break;
        case TAP_VBUCKET_SET:
            assert(nengine_specific == 4);
            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine_specific);
            state = static_cast<vbucket_state_t>(ntohl(state));
            if (state == vbucket_state_active) {
                allows_more_mutations = false;
            }
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_filter_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (uint16_t vbid = 0; vbid < 4; ++vbid) {
        check(set_vbucket_state(h, h1, vbid, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    const int num_keys = 40;
    bool keys[num_keys];
    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, ii % 4) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[3];
    vbucketfilter[0] = htons(0);
    vbucketfilter[1] = htons(1);
    vbucketfilter[2] = htons(2);
    uint64_t checkpointIds[3];
    checkpointIds[0] = htonll(0);
    checkpointIds[1] = htonll(0);
    checkpointIds[2] = htonll(0);

    uint16_t numOfVBs = htons(2); // Start with vbuckets 0 and 1
    char *userdata = static_cast<char*>(calloc(1, 28));
    char *ptr = userdata;
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids
        memcpy(ptr, &vbucketfilter[i], sizeof(uint16_t));
        ptr += sizeof(uint16_t);
    }
    memcpy(ptr, &numOfVBs, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    for (int i = 0; i < 2; ++i) { // vbucket ids and their checkpoint ids
        memcpy(ptr, &vbucketfilter[i], sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, &checkpointIds[i], sizeof(uint64_t));
        ptr += sizeof(uint64_t);
    }

    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT,
                                             static_cast<void*>(userdata),
                                             28); // userdata length
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

    uint16_t unlikely_vbucket_identifier = 17293;
    std::string key;
    bool done = false;
    bool filter_change_done = false;
    uint16_t vbid;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            done = true;
            // Check if all the items except for vbucket 3 are received
            for (int ii = 0; ii < num_keys; ++ii) {
                if ((ii % 4) != 3 && !keys[ii]) {
                    done = false;
                    break;
                }
            }
            if (!done) {
                testHarness.waitfor_cookie(cookie);
            }
            break;
        case TAP_NOOP:
            break;
        case TAP_OPAQUE:
            if (nengine_specific == sizeof(uint32_t)) {
                uint32_t opaque_code;
                memcpy(&opaque_code, engine_specific, sizeof(opaque_code));
                opaque_code = ntohl(opaque_code);
                if (opaque_code == TAP_OPAQUE_COMPLETE_VB_FILTER_CHANGE) {
                    filter_change_done = true;
                }
            }
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
            break;

        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out the key");
            vbid = atoi(key.c_str()) % 4;
            check(vbid == vbucket, "Incorrect vbucket id");
            check(vbid != 3,
                  "Received an item for a vbucket we don't subscribe to");
            keys[atoi(key.c_str())] = true;
            ++found;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            // We've got some of the elements.. Let's change the filter
            // and get the rest
            if (found == 10) {
                size_t header_size = sizeof(protocol_binary_request_no_extras);
                char *pkt_raw = static_cast<char*>(calloc(1, header_size + name.length() + 32));
                protocol_binary_request_no_extras *req;
                req = (protocol_binary_request_no_extras *)pkt_raw;

                numOfVBs = htons(3); // vbuckets 0, 1, 2
                pkt_raw += header_size;
                memcpy(pkt_raw, name.c_str(), name.length());
                pkt_raw += name.length();
                memcpy(pkt_raw, &numOfVBs, sizeof(uint16_t));
                pkt_raw += sizeof(uint16_t);
                for (int i = 0; i < 3; ++i) {
                    memcpy(pkt_raw, &vbucketfilter[i], sizeof(uint16_t));
                    pkt_raw += sizeof(uint16_t);
                    memcpy(pkt_raw, &checkpointIds[i], sizeof(uint64_t));
                    pkt_raw += sizeof(uint64_t);
                }

                req->message.header.request.opcode = CMD_CHANGE_VB_FILTER;
                req->message.header.request.bodylen = htonl(name.length() + 32);
                req->message.header.request.keylen = htons(name.length());

                protocol_binary_request_header *pkt;
                pkt = reinterpret_cast<protocol_binary_request_header*>(req);
                check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
                      "Failed to change the TAP VB filter.");
                check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      "Expected success response from changing the TAP VB filter.");
                free((char *)req);
            }
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    testHarness.unlock_cookie(cookie);

    assert(filter_change_done);
    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:qlen", "tap") == 0,
          "queue should be empty");
    free(userdata);

    return SUCCESS;
}

static enum test_result test_tap_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");
    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "0.05") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "2") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect grace period value");
    return SUCCESS;
}

static enum test_result test_tap_default_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");

    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "5") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "1000") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "300") == 0, "Incorrect grace period value");

    return SUCCESS;
}

static enum test_result test_tap_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    bool receivedKeys[nkeys];
    bool nackKeys[nkeys];

    for (int i = 0; i < nkeys; ++i) {
        nackKeys[i] = true;
        receivedKeys[i] = false;
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_value(h, h1, ss.str().c_str(), &info), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int index;
    int numRollbacks = 1000;

    do {
        if (numRollbacks > 0) {
            if (random() % 4 == 0) {
                iter = NULL;
            }
        }

        if (iter == NULL) {
            testHarness.unlock_cookie(cookie);
            iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                        name.length(),
                                        TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                        TAP_CONNECT_CHECKPOINT |
                                        TAP_CONNECT_SUPPORT_ACK |
                                        TAP_CONNECT_FLAG_DUMP,
                                        static_cast<void*>(vbucketfilter),
                                        4);
            check(iter != NULL, "Failed to create a tap iterator");
            testHarness.lock_cookie(cookie);
        }

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
            if (numRollbacks > 0) {
                if (random() % 4 == 0) {
                    iter = NULL;
                }
            }
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, NULL, 0,
                           0, 0, 0, NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out key");
            index = atoi(key.c_str());
            check(index >= 0 && index <= nkeys, "Illegal key returned");

            testHarness.unlock_cookie(cookie);
            if (nackKeys[index]) {
                nackKeys[index] = false;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, NULL, 0, 0);
            } else {
                receivedKeys[index] = true;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, NULL, 0, 0);
            }
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);

            break;
        case TAP_CHECKPOINT_START:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_CHECKPOINT_END:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, NULL, 0, 0);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    for (int ii = 0; ii < nkeys; ++ii) {
        check(receivedKeys[ii], "Did not receive all of the keys");
    }

    testHarness.unlock_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_tap_implicit_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_value(h, h1, ss.str().c_str(), &info), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int mutations = 0;
    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            }
            if (seqno == static_cast<uint32_t>(4294967294UL)) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < static_cast<uint32_t>(4294967295UL));

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            }
            if (seqno == 1) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < 1);

    /* Now just get the rest */
    do {

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);
        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            } else if (event == TAP_DISCONNECT) {
                done = true;
            }
            if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, NULL, 0, 0);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (!done);
    testHarness.unlock_cookie(cookie);
    check(mutations == 11, "Expected 11 mutations to be returned");
    return SUCCESS;
}

static enum test_result test_set_tap_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    set_param(h, h1, engine_param_tap, "tap_keepalive", "600");
    check(get_int_stat(h, h1, "ep_tap_keepalive") == 600,
          "Incorrect tap_keepalive value.");
    set_param(h, h1, engine_param_tap, "tap_keepalive", "5000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected an invalid value error due to exceeding a max value allowed");
    return SUCCESS;
}

static enum test_result test_tap_noop_config_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 200,
          "Expected tap_noop_interval == 200");
    return SUCCESS;
}

static enum test_result test_tap_noop_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 10,
          "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_notify(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    int ii = 0;
    char buffer[1024];
    ENGINE_ERROR_CODE r;

    const void *cookie = testHarness.create_cookie();
    do {
        std::stringstream ss;
        ss << "Key-"<< ++ii;
        std::string key = ss.str();

        r = h1->tap_notify(h, cookie, NULL, 0, 1, 0, TAP_MUTATION, 0,
                           key.c_str(), key.length(), 0, 0, 0, buffer, 1024, 0);
    } while (r == ENGINE_SUCCESS);
    check(r == ENGINE_TMPFAIL, "non-acking streams should etmpfail");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_checkpoint_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    for (int i = 0; i < 5001; i++) {
        char key[8];
        sprintf(key, "key%d", i);
        check(store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0)
                    == ENGINE_SUCCESS, "Failed to store an item.");
        h1->release(h, NULL, itm);
    }
    check(get_int_stat(h, h1, "vb_0:open_checkpoint_id", "checkpoint") == 2,
          "New checkpoint wasn't create after 5001 item creates");
    check(get_int_stat(h, h1, "vb_0:num_open_checkpoint_items", "checkpoint") == 1,
          "New open checkpoint should has only one dirty item");
    return SUCCESS;
}

static enum test_result test_checkpoint_timeout(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    check(store(h, h1, NULL, OPERATION_SET, "key", "value", &itm, 0, 0)
                == ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, itm);
    testHarness.time_travel(600);
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 2, "checkpoint");
    return SUCCESS;
}

static enum test_result test_checkpoint_deduplication(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item* itm;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 4500; j++) {
            char key[8];
            sprintf(key, "key%d", j);
            check(store(h, h1, NULL, OPERATION_SET, key, "value", &itm, 0, 0)
                        == ENGINE_SUCCESS, "Failed to store an item.");
            h1->release(h, NULL, itm);
        }
    }
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoint_items", 4501, "checkpoint");
    return SUCCESS;
}

static enum test_result test_novb0(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_vbucket_missing(h, h1, 0), "vb0 existed and shouldn't have.");
    return SUCCESS;
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
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
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    wait_for_persisted_value(h, h1, "key", value);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);
    int mem_used = get_int_stat(h, h1, "mem_used");
    int cache_size = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int value_size = get_int_stat(h, h1, "ep_value_size");
    check((mem_used - overhead) > cache_size,
          "ep_kv_size should be greater than the hashtable cache size due to the checkpoint overhead");
    evict_key(h, h1, "key", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_total_cache_size") <= cache_size,
          "Evict a value shouldn't increase the total cache size");
    check(get_int_stat(h, h1, "mem_used") < mem_used,
          "Expected mem_used to decrease when an item is evicted");

    check_key_value(h, h1, "key", value, strlen(value), 0); // Load an item from disk again.

    check(get_int_stat(h, h1, "mem_used") == mem_used,
          "Expected mem_used to remain the same after an item is loaded from disk");
    check(get_int_stat(h, h1, "ep_value_size") == value_size,
          "Expected ep_value_size to remain the same after item is loaded from disk");

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

    check_key_value(h, h1, "a", "b\r\n", 3, 0);

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
    testHarness.time_travel(43);
    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    checkeq(1, get_int_stat(h, h1, "paged_out_time_32,64", "timings"),
            "Expected one sample from 32s to 64s.");

    checkeq(1, get_int_stat(h, h1, "ep_bg_num_samples"),
            "Expected one sample");

    check(vals.find("ep_bg_min_wait") != vals.end(), "Found no ep_bg_min_wait.");
    check(vals.find("ep_bg_max_wait") != vals.end(), "Found no ep_bg_max_wait.");
    check(vals.find("ep_bg_wait_avg") != vals.end(), "Found no ep_bg_wait_avg.");
    check(vals.find("ep_bg_min_load") != vals.end(), "Found no ep_bg_min_load.");
    check(vals.find("ep_bg_max_load") != vals.end(), "Found no ep_bg_max_load.");
    check(vals.find("ep_bg_load_avg") != vals.end(), "Found no ep_bg_load_avg.");

    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, 0);
    check(get_int_stat(h, h1, "ep_bg_num_samples") == 2,
          "Expected one sample");

    return SUCCESS;
}

static enum test_result test_key_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to store an item.");
    h1->release(h, NULL, i);
    // set (k2,v2) in vbucket 1
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to store an item.");
    h1->release(h, NULL, i);

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "key k2 1";
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vkey_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed set vbucket 2 state.");
    check(set_vbucket_state(h, h1, 3, vbucket_state_active), "Failed set vbucket 3 state.");
    check(set_vbucket_state(h, h1, 4, vbucket_state_active), "Failed set vbucket 4 state.");

    wait_for_persisted_value(h, h1, "k1", "v1");
    wait_for_persisted_value(h, h1, "k2", "v2", 1);
    wait_for_persisted_value(h, h1, "k3", "v3", 2);
    wait_for_persisted_value(h, h1, "k4", "v4", 3);
    wait_for_persisted_value(h, h1, "k5", "v5", 4);

    check(set_vbucket_state(h, h1, 2, vbucket_state_replica), "Failed to set VB2 state.");
    check(set_vbucket_state(h, h1, 3, vbucket_state_pending), "Failed to set VB3 state.");
    check(set_vbucket_state(h, h1, 4, vbucket_state_dead), "Failed to set VB4 state.");

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "vkey k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "vkey k2 1";
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k3" and vbucket "2"
    const char *statkey3 = "vkey k3 2";
    check(h1->get_stats(h, cookie, statkey3, strlen(statkey3), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
          "Found no key_last_modification_time");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k4" and vbucket "3"
    const char *statkey4 = "vkey k4 3";
    check(h1->get_stats(h, cookie, statkey4, strlen(statkey4), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
          "Found no key_last_modification_time");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k5" and vbucket "4"
    const char *statkey5 = "vkey k5 4";
    check(h1->get_stats(h, cookie, statkey5, strlen(statkey5), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
          "Found no key_last_modification_time");
    check(vals.find("key_vb_state") != vals.end(), "Found no key_vb_state");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_warmup_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed to set VB0 state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set VB1 state.");

    for (int i = 0; i < 5000; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it),
              "Error setting.");
        h1->release(h, NULL, it);
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    check(vals.find("ep_warmup_thread") != vals.end(), "Found no ep_warmup_thread");
    check(vals.find("ep_warmup_value_count") != vals.end(), "Found no ep_warmup_value_count");
    check(vals.find("ep_warmup_key_count") != vals.end(), "Found no ep_warmup_key_count");
    check(vals.find("ep_warmup_dups") != vals.end(), "Found no ep_warmup_dups");
    check(vals.find("ep_warmup_oom") != vals.end(), "Found no ep_warmup_oom");
    check(vals.find("ep_warmup_time") != vals.end(), "Found no ep_warmup_time");
    std::string warmup_time = vals["ep_warmup_time"];
    assert(atoi(warmup_time.c_str()) > 0);

    vals.clear();
    check(h1->get_stats(h, NULL, "prev-vbucket", 12, add_stats) == ENGINE_SUCCESS,
          "Failed to get the previous state of vbuckets");
    check(vals.find("vb_0") != vals.end(), "Found no previous state for VB0");
    check(vals.find("vb_1") != vals.end(), "Found no previous state for VB1");
    std::string vb0_prev_state = vals["vb_0"];
    std::string vb1_prev_state = vals["vb_1"];
    assert(strcmp(vb0_prev_state.c_str(), "active") == 0);
    assert(strcmp(vb1_prev_state.c_str(), "replica") == 0);

    return SUCCESS;
}

static enum test_result test_cbd_225(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    time_t token1 = 0;
    time_t token2 = 0;
    time_t token3 = 0;

    // get engine startup token
    token1 = get_int_stat(h, h1, "ep_startup_time");
    check(token1 != 0, "Expected non-zero startup token");

    // store some random data
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    // check token again, which should be the same as before
    token2 = get_int_stat(h, h1, "ep_startup_time");
    check(token2 == token1, "Expected the same startup token");

    // reload the engine
    testHarness.time_travel(10);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    // check token, this time we should get a different one
    token3 = get_int_stat(h, h1, "ep_startup_time");
    check(token3 != token1, "Expected a different startup token");

    return SUCCESS;
}

static enum test_result test_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // Verify initial case.
    verify_curr_items(h, h1, 0, "init");

    // Verify set and add case
    check(store(h, h1, NULL, OPERATION_ADD,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 3, "three items stored");
    assert(3 == get_int_stat(h, h1, "ep_total_enqueued"));

    wait_for_flusher_to_settle(h, h1);

    // Verify delete case.
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    verify_curr_items(h, h1, 3, "one item deleted - not persisted");

    wait_for_stat_change(h, h1, "curr_items", 3);
    verify_curr_items(h, h1, 2, "one item deleted - persisted");

    // Verify flush case (remove the two remaining from above)
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    verify_curr_items(h, h1, 0, "flush");

    // Verify dead vbucket case.
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 0, "dead vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 with a dead vbucket");

    // Then resurrect.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 3, "resurrected vbucket");

    // Now completely delete it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 0);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success deleting vbucket.");
    verify_curr_items(h, h1, 0, "del vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 after deleting a vbucket");

    return SUCCESS;
}

static enum test_result test_cbd_226(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    size_t num_completed_flush = 0;
    size_t temp1 = 0;
    size_t temp2 = 0;

    num_completed_flush = get_int_stat(h, h1, "ep_flusher_num_completed");

    // store some random data, and flush them to disk
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    wait_for_flusher_to_settle(h, h1);

    // check the stat again
    temp1 = get_int_stat(h, h1, "ep_flusher_num_completed");
    check((temp1 - num_completed_flush) > 0, "Expected some completed flush");

    // again, store some random data, and flush them to disk
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    wait_for_flusher_to_settle(h, h1);

    // check the stat again, expect more flush
    temp2 = get_int_stat(h, h1, "ep_flusher_num_completed");
    check((temp2 - temp1) > 0, "Expected more completed flush");

    return SUCCESS;
}

static enum test_result test_value_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    item *i = NULL;
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    check(get_int_stat(h, h1, "ep_num_active_non_resident") == 0,
          "Expected all active vbucket items to be resident");


    stop_persistence(h, h1);
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k1", 0, "Can't eject: Dirty or a small object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    evict_key(h, h1, "k2", 1, "Can't eject: Dirty or a small object.", true);
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1, "k1", 0, "Ejected.");
    evict_key(h, h1, "k2", 1, "Ejected.");

    check(get_int_stat(h, h1, "vb_active_num_non_resident") == 2,
          "Expected two non-resident items for active vbuckets");

    evict_key(h, h1, "k1", 0, "Already ejected.");
    evict_key(h, h1, "k2", 1, "Already ejected.");

    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        "missing-key", "");
    pkt->request.vbucket = htons(0);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to evict key.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");

    check_key_value(h, h1, "k1", "v1", 2);
    checkeq(1, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected only one active vbucket item to be non-resident");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica), "Failed to set vbucket state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    checkeq(0, get_int_stat(h, h1, "vb_active_num_non_resident"),
            "Expected no non-resident items");

    return SUCCESS;
}

static enum test_result test_mb5172(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key-1", "value-1", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key-2", "value-2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");

    // restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    return SUCCESS;
}


static enum test_result test_mb3169(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    uint64_t cas(0);
    uint64_t result(0);
    check(store(h, h1, NULL, OPERATION_SET, "set", "value", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "incr", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "delete", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "get", "getvalue", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1, "set", 0, "Ejected.");
    evict_key(h, h1, "incr", 0, "Ejected.");
    evict_key(h, h1, "delete", 0, "Ejected.");
    evict_key(h, h1, "get", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 4,
          "Expected four items to be resident");

    check(store(h, h1, NULL, OPERATION_SET, "set", "value2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    h1->release(h, NULL, i);

    checkeq(3, get_int_stat(h, h1, "ep_num_non_resident"),
          "Expected mutation to mark item resident");

    check(h1->arithmetic(h, NULL, "incr", 4, true, false, 1, 1, 0,
                         &cas, &result,
                         0)  == ENGINE_SUCCESS, "Incr failed");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 2,
          "Expected incr to mark item resident");

    check(h1->remove(h, NULL, "delete", 6, 0, 0) == ENGINE_SUCCESS,
          "Delete failed");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected delete to remove non-resident item");

    check_key_value(h, h1, "get", "getvalue", 8);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_duplicate_items_disk(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), "value", &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");
    int vb_del_num = get_int_stat(h, h1, "ep_vbucket_del");
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_vbucket_del", vb_del_num);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");
    // Make sure that a key/value item is persisted correctly
    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, h1, it->c_str(), 1, "Ejected.");
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 1);
    }
    check(get_int_stat(h, h1, "ep_warmup_dups") == 0,
          "Expected no duplicate items from disk");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_golden(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

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

    itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") == overhead,
          "mem_used (ep_kv_size + ep_overhead) should be greater than ep_overhead");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_paged_rm(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    // Delete the value and make sure things return correctly.
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") == overhead,
          "mem_used (ep_kv_size + ep_overhead) should be greater than ep_overhead");

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

    check_key_value(h, h1, "k1", "14", 2);

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
    h1->release(h, NULL, i);

    check_key_value(h, h1, "k1", "new value", 9);

    checkeq(0, get_int_stat(h, h1, "ep_bg_fetched"), "bg fetched something");

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
        td->h1->release(td->h, NULL, i);

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

    set_param(h, h1, engine_param_flush, "bg_fetch_delay", "3");

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

    set_param(h, h1, engine_param_flush, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_incr_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    // Value is as it was before.
    check_key_value(h, h1, "k1", "13", 2);

    // Should have bg_fetched to retrieve it even with a concurrent
    // incr.  We *may* at this point have also completed the incr.
    // 1 == get only, 2 == get+incr.
    assert(get_int_stat(h, h1, "ep_bg_fetched") >= 1);

    // Give incr time to finish (it's doing another background fetch)
    wait_for_stat_change(h, h1, "ep_bg_fetched", 1);
    wait_for_stat_change(h, h1, "ep_total_enqueued", 1);

    // The incr mutated the value.
    check_key_value(h, h1, "k1", "14", 2);

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_rm_race(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_param(h, h1, engine_param_flush, "bg_fetch_delay", "3");

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

static enum test_result test_multi_dispatcher_conf(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "dispatcher", strlen("dispatcher"),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals.find("ro_dispatcher:status") == vals.end()) {
        std::cerr << "Expected ro_dispatcher to be running." << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

static enum test_result test_not_multi_dispatcher_conf(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "dispatcher", strlen("dispatcher"),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals.find("ro_dispatcher:status") != vals.end()) {
        std::cerr << "Expected ro_dispatcher to not be running." << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

static bool epsilon(int val, int target, int ep=5) {
    return abs(val - target) < ep;
}

static enum test_result test_max_size_settings(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    check(get_int_stat(h, h1, "ep_max_data_size") == 1000, "Incorrect initial size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 600),
          "Incorrect initial low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 750),
          "Incorrect initial high wat.");

    set_param(h, h1, engine_param_flush, "max_size", "1000000");

    check(get_int_stat(h, h1, "ep_max_data_size") == 1000000,
          "Incorrect new size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 600000),
          "Incorrect larger low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 750000),
          "Incorrect larger high wat.");

    set_param(h, h1, engine_param_flush, "mem_low_wat", "700000");
    set_param(h, h1, engine_param_flush, "mem_high_wat", "800000");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 700000,
          "Incorrect even larger low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 800000,
          "Incorrect even larger high wat.");

    set_param(h, h1, engine_param_flush, "max_size", "100");

    check(get_int_stat(h, h1, "ep_max_data_size") == 100,
          "Incorrect smaller size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 60),
          "Incorrect smaller low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 75),
          "Incorrect smaller high wat.");

    set_param(h, h1, engine_param_flush, "mem_low_wat", "50");
    set_param(h, h1, engine_param_flush, "mem_high_wat", "70");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 50,
          "Incorrect even smaller low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 70,
          "Incorrect even smaller high wat.");

    return SUCCESS;
}

static enum test_result test_validate_engine_handle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    (void)h;
    check(h1->get_stats_struct == NULL, "get_stats_struct member should be initialized to NULL");
    check(h1->aggregate_stats == NULL, "aggregate_stats member should be initialized to NULL");
    check(h1->unknown_command != NULL, "unknown_command member should be initialized to a non-NULL value");
    check(h1->tap_notify != NULL, "tap_notify member should be initialized to a non-NULL value");
    check(h1->get_tap_iterator != NULL, "get_tap_iterator member should be initialized to a non-NULL value");
    check(h1->errinfo == NULL, "errinfo member should be initialized to NULL");

    return SUCCESS;
}

static enum test_result test_kill9_bucket(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-0-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    // Last parameter indicates the force shutdown for the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    keys.clear();
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-1-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }

    return SUCCESS;
}

static protocol_binary_request_header* create_restore_file_packet(const char *fnm)
{
    protocol_binary_request_header *header;
    uint32_t len = strlen(fnm);
    header = (protocol_binary_request_header *)calloc(sizeof(*header), + len);
    header->request.opcode = CMD_RESTORE_FILE;
    header->request.keylen = htons((uint16_t)len);
    header->request.bodylen = htonl(len);
    memcpy(header + 1, fnm, len);
    return header;
}

static enum test_result test_restore_not_enabled(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    protocol_binary_request_header *req = create_restore_file_packet("foo");
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
          "The server should not allow restore to be initiated");
    free(req);
    return SUCCESS;
}

static void complete_restore(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    protocol_binary_request_header header;
    memset(&header, 0, sizeof(header));
    header.request.opcode = CMD_RESTORE_COMPLETE;

    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, &header, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should disable restore");
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.empty(), "restore should be disabled");
}

static enum test_result test_restore_no_such_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    protocol_binary_request_header *req = create_restore_file_packet("@@@no-such-file@@@");
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "The server should check if the file exists");
    free(req);
    complete_restore(h, h1);
    return SUCCESS;
}

static enum test_result test_restore_invalid_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/sizes");
    check(access(cwd, F_OK) == 0, "Could not find sizes");
    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie");
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") != vals.end(),
          "Expected the restore manager to set an error message");
    check(vals["ep_restore:number_busy"] == "0", "Expected no data change");
    check(vals["ep_restore:number_skipped"] == "0", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "0", "Expected no data change");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "Expected no data change");

    complete_restore(h, h1);
    return SUCCESS;
}

static enum test_result test_restore_data_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item *it = NULL;
    ENGINE_ERROR_CODE r;

    r = h1->get(h, NULL, &it, "foo", 3, 0);
    check(r == ENGINE_TMPFAIL, "Data miss should be tmpfail");

    r = h1->allocate(h, NULL, &it, "foo", 3, 100, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");

    uint64_t cas;
    r = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(r == ENGINE_TMPFAIL, "Add shouldn't work during restore.");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_TMPFAIL, "Replace shouldn't work for a missing item during restore.");
    r = h1->remove(h, NULL, "foobar", 6, 0, 0);
    check(r == ENGINE_TMPFAIL, "Delete of non-existing item should work during restore");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_SUCCESS, "Replace should work for existing objects.");
    r = h1->remove(h, NULL, "foo", 3, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    h1->release(h, NULL, it);

    uint64_t value;
    r = h1->arithmetic(h, NULL, "bar", 3, true, false, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_TMPFAIL, "incr of nonexistsing key (without create) should be tmpfail");
    r = h1->arithmetic(h, NULL, "bar", 3, true, true, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_TMPFAIL, "incr of nonexistsing key (with create) should tmpfail");

    complete_restore(h, h1);
    // Now we should be in operational mode...
    r = h1->get(h, NULL, &it, "foo", 3, 0);
    check(r == ENGINE_KEY_ENOENT, "Key shouldn't be there");

    r = h1->allocate(h, NULL, &it, "foo", 3, 100, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");

    r = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(r == ENGINE_SUCCESS, "Add should work for missing items.");
    r = h1->remove(h, NULL, "foo", 3, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_NOT_STORED, "Replace shouldn't work for a missing item.");
    r = h1->arithmetic(h, NULL, "bar", 3, true, false, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_KEY_ENOENT, "incr of nonexistsing key (without create) shouldn't work");
    r = h1->arithmetic(h, NULL, "bar", 3, true, true, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_SUCCESS, "incr of nonexistsing key (with create) should work");
    h1->release(h, NULL, it);
    return SUCCESS;
}

static void ensure_file(const char *fname)
{
    if (access(fname, F_OK) != 0) {
        std::stringstream ss;
        ss << "No such file: " << fname;
        check(access(fname, F_OK) == 0, ss.str().c_str());
    }
}

static enum test_result test_restore_clean(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0001.mbb");
    ensure_file(cwd);

    for (uint16_t ii = 0; ii < 100; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "0", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "9060", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "1", "We don't have all vbuckets");
    complete_restore(h, h1);
    return SUCCESS;
}

static enum test_result test_restore_clean_vbucket_subset(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0001.mbb");
    ensure_file(cwd);
    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "0", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "912", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "8148", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "1", "We don't have all vbuckets");
    complete_restore(h, h1);
    return SUCCESS;
}

#ifdef future
static enum test_result test_restore_multi(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (uint16_t ii = 0; ii < 1000; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    for (int ii = 2; ii > 0; ii--) {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            fprintf(stderr, "Invoking getcwd failed!!!\n");
            return FAIL;
        }
        sprintf(cwd + strlen(cwd), "/mbbackup-%04d.mbb", ii);
        ensure_file(cwd);
        protocol_binary_request_header *req = create_restore_file_packet(cwd);
        ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response);
        check(r == ENGINE_SUCCESS, "The server should know the command");
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "The server should start the backup");
        free(req);
        waitfor_restore_state(h, h1, "zombie", 2000);
    }

    vals.clear();
    dump_stats = true;
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "5", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "18118", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "2", "We don't have all vbuckets");
    complete_restore(h, h1);
    return SUCCESS;
}
#endif

static enum test_result test_restore_with_data(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0001.mbb");
    ensure_file(cwd);
    protocol_binary_request_header *req = create_restore_file_packet(cwd);

    for (uint16_t ii = 0; ii < 100; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    item *it = NULL;
    uint64_t cas;
    ENGINE_ERROR_CODE r;

    r = h1->allocate(h, NULL, &it, "mykey1", 6, 100, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    h1->release(h, NULL, it);

    r = h1->allocate(h, NULL, &it, "mykey2", 6, 100, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    r = h1->remove(h, NULL, "mykey2", 6, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    h1->release(h, NULL, it);

    r = h1->allocate(h, NULL, &it, "mykey3", 6, 100, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    h1->release(h, NULL, it);

    r = h1->unknown_command(h, NULL, req, add_response);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "3", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "9057", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    complete_restore(h, h1);
    return SUCCESS;
}

static enum test_result test_create_new_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Inserting more than 500 items will cause a new open checkpoint with id 2
    // to be created.
    for (int j = 0; j < 600; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), ss.str().c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    protocol_binary_request_no_extras req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    // Command to create a new checkpoint with id 3 by force.
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = CMD_CREATE_CHECKPOINT;
    req.message.header.request.vbucket = htons(0);

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to create a new checkpoint.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success response from creating a new checkpoint");

    check(get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id", "checkpoint 0") == 2,
          "Last closed checkpoint Id for VB 0 should be 2");

    return SUCCESS;
}

static enum test_result test_extend_open_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char *pkt_raw = static_cast<char*>(calloc(1,
                                              sizeof(protocol_binary_request_header)
                                              + 4));
    protocol_binary_request_header *pkt = (protocol_binary_request_header*) pkt_raw;

    // Command to extend the open checkpoint.
    pkt->request.magic = PROTOCOL_BINARY_REQ;
    pkt->request.opcode = CMD_EXTEND_CHECKPOINT;
    pkt->request.vbucket = htons(0);
    pkt->request.bodylen = htonl(4);
    uint32_t val = htonl(1);
    memcpy(pkt_raw + sizeof(protocol_binary_request_header), &val, sizeof(uint32_t));

    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to extend the open checkpoint.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success response from extending the open checkpoint");

    // Inserting more than 500 items should not create a new checkpoint.
    for (int j = 0; j < 1000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET,
              ss.str().c_str(), ss.str().c_str(), &i, 0, 0) == ENGINE_SUCCESS,
              "Failed to store a value");
        h1->release(h, NULL, i);
    }

    check(get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id", "checkpoint 0") == 0,
          "Last closed checkpoint Id for VB 0 should be still 0");

    set_param(h, h1, engine_param_flush, "max_size", "100000");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 75000),
          "Incorrect larger high wat.");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    testHarness.time_travel(60);
    // Wait until the current open checkpoint is closed and purged from memory.
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(h1->get_stats(h, NULL, "checkpoint 0", 12, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string extension_enabled = vals["vb_0:checkpoint_extension"];
    assert(strcmp(extension_enabled.c_str(), "false") == 0);
    check(get_int_stat(h, h1, "vb_0:last_closed_checkpoint_id", "checkpoint 0") == 1,
          "Last closed checkpoint Id for VB 0 should be 1");

    free(pkt_raw);
    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, engine_param_checkpoint, "chk_max_items", "1000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_max_item param");
    set_param(h, h1, engine_param_checkpoint, "chk_period", "100");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_period param");
    set_param(h, h1, engine_param_checkpoint, "max_checkpoints", "2");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set max_checkpoints param");

    set_param(h, h1, engine_param_checkpoint, "chk_max_items", "50");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_max_items param");
    set_param(h, h1, engine_param_checkpoint, "chk_period", "10");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_period param");
    set_param(h, h1, engine_param_checkpoint, "max_checkpoints", "6");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for max_checkpoints param");

    return SUCCESS;
}

static enum test_result test_revid(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_get_meta req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_GET_META;
    msg.req.message.header.request.extlen = 0;
    msg.req.message.header.request.keylen = ntohs(10);
    msg.req.message.header.request.vbucket = htons(0);
    msg.req.message.header.request.bodylen = htonl(10);
    memcpy(msg.buffer + sizeof(msg.req.bytes), "test_revid", 10);


    for (uint32_t ii = 1; ii < 10; ++ii) {
        item *it;
        check(store(h, h1, NULL, OPERATION_SET, "test_revid", "foo", &it, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, it);

        ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                    add_response);

        check(ret == ENGINE_SUCCESS, "Failed to get meta data");
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
        check(last_body[0] == 0x01, "Expected REVID meta");
        check(last_body[1] == 20, "Expected 22 bytes long revid");
        uint32_t seqno;
        memcpy(&seqno, last_body + 2, 4);
        seqno = ntohl(seqno);
        checkeq(ii, seqno, "Unexpected sequence number");
    }

    return SUCCESS;
}

static void set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          const char *key, const size_t keylen,
                          const char *val, const size_t vallen,
                          const uint32_t vb, ItemMetaData *itemMeta,
                          uint64_t cas_for_set)
{
    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_set_with_meta req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    // size of meta data encoded, see function encodeMeta() for layout
    size_t nb = 22;
    // extlen of operation SET_WITH_META is 12
    uint8_t extlen = 12;
    size_t bodyLen = keylen + extlen + vallen + nb;

    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_SET_WITH_META;
    msg.req.message.header.request.extlen = extlen;
    msg.req.message.header.request.keylen = ntohs(keylen);
    msg.req.message.header.request.vbucket = htons(vb);
    msg.req.message.header.request.bodylen = htonl(bodyLen);
    msg.req.message.header.request.cas = htonll(cas_for_set);

    memcpy(msg.buffer + sizeof(msg.req.bytes), key, keylen);
    // if comes with value
    if( vallen > 0 && val ) {
        memcpy(msg.buffer + sizeof(msg.req.bytes) + keylen, val, vallen);
    }
    msg.req.message.body.nmeta_bytes = ntohl(nb);
    msg.req.message.body.flags = ntohl(itemMeta->flags);
    msg.req.message.body.expiration = 0;

    // encode the revid:
    uint8_t* meta_data = (uint8_t*)msg.buffer + sizeof(msg.req.bytes) + keylen + vallen;
    itemMeta->encode(meta_data, nb);

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                add_response);

    check(ret == ENGINE_SUCCESS, "Expected to be able to store with meta");

    return;
}

static void add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          const char *key, const size_t keylen,
                          const char *val, const size_t vallen,
                          const uint32_t vb, ItemMetaData *itemMeta)
{
    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_set_with_meta req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    // size of meta data encoded, see function encodeMeta() for layout
    size_t nb = 22;
    // extlen of operation ADD_WITH_META is 12
    uint8_t extlen = 12;
    size_t bodyLen = keylen + extlen + vallen + nb;

    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_ADD_WITH_META;
    msg.req.message.header.request.extlen = extlen;
    msg.req.message.header.request.keylen = ntohs(keylen);
    msg.req.message.header.request.vbucket = htons(vb);
    msg.req.message.header.request.bodylen = htonl(bodyLen);
    memcpy(msg.buffer + sizeof(msg.req.bytes), key, keylen);
    // if comes with value
    if( vallen > 0 && val ) {
        memcpy(msg.buffer + sizeof(msg.req.bytes) + keylen, val, vallen);
    }

    msg.req.message.body.nmeta_bytes = ntohl(nb);
    msg.req.message.body.flags = ntohl(itemMeta->flags);
    msg.req.message.body.expiration = 0;

    // encode the revid:
    uint8_t* meta_data = (uint8_t*)msg.buffer + sizeof(msg.req.bytes) + keylen + vallen;
    itemMeta->encode(meta_data, nb);

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                add_response);

    check(ret == ENGINE_SUCCESS, "Expected to be able to store with meta");

    return;
}

static void del_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          const char *key, const size_t keylen,
                          const uint32_t vb, ItemMetaData *itemMeta,
                          uint64_t cas_for_delete = 0)
{
    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_delete_with_meta req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    // size of meta data encoded, see function encodeMeta() for layout
    size_t nb = 22;
    // extlen of operation DEL_WITH_META is 4
    const uint8_t extlen = 4;
    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_DEL_WITH_META;
    msg.req.message.header.request.extlen = extlen;
    msg.req.message.header.request.keylen = ntohs(keylen);
    msg.req.message.header.request.vbucket = htons(vb);
    msg.req.message.header.request.bodylen = htonl(keylen + extlen + nb);
    msg.req.message.header.request.cas = htonll(cas_for_delete);
    memcpy(msg.buffer + sizeof(msg.req.bytes), key, keylen);
    msg.req.message.body.nmeta_bytes = ntohl(nb);

    uint8_t* meta_data = (uint8_t*)msg.buffer + sizeof(msg.req.bytes) + keylen;
    itemMeta->encode(meta_data, nb);

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                add_response);

    check(ret == ENGINE_SUCCESS, "Expected to be able to delete with meta");

    return;
}

static enum test_result test_regression_mb4314(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    union {
        protocol_binary_request_header pkt;
        protocol_binary_request_set_with_meta req;
        char buffer[1024];
    } msg;
    memset(&msg.req, 0, sizeof(msg));

    msg.req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    msg.req.message.header.request.opcode = CMD_SET_WITH_META;
    msg.req.message.header.request.extlen = 12;
    msg.req.message.header.request.keylen = ntohs(22);
    msg.req.message.header.request.vbucket = htons(0);
    msg.req.message.header.request.bodylen = htonl(12 + 22 + 22);
    memcpy(msg.buffer + sizeof(msg.req.bytes), "test_regression_mb4314", 22);
    msg.req.message.body.nmeta_bytes = ntohl(22);
    msg.req.message.body.flags = ntohl(0xdeadbeef);
    msg.req.message.body.expiration = 0;

    size_t nb = 22;
    // encode the revid:
    ItemMetaData md(0xdeadbeef, 10, 0xdeadbeef, 0);
    md.encode((uint8_t*)msg.buffer + sizeof(msg.req.bytes) + 22, nb);

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, &msg.pkt,
                                                add_response);
    check(ret == ENGINE_SUCCESS, "Expected to be able to store with meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // Now try to read the item back:
    item *it = NULL;
    ret = h1->get(h, NULL, &it, "test_regression_mb4314", 22, 0);
    check(ret == ENGINE_SUCCESS, "Expected to get the item back!");
    h1->release(h, NULL, it);

    return SUCCESS;
}

static enum test_result test_mb3466(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    check(vals.find("mem_used") != vals.end(),
          "Expected \"mem_used\" to be returned");
    check(vals.find("bytes") != vals.end(),
          "Expected \"bytes\" to be returned");
    std::string memUsed = vals["mem_used"];
    std::string bytes = vals["bytes"];
    check(memUsed == bytes,
          "Expected mem_used and bytes to have the same value");

    return SUCCESS;
}

// ------------------- beginning of XDCR unit tests -----------------------//
static enum test_result test_get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "test_get_meta";
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    Item *it = reinterpret_cast<Item*>(i);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    ItemMetaData itm_meta;
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(itm_meta.seqno == it->getSeqno(), "Expected seqno to match");
    check(itm_meta.cas == it->getCas(), "Expected cas to match");
    check(itm_meta.exptime == it->getExptime(), "Expected exptime to match");
    check(itm_meta.flags == it->getFlags(), "Expected flags to match");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";
    item *i = NULL;

    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    Item *it = reinterpret_cast<Item*>(i);
    wait_for_flusher_to_settle(h, h1);

    check(h1->remove(h, NULL, key, strlen(key), it->getCas(), 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");

    ItemMetaData itm_meta;
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.seqno == it->getSeqno() + 1, "Expected seqno to match");
    check(itm_meta.cas == it->getCas() + 1, "Expected cas to match");
    check(itm_meta.flags == it->getFlags(), "Expected flags to match");

    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key = "k1";
    ItemMetaData itm_meta;

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);
    // check the stat
    size_t temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(!get_meta(h, h1, key, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_get_meta_with_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    ItemMetaData itm_meta;
    size_t temp = 0;
    // test get_meta followed by get for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_SUCCESS, "Expected get success");
    h1->release(h, NULL, i);
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by get for a deleted key. should fail.
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(h1->get(h, NULL, &i, key1, strlen(key1), 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect more getMeta ops");

    // test get_meta followed by get for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(h1->get(h, NULL, &i, key2, strlen(key2), 0) == ENGINE_KEY_ENOENT, "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 3, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_get_meta_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    ItemMetaData itm_meta;
    size_t temp = 0;

    int curri, tempi;

    // test get_meta followed by set for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");
    h1->release(h, NULL, i);

    // check curr, temp item counts
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    // test get_meta followed by set for a deleted key. should pass.
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect more getMeta ops");
    h1->release(h, NULL, i);

    // test get_meta followed by set for a nonexistent key. should pass.
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(store(h, h1, NULL, OPERATION_SET, key2, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 3, "Failed operation should also count");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_get_meta_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    char const *key2 = "key2";

    item *i = NULL;
    ItemMetaData itm_meta;
    size_t temp = 0;

    // test get_meta followed by delete for an existing key. should pass.
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 0, "Expect zero getMeta ops");
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 1, "Expect one getMeta op");

    // test get_meta followed by delete for a deleted key. should fail.
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_KEY_ENOENT,
          "Expected enoent");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 2, "Expect more getMeta op");

    // test get_meta followed by delete for a nonexistent key. should fail.
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    check(h1->remove(h, NULL, key2, strlen(key2), 0, 0) == ENGINE_KEY_ENOENT,
          "Expected enoent");
    // check the stat again
    temp = get_int_stat(h, h1, "ep_num_ops_get_meta");
    check(temp == 3, "Failed operation should also count");

    return SUCCESS;
}

static enum test_result test_add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const char *key = "mykey";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    size_t temp = 0;

    // put some random metadata
    itemMeta.seqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // store an item with meta data
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // store the item again, expect key exists
    add_with_meta(h, h1, key, keylen, NULL, 0, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected add to fail when the item exists already");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Failed op does not count");

    return SUCCESS;
}

static enum test_result test_delete_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // put some random meta data
    itemMeta.seqno = 10;
    itemMeta.cas = 0xdeadbeef;
    itemMeta.exptime = 0;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key,
                "somevalue", &i) == ENGINE_SUCCESS, "Failed set.");

    // delete an item with meta data
    del_with_meta(h, h1, key, keylen, 0, &itemMeta);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect more setMeta ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_deleted(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    item *i = NULL;
    size_t temp = 0;
    int curri, tempi;

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // add a key
    check(store(h, h1, NULL, OPERATION_SET, key, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    // delete the key
    check(h1->remove(h, NULL, key, keylen, 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    // get metadata of deleted key
    ItemMetaData itm_meta;
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;
    // put some random metadata and delete the item with new meta data
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Faild ops does not count");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect some ops");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // get metadata again to verify that delete with meta was successful
    ItemMetaData itm_meta2;
    check(get_meta(h, h1, key, itm_meta2), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.seqno == itm_meta2.seqno, "Expected seqno to match");
    check(itm_meta.cas == itm_meta2.cas, "Expected cas to match");
    check(itm_meta.exptime == itm_meta2.exptime, "Expected exptime to match");
    check(itm_meta.flags == itm_meta2.flags, "Expected flags to match");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_nonexistent(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    const char *key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itm_meta;
    size_t temp = 0;
    int curri, tempi;

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero setMeta ops");

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);

    // get metadata of nonexistent key
    check(!get_meta(h, h1, key, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // this is the cas to be used with a subsequent delete with meta
    uint64_t valid_cas = last_cas;
    uint64_t invalid_cas = 2012;

    // do delete with meta
    // put some random metadata and delete the item with new meta data
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do delete with meta with an incorrect cas value. should fail.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, invalid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // do delete with meta with the correct cas value. should pass.
    del_with_meta(h, h1, key, keylen, 0, &itm_meta, valid_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect one op");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected zero curr_items");

    // get metadata again to verify that delete with meta was successful
    ItemMetaData itm_meta2;
    check(get_meta(h, h1, key, itm_meta2), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    check(itm_meta.seqno == itm_meta2.seqno, "Expected seqno to match");
    check(itm_meta.cas == itm_meta2.cas, "Expected cas to match");
    check(itm_meta.exptime == itm_meta2.exptime, "Expected exptime to match");
    check(itm_meta.flags == itm_meta2.flags, "Expected flags to match");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    const size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    const size_t keylen2 = strlen(key2);

    item *i = NULL;
    ItemMetaData itm_meta;
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    check(h1->remove(h, NULL, key1, keylen1, 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a nonexistent key. should fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key2, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_delete_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    uint16_t keylen1 = (uint16_t)strlen(key1);
    char const *key2 = "key2";
    uint16_t keylen2 = (uint16_t)strlen(key2);
    item *i = NULL;
    ItemMetaData itm_meta;
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent delete
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");

    // attempt delete_with_meta. should fail since cas is no longer valid.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete itself will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete
    check(h1->remove(h, NULL, key1, keylen1, 0, 0) == ENGINE_KEY_ENOENT,
          "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key1, keylen1, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 1, "Expect some ops");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete itself will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent delete
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_KEY_ENOENT,
          "Delete failed");

    // attempt delete_with_meta. should pass.
    del_with_meta(h, h1, key2, keylen2, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected delete_with_meta success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_del_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    size_t newValLen = strlen(newVal);
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    // create a new key
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i) == ENGINE_SUCCESS,
          "Failed set.");

    // get metadata for the key
    ItemMetaData itm_meta;
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 300;
    itm_meta.flags = 0xdeadbeef;

    // do set with meta with an incorrect cas value. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some ops");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(itm_meta.seqno == 10, "Expected seqno to match");
    check(itm_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(itm_meta.flags == 0xdeadbeef, "Expected flags to match");

    // Make sure the item expiration was processed correctly
    testHarness.time_travel(301);
    check(h1->get(h, NULL, &i, key, keylen, 0) == ENGINE_KEY_ENOENT, "Failed to get value.");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_deleted(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    const char* newVal = "someothervalue";
    uint16_t newValLen = (uint16_t)strlen(newVal);
    size_t temp = 0;
    int curri, tempi;

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    // create a new key
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    // delete the key
    check(h1->remove(h, NULL, key, strlen(key), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    // get metadata for the key
    ItemMetaData itm_meta;
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set with meta with an incorrect cas value. should fail.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, newVal, newValLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some ops");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(itm_meta.seqno == 10, "Expected seqno to match");
    check(itm_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(itm_meta.exptime == 1735689600, "Expected exptime to match");
    check(itm_meta.flags == 0xdeadbeef, "Expected flags to match");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_nonexistent(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char* key = "set_with_meta_key";
    size_t keylen = strlen(key);
    const char* val = "somevalue";
    size_t valLen = strlen(val);
    ItemMetaData itm_meta;
    size_t temp = 0;
    int curri, tempi;

    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    // wait until the vb snapshot has run
    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);
    // get metadata for the key
    check(!get_meta(h, h1, key, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // this is the cas to be used with a subsequent set with meta
    uint64_t cas_for_set = last_cas;
    // init some random metadata
    itm_meta.seqno = 10;
    itm_meta.cas = 0xdeadbeef;
    itm_meta.exptime = 1735689600; // expires in 2025
    itm_meta.flags = 0xdeadbeef;

    // do set with meta with an incorrect cas value. should fail.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, 1229);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // do set with meta with the correct cas value. should pass.
    set_with_meta(h, h1, key, keylen, val, valLen, 0, &itm_meta, cas_for_set);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some ops");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    // get metadata again to verify that set with meta was successful
    check(get_meta(h, h1, key, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(itm_meta.seqno == 10, "Expected seqno to match");
    check(itm_meta.cas == 0xdeadbeef, "Expected cas to match");
    check(itm_meta.exptime == 1735689600, "Expected exptime to match");
    check(itm_meta.flags == 0xdeadbeef, "Expected flags to match");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == 1, "Expected single curr_items");

    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    size_t keylen2 = strlen(key2);
    ItemMetaData itm_meta;
    item *i = NULL;
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero ops");

    //
    // test race with a concurrent set for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a deleted key. should fail.
    //

    // do get_meta for the deleted key
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key1, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    //
    // test race with a concurrent set for a nonexistent key. should fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent set that changes the cas
    check(store(h, h1, NULL, OPERATION_SET, key2, "someothervalue", &i) == ENGINE_SUCCESS,
          "Failed set.");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key2, keylen2, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Failed op does not count");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_set_with_meta_race_with_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char const *key1 = "key1";
    size_t keylen1 = strlen(key1);
    char const *key2 = "key2";
    size_t keylen2 = strlen(key2);
    ItemMetaData itm_meta;
    item *i = NULL;
    size_t temp = 0;
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for an existing key. should fail.
    //

    // create a new key and do get_meta
    check(store(h, h1, NULL, OPERATION_SET, key1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    // do a concurrent delete that changes the cas
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");

    // attempt set_with_meta. should fail since cas is no longer valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
          "Expected invalid cas error");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 0, "Expect zero op");

    //
    // test race with a concurrent delete for a deleted key. should pass since
    // the delete will fail.
    //

    // do get_meta for the deleted key
    wait_for_flusher_to_settle(h, h1);
    check(get_meta(h, h1, key1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");

    // do a concurrent delete. should fail.
    check(h1->remove(h, NULL, key1, strlen(key1), 0, 0) == ENGINE_KEY_ENOENT,
          "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key1, keylen1, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 1, "Expect some op");

    //
    // test race with a concurrent delete for a nonexistent key. should pass
    // since the delete will fail.
    //

    // do get_meta for a nonexisting key
    check(!get_meta(h, h1, key2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // do a concurrent delete. should fail.
    check(h1->remove(h, NULL, key2, strlen(key2), 0, 0) == ENGINE_KEY_ENOENT,
          "Delete failed");

    // attempt set_with_meta. should pass since cas is still valid.
    set_with_meta(h, h1, key2, keylen2, NULL, 0, 0, &itm_meta, last_cas);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    // check the stat
    temp = get_int_stat(h, h1, "ep_num_ops_set_meta");
    check(temp == 2, "Expect some ops");

    h1->release(h, NULL, i);
    return SUCCESS;
}

static enum test_result test_temp_item_deletion(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    // Do get_meta for an existing key
    char const *k1 = "k1";
    item *i = NULL;
    int tempi, curri;

    check(store(h, h1, NULL, OPERATION_SET, k1, "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    wait_for_flusher_to_settle(h, h1);

    check(h1->remove(h, NULL, k1, strlen(k1), 0, 0) == ENGINE_SUCCESS,
          "Delete failed");
    wait_for_flusher_to_settle(h, h1);

    ItemMetaData itm_meta;
    check(get_meta(h, h1, k1, itm_meta), "Expected to get meta");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    check(last_deleted_flag, "Expected deleted flag to be set");
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 1, "Expected single temp_items");
    check(curri == tempi, "Expected single curr_items");

    // Do get_meta for a non-existing key
    char const *k2 = "k2";
    check(!get_meta(h, h1, k2, itm_meta), "Expected get meta to return false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Expected enoent");

    // Trigger the expiry pager and verify that two temp items are deleted
    testHarness.time_travel(30);
    wait_for_stat_to_be(h, h1, "ep_expired_pager", 2);
    curri = get_int_stat(h, h1, "curr_items");
    tempi = get_int_stat(h, h1, "curr_temp_items");
    check(tempi == 0, "Expected zero temp_items");
    check(curri == tempi, "Expected zero curr_items");

    h1->release(h, NULL, i);
    return SUCCESS;
}
// ------------------------------ end of XDCR unit tests -----------------------//

static enum test_result test_observe_no_data(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(CMD_OBSERVE, 0);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);
    return SUCCESS;
}

static enum test_result test_observe_single_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    stop_persistence(h, h1);

    // Set an item
    item *it = NULL;
    uint64_t cas1;
    check(h1->allocate(h, NULL, &it, "key", 3, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas1, OPERATION_SET, 0)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);

    // Do an observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key"] = 0;
    protocol_binary_request_header *pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);

    // Check that the key is not persisted
    uint16_t vb;
    uint16_t keylen;
    char key[3];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body, sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 3, "Wrong keylen in result");
    memcpy(&key, last_body + 4, ntohs(keylen));
    check(strncmp(key, "key", 3) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 7, sizeof(uint8_t));
    check(persisted == 0, "Expected persisted in result");
    memcpy(&cas, last_body + 8, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_multi_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Create some vbuckets
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Set some keys to observe
    item *it = NULL;
    uint64_t cas1, cas2, cas3;
    check(h1->allocate(h, NULL, &it, "key1", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas1, OPERATION_SET, 0)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);

    check(h1->allocate(h, NULL, &it, "key2", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas2, OPERATION_SET, 1)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);

    check(h1->allocate(h, NULL, &it, "key3", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas3, OPERATION_SET, 1)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    obskeys["key2"] = 1;
    obskeys["key3"] = 1;
    protocol_binary_request_header *pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");

    wait_for_flusher_to_settle(h, h1);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);

    // Check the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body, sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 8, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    memcpy(&vb, last_body + 17, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 19, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 21, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 25, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 26, sizeof(uint64_t));
    check(ntohll(cas) == cas2, "Wrong cas in result");

    memcpy(&vb, last_body + 34, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 36, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 38, ntohs(keylen));
    check(strncmp(key, "key3", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 42, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 43, sizeof(uint64_t));
    check(ntohll(cas) == cas3, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_multiple_observes(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Holds the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    // Set some keys
    item *it = NULL;
    uint64_t cas1, cas2;
    check(h1->allocate(h, NULL, &it, "key1", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas1, OPERATION_SET, 0)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);
    check(h1->allocate(h, NULL, &it, "key2", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas2, OPERATION_SET, 0)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    protocol_binary_request_header *pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);

    memcpy(&vb, last_body, sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 8, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");
    check(last_bodylen == 17, "Incorrect body length");

    // Do another observe
    obskeys.clear();
    obskeys["key2"] = 0;
    pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);

    memcpy(&vb, last_body, sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 4, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 8, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas2, "Wrong cas in result");
    check(last_bodylen == 17, "Incorrect body length");

    return SUCCESS;
}

static enum test_result test_observe_with_not_found(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Create some vbuckets
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    // Set some keys
    item *it = NULL;
    uint64_t cas1, cas3;
    check(h1->allocate(h, NULL, &it, "key1", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas1, OPERATION_SET, 0)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);

    check(h1->allocate(h, NULL, &it, "key3", 4, 100, 0, 0)== ENGINE_SUCCESS,
          "Allocation failed.");
    check(h1->store(h, NULL, it, &cas3, OPERATION_SET, 1)== ENGINE_SUCCESS,
          "Set should work.");
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Do observe
    std::map<std::string, uint16_t> obskeys;
    obskeys["key1"] = 0;
    obskeys["key2"] = 0;
    obskeys["key3"] = 1;
    protocol_binary_request_header *pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "Expected success");
    free(pkt);

    // Check the result
    uint16_t vb;
    uint16_t keylen;
    char key[10];
    uint8_t persisted;
    uint64_t cas;

    memcpy(&vb, last_body, sizeof(uint16_t));
    check(ntohs(vb) == 0, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 2, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 4, ntohs(keylen));
    check(strncmp(key, "key1", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 8, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 9, sizeof(uint64_t));
    check(ntohll(cas) == cas1, "Wrong cas in result");

    memcpy(&keylen, last_body + 19, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 21, ntohs(keylen));
    check(strncmp(key, "key2", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 25, sizeof(uint8_t));
    check(persisted == 128, "Expected key_not_found key status");

    memcpy(&vb, last_body + 34, sizeof(uint16_t));
    check(ntohs(vb) == 1, "Wrong vbucket in result");
    memcpy(&keylen, last_body + 36, sizeof(uint16_t));
    check(ntohs(keylen) == 4, "Wrong keylen in result");
    memcpy(&key, last_body + 38, ntohs(keylen));
    check(strncmp(key, "key3", 4) == 0, "Wrong key in result");
    memcpy(&persisted, last_body + 42, sizeof(uint8_t));
    check(persisted == 1, "Expected persisted in result");
    memcpy(&cas, last_body + 43, sizeof(uint64_t));
    check(ntohll(cas) == cas3, "Wrong cas in result");

    return SUCCESS;
}

static enum test_result test_observe_errors(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::map<std::string, uint16_t> obskeys;

    // Check not my vbucket error
    obskeys["key"] = 1;
    protocol_binary_request_header *pkt = createObservePacket(obskeys);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Expected not my vbucket");
    free(pkt);

    // Check invalid packets
    pkt = createPacket(CMD_OBSERVE, 0, NULL, "0");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Expected invalid");
    free(pkt);

    pkt = createPacket(CMD_OBSERVE, 0, NULL, "0000");
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Observe failed.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Expected invalid");
    free(pkt);

    return SUCCESS;
}

static enum test_result test_compact_mutation_log(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    std::vector<std::string> keys;
    for (int j = 0; j < 1000; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int compactor_runs = get_int_stat(h, h1, "ep_mlog_compactor_runs");

    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);

    for (it = keys.begin(); it != keys.end(); ++it) {
        check(h1->remove(h, NULL, (*it).c_str(), (*it).length(), 0, 0) == ENGINE_SUCCESS,
              "Failed to remove a key");
    }
    wait_for_flusher_to_settle(h, h1);

    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    testHarness.time_travel(10);
    wait_for_flusher_to_settle(h, h1);

    // Wait until the current open checkpoint is closed and purged from memory.
    wait_for_stat_change(h, h1, "ep_mlog_compactor_runs", compactor_runs);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    assert(get_int_stat(h, h1, "curr_items") == 1000);
    check(get_int_stat(h, h1, "count_new", "klog") == 1000,
          "Number of new log entries should be 2000");
    check(get_int_stat(h, h1, "count_del", "klog") == 0,
          "Number of delete log entries should be 0");

    return SUCCESS;
}

static enum test_result test_CBD_152(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // turn off flushall_enabled parameter
    set_param(h, h1, engine_param_flush, "flushall_enabled", "false");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set flushall_enabled param");

    // store a key and check its existence
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);

    check_key_value(h, h1, "key", "somevalue", 9);
    // expect error msg engine does not support operation
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP, "Flush should be disabled");
    //check the key
    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");

    // turn on flushall_enabled parameter
    set_param(h, h1, engine_param_flush, "flushall_enabled", "true");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set flushall_enabled param");
    // flush should succeed
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Flush should be enabled");
    //expect missing key
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static McCouchMockServer *mccouchMock;

static enum test_result prepare(engine_test_t *test) {
#ifdef __sun
        // Some of the tests doesn't work on Solaris.. Don't know why yet..
        if (strstr(test->name, "concurrent set") != NULL ||
            strstr(test->name, "retain rowid over a soft delete") != NULL)
        {
            return SKIPPED;
        }
#endif


    if (test->cfg == NULL || strstr(test->cfg, "backend") == NULL) {
        return rmdb();
    }

    enum test_result ret = rmdb();
    if (ret != SUCCESS) {
        return ret;
    }

    if (strstr(test->cfg, "backend=sqlite") != NULL) {
        // No specialized init needed yet..
    } else if (strstr(test->cfg, "backend=couchdb") != NULL) {
#ifndef HAVE_LIBCOUCHSTORE
        (void)mccouchMock;
        return SKIPPED;
#else
        /* Start a mock server... */
        int port;
        mccouchMock = new McCouchMockServer(port);
        char config[1024];
        sprintf(config, "%s;couch_port=%d", test->cfg, port);
        test->cfg = strdup(config);
        std::string dbname;
        const char *nm = strstr(test->cfg, "dbname=");
        if (nm == NULL) {
            dbname.assign("/tmp/test.db");
        } else {
            dbname.assign(nm + 7);
            std::string::size_type end = dbname.find(';');
            if (end != dbname.npos) {
                dbname = dbname.substr(0, end);
            }
        }
        if (dbname.find("/non/") == dbname.npos) {
            mkdir(dbname.c_str(), 0777);
        }
#endif
    } else {
        // unknow backend!
        using namespace std;

        cerr << endl << "Unknown backend specified! " << endl
             << test->cfg << endl;

        return FAIL;
    }
    return SUCCESS;
}

static void cleanup(engine_test_t *test, enum test_result result) {
    (void)result;
    // Nuke the database files we created
    rmdb();
    free(const_cast<char*>(test->name));
    free(const_cast<char*>(test->cfg));
    delete mccouchMock;
    mccouchMock = 0;
}

// the backends is a bitmask for which backend to run the given test
// for.
#define BACKEND_SQLITE 2
#define BACKEND_COUCH 1
#define BACKEND_ALL 0x3
#define SKIP_TEST 0x4
#define BACKEND_VARIANTS 2

class TestCase {
public:
    TestCase(const char *_name,
             enum test_result(*_tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             bool(*_test_setup)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             bool(*_test_teardown)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
             const char *_cfg,
             enum test_result (*_prepare)(engine_test_t *test),
             void (*_cleanup)(engine_test_t *test, enum test_result result),
             int _backends) : name(_name), cfg(_cfg), backends(_backends) {
        memset(&test, 0, sizeof(test));
        test.tfun = _tfun;
        test.test_setup = _test_setup;
        test.test_teardown = _test_teardown;
        test.prepare = _prepare;
        test.cleanup = _cleanup;
    }

    TestCase(const TestCase &o) : name(o.name),
                                  cfg(o.cfg),
                                  backends(o.backends) {
        memset(&test, 0, sizeof(test));
        test.tfun = o.test.tfun;
        test.test_setup = o.test.test_setup;
        test.test_teardown = o.test.test_teardown;
    }

    const char *getName() {
        return name;
    }

    engine_test_t *getTest(int backend) {
        engine_test_t *ret = 0;

        if (backends & backend) {
            ret = new engine_test_t;
            *ret = test;

            std::string nm(name);
            std::stringstream ss;
            if (cfg != 0) {
                ss << cfg << ";";
            } else {
                ss << "flushall_enabled=true;";
            }

            switch (backend) {
            case BACKEND_SQLITE:
                nm.append(" (sqlite)");
                if (cfg == NULL || strstr(cfg, "backend") == NULL) {
                    ss << "backend=sqlite";
                }
                break;
            case BACKEND_COUCH:
                nm.append(" (couchstore)");
                ss << "backend=couchdb;couch_response_timeout=3000";
                break;
            case SKIP_TEST:
                nm.append(" (skipped)");
                ret->tfun = skipped_test_function;
            default:
                abort();
            }
            ret->name = strdup(nm.c_str());
            std::string config = ss.str();
            if (config.length() == 0) {
                ret->cfg = 0;
            } else {
                ret->cfg = strdup(config.c_str());
            }
        }

        return ret;
    }

private:
    engine_test_t test;
    const char *name;
    const char *cfg;
    int backends;
};

static engine_test_t *testcases;

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {
    TestCase tc[] = {
        TestCase("validate engine handle", test_validate_engine_handle,
                 NULL, teardown, "db_strategy=singleDB;dbname=:memory:",
                 prepare, cleanup, BACKEND_SQLITE),
        // basic tests
        TestCase("test alloc limit", test_alloc_limit, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test init failure", test_init_fail, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_SQLITE),
        TestCase("test total memory limit", test_memory_limit,
                 test_setup, teardown,
                 "max_size=5492;ht_locks=1;ht_size=3;chk_remover_stime=1;chk_period=60;mutation_mem_threshold=0.9",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test max_size changes", test_max_size_settings,
                 test_setup, teardown,
                 "max_size=1000;ht_locks=1;ht_size=3", prepare,
                 cleanup, BACKEND_ALL),
        TestCase("test whitespace dbname", test_whitespace_db,
                 test_setup, teardown,
                 "dbname=" WHITESPACE_DB ";ht_locks=1;ht_size=3",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test db shards", test_db_shards, test_setup,
                 teardown, "db_shards=5;db_strategy=multiDB", prepare, cleanup,
                 BACKEND_SQLITE),
        TestCase("test single db strategy", test_single_db_strategy,
                 test_setup, teardown, "db_strategy=singleDB",
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("test single in-memory db strategy", test_single_db_strategy,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:", prepare, cleanup,
                 BACKEND_SQLITE),
        TestCase("get miss", test_get_miss, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set", test_set, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("concurrent set", test_conc_set, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set+get hit", test_set_get_hit, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set+get hit with max_txn_size", test_set_get_hit,
                 test_setup, teardown,
                 "ht_locks=1;ht_size=3;max_txn_size=10", prepare, cleanup,
                 BACKEND_ALL),
        TestCase("getl", test_getl, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("unl",  test_unl, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set+get hit (bin)", test_set_get_hit_bin,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("set+change flags", test_set_change_flags,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("add", test_add, test_setup, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("add+add(same cas)", test_add_add_with_cas,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("cas", test_cas, test_setup, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("append", test_append, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("prepend", test_prepend, test_setup, teardown,
                 NULL, prepare, cleanup,  BACKEND_ALL),
        TestCase("replace", test_replace, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("incr miss", test_incr_miss, test_setup,
                 teardown, NULL, prepare, cleanup,  BACKEND_ALL),
        TestCase("incr", test_incr, test_setup, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("incr with default", test_incr_default,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("incr expiry", test_bug2799, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test touch", test_touch, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test gat", test_gat, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test gatq", test_gatq, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test mb5215", test_mb5215, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_COUCH),
        TestCase("delete", test_delete, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set/delete", test_set_delete, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set/delete (invalid cas)", test_set_delete_invalid_cas,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("delete/set/delete", test_delete_set, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("retain rowid over a soft delete", test_bug2509,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket deletion doesn't affect new data", test_bug2761,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_SQLITE),
        TestCase("start transaction failure handling", test_bug2830,
                 test_setup, teardown,
                 "db_shards=1;ht_size=13;ht_locks=7;db_strategy=multiDB",
                 prepare, cleanup, SKIP_TEST),
        TestCase("non-resident decrementers", test_mb3169,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("resident ratio after warmup", test_mb5172,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("flush", test_flush, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("flush with stats", test_flush_stats, test_setup,
                 teardown,
                 "flushall_enabled=true;chk_remover_stime=1;chk_period=60",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("flush multi vbuckets", test_flush_multiv,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("flush multi vbuckets single mt", test_flush_multiv,
                 test_setup, teardown,
                 "flushall_enabled=true;db_strategy=singleMTDB;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3", prepare, cleanup, BACKEND_ALL),
        TestCase("flush multi vbuckets multi mt", test_flush_multiv,
                 test_setup, teardown,
                 "flushall_enabled=true;db_strategy=multiMTDB;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3", prepare, cleanup, BACKEND_ALL),
        TestCase("flush multi vbuckets multi mt vb", test_flush_multiv,
                 test_setup, teardown,
                 "flushall_enabled=true;db_strategy=multiMTVBDB;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3", prepare, cleanup, BACKEND_ALL),
        TestCase("flush_disabled", test_flush_disabled, test_setup,
                 teardown,
                 "flushall_enabled=false;db_strategy=multiMTVBDB;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3", prepare, cleanup, BACKEND_ALL),
        TestCase("flushall params", test_CBD_152, test_setup,
                 teardown,
                 "flushall_enabled=true;db_strategy=multiMTVBDB;max_vbuckets=16;"
                 "ht_size=7;ht_locks=3",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("expiry", test_expiry, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("expiry_loader", test_expiry_loader, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("expiry_flush", test_expiry_flush, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("expiry_duplicate_warmup", test_bug3454, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("expiry_no_items_warmup", test_bug3522, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("replica read", test_get_replica, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("replica read: invalid state - active",
                 test_get_replica_active_state,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("replica read: invalid state - pending",
                 test_get_replica_pending_state,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("replica read: invalid state - dead",
                 test_get_replica_dead_state,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("replica read: invalid key", test_get_replica_invalid_key,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test observe no data", test_observe_no_data, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test observe single key", test_observe_single_key, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test observe multi key", test_observe_multi_key, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test multiple observes", test_multiple_observes, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test observe with not found", test_observe_with_not_found, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test observe not my vbucket", test_observe_errors, NULL, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        // Stats tests
        TestCase("stats", test_stats, test_setup, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("io stats", test_io_stats, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("bg stats", test_bg_stats, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("mem stats", test_mem_stats, test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup,
                 BACKEND_ALL),
        TestCase("stats key", test_key_stats, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("stats vkey", test_vkey_stats, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("warmup stats", test_warmup_stats, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("stats curr_items", test_curr_items, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("startup token stat", test_cbd_225, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("number of completed flush stat", test_cbd_226, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),

        // eviction
        TestCase("value eviction", test_value_eviction, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        // duplicate items on disk
        TestCase("duplicate items on disk", test_duplicate_items_disk,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        // special non-Ascii keys
        TestCase("test special char keys", test_specialKeys, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test binary keys", test_binKeys, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        // tap tests
        TestCase("set tap param", test_set_tap_param, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("tap_noop_interval default config",
                 test_tap_noop_config_default,
                 test_setup, teardown, NULL , prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap_noop_interval config", test_tap_noop_config,
                 test_setup, teardown,
                 "tap_noop_interval=10", prepare, cleanup, BACKEND_ALL),
        TestCase("tap receiver mutation", test_tap_rcvr_mutate,
                 test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("tap receiver checkpoint start/end", test_tap_rcvr_checkpoint,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver mutation (dead)", test_tap_rcvr_mutate_dead,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver mutation (pending)",
                 test_tap_rcvr_mutate_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver mutation (replica)",
                 test_tap_rcvr_mutate_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver delete", test_tap_rcvr_delete,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver delete (dead)", test_tap_rcvr_delete_dead,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver delete (pending)", test_tap_rcvr_delete_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap receiver delete (replica)", test_tap_rcvr_delete_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap stream", test_tap_stream, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("tap stream send deletes", test_tap_sends_deleted, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),
        TestCase("tap agg stats", test_tap_agg_stats, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("tap takeover (with concurrent mutations)", test_tap_takeover,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap filter stream", test_tap_filter_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3", prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap default config", test_tap_default_config,
                 test_setup, teardown, NULL , prepare, cleanup,
                 BACKEND_ALL),
        TestCase("tap config", test_tap_config, test_setup,
                 teardown,
                 "tap_backoff_period=0.05;tap_ack_interval=10;tap_ack_window_size=2;tap_ack_grace_period=10",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("tap acks stream", test_tap_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;chk_max_items=500",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("tap implicit acks stream", test_tap_implicit_ack_stream,
                 test_setup, teardown,
                 "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;tap_ack_initial_sequence_number=4294967290;chk_max_items=500",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("tap notify", test_tap_notify, test_setup,
                 teardown, "max_size=1048576", prepare, cleanup, BACKEND_ALL),
        // restart tests
        TestCase("test restart", test_restart, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("set+get+restart+hit (bin)", test_restart_bin_val,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("flush+restart", test_flush_restart, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("flush multiv+restart", test_flush_multiv_restart,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test kill -9 bucket", test_kill9_bucket,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test restart with non-empty DB and empty mutation log",
                 test_mb4898, test_setup, teardown, NULL, prepare,
                 cleanup, BACKEND_SQLITE),
        // disk>RAM tests
        TestCase("verify not multi dispatcher", test_not_multi_dispatcher_conf,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_SQLITE),
        TestCase("disk>RAM golden path", test_disk_gt_ram_golden,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM paged-out rm", test_disk_gt_ram_paged_rm,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60", prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM update paged-out", test_disk_gt_ram_update_paged_out,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM delete paged-out", test_disk_gt_ram_delete_paged_out,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM paged-out incr", test_disk_gt_ram_incr,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM set bgfetch race", test_disk_gt_ram_set_race,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_SQLITE),
        TestCase("disk>RAM incr bgfetch race", test_disk_gt_ram_incr_race,
                 test_setup, teardown, NULL, prepare, cleanup, SKIP_TEST),
        TestCase("disk>RAM delete bgfetch race", test_disk_gt_ram_rm_race,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_SQLITE),
        // disk>RAM tests with WAL
        TestCase("verify multi dispatcher", test_multi_dispatcher_conf,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("verify multi dispatcher override",
                 test_not_multi_dispatcher_conf, test_setup,
                 teardown, MULTI_DISPATCHER_CONFIG ";concurrentDB=false",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("disk>RAM golden path (wal)", test_disk_gt_ram_golden,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("disk>RAM paged-out rm (wal)", test_disk_gt_ram_paged_rm,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("disk>RAM update paged-out (wal)",
                 test_disk_gt_ram_update_paged_out, test_setup,
                 teardown, MULTI_DISPATCHER_CONFIG, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM delete paged-out (wal)",
                 test_disk_gt_ram_delete_paged_out, test_setup,
                 teardown, MULTI_DISPATCHER_CONFIG, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("disk>RAM paged-out incr (wal)", test_disk_gt_ram_incr,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("disk>RAM set bgfetch race (wal)", test_disk_gt_ram_set_race,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("disk>RAM incr bgfetch race (wal)", test_disk_gt_ram_incr_race,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, SKIP_TEST),
        TestCase("disk>RAM delete bgfetch race (wal)", test_disk_gt_ram_rm_race,
                 test_setup, teardown, MULTI_DISPATCHER_CONFIG,
                 prepare, cleanup, BACKEND_SQLITE),
        // vbucket negative tests
        TestCase("vbucket incr (dead)", test_wrong_vb_incr,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket incr (pending)", test_vb_incr_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket incr (replica)", test_vb_incr_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket get (dead)", test_wrong_vb_get,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket get (pending)", test_vb_get_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket get (replica)", test_vb_get_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket getl (dead)", NULL, NULL, teardown, NULL, prepare,
                 cleanup, BACKEND_ALL),
        TestCase("vbucket getl (pending)", NULL, NULL, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("vbucket getl (replica)", NULL, NULL, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),
        TestCase("vbucket set (dead)", test_wrong_vb_set,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket set (pending)", test_vb_set_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket set (replica)", test_vb_set_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket replace (dead)", test_wrong_vb_replace,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket replace (pending)", test_vb_replace_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket replace (replica)", test_vb_replace_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket add (dead)", test_wrong_vb_add,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket add (pending)", test_vb_add_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket add (replica)", test_vb_add_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket cas (dead)", test_wrong_vb_cas,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket cas (pending)", test_vb_cas_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket cas (replica)", test_vb_cas_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket append (dead)", test_wrong_vb_append,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket append (pending)", test_vb_append_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket append (replica)", test_vb_append_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket prepend (dead)", test_wrong_vb_prepend,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket prepend (pending)", test_vb_prepend_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket prepend (replica)", test_vb_prepend_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket del (dead)", test_wrong_vb_del,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket del (pending)", test_vb_del_pending,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("vbucket del (replica)", test_vb_del_replica,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        // Vbucket management tests
        TestCase("no vb0 at startup", test_novb0,
                 test_setup, teardown, "vb0=false", prepare,
                 cleanup, BACKEND_ALL),
        TestCase("test vbucket get", test_vbucket_get, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test vbucket get missing", test_vbucket_get_miss,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test vbucket create", test_vbucket_create,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test async vbucket destroy", test_async_vbucket_destroy,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test sync vbucket destroy", test_sync_vbucket_destroy,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test async vbucket destroy (multitable)", test_async_vbucket_destroy,
                 test_setup, teardown,
                 "db_strategy=multiMTVBDB;max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test sync vbucket destroy (multitable)", test_sync_vbucket_destroy,
                 test_setup, teardown,
                 "db_strategy=multiMTVBDB;max_vbuckets=16;ht_size=7;ht_locks=3",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test vbucket destroy stats", test_vbucket_destroy_stats,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_period=60",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test async vbucket destroy restart",
                 test_async_vbucket_destroy_restart, test_setup, teardown,
                 NULL, prepare, cleanup, BACKEND_ALL),
        TestCase("test sync vbucket destroy restart",
                 test_sync_vbucket_destroy_restart, NULL, teardown, NULL,
                 prepare, cleanup, BACKEND_ALL),

        // checkpoint tests
        TestCase("checkpoint: create a new checkpoint",
                 test_create_new_checkpoint,
                 test_setup, teardown,
                 "chk_max_items=500;item_num_based_new_chk=true",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("checkpoint: extend the open checkpoint",
                 test_extend_open_checkpoint,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=500",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("checkpoint: validate checkpoint config params",
                 test_validate_checkpoint_params,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_ALL),
        TestCase("test checkpoint create", test_checkpoint_create,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test checkpoint timeout", test_checkpoint_timeout,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup, BACKEND_ALL),
        TestCase("test checkpoint deduplication", test_checkpoint_deduplication,
                 test_setup, teardown,
                 "chk_max_items=5000;chk_period=600",
                 prepare, cleanup, BACKEND_ALL),

        // Restore tests
        TestCase("restore: not enabled", test_restore_not_enabled,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:",
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("restore: no such file", test_restore_no_such_file,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:;restore_mode=true",
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("restore: invalid file", test_restore_invalid_file,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:;restore_mode=true",
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("restore: data miss during restore", test_restore_data_miss,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:;restore_mode=true",
                 prepare, cleanup, BACKEND_SQLITE),
        TestCase("restore: no data in there", test_restore_clean,
                 test_setup, teardown,
                 "restore_mode=true", prepare, cleanup, BACKEND_ALL),
        TestCase("restore: no data in there (with partial vbucket list)",
                 test_restore_clean_vbucket_subset,
                 test_setup, teardown,
                 "restore_mode=true", prepare, cleanup, BACKEND_ALL),
        TestCase("restore: with keys", test_restore_with_data,
                 test_setup, teardown,
                 "db_strategy=singleDB;dbname=:memory:;restore_mode=true",
                 prepare, cleanup, BACKEND_SQLITE),
#ifdef future
        TestCase("restore: multiple incrementalfiles", test_restore_multi,
                 NULL, teardown,
                 "db_strategy=singleDB;dbname=:memory:;restore_mode=true",
                 prepare, cleanup, BACKEND_SQLITE),
#endif
        // revision id's
        TestCase("revision sequence numbers", test_revid,
                 test_setup, teardown, NULL, prepare,
                 cleanup, BACKEND_ALL),

        TestCase("mb-4314", test_regression_mb4314, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),

        TestCase("mb-3466", test_mb3466, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),

        // XDCR unit tests
        TestCase("get meta", test_get_meta, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("get meta deleted", test_get_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("get meta nonexistent", test_get_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("get meta followed by get", test_get_meta_with_get,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("get meta followed by set", test_get_meta_with_set,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("get meta followed by delete", test_get_meta_with_delete,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("add with meta", test_add_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_ALL),

        TestCase("delete with meta", test_delete_with_meta,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("delete with meta deleted", test_delete_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("delete with meta nonexistent",
                 test_delete_with_meta_nonexistent, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("delete_with_meta race with concurrent delete",
                 test_delete_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("delete_with_meta race with concurrent set",
                 test_delete_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("set with meta", test_set_with_meta, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("set with meta deleted", test_set_with_meta_deleted,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("set with meta nonexistent", test_set_with_meta_nonexistent,
                 test_setup, teardown, NULL, prepare, cleanup,
                 BACKEND_COUCH),

        TestCase("set_with_meta race with concurrent set",
                 test_set_with_meta_race_with_set, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("set_with_meta race with concurrent delete",
                 test_set_with_meta_race_with_delete, test_setup,
                 teardown, NULL, prepare, cleanup, BACKEND_COUCH),

        TestCase("temp item deletion", test_temp_item_deletion,
                 test_setup,teardown,
                 "exp_pager_stime=3", prepare, cleanup, BACKEND_COUCH),

        // mutation log compactor tests
        TestCase("compact a mutation log", test_compact_mutation_log,
                 test_setup, teardown,
                 "klog_path=/tmp/mutation.log;klog_max_log_size=32768;"
                 "klog_max_entry_ratio=2;klog_compactor_stime=5",
                 prepare, cleanup, BACKEND_ALL),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup, BACKEND_ALL)
    };

    // Calculate the size of the tests..
    int num = 0;
    while (tc[num].getName() != 0) {
        ++num;
    }

    int total = num * BACKEND_VARIANTS;
    testcases = static_cast<engine_test_t*>(calloc(total + 1,
                                                   sizeof(engine_test_t)));

    int ii = 0;
    for (int variant = 1; variant <= BACKEND_VARIANTS; variant <<= 1) {
        for (int jj = 0; jj < num; ++jj) {
            engine_test_t *r = tc[jj].getTest(variant);
            if (r != 0) {
                testcases[ii++] = *r;
            }
        }
    }

    return testcases;
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    putenv(const_cast<char*>("EP-ENGINE-TESTSUITE=true"));
    testHarness = *th;
    return true;
}


MEMCACHED_PUBLIC_API
bool teardown_suite() {
    free(testcases);
    testcases = NULL;
    return true;
}

} // extern "C"
