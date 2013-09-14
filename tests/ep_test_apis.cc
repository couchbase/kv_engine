/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include <assert.h>
#include <memcached/util.h>
#include <stdlib.h>
#include <string.h>
#include <platform/platform.h>

#include <algorithm>
#include <iostream>
#include <sstream>

#include "ep_test_apis.h"

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

extern "C" bool abort_msg(const char *expr, const char *msg, int line);

std::map<std::string, std::string> vals;
bool dump_stats = false;
protocol_binary_response_status last_status =
    static_cast<protocol_binary_response_status>(0);
uint32_t last_bodylen = 0;
char *last_key = NULL;
char *last_body = NULL;
bool last_deleted_flag = false;
uint64_t last_cas = 0;
ItemMetaData last_meta;

extern "C" bool add_response_get_meta(const void *key, uint16_t keylen,
                                      const void *ext, uint8_t extlen,
                                      const void *body, uint32_t bodylen,
                                      uint8_t datatype, uint16_t status,
                                      uint64_t cas, const void *cookie);
void encodeExt(char *buffer, uint32_t val);
void encodeWithMetaExt(char *buffer, ItemMetaData *meta);

void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    usleep(*sleepTime);
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

bool add_response(const void *key, uint16_t keylen, const void *ext,
                  uint8_t extlen, const void *body, uint32_t bodylen,
                  uint8_t datatype, uint16_t status, uint64_t cas,
                  const void *cookie) {
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

bool add_response_get_meta(const void *key, uint16_t keylen, const void *ext,
                           uint8_t extlen, const void *body, uint32_t bodylen,
                           uint8_t datatype, uint16_t status, uint64_t cas,
                           const void *cookie) {
    (void)datatype;
    (void)cookie;
    const uint8_t* ext_bytes = reinterpret_cast<const uint8_t*> (ext);
    if (ext && extlen > 0) {
        uint32_t flags;
        memcpy(&flags, ext_bytes, 4);
        last_deleted_flag = ntohl(flags) & GET_META_ITEM_DELETED_FLAG;
        memcpy(&last_meta.flags, ext_bytes + 4, 4);
        memcpy(&last_meta.exptime, ext_bytes + 8, 4);
        last_meta.exptime = ntohl(last_meta.exptime);
        memcpy(&last_meta.revSeqno, ext_bytes + 12, 8);
        last_meta.revSeqno = ntohll(last_meta.revSeqno);
        last_meta.cas = cas;
    }
    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

bool add_response_ret_meta(const void *key, uint16_t keylen, const void *ext,
                           uint8_t extlen, const void *body, uint32_t bodylen,
                           uint8_t datatype, uint16_t status, uint64_t cas,
                           const void *cookie) {
    (void)datatype;
    (void)cookie;
    const uint8_t* ext_bytes = reinterpret_cast<const uint8_t*> (ext);
    if (ext && extlen == 16) {
        memcpy(&last_meta.flags, ext_bytes, 4);
        memcpy(&last_meta.exptime, ext_bytes + 4, 4);
        last_meta.exptime = ntohl(last_meta.exptime);
        memcpy(&last_meta.revSeqno, ext_bytes + 8, 8);
        last_meta.revSeqno = ntohll(last_meta.revSeqno);
        last_meta.cas = cas;
    }
    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

void add_stats(const char *key, const uint16_t klen, const char *val,
               const uint32_t vlen, const void *cookie) {
    (void)cookie;
    std::string k(key, klen);
    std::string v(val, vlen);

    if (dump_stats) {
        std::cout << "stat[" << k << "] = " << v << std::endl;
    }

    vals[k] = v;
}

void encodeExt(char *buffer, uint32_t val) {
    val = htonl(val);
    memcpy(buffer, (char*)&val, sizeof(val));
}

void encodeWithMetaExt(char *buffer, ItemMetaData *meta) {
    uint32_t flags = meta->flags;
    uint32_t exp = htonl(meta->exptime);
    uint64_t seqno = htonll(meta->revSeqno);
    uint64_t cas = htonll(meta->cas);

    memcpy(buffer, (char*)&flags, sizeof(flags));
    memcpy(buffer + 4, (char*)&exp, sizeof(exp));
    memcpy(buffer + 8, (char*)&seqno, sizeof(seqno));
    memcpy(buffer + 16, (char*)&cas, sizeof(cas));
}

protocol_binary_request_header* createPacket(uint8_t opcode,
                                             uint16_t vbid,
                                             uint64_t cas,
                                             const char *ext,
                                             uint32_t extlen,
                                             const char *key,
                                             uint32_t keylen,
                                             const char *val,
                                             uint32_t vallen) {
    char *pkt_raw;
    uint32_t headerlen = sizeof(protocol_binary_request_header);
    pkt_raw = static_cast<char*>(calloc(1, headerlen + extlen + keylen + vallen));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.keylen = htons(keylen);
    req->request.extlen = extlen;
    req->request.vbucket = htons(vbid);
    req->request.bodylen = htonl(keylen + vallen + extlen);
    req->request.cas = ntohll(cas);

    if (extlen > 0) {
        memcpy(pkt_raw + headerlen, ext, extlen);
    }

    if (keylen > 0) {
        memcpy(pkt_raw + headerlen + extlen, key, keylen);
    }

    if (vallen > 0) {
        memcpy(pkt_raw + headerlen + extlen + keylen, val, vallen);
    }

    return req;
}

void add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   bool skipConflictResolution) {
    int blen = skipConflictResolution ? 28 : 24;
    char *ext = new char[blen];
    encodeWithMetaExt(ext, itemMeta);

    if (skipConflictResolution) {
        uint32_t flag = SKIP_CONFLICT_RESOLUTION_FLAG;
        flag = htonl(flag);
        memcpy(ext + 24, (char*)&flag, sizeof(flag));
    }

    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_ADD_WITH_META, vb, 0, ext, blen, key, keylen,
                       val, vallen);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to store with meta");
    delete[] ext;
}

void changeVBFilter(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, std::string name,
                    std::map<uint16_t, uint64_t> &filtermap) {
    std::stringstream value;
    uint16_t vbs = htons(filtermap.size());
    std::map<uint16_t, uint64_t>::iterator it;

    value.write((char*) &vbs, sizeof(uint16_t));
    for (it = filtermap.begin(); it != filtermap.end(); ++it) {
        uint16_t vb = htons(it->first);
        uint64_t chkid = htonll(it->second);
        value.write((char*) &vb, sizeof(uint16_t));
        value.write((char*) &chkid, sizeof(uint64_t));
    }

    protocol_binary_request_header *request;
    request = createPacket(CMD_CHANGE_VB_FILTER, 0, 0, NULL, 0, name.c_str(),
                       name.length(), value.str().data(), value.str().length());
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to change the TAP VB filter.");
    free(request);
}

void createCheckpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *request = createPacket(CMD_CREATE_CHECKPOINT);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to create a new checkpoint.");
    free(request);
}

ENGINE_ERROR_CODE del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                      uint64_t cas, uint16_t vbucket, const void* cookie) {
    return h1->remove(h, cookie, key, strlen(key), &cas, vbucket);
}

void del_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const uint32_t vb,
                   ItemMetaData *itemMeta, uint64_t cas_for_delete,
                   bool skipConflictResolution) {
    int blen = skipConflictResolution ? 28 : 24;
    char *ext = new char[blen];
    encodeWithMetaExt(ext, itemMeta);

    if (skipConflictResolution) {
        uint32_t flag = SKIP_CONFLICT_RESOLUTION_FLAG;
        flag = htonl(flag);
        memcpy(ext + 24, (char*)&flag, sizeof(flag));
    }
    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_DEL_WITH_META, vb, cas_for_delete, ext, blen, key,
                       keylen);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to delete with meta");
    delete[] ext;
}

void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
               uint16_t vbucketId, const char *msg, bool expectError) {
    int nonResidentItems = get_int_stat(h, h1, "ep_num_non_resident");
    int numEjectedItems = get_int_stat(h, h1, "ep_num_value_ejects");
    protocol_binary_request_header *pkt = createPacket(CMD_EVICT_KEY, 0, 0,
                                                       NULL, 0, key, strlen(key));
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

    check(get_int_stat(h, h1, "ep_num_non_resident") == nonResidentItems,
          "Incorrect number of non-resident items");
    check(get_int_stat(h, h1, "ep_num_value_ejects") == numEjectedItems,
          "Incorrect number of ejected items");

    if (msg != NULL && strcmp(last_body, msg) != 0) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_body);
        abort();
    }
}

size_t estimateVBucketMove(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         uint16_t vbid, const char* tap_name) {
    std::stringstream ss;
    ss << "tap-vbtakeover " << vbid;
    if (tap_name) {
      ss << " " << tap_name;
    }
    return get_int_stat(h, h1, "estimate", ss.str().c_str());
}

void extendCheckpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      uint32_t checkpoint_num) {
    char val[4];
    encodeExt(val, checkpoint_num);
    protocol_binary_request_header *request;
    request = createPacket(CMD_EXTEND_CHECKPOINT, 0, 0, NULL, 0, NULL, 0, val, 4);
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to extend the open checkpoint.");
    free(request);
}

ENGINE_ERROR_CODE checkpointPersistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        uint64_t checkpoint_id) {
    checkpoint_id = htonll(checkpoint_id);
    protocol_binary_request_header *request;
    request = createPacket(CMD_CHECKPOINT_PERSISTENCE, 0, 0, NULL, 0, NULL, 0,
                           (const char *)&checkpoint_id, sizeof(uint64_t));
    ENGINE_ERROR_CODE rv = h1->unknown_command(h, NULL, request, add_response);
    free(request);
    return rv;
}

void gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
         uint16_t vb, uint32_t exp, bool quiet) {
    char ext[4];
    uint8_t opcode = quiet ? PROTOCOL_BINARY_CMD_GATQ : PROTOCOL_BINARY_CMD_GAT;
    uint32_t keylen = key ? strlen(key) : 0;
    protocol_binary_request_header *request;
    encodeExt(ext, exp);
    request = createPacket(opcode, vb, 0, ext, 4, key, keylen);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call gat");
    free(request);
}

bool get_item_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item_info *info,
                   const char* key, uint16_t vb) {
    item *i = NULL;
    if (h1->get(h, NULL, &i, key, strlen(key), vb) != ENGINE_SUCCESS) {
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

bool get_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item *i,
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

void getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
          uint16_t vb, uint32_t lock_timeout) {
    char ext[4];
    uint32_t keylen = key ? strlen(key) : 0;
    protocol_binary_request_header *request;
    encodeExt(ext, lock_timeout);
    request = createPacket(CMD_GET_LOCKED, vb, 0, ext, 4, key, keylen);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call getl");
    free(request);
}

bool get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key) {
    protocol_binary_request_header *req = createPacket(CMD_GET_META, 0, 0, NULL,
                                                       0, key, strlen(key));

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, NULL, req,
                                                add_response_get_meta);
    check(ret == ENGINE_SUCCESS, "Expected get_meta call to be successful");
    if (last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return true;
    }
    return false;
}

void observe(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
             std::map<std::string, uint16_t> obskeys) {
    std::stringstream value;
    std::map<std::string, uint16_t>::iterator it;
    for (it = obskeys.begin(); it != obskeys.end(); ++it) {
        uint16_t vb = htons(it->second);
        uint16_t keylen = htons(it->first.length());
        value.write((char*) &vb, sizeof(uint16_t));
        value.write((char*) &keylen, sizeof(uint16_t));
        value.write(it->first.c_str(), it->first.length());
    }

    protocol_binary_request_header *request;
    request = createPacket(CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0,
                           value.str().data(), value.str().length());
    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Observe call failed");
    free(request);
}

void get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
                 uint16_t vbid) {
    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_GET_REPLICA, vbid, 0, NULL, 0, key, strlen(key));
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
                              "Get Replica Failed");
    free(pkt);
}

protocol_binary_request_header* prepare_get_replica(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1,
                                                    vbucket_state_t state,
                                                    bool makeinvalidkey) {
    uint16_t id = 0;
    const char *key = "k0";
    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_GET_REPLICA, id, 0, NULL, 0, key, strlen(key));

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

bool set_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, engine_param_t paramtype,
               const char *param, const char *val) {
    char ext[4];
    protocol_binary_request_header *pkt;
    encodeExt(ext, static_cast<uint32_t>(paramtype));
    pkt = createPacket(CMD_SET_PARAM, 0, 0, ext, sizeof(engine_param_t), param,
                       strlen(param), val, strlen(val));

    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       uint16_t vb, vbucket_state_t state) {

    char ext[4];
    protocol_binary_request_header *pkt;
    encodeExt(ext, static_cast<uint32_t>(state));
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_VBUCKET, vb, 0, ext, 4);

    if (h1->unknown_command(h, NULL, pkt, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

void set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_set, bool skipConflictResolution) {
    int blen = skipConflictResolution ? 28 : 24;
    char *ext = new char[blen];
    encodeWithMetaExt(ext, itemMeta);

    if (skipConflictResolution) {
        uint32_t flag = SKIP_CONFLICT_RESOLUTION_FLAG;
        flag = htonl(flag);
        memcpy(ext + 24, (char*)&flag, sizeof(flag));
    }

    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_SET_WITH_META, vb, cas_for_set, ext, blen, key, keylen,
                       val, vallen);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Expected to be able to store with meta");
    delete[] ext;
}

void return_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                 const size_t keylen, const char *val, const size_t vallen,
                 const uint32_t vb, const uint64_t cas, const uint32_t flags,
                 const uint32_t exp, const uint32_t type) {
    char ext[12];
    encodeExt(ext, type);
    encodeExt(ext + 4, flags);
    encodeExt(ext + 8, exp);
    protocol_binary_request_header *pkt;
    pkt = createPacket(CMD_RETURN_META, vb, cas, ext, 12, key, keylen, val,
                       vallen);
    check(h1->unknown_command(h, NULL, pkt, add_response_ret_meta)
              == ENGINE_SUCCESS, "Expected to be able to store ret meta");
    free(pkt);
}

void set_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas, const uint32_t flags,
                  const uint32_t exp) {
    return_meta(h, h1, key, keylen, val, vallen, vb, cas, flags, exp,
                SET_RET_META);
}

void add_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas, const uint32_t flags,
                  const uint32_t exp) {
    return_meta(h, h1, key, keylen, val, vallen, vb, cas, flags, exp,
                ADD_RET_META);
}

void del_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const uint32_t vb, const uint64_t cas) {
    return_meta(h, h1, key, keylen, NULL, 0, vb, cas, 0, 0,
                DEL_RET_META);
}

void disable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(CMD_DISABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to disable data traffic");
    free(pkt);
}

void enable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(CMD_ENABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to enable data traffic");
    free(pkt);
}

void start_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(CMD_START_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error starting persistence.");
}

void stop_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    while (true) {
        useconds_t sleepTime = 128;
        if (get_str_stat(h, h1, "ep_flusher_state", 0) == "running") {
            break;
        }
        decayingSleep(&sleepTime);
    }

    protocol_binary_request_header *pkt = createPacket(CMD_STOP_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error stopping persistence.");
}

ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const void *cookie, ENGINE_STORE_OPERATION op,
                        const char *key, const char *value, item **outitem,
                        uint64_t casIn, uint16_t vb, uint32_t exp) {
    return storeCasVb11(h, h1, cookie, op, key, value, strlen(value),
                        9258, outitem, casIn, vb, exp);
}

ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie, ENGINE_STORE_OPERATION op,
                               const char *key, const char *value, size_t vlen,
                               uint32_t flags, item **outitem, uint64_t casIn,
                               uint16_t vb, uint32_t exp) {
    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = h1->allocate(h, cookie, &it,
                                        key, strlen(key),
                                        vlen, flags, exp);
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

void touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
           uint16_t vb, uint32_t exp) {
    char ext[4];
    uint32_t keylen = key ? strlen(key) : 0;
    protocol_binary_request_header *request;
    encodeExt(ext, exp);
    request = createPacket(PROTOCOL_BINARY_CMD_TOUCH, vb, 0, ext, 4, key, keylen);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call touch");
    free(request);
}

void unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
         uint16_t vb, uint64_t cas) {
    uint32_t keylen = key ? strlen(key) : 0;
    protocol_binary_request_header *request;
    request = createPacket(CMD_UNLOCK_KEY, vb, cas, NULL, 0, key, keylen);

    check(h1->unknown_command(h, NULL, request, add_response) == ENGINE_SUCCESS,
          "Failed to call unl");
    free(request);
}

void vbucketDelete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                   const char* args) {
    uint32_t argslen = args ? strlen(args) : 0;
    protocol_binary_request_header *pkt =
        createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, vb, 0, NULL, 0, NULL, 0,
                     args, argslen);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
}

ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char* key, uint16_t vbucket) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), vbucket);
    if (rv == ENGINE_SUCCESS) {
        h1->release(h, NULL, i);
    }
    return rv;
}

bool verify_vbucket_missing(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
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

bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                          vbucket_state_t expected, bool mute) {
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_GET_VBUCKET, vb, 0);

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

int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                 const char *statkey) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    std::string s = vals[statname];
    return atoi(s.c_str());
}

std::string get_str_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *statname, const char *statkey) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS, "Failed to get stats.");
    std::string s = vals[statname];
    return s;
}

void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int exp,
                       const char *msg) {
    int curr_items = get_int_stat(h, h1, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

void wait_for_stat_change(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          const char *stat, int initial) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, stat) == initial) {
        decayingSleep(&sleepTime);
    }
}

void wait_for_stat_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *stat, int final, const char* stat_key) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, stat, stat_key) != final) {
        decayingSleep(&sleepTime);
    }
}

void wait_for_memory_usage_below(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 int mem_threshold) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "mem_used") > mem_threshold) {
        decayingSleep(&sleepTime);
    }
}

bool wait_for_warmup_complete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
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

void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_queue_size") > 0) {
        decayingSleep(&sleepTime);
    }
    wait_for_stat_change(h, h1, "ep_commit_num", 0);
}

void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const char *key, const char *val,
                              uint16_t vbucketId) {

    item *i = NULL;
    int commitNum = get_int_stat(h, h1, "ep_commit_num");
    check(ENGINE_SUCCESS == store(h, h1, NULL, OPERATION_SET, key, val, &i, 0,
                                  vbucketId),
            "Failed to store an item.");

    // Wait for persistence...
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_commit_num", commitNum);
    h1->release(h, NULL, i);
}
