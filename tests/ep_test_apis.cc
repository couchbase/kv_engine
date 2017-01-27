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
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>

#include "mock/mock_dcp.h"

template<typename T> class HistogramStats;

// Due to the limitations of the add_stats callback (essentially we cannot pass
// a context into it) we instead have a single, global `vals` map. The
// vals_mutex is to ensure serialised modifications to this data structure.
std::mutex vals_mutex;
statistic_map vals;

// get_stat and get_histo_stat can only be called one at a time as they use
// the three global variables (requested_stat_name, actual_stat_value and
// histogram_stat_int_value).  Therefore the two functions need to acquire a
// lock and keep it for the whole function duration.

// The requested_stat_name and actual_stat_value are used in an optimized
// add_stats callback (add_individual_stat) which checks for one stat
// (and hence doesn't have to keep a map of all of them).
struct {
    std::mutex mutex;
    std::string requested_stat_name;
    std::string actual_stat_value;
    /* HistogramStats<T>* is supported C++14 onwards.
     * Until then use a separate ptr for each type.
     */
    HistogramStats<int>* histogram_stat_int_value;
} get_stat_context;

bool dump_stats = false;
std::atomic<protocol_binary_response_status> last_status(
    static_cast<protocol_binary_response_status>(0));
std::string last_key;
std::string last_body;
bool last_deleted_flag(false);
std::atomic<uint8_t> last_conflict_resolution_mode(static_cast<uint8_t>(-1));
std::atomic<uint64_t> last_cas(0);
std::atomic<uint8_t> last_datatype(0x00);
ItemMetaData last_meta;
std::atomic<uint64_t> last_uuid(0);
std::atomic<uint64_t> last_seqno(0);

/* HistogramBinStats is used to hold a histogram bin object a histogram stat.
   This is a class used to hold already computed stats. Hence we do not expect
   any change once a bin object is created */
template<typename T>
class HistogramBinStats {
public:
    HistogramBinStats(const T& s, const T& e, uint64_t count)
        : start_(s), end_(e), count_(count) { }

    T start() const {
        return start_;
    }

    T end() const {
        return end_;
    }

    uint64_t count() const {
        return count_;
    }

private:
    T start_;
    T end_;
    uint64_t count_;
};


/* HistogramStats is used to hold necessary info from a histogram stat.
   Since this class used to hold already computed stats, only write apis to add
   new bins is implemented */
template<typename T>
class HistogramStats {
public:
    HistogramStats() : total_count(0) {}

    /* Add a new bin */
    void add_bin(const T& start, const T& end, uint64_t count) {
        bins.push_back(HistogramBinStats<T>(start, end, count));
        total_count += count;
    }

    /* Num of bins in the histogram */
    size_t num_bins() const {
        return bins.size();
    }

    uint64_t total() const {
        return total_count;
    }

    /* Add a bin iterator when needed */
private:
    /* List of all the bins in the histogram stats */
    std::list<HistogramBinStats<T>> bins;
    /* Total number of samples across all histogram bins */
    uint64_t total_count;
};

static void get_histo_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  const char *statname, const char *statkey);

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

ENGINE_ERROR_CODE vb_map_response(const void *cookie,
                                  const void *map,
                                  size_t mapsize) {
    (void)cookie;
    last_body.assign(static_cast<const char*>(map), mapsize);
    return ENGINE_SUCCESS;
}

bool add_response(const void *key, uint16_t keylen, const void *ext,
                  uint8_t extlen, const void *body, uint32_t bodylen,
                  uint8_t datatype, uint16_t status, uint64_t cas,
                  const void *cookie) {
    (void)ext;
    (void)extlen;
    (void)cookie;
    last_status.store(static_cast<protocol_binary_response_status>(status));
    last_body.assign(static_cast<const char*>(body), bodylen);
    last_key.assign(static_cast<const char*>(key), keylen);
    last_cas.store(cas);
    last_datatype.store(datatype);
    return true;
}

bool add_response_get_meta(const void *key, uint16_t keylen, const void *ext,
                           uint8_t extlen, const void *body, uint32_t bodylen,
                           uint8_t datatype, uint16_t status, uint64_t cas,
                           const void *cookie) {
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
        if (extlen > 20) {
            memcpy(&last_conflict_resolution_mode, ext_bytes + 20, 1);
        }
    }
    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

bool add_response_set_del_meta(const void *key, uint16_t keylen, const void *ext,
                               uint8_t extlen, const void *body, uint32_t bodylen,
                               uint8_t datatype, uint16_t status, uint64_t cas,
                               const void *cookie) {
    (void)cookie;
    const uint8_t* ext_bytes = reinterpret_cast<const uint8_t*> (ext);
    if (ext && extlen > 0) {
        uint64_t vb_uuid;
        uint64_t seqno;
        memcpy(&vb_uuid, ext_bytes, 8);
        memcpy(&seqno, ext_bytes + 8, 8);
        last_uuid.store(ntohll(vb_uuid));
        last_seqno.store(ntohll(seqno));
    }

    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

bool add_response_ret_meta(const void *key, uint16_t keylen, const void *ext,
                           uint8_t extlen, const void *body, uint32_t bodylen,
                           uint8_t datatype, uint16_t status, uint64_t cas,
                           const void *cookie) {
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

    std::lock_guard<std::mutex> lh(vals_mutex);
    vals[k] = v;
}

/* Callback passed to engine interface `get_stats`, used by get_int_stat and
 * friends to lookup a specific stat. If `key` matches the requested key name,
 * then record its value in actual_stat_value.
 */
void add_individual_stat(const char *key, const uint16_t klen, const char *val,
               const uint32_t vlen, const void *cookie) {

    if (get_stat_context.actual_stat_value.empty() &&
            get_stat_context.requested_stat_name.compare(
                    0, get_stat_context.requested_stat_name.size(),
                    key, klen) == 0) {
        get_stat_context.actual_stat_value = std::string(val, vlen);
    }
}

void add_individual_histo_stat(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               const void *cookie) {
    /* Convert key to string */
    std::string key_str(key, klen);
    size_t pos1 = key_str.find(get_stat_context.requested_stat_name);
    if (pos1 != std::string::npos)
    {
        get_stat_context.actual_stat_value.append(val, vlen);
        /* Parse start and end from the key.
           Key is in the format task_name_START,END (backfill_tasks_20,100) */
        pos1 += get_stat_context.requested_stat_name.length();
        /* Find ',' to move to end of bin_start */
        size_t pos2 = key_str.find(',', pos1);
        if ((std::string::npos == pos2) || (pos1 >= pos2)) {
            throw std::invalid_argument("Malformed histogram stat: " + key_str);
        }
        int start = std::stoi(std::string(key_str, pos1, pos2));

        /* Move next to ',' for starting character of bin_end */
        pos1 = pos2 + 1;
        /* key_str ends with bin_end */
        pos2 = key_str.length();
        if (pos1 >= pos2) {
            throw std::invalid_argument("Malformed histogram stat: " + key_str);
        }
        int end = std::stoi(std::string(key_str, pos1, pos2));
        get_stat_context.histogram_stat_int_value->add_bin(start, end,
                                                           std::stoull(val));
    }
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
                                             uint8_t extlen,
                                             const char *key,
                                             uint32_t keylen,
                                             const char *val,
                                             uint32_t vallen,
                                             uint8_t datatype,
                                             const char *meta,
                                             uint16_t nmeta) {
    char *pkt_raw;
    uint32_t headerlen = sizeof(protocol_binary_request_header);
    pkt_raw = static_cast<char*>(cb_calloc(1, headerlen + extlen + keylen + vallen + nmeta));
    cb_assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.keylen = htons(keylen);
    req->request.extlen = extlen;
    req->request.vbucket = htons(vbid);
    req->request.bodylen = htonl(keylen + vallen + extlen + nmeta);
    req->request.cas = htonll(cas);
    req->request.datatype = datatype;

    if (extlen > 0) {
        memcpy(pkt_raw + headerlen, ext, extlen);
    }

    if (keylen > 0) {
        memcpy(pkt_raw + headerlen + extlen, key, keylen);
    }

    if (vallen > 0) {
        memcpy(pkt_raw + headerlen + extlen + keylen, val, vallen);
    }

    // Extended meta: To be used for set_with_meta/del_with_meta/add_with_meta
    if (meta && nmeta > 0) {
        memcpy(pkt_raw + headerlen + extlen + keylen + vallen,
               meta, nmeta);
    }

    return req;
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
    request = createPacket(PROTOCOL_BINARY_CMD_CHANGE_VB_FILTER, 0, 0, NULL, 0, name.c_str(),
                       name.length(), value.str().data(), value.str().length());
    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to change the TAP VB filter.");
    cb_free(request);
}

void createCheckpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *request = createPacket(PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT);
    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to create a new checkpoint.");
    cb_free(request);
}

ENGINE_ERROR_CODE del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                      uint64_t cas, uint16_t vbucket, const void* cookie) {
    mutation_descr_t mut_info;
    return h1->remove(h, cookie, DocKey(key, testHarness.doc_namespace),
                      &cas, vbucket, &mut_info);
}

ENGINE_ERROR_CODE del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                      uint64_t* cas, uint16_t vbucket, const void* cookie,
                      mutation_descr_t* mut_info) {
    return h1->remove(h, cookie, DocKey(key, testHarness.doc_namespace),
                      cas, vbucket, mut_info);
}

void del_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const uint32_t vb,
                   ItemMetaData *itemMeta, uint64_t cas_for_delete,
                   uint32_t options, const void *cookie,
                   const std::vector<char>& nmeta) {
    int blen = 24;
    std::unique_ptr<char[]> ext(new char[30]);
    std::unique_ptr<ExtendedMetaData> emd;

    encodeWithMetaExt(ext.get(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.get() + blen, (char*)&optionsSwapped, sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (nmeta.size() > 0) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.get() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_WITH_META, vb, cas_for_delete,
                       ext.get(), blen, key, keylen, NULL, 0,
                       PROTOCOL_BINARY_RAW_BYTES, nmeta.data(), nmeta.size());

    check(h1->unknown_command(h, cookie, pkt, add_response_set_del_meta, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Expected to be able to delete with meta");
    cb_free(pkt);
}

void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
               uint16_t vbucketId, const char *msg, bool expectError) {
    int nonResidentItems = get_int_stat(h, h1, "ep_num_non_resident");
    int numEjectedItems = get_int_stat(h, h1, "ep_num_value_ejects");
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_EVICT_KEY, 0, 0,
                                                       NULL, 0, key, strlen(key));
    pkt->request.vbucket = htons(vbucketId);

    checkeq(ENGINE_SUCCESS,
            h1->unknown_command(h, NULL, pkt, add_response,
                                testHarness.doc_namespace),
          "Failed to perform CMD_EVICT_KEY.");

    cb_free(pkt);
    if (expectError) {
        checkeq(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, last_status.load(),
                "evict_key: expected KEY_EEXISTS when evicting key");
    } else {
        if (last_body != "Already ejected.") {
            nonResidentItems++;
            numEjectedItems++;
        }
        checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
                "evict_key: expected SUCCESS when evicting key.");
    }

    checkeq(nonResidentItems, get_int_stat(h, h1, "ep_num_non_resident"),
          "Incorrect number of non-resident items");
    checkeq(numEjectedItems, get_int_stat(h, h1, "ep_num_value_ejects"),
          "Incorrect number of ejected items");

    if (msg != NULL && last_body != msg) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_body.c_str());
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

ENGINE_ERROR_CODE checkpointPersistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        uint64_t checkpoint_id, uint16_t vb) {
    checkpoint_id = htonll(checkpoint_id);
    protocol_binary_request_header *request;
    request = createPacket(PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE, vb, 0, NULL, 0, NULL, 0,
                           (const char *)&checkpoint_id, sizeof(uint64_t));
    ENGINE_ERROR_CODE rv = h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace);
    cb_free(request);
    return rv;
}

ENGINE_ERROR_CODE seqnoPersistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                   uint16_t vbucket, uint64_t seqno) {
    seqno = htonll(seqno);
    char buffer[8];
    memcpy(buffer, &seqno, sizeof(uint64_t));
    protocol_binary_request_header* request =
        createPacket(PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE, vbucket, 0, buffer, 8);

    ENGINE_ERROR_CODE rv = h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace);
    cb_free(request);
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

    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to call gat");
    cb_free(request);
}

bool get_item_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item_info *info,
                   const char* key, uint16_t vb) {
    item *i = NULL;
    if (get(h, h1, NULL, &i, key, vb) != ENGINE_SUCCESS) {
        return false;
    }
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
    if (!h1->get_item_info(h, NULL, i, &info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    key.assign((const char*)info.key, info.nkey);
    return true;
}

ENGINE_ERROR_CODE getl(ENGINE_HANDLE* h,
                       ENGINE_HANDLE_V1* h1,
                       const void* cookie,
                       item** item,
                       const char* key,
                       uint16_t vb, uint32_t lock_timeout) {

    return h1->get_locked(h, cookie, item,
                          DocKey(key, testHarness.doc_namespace),
                          vb, lock_timeout);
}

bool get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
              bool reqExtMeta, const void* cookie) {

    protocol_binary_request_header *req;
    if (reqExtMeta) {
        uint8_t ext = 0x01;
        req = createPacket(PROTOCOL_BINARY_CMD_GET_META, 0, 0,
                           (char*)&ext, sizeof(ext), key, strlen(key));
    } else {
        req = createPacket(PROTOCOL_BINARY_CMD_GET_META, 0, 0,
                           NULL, 0, key, strlen(key));
    }

    ENGINE_ERROR_CODE ret = h1->unknown_command(h, cookie, req,
                                                add_response_get_meta,
                                                testHarness.doc_namespace);
    if (ret == ENGINE_EWOULDBLOCK) {
        last_status = static_cast<protocol_binary_response_status>(ENGINE_EWOULDBLOCK);
    } else {
        check(ret == ENGINE_SUCCESS,
              "Expected get_meta call to be successful");
    }
    cb_free(req);
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
    request = createPacket(PROTOCOL_BINARY_CMD_OBSERVE, 0, 0, NULL, 0, NULL, 0,
                           value.str().data(), value.str().length());
    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Observe call failed");
    cb_free(request);
}

void observe_seqno(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                   uint16_t vb_id, uint64_t uuid) {
    protocol_binary_request_header *request;
    uint64_t vb_uuid = htonll(uuid);
    std::stringstream data;
    data.write((char *) &vb_uuid, sizeof(uint64_t));

    request = createPacket(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO, vb_id, 0, NULL, 0,
                           NULL, 0, data.str().data(), data.str().length());
    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Observe_seqno call failed");
    cb_free(request);
}

void get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
                 uint16_t vbid) {
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_GET_REPLICA, vbid, 0, NULL, 0, key, strlen(key));
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
                              "Get Replica Failed");
    cb_free(pkt);
}

protocol_binary_request_header* prepare_get_replica(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1,
                                                    vbucket_state_t state,
                                                    bool makeinvalidkey) {
    uint16_t id = 0;
    const char *key = "k0";
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_GET_REPLICA, id, 0, NULL, 0, key, strlen(key));

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

bool set_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, protocol_binary_engine_param_t paramtype,
               const char *param, const char *val, uint16_t vb) {
    char ext[4];
    protocol_binary_request_header *pkt;
    encodeExt(ext, static_cast<uint32_t>(paramtype));
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_PARAM, vb, 0, ext, sizeof(protocol_binary_engine_param_t), param,
                       strlen(param), val, strlen(val));

    if (h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) != ENGINE_SUCCESS) {
        cb_free(pkt);
        return false;
    }

    cb_free(pkt);
    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       uint16_t vb, vbucket_state_t state) {

    char ext[4];
    protocol_binary_request_header *pkt;
    encodeExt(ext, static_cast<uint32_t>(state));
    pkt = createPacket(PROTOCOL_BINARY_CMD_SET_VBUCKET, vb, 0, ext, 4);

    if (h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) != ENGINE_SUCCESS) {
        return false;
    }

    cb_free(pkt);
    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool get_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       vbucket_state_t state, const void *cookie) {
    protocol_binary_request_header *pkt;
    if (state) {
        char ext[sizeof(vbucket_state_t)];
        encodeExt(ext, static_cast<uint32_t>(state));
        pkt = createPacket(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS, 0, 0, ext,
                           sizeof(vbucket_state_t));
    } else {
        pkt = createPacket(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS);
    }

    check(h1->unknown_command(h, cookie, pkt, add_response, testHarness.doc_namespace) ==
          ENGINE_SUCCESS, "Error in getting all vb info");

    cb_free(pkt);
    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

void verify_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          int vb_start, int vb_end) {
    const int per_vb_resp_size = sizeof(uint16_t) + sizeof(uint64_t);
    const int high_seqno_offset = sizeof(uint16_t);

    /* Check if the total response length is as expected. We expect 10 bytes
     (2 for vb_id + 8 for seqno) */
    checkeq((vb_end - vb_start + 1) * per_vb_resp_size,
            static_cast<int>(last_body.size()),
            "Failed to get all vb info.");
    /* Check if the contents are correct */
    for (int i = 0; i < (vb_end - vb_start + 1); i++) {
        /* Check for correct vb_id */
        checkeq(static_cast<const uint16_t>(vb_start + i),
                ntohs(*(reinterpret_cast<const uint16_t*>(last_body.data() +
                                                          per_vb_resp_size*i))),
              "vb_id mismatch");
        /* Check for correct high_seqno */
        std::string vb_stat_seqno("vb_" + std::to_string(vb_start + i) +
                                  ":high_seqno");
        uint64_t high_seqno_vb =
        get_ull_stat(h, h1, vb_stat_seqno.c_str(), "vbucket-seqno");
        checkeq(high_seqno_vb,
                ntohll(*(reinterpret_cast<const uint64_t*>(last_body.data() +
                                                           per_vb_resp_size*i +
                                                           high_seqno_offset))),
                "high_seqno mismatch");
    }
}

static void store_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                     protocol_binary_command cmd, const char *key,
                     const size_t keylen, const char *val, const size_t vallen,
                     const uint32_t vb, ItemMetaData *itemMeta,
                     uint64_t cas_for_store, uint32_t options,
                     uint8_t datatype, const void *cookie,
                     const std::vector<char>& nmeta) {
    int blen = 24;
    std::unique_ptr<char[]> ext(new char[30]);
    std::unique_ptr<ExtendedMetaData> emd;

    encodeWithMetaExt(ext.get(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.get() + blen, (char*)&optionsSwapped, sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (nmeta.size() > 0) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.get() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    protocol_binary_request_header *pkt;
    pkt = createPacket(cmd, vb, cas_for_store, ext.get(), blen, key, keylen,
                       val, vallen, datatype, nmeta.data(), nmeta.size());

    check(h1->unknown_command(h, cookie, pkt, add_response_set_del_meta, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Expected to be able to store with meta");
    cb_free(pkt);
}

void set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_set, uint32_t options, uint8_t datatype,
                   const void *cookie, const std::vector<char>& nmeta) {
    store_with_meta(h, h1, PROTOCOL_BINARY_CMD_SET_WITH_META, key, keylen, val,
                    vallen, vb, itemMeta, cas_for_set, options, datatype,
                    cookie, nmeta);
}

void add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_add, uint32_t options, uint8_t datatype,
                   const void *cookie, const std::vector<char>& nmeta) {
    store_with_meta(h, h1, PROTOCOL_BINARY_CMD_ADD_WITH_META, key, keylen, val,
                    vallen, vb, itemMeta, cas_for_add, options, datatype,
                    cookie, nmeta);
}

void return_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                 const size_t keylen, const char *val, const size_t vallen,
                 const uint32_t vb, const uint64_t cas, const uint32_t flags,
                 const uint32_t exp, const uint32_t type, uint8_t datatype,
                 const void *cookie) {
    char ext[12];
    encodeExt(ext, type);
    encodeExt(ext + 4, flags);
    encodeExt(ext + 8, exp);
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_RETURN_META, vb, cas, ext, 12, key, keylen, val,
                       vallen, datatype);
    check(h1->unknown_command(h, cookie, pkt, add_response_ret_meta, testHarness.doc_namespace)
              == ENGINE_SUCCESS, "Expected to be able to store ret meta");
    cb_free(pkt);
}

void set_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas, const uint32_t flags,
                  const uint32_t exp, uint8_t datatype, const void *cookie) {
    return_meta(h, h1, key, keylen, val, vallen, vb, cas, flags, exp,
                SET_RET_META, datatype, cookie);
}

void add_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas, const uint32_t flags,
                  const uint32_t exp, uint8_t datatype, const void *cookie) {
    return_meta(h, h1, key, keylen, val, vallen, vb, cas, flags, exp,
                ADD_RET_META, datatype, cookie);
}

void del_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const uint32_t vb, const uint64_t cas,
                  const void *cookie) {
    return_meta(h, h1, key, keylen, NULL, 0, vb, cas, 0, 0,
                DEL_RET_META, 0x00, cookie);
}

void disable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to disable data traffic");
    cb_free(pkt);
}

void enable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to send data traffic command to the server");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to enable data traffic");
    cb_free(pkt);
}

void start_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_START_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error starting persistence.");
    cb_free(pkt);
}

void stop_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isPersistentBucket(h, h1)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    useconds_t sleepTime = 128;
    while (true) {
        if (get_str_stat(h, h1, "ep_flusher_state", 0) == "running") {
            break;
        }
        decayingSleep(&sleepTime);
    }

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to stop persistence.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Error stopping persistence.");
    cb_free(pkt);
}

ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const void *cookie, ENGINE_STORE_OPERATION op,
                        const char *key, const char *value, item **outitem,
                        uint64_t casIn, uint16_t vb, uint32_t exp,
                        uint8_t datatype, DocumentState docState) {
    return storeCasVb11(h, h1, cookie, op, key, value, strlen(value),
                        9258, outitem, casIn, vb, exp, datatype, docState);
}

ENGINE_ERROR_CODE storeCasOut(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const void *cookie, const uint16_t vb,
                              const std::string& key, const std::string& value,
                              const protocol_binary_datatype_t datatype,
                              item*& out_item, uint64_t& out_cas,
                              DocumentState docState) {
    item *it = NULL;
    checkeq(ENGINE_SUCCESS,
            allocate(h, h1, NULL, &it, key, value.size(), 0, 0, datatype, vb),
           "Allocation failed.");
    item_info info;
    check(h1->get_item_info(h, h1, it, &info), "Unable to get item_info");
    memcpy(info.value[0].iov_base, value.data(), value.size());
    ENGINE_ERROR_CODE res = h1->store(h, NULL, it, &out_cas,
                                      OPERATION_SET, docState);
    h1->release(h, NULL, it);
    return res;
}

ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie, ENGINE_STORE_OPERATION op,
                               const char *key, const char *value, size_t vlen,
                               uint32_t flags, item **outitem, uint64_t casIn,
                               uint16_t vb, uint32_t exp, uint8_t datatype,
                               DocumentState docState) {
    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = allocate(h, h1, cookie, &it, key, vlen, flags, exp,
                                    datatype, vb);
    if (rv != ENGINE_SUCCESS) {
        return rv;
    }

    item_info info;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    cb_assert(info.value[0].iov_len == vlen);
    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, docState);

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

    check(h1->unknown_command(h, NULL, request, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to call touch");
    cb_free(request);
}

ENGINE_ERROR_CODE unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const void* cookie, const char* key,
                      uint16_t vb, uint64_t cas) {

    return h1->unlock(h, cookie, DocKey(key, testHarness.doc_namespace),
                      vb, cas);
}

void compact_db(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                     const uint16_t vbucket_id,
                     const uint16_t db_file_id,
                     const uint64_t purge_before_ts,
                     const uint64_t purge_before_seq,
                     const uint8_t  drop_deletes) {
    protocol_binary_request_compact_db req;
    memset(&req, 0, sizeof(req));
    req.message.body.purge_before_ts  = htonll(purge_before_ts);
    req.message.body.purge_before_seq = htonll(purge_before_seq);
    req.message.body.drop_deletes     = drop_deletes;

    std::string backend = get_str_stat(h, h1, "ep_backend");
    uint16_t vbid;

    if (backend == "forestdb") {
        req.message.body.db_file_id = db_file_id;
        vbid = 0xFFFF;
    } else {
        vbid = vbucket_id;
    }

    const char *args = (const char *)&(req.message.body);
    uint32_t argslen = 24;

    protocol_binary_request_header *pkt =
        createPacket(PROTOCOL_BINARY_CMD_COMPACT_DB, vbid, 0, args, argslen,  NULL, 0,
                     NULL, 0);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to request compact vbucket");
    cb_free(pkt);
}

void vbucketDelete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                   const char* args) {
    uint32_t argslen = args ? strlen(args) : 0;
    protocol_binary_request_header *pkt =
        createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, vb, 0, NULL, 0, NULL, 0,
                     args, argslen);
    check(h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    cb_free(pkt);
}

ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char* key, uint16_t vbucket) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = get(h, h1, NULL, &i, key, vbucket);
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
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }

    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        if (vals.find(vbstr) == vals.end()) {
            return true;
        }

        std::cerr << "Expected bucket missing, got " <<
                vals[vbstr] << std::endl;
    }
    return false;
}

bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                          vbucket_state_t expected, bool mute) {
    protocol_binary_request_header *pkt;
    pkt = createPacket(PROTOCOL_BINARY_CMD_GET_VBUCKET, vb, 0);

    ENGINE_ERROR_CODE errcode = h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace);
    cb_free(pkt);
    if (errcode != ENGINE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Error code when getting vbucket %d\n", errcode);
        }
        return false;
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Last protocol status was %d (%s)\n",
                    last_status.load(),
                    last_body.size() > 0 ? last_body.c_str() : "unknown");
        }
        return false;
    }

    vbucket_state_t state;
    memcpy(&state, last_body.data(), sizeof(state));
    state = static_cast<vbucket_state_t>(ntohl(state));
    return state == expected;
}

void sendDcpAck(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                const void* cookie, protocol_binary_command opcode,
                protocol_binary_response_status status, uint32_t opaque) {
    protocol_binary_response_header pkt;
    pkt.response.magic = PROTOCOL_BINARY_RES;
    pkt.response.opcode = opcode;
    pkt.response.status = htons(status);
    pkt.response.opaque = opaque;

    check(h1->dcp.response_handler(h, cookie, &pkt) == ENGINE_SUCCESS,
          "Expected success");
}

class engine_error : public std::exception {
public:
    engine_error(ENGINE_ERROR_CODE code_)
        : code(code_) {}

    virtual const char* what() const NOEXCEPT {
        return "engine_error";
    }

    ENGINE_ERROR_CODE code;
};

/* The following set of functions get a given stat as the specified type
 * (int, float, unsigned long, string, bool, etc).
 * If the engine->get_stats() call fails, throws a engine_error exception.
 * If the given statname doesn't exist under the given statname, throws a
 * std::out_of_range exception.
 */
template<>
int get_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
             const char *statname, const char *statkey) {
    return std::stoi(get_str_stat(h, h1, statname, statkey));
}
template<>
uint64_t get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                  const char* statname, const char* statkey) {
    return std::stoull(get_str_stat(h, h1, statname, statkey));
}

template<>
bool get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
              const char* statname, const char* statkey) {
    return get_str_stat(h, h1, statname, statkey) == "true";
}

template<>
std::string get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                     const char* statname, const char* statkey) {
    std::lock_guard<std::mutex> lh(get_stat_context.mutex);

    get_stat_context.requested_stat_name = statname;
    get_stat_context.actual_stat_value.clear();

    ENGINE_ERROR_CODE err = h1->get_stats(h, NULL, statkey,
                                          statkey == NULL ? 0 : strlen(statkey),
                                          add_individual_stat);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }

    if (get_stat_context.actual_stat_value.empty()) {
        throw std::out_of_range(std::string("Failed to find requested statname '") +
                                statname + "'");
    }

    // Here we are explictly forcing a copy of the object to work
    // around std::string copy-on-write data-race issues seen on some
    // versions of libstdc++ - see MB-18510 / MB-19688.
    return std::string(get_stat_context.actual_stat_value.begin(),
                       get_stat_context.actual_stat_value.end());
}

/// Backward-compatible functions (encode type name in function name).
int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
             const char *statkey) {
    return get_stat<int>(h, h1, statname, statkey);
}

float get_float_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                     const char *statkey) {
    return std::stof(get_str_stat(h, h1, statname, statkey));
}

uint32_t get_ul_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                      const char *statkey) {
    return std::stoul(get_str_stat(h, h1, statname, statkey));
}

uint64_t get_ull_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                      const char *statkey) {
    return get_stat<uint64_t>(h, h1, statname, statkey);
}

std::string get_str_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *statname, const char *statkey) {
    return get_stat<std::string>(h, h1, statname, statkey);
}

bool get_bool_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                   const char *statkey) {
    const auto s = get_str_stat(h, h1, statname, statkey);

    if (s == "true") {
        return true;
    } else if (s == "false") {
        return false;
    } else {
        throw std::invalid_argument("Unable to convert string '" + s + "' to type bool");
    }
}

/* Fetches the value for a given statname in the given statkey set.
 * @return te value of statname, or default_value if that statname was not
 * found.
 */
int get_int_stat_or_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            int default_value, const char *statname,
                            const char *statkey) {
    try {
        return get_int_stat(h, h1, statname, statkey);
    } catch (std::out_of_range&) {
        return default_value;
    }
}

uint64_t get_histo_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey,
                        const Histo_stat_info histo_info)
{
    std::lock_guard<std::mutex> lh(get_stat_context.mutex);

    get_stat_context.histogram_stat_int_value = new HistogramStats<int>();
    get_histo_stat(h, h1, statname, statkey);

    /* Get the necessary info from the histogram */
    uint64_t ret_val = 0;
    switch (histo_info) {
        case Histo_stat_info::TOTAL_COUNT:
            ret_val = get_stat_context.histogram_stat_int_value->total();
            break;
        case Histo_stat_info::NUM_BINS:
            ret_val =
                    static_cast<uint64_t>(get_stat_context.
                                          histogram_stat_int_value->num_bins());
            break;
    }

    delete get_stat_context.histogram_stat_int_value;
    return ret_val;
}

static void get_histo_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  const char *statname, const char *statkey)
{
    get_stat_context.requested_stat_name = statname;
    /* Histo stats for tasks are append as task_name_START,END.
       Hence append _ */
    get_stat_context.requested_stat_name.append("_");

    ENGINE_ERROR_CODE err = h1->get_stats(h, NULL, statkey,
                                          statkey == NULL ? 0 : strlen(statkey),
                                          add_individual_histo_stat);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }

    return;
}

statistic_map get_all_stats(ENGINE_HANDLE *h,ENGINE_HANDLE_V1 *h1,
                            const char *statset) {
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }
    ENGINE_ERROR_CODE err = h1->get_stats(h, NULL, statset,
                                          statset == NULL ? 0 : strlen(statset),
                                          add_stats);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }

    std::lock_guard<std::mutex> lh(vals_mutex);
    return vals;
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

void wait_for_stat_to_be_gte(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char *stat, int final,
                             const char* stat_key,
                             const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be greater or equal than", stat,
                                         stat_key, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, h1, stat, stat_key);
        if (current >= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_stat_to_be_lte(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char *stat, int final,
                             const char* stat_key,
                             const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be less than or equal to", stat,
                                         stat_key, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, h1, stat, stat_key);
        if (current <= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_expired_items_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  int final,
                                  const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be", "expired items",
                                         NULL, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, h1, "ep_expired_access") +
                       get_int_stat(h, h1, "ep_expired_compactor") +
                       get_int_stat(h, h1, "ep_expired_pager");
        if (current == final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_memory_usage_below(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 int mem_threshold,
                                 const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be below", "mem_used", NULL,
                                         mem_threshold,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, h1, "mem_used");
        if (current <= mem_threshold) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

bool wait_for_warmup_complete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isWarmupEnabled(h, h1)) {
        return true;
    }

    useconds_t sleepTime = 128;
    do {
        try {
            if (get_str_stat(h, h1, "ep_warmup_thread", "warmup") == "complete") {
                return true;
            }
        } catch (engine_error&) {
            // If the stat call failed then the warmup stats group no longer
            // exists and hence warmup is complete.
            return true;
        }
        decayingSleep(&sleepTime);
    } while(true);
}

void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!isPersistentBucket(h, h1)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    wait_for_stat_to_be(h, h1, "ep_queue_size", 0);

    // We also need to to wait for any outstanding flushes to disk to
    // complete - specifically so when in full eviction mode we have
    // waited for the item counts in each vBucket to be synced with
    // the number of items on disk. See
    // EPBucket::commit().
    wait_for_stat_to_be(h, h1, "ep_flusher_todo", 0);
}

void wait_for_rollback_to_finish(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_rollback_count") == 0) {
        decayingSleep(&sleepTime);
    }
}

void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const char *key, const char *val,
                              uint16_t vbucketId) {

    item *i = NULL;
    int commitNum = 0;
    if (isPersistentBucket(h, h1)) {
         commitNum = get_int_stat(h, h1, "ep_commit_num");
    }
    check(ENGINE_SUCCESS == store(h, h1, NULL, OPERATION_SET, key, val, &i, 0,
                                  vbucketId),
            "Failed to store an item.");

    if (isPersistentBucket(h, h1)) {
        // Wait for persistence...
        wait_for_flusher_to_settle(h, h1);
        wait_for_stat_change(h, h1, "ep_commit_num", commitNum);
    }
    h1->release(h, NULL, i);
}

void set_degraded_mode(ENGINE_HANDLE *h,
                       ENGINE_HANDLE_V1 *h1,
                       const void* cookie,
                       bool enable)
{
    protocol_binary_request_header *pkt;
    if (enable) {
        pkt = createPacket(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC, 0, 0);
    } else {
        pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC, 0, 0);
    }

    ENGINE_ERROR_CODE errcode = h1->unknown_command(h, NULL, pkt, add_response, testHarness.doc_namespace);
    cb_free(pkt);
    if (errcode != ENGINE_SUCCESS) {
        std::cerr << "Failed to set degraded mode to " << enable
                  << ". api call return engine code: " << errcode << std::endl;
        cb_assert(false);
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        std::cerr << "Failed to set degraded mode to " << enable
                  << ". protocol code: " << last_status << std::endl;
        if (last_body.size() > 0) {
            std::cerr << "\tBody: [" << last_body << "]" << std::endl;
        }

        cb_assert(false);
    }
}

bool abort_msg(const char *expr, const char *msg, const char *file, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            file, line, msg, expr);
    abort();
}

/* Helper function to validate the return from store() */
void validate_store_resp(ENGINE_ERROR_CODE ret, int& num_items)
{
    switch (ret) {
        case ENGINE_SUCCESS:
            num_items++;
            break;
        case ENGINE_TMPFAIL:
            /* TMPFAIL means we are hitting high memory usage; retry */
            break;
        default:
            check(false,
                  ("write_items_upto_mem_perc: Unexpected response from "
                   "store(): " + std::to_string(ret)).c_str());
            break;
    }
}

void write_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int num_items,
                 int start_seqno, const char *key_prefix, const char *value,
                 uint32_t expiry, uint16_t vb)
{
    int j = 0;
    while (1) {
        if (j == num_items) {
            break;
        }
        item *i = nullptr;
        std::string key(key_prefix + std::to_string(j + start_seqno));
        ENGINE_ERROR_CODE ret = store(h, h1, nullptr, OPERATION_SET,
                                      key.c_str(), value, &i, /*cas*/0, vb,
                                      expiry);
        h1->release(h, nullptr, i);
        validate_store_resp(ret, j);
    }
}

/* Helper function to write unique items starting from keyXX until memory usage
   hits "mem_thresh_perc" (XX is start_seqno) */
int write_items_upto_mem_perc(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int mem_thresh_perc, int start_seqno,
                              const char *key_prefix, const char *value)
{
    float maxSize = static_cast<float>(get_int_stat(h, h1, "ep_max_size",
                                                    "memory"));
    float mem_thresh = static_cast<float>(mem_thresh_perc) / (100.0);
    int num_items = 0;
    while (1) {
        /* Load items into server until mem_thresh_perc of the mem quota
         is used. Getting stats is expensive, only check every 100
         iterations. */
        if ((num_items % 100) == 0) {
            float memUsed = float(get_int_stat(h, h1, "mem_used", "memory"));
            if (memUsed > (maxSize * mem_thresh)) {
                /* Persist all items written so far. */
                break;
            }
        }
        item *itm = nullptr;
        std::string key("key" + std::to_string(num_items + start_seqno));
        ENGINE_ERROR_CODE ret = store(h, h1, nullptr, OPERATION_SET,
                                      key.c_str(), "somevalue", &itm);
        h1->release(h, nullptr, itm);
        validate_store_resp(ret, num_items);
    }
    return num_items;
}


uint64_t get_CAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                 const std::string& key) {
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            get(h, h1, nullptr, &i, key, 0),
            "get_CAS: Failed to get key");

    item_info info;
    check(h1->get_item_info(h, NULL, i, &info),
          "get_CAS: Failed to get item info for key");
    h1->release(h, NULL, i);

    return info.cas;
}

ENGINE_ERROR_CODE allocate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           const void* cookie, item** outitem,
                           const std::string& key, size_t nbytes, int flags,
                           rel_time_t exptime, uint8_t datatype, uint16_t vb) {
    return h1->allocate(h, cookie, outitem,
                        DocKey(key, testHarness.doc_namespace),
                        nbytes, flags, exptime, datatype, vb);
}

ENGINE_ERROR_CODE get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const void* cookie, item** item, const std::string& key,
                      uint16_t vb, DocumentState docState) {
    return h1->get(h, cookie, item, DocKey(key, testHarness.doc_namespace),
                   vb, docState);
}
