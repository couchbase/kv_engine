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

#ifndef TESTS_EP_TEST_APIS_H_
#define TESTS_EP_TEST_APIS_H_ 1

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include <map>
#include <string>

#include "ep-engine/command_ids.h"
#include "ext_meta_parser.h"
#include "item.h"

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __FILE__, __LINE__))

extern "C" bool abort_msg(const char *expr, const char *msg,
                          const char *file, int line);

#ifdef __cplusplus
extern "C" {
#endif

bool add_response(const void *key, uint16_t keylen, const void *ext,
                  uint8_t extlen, const void *body, uint32_t bodylen,
                  uint8_t datatype, uint16_t status, uint64_t cas,
                  const void *cookie);

void add_stats(const char *key, const uint16_t klen, const char *val,
               const uint32_t vlen, const void *cookie);

ENGINE_ERROR_CODE vb_map_response(const void *cookie, const void *map,
                                  size_t mapsize);

#ifdef __cplusplus
}
#endif

extern AtomicValue<protocol_binary_response_status> last_status;
extern std::string last_key;
extern std::string last_body;
extern bool dump_stats;
extern std::map<std::string, std::string> vals;
extern uint32_t last_bodylen;
extern AtomicValue<uint64_t> last_cas;
extern AtomicValue<uint8_t> last_datatype;
extern AtomicValue<uint64_t> last_uuid;
extern AtomicValue<uint64_t> last_seqno;
extern bool last_deleted_flag;
extern uint8_t last_conflict_resolution_mode;
extern ItemMetaData last_meta;

extern uint8_t dcp_last_op;
extern uint8_t dcp_last_status;
extern uint8_t dcp_last_nru;
extern uint16_t dcp_last_vbucket;
extern uint32_t dcp_last_opaque;
extern uint32_t dcp_last_flags;
extern uint32_t dcp_last_stream_opaque;
extern uint32_t dcp_last_locktime;
extern uint32_t dcp_last_packet_size;
extern uint64_t dcp_last_cas;
extern uint64_t dcp_last_start_seqno;
extern uint64_t dcp_last_end_seqno;
extern uint64_t dcp_last_vbucket_uuid;
extern uint64_t dcp_last_high_seqno;
extern uint64_t dcp_last_byseqno;
extern uint64_t dcp_last_revseqno;
extern uint64_t dcp_last_snap_start_seqno;
extern uint64_t dcp_last_snap_end_seqno;
extern std::string dcp_last_meta;
extern std::string dcp_last_value;
extern std::string dcp_last_key;
extern vbucket_state_t dcp_last_vbucket_state;


void decayingSleep(useconds_t *sleepTime);


protocol_binary_request_header* createPacket(uint8_t opcode,
                                             uint16_t vbid = 0,
                                             uint64_t cas = 0,
                                             const char *ext = NULL,
                                             uint8_t extlen = 0,
                                             const char *key = NULL,
                                             uint32_t keylen = 0,
                                             const char *val = NULL,
                                             uint32_t vallen = 0,
                                             uint8_t datatype = 0x00,
                                             const char *meta = NULL,
                                             uint16_t nmeta = 0) CB_MUST_USE_RESULT;

// Basic Operations
ENGINE_ERROR_CODE del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                      uint64_t cas, uint16_t vbucket, const void* cookie = NULL);
void disable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void enable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
               uint16_t vbucketId = 0, const char *msg = NULL,
               bool expectError = false);
size_t estimateVBucketMove(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           uint16_t vbid = 0, const char* tap_name = "");
void gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
         uint16_t vb, uint32_t exp, bool quiet = false);
bool get_item_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item_info *info,
                   const char* key, uint16_t vb = 0);
bool get_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item *i,
             std::string &key);
void getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key, uint16_t vb,
          uint32_t lock_timeout);
void get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
                 uint16_t vb);
void observe(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
             std::map<std::string, uint16_t> obskeys);
void observe_seqno(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb_id ,
                   uint64_t uuid);

protocol_binary_request_header*
prepare_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                    vbucket_state_t state, bool makeinvalidkey = false) CB_MUST_USE_RESULT;

bool set_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, protocol_binary_engine_param_t paramtype,
               const char *param, const char *val);
bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       uint16_t vb, vbucket_state_t state);
bool get_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       vbucket_state_t state, const void *cookie);
void verify_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          int vb_start, int vb_end);
void start_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void stop_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const void *cookie, ENGINE_STORE_OPERATION op,
                        const char *key, const char *value, item **outitem,
                        uint64_t casIn = 0, uint16_t vb = 0,
                        uint32_t exp = 3600, uint8_t datatype = 0x00);

/* Stores the specified document; returning the new CAS value via
 * {out_cas}.
 */
ENGINE_ERROR_CODE storeCasOut(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const void *cookie, const uint16_t vb,
                              const std::string& key, const std::string& value,
                              const protocol_binary_datatypes datatype,
                              item*& out_item, uint64_t& out_cas);

ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie, ENGINE_STORE_OPERATION op,
                               const char *key, const char *value, size_t vlen,
                               uint32_t flags, item **outitem, uint64_t casIn,
                               uint16_t vb, uint32_t exp = 3600,
                               uint8_t datatype = 0x00);
void touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
           uint16_t vb, uint32_t exp);
void unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
         uint16_t vb, uint64_t cas = 0);
ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char* key, uint16_t vbucket = 0);
bool verify_vbucket_missing(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            uint16_t vb);
bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                          vbucket_state_t expected, bool mute = false);

void sendDcpAck(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                const void* cookie, protocol_binary_command opcode,
                protocol_binary_response_status status, uint32_t opaque);

// Checkpoint Operations
void createCheckpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void extendCheckpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      uint32_t checkpoint_num);
ENGINE_ERROR_CODE checkpointPersistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        uint64_t checkpoint_id, uint16_t vb);
ENGINE_ERROR_CODE seqnoPersistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                   uint16_t vbucket, uint64_t seqno);

// Stats Operations
int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                 const char *statkey = NULL);
float get_float_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                     const char *statkey = NULL);
uint64_t get_ull_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                      const char *statkey = NULL);
std::string get_str_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *statname, const char *statkey = NULL);
bool get_bool_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                   const char *statname, const char *statkey = NULL);

// Returns the value of the given stat, or the default value if the stat isn't
// present.
int get_int_stat_or_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            int default_value, const char *statname,
                            const char *statkey = NULL);

void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int exp,
                       const char *msg);
void wait_for_stat_change(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          const char *stat, int initial,
                          const char *statkey = NULL,
                          const time_t wait_time = 60);
void wait_for_stat_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *stat,
                         int final, const char* stat_key = NULL,
                         const time_t wait_time = 60);
void wait_for_stat_to_be_gte(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char *stat, int final,
                             const char* stat_key = NULL,
                             const time_t wait_time = 60);
void wait_for_expired_items_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  int final, const time_t wait_time = 60);
void wait_for_str_stat_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char *stat, const char* final,
                             const char* stat_key,
                             const time_t wait_time = 60);
bool wait_for_warmup_complete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_rollback_to_finish(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const char *key, const char *val,
                              uint16_t vbucketId = 0);

void wait_for_memory_usage_below(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 int mem_threshold, const time_t wait_time = 60);

// Tap Operations
void changeVBFilter(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, std::string name,
                    std::map<uint16_t, uint64_t> &filtermap);

// VBucket operations
void vbucketDelete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint16_t vb,
                   const char* args = NULL);

void compact_db(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                const uint16_t vbid,
                const uint64_t purge_before_ts,
                const uint64_t purge_before_seq,
                const uint8_t  drop_deletes);

// XDCR Operations
void set_drift_counter_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             int64_t initialDrift, uint8_t timeSync);
void add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   bool skipConflictResolution = false,
                   uint8_t datatype = 0x00, bool includeExtMeta = false,
                   int64_t adjusted_time = 0);
bool get_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
              bool reqExtMeta = false);
void del_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const uint32_t vb,
                   ItemMetaData *itemMeta, uint64_t cas_for_delete = 0,
                   bool skipConflictResolution = false,
                   bool includeExtMeta = false,
                   int64_t adjustedTime = 0, uint8_t conflictResMode = 0,
                   const void *cookie = NULL);
void set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_set, bool skipConflictResolution = false,
                   uint8_t datatype = 0x00, bool includeExtMeta = false,
                   int64_t adjustedTime = 0, uint8_t conflictResMode = 0,
                   const void *cookie = NULL);
void return_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                 const size_t keylen, const char *val, const size_t vallen,
                 const uint32_t vb, const uint64_t cas, const uint32_t flags,
                 const uint32_t exp, const uint32_t type,
                 uint8_t datatype = 0x00, const void *cookie = NULL);
void set_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas = 0,
                  const uint32_t flags = 0, const uint32_t exp = 0,
                  uint8_t datatype = 0x00, const void *cookie = NULL);
void add_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const char *val, const size_t vallen,
                  const uint32_t vb, const uint64_t cas = 0,
                  const uint32_t flags = 0, const uint32_t exp = 0,
                  uint8_t datatype = 0x00, const void *cookie = NULL);
void del_ret_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                  const size_t keylen, const uint32_t vb,
                  const uint64_t cas = 0, const void *cookie = NULL);

// DCP Operations
void dcp_step(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const void* cookie);

void set_degraded_mode(ENGINE_HANDLE *h,
                       ENGINE_HANDLE_V1 *h1,
                       const void* cookie,
                       bool enable);

#endif  // TESTS_EP_TEST_APIS_H_
