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
#include <relaxed_atomic.h>

#include <map>
#include <string>

#include "ep_types.h"
#include "ext_meta_parser.h"
#include "item.h"

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __FILE__, __LINE__))

extern "C" bool abort_msg(const char *expr, const char *msg,
                          const char *file, int line) CB_ATTR_NORETURN;

#ifdef __cplusplus
extern "C" {
#endif

bool add_response(const void *key, uint16_t keylen, const void *ext,
                  uint8_t extlen, const void *body, uint32_t bodylen,
                  uint8_t datatype, uint16_t status, uint64_t cas,
                  const void *cookie);

void add_stats(const char* key,
               const uint16_t klen,
               const char* val,
               const uint32_t vlen,
               gsl::not_null<const void*> cookie);

ENGINE_ERROR_CODE vb_map_response(const void *cookie, const void *map,
                                  size_t mapsize);

#ifdef __cplusplus
}
#endif

const uint8_t dcp_stream_end_resp_base_msg_bytes = 28;
const uint8_t dcp_snapshot_marker_base_msg_bytes = 44;
const uint8_t dcp_mutation_base_msg_bytes = 55;
const uint8_t dcp_deletion_base_msg_bytes = 42;

extern std::atomic<protocol_binary_response_status> last_status;
extern std::string last_key;
extern std::string last_body;
extern std::string last_ext;
extern bool dump_stats;

// TODO: make `vals` non-public
extern std::map<std::string, std::string> vals;

extern std::atomic<uint64_t> last_cas;
extern std::atomic<uint8_t> last_datatype;
extern std::atomic<uint64_t> last_uuid;
extern std::atomic<uint64_t> last_seqno;
extern bool last_deleted_flag;
extern std::atomic<uint8_t> last_conflict_resolution_mode;
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
extern Couchbase::RelaxedAtomic<uint64_t> dcp_last_byseqno;
extern uint64_t dcp_last_revseqno;
extern uint64_t dcp_last_snap_start_seqno;
extern uint64_t dcp_last_snap_end_seqno;
extern std::string dcp_last_meta;
extern std::string dcp_last_value;
extern std::string dcp_last_key;
extern vbucket_state_t dcp_last_vbucket_state;
extern protocol_binary_datatype_t dcp_last_datatype;

/* This is an enum class to indicate what stats are required from the
   HistogramStats. */
enum class Histo_stat_info {
    /* Total number of samples across all the bins in the histogram stat */
    TOTAL_COUNT,
    /* Number of bins in the histogram stat */
    NUM_BINS
};

/**
 * Helper class used when waiting on statistics to reach a certain value -
 * aggregates how long we have been waiting and aborts if the maximum wait time
 * is exceeded.
 */
template <typename T>
class WaitTimeAccumulator
{
public:
    WaitTimeAccumulator(const char* compare_name,
                        const char* stat_, const char* stat_key,
                        const T final_, const time_t wait_time_in_secs)
        : compareName(compare_name),
          stat(stat_),
          statKey(stat_key),
          final(final_),
          maxWaitTime(wait_time_in_secs * 1000 * 1000),
          totalSleepTime(0) {}

    void incrementAndAbortIfLimitReached(T last_value,
                                         const useconds_t sleep_time)
    {
        totalSleepTime += sleep_time;
        if (totalSleepTime >= maxWaitTime) {
            std::cerr << "Exceeded maximum wait time of " << maxWaitTime
                    << "us waiting for stat '" << stat;
            if (statKey != NULL) {
                std::cerr << "(" << statKey << ")";
            }
            std::cerr << "' " << compareName << " " << final << " (last value:"
            << last_value << ") - aborting." << std::endl;
            abort();
        }
    }

private:
    const char* compareName;
    const char* stat;
    const char* statKey;
    const T final;
    const useconds_t maxWaitTime;
    useconds_t totalSleepTime;
};

/**
 * Raw meta-data allowing 64-bit rev-seqno
 */
class RawItemMetaData {
public:
    RawItemMetaData()
        : cas(0), revSeqno(DEFAULT_REV_SEQ_NUM), flags(0), exptime(0) {
    }

    RawItemMetaData(uint64_t c, uint64_t s, uint32_t f, time_t e)
        : cas(c),
          revSeqno(s == 0 ? DEFAULT_REV_SEQ_NUM : s),
          flags(f),
          exptime(e) {
    }

    uint64_t cas;
    uint64_t revSeqno;
    uint32_t flags;
    time_t exptime;
};

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
                      uint64_t cas, uint16_t vbucket,
                      const void* cookie = nullptr);

ENGINE_ERROR_CODE del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                      uint64_t* cas, uint16_t vbucket, const void* cookie,
                      mutation_descr_t* mut_info);

/** Simplified version of store for handling the common case of performing
 * a delete with a value.
 */
ENGINE_ERROR_CODE delete_with_value(
        ENGINE_HANDLE* h,
        ENGINE_HANDLE_V1* h1,
        const void* cookie,
        uint64_t cas,
        const char* key,
        cb::const_char_buffer value,
        cb::mcbp::Datatype datatype = cb::mcbp::Datatype::Raw);

void disable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void enable_traffic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
               uint16_t vbucketId = 0, const char *msg = NULL,
               bool expectError = false);
cb::EngineErrorItemPair gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            const char* key, uint16_t vb, uint32_t exp);
bool get_item_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item_info *info,
                   const char* key, uint16_t vb = 0);
bool get_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item *i,
             std::string &key);

cb::EngineErrorItemPair getl(ENGINE_HANDLE* h,
                             ENGINE_HANDLE_V1* h1,
                             const void* cookie,
                             const char* key,
                             uint16_t vb,
                             uint32_t lock_timeout);

void get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* key,
                 uint16_t vb);
ENGINE_ERROR_CODE observe(ENGINE_HANDLE* h,
                          ENGINE_HANDLE_V1* h1,
                          std::map<std::string, uint16_t> obskeys);
ENGINE_ERROR_CODE observe_seqno(ENGINE_HANDLE* h,
                                ENGINE_HANDLE_V1* h1,
                                uint16_t vb_id,
                                uint64_t uuid);

protocol_binary_request_header*
prepare_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                    vbucket_state_t state, bool makeinvalidkey = false) CB_MUST_USE_RESULT;

bool set_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, protocol_binary_engine_param_t paramtype,
               const char *param, const char *val, uint16_t vb = 0);
bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       uint16_t vb, vbucket_state_t state);
bool get_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                       vbucket_state_t state, const void *cookie);
void verify_all_vb_seqnos(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                          int vb_start, int vb_end);
void start_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void stop_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

/**
 * Store an item.
 *
 * @param outitem If non-null, address of the stored item is saved here.
 * @return
 */
ENGINE_ERROR_CODE store(ENGINE_HANDLE* h,
                        ENGINE_HANDLE_V1* h1,
                        const void* cookie,
                        ENGINE_STORE_OPERATION op,
                        const char* key,
                        const char* value,
                        item** outitem = nullptr,
                        uint64_t casIn = 0,
                        uint16_t vb = 0,
                        uint32_t exp = 3600,
                        uint8_t datatype = 0x00,
                        DocumentState docState = DocumentState::Alive);

cb::EngineErrorItemPair allocate(ENGINE_HANDLE* h,
                                 ENGINE_HANDLE_V1* h1,
                                 const void* cookie,
                                 const std::string& key,
                                 size_t nbytes,
                                 int flags,
                                 rel_time_t exptime,
                                 uint8_t datatype,
                                 uint16_t vb);

cb::EngineErrorItemPair get(
        ENGINE_HANDLE* h,
        ENGINE_HANDLE_V1* h1,
        const void* cookie,
        const std::string& key,
        uint16_t vb,
        DocStateFilter documentStateFilter = DocStateFilter::Alive);

/* Stores the specified document; returning the new CAS value via
 * {out_cas}.
 */
ENGINE_ERROR_CODE storeCasOut(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const void *cookie, const uint16_t vb,
                              const std::string& key, const std::string& value,
                              const protocol_binary_datatype_t datatype,
                              item*& out_item, uint64_t& out_cas,
                              DocumentState docState = DocumentState::Alive);

cb::EngineErrorItemPair storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie, ENGINE_STORE_OPERATION op,
                               const char *key, const char *value, size_t vlen,
                               uint32_t flags, uint64_t casIn,
                               uint16_t vb, uint32_t exp = 3600,
                               uint8_t datatype = 0x00,
                               DocumentState docState = DocumentState::Alive);
ENGINE_ERROR_CODE touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char* key, uint16_t vb, uint32_t exp);
ENGINE_ERROR_CODE unl(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                      const void* cookie, const char* key,
                      uint16_t vb, uint64_t cas = 0);
ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char* key, uint16_t vbucket = 0);

/**
 * Attempts to fetch the given key. On success returns ENGINE_SUCCESS and the
 * value, on failure returns the reason and an empty string.
 */
std::pair<ENGINE_ERROR_CODE, std::string> get_value(ENGINE_HANDLE* h,
                                                    ENGINE_HANDLE_V1* h1,
                                                    const void* cookie,
                                                    const char* key,
                                                    uint16_t vbucket,
                                                    DocStateFilter state);

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
ENGINE_ERROR_CODE seqnoPersistence(ENGINE_HANDLE* h,
                                   ENGINE_HANDLE_V1* h1,
                                   const void* cookie,
                                   uint16_t vbucket,
                                   uint64_t seqno);

// Stats Operations
int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                 const char *statkey = NULL);
float get_float_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                     const char *statkey = NULL);
uint32_t get_ul_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                     const char *statname, const char *statkey = NULL);
uint64_t get_ull_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *statname,
                      const char *statkey = NULL);
std::string get_str_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *statname, const char *statkey = NULL);
bool get_bool_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                   const char *statname, const char *statkey = NULL);

ENGINE_ERROR_CODE get_stats(gsl::not_null<ENGINE_HANDLE*> h,
                            cb::const_char_buffer key,
                            ADD_STAT callback);

/* This is used to get stat info specified by 'histo_info' from histogram of
 * "statname" which is got by running stats on "statkey"
 */
uint64_t get_histo_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey,
                        const Histo_stat_info histo_info);

typedef std::map<std::string, std::string> statistic_map;

/* Returns a map of all statistics for the given statistic set.
 * @param statset The set of statistics to fetch. May be nullptr, in which case
 *                the default set will be returned.
 */
statistic_map get_all_stats(ENGINE_HANDLE *h,ENGINE_HANDLE_V1 *h1,
                            const char *statset = nullptr);

// Returns the value of the given stat, or the default value if the stat isn't
// present.
int get_int_stat_or_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            int default_value, const char *statname,
                            const char *statkey = NULL);

/**
 * Templated function prototype to return a stat of the given type.
 * Should replace above uses of get_XXX_stat with this.
 */
template <typename T>
T get_stat(ENGINE_HANDLE* h,
           ENGINE_HANDLE_V1* h1,
           const char* statname,
           const char* statkey = nullptr);

// Explicit template instantiations declarations of get_stat<T>
template<>
std::string get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                     const char *statname, const char *statkey);
template<>
int get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
             const char *statname, const char *statkey);

template<>
uint64_t get_stat(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                  const char *statname, const char *statkey);

void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int exp,
                       const char *msg);
template<typename T>
void wait_for_stat_change(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                          const char* stat, T initial,
                          const char* stat_key = nullptr,
                          const time_t max_wait_time_in_secs = 60);

template<typename T>
void wait_for_stat_to_be(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1,
                         const char* stat, T final,
                         const char* stat_key = nullptr,
                         const time_t max_wait_time_in_secs = 60);

void wait_for_stat_to_be_gte(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const char *stat, int final,
                             const char* stat_key = NULL,
                             const time_t max_wait_time_in_secs = 60);

template <typename T>
void wait_for_stat_to_be_lte(ENGINE_HANDLE* h,
                             ENGINE_HANDLE_V1* h1,
                             const char* stat,
                             T final,
                             const char* stat_key = NULL,
                             const time_t max_wait_time_in_secs = 60);

void wait_for_expired_items_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  int final,
                                  const time_t max_wait_time_in_secs = 60);
bool wait_for_warmup_complete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_item_compressor_to_settle(ENGINE_HANDLE* h, ENGINE_HANDLE_V1* h1);
void wait_for_rollback_to_finish(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              const char *key, const char *val,
                              uint16_t vbucketId = 0);

void wait_for_memory_usage_below(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 int mem_threshold,
                                 const time_t max_wait_time_in_secs = 60);

/**
 * Repeat a functor returning bool upto max repeat times, sleeping
 * inbetween for sleep_time. return True if the functor returns
 * true or False if the functor did not succeed at all
 */
bool repeat_till_true(std::function<bool()> functor,
                      uint16_t max_repeat = 50,
                      std::chrono::microseconds sleepTime =
                              std::chrono::microseconds(1000 * 100));

// VBucket operations
ENGINE_ERROR_CODE vbucketDelete(ENGINE_HANDLE* h,
                                ENGINE_HANDLE_V1* h1,
                                uint16_t vb,
                                const char* args = NULL);

void compact_db(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                const uint16_t vbid,
                const uint16_t db_file_id,
                const uint64_t purge_before_ts,
                const uint64_t purge_before_seq,
                const uint8_t  drop_deletes);

// XDCR Operations
void set_drift_counter_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             int64_t initialDrift);

bool get_meta(ENGINE_HANDLE* h,
              ENGINE_HANDLE_V1* h1,
              const char* key,
              const void* cookie = nullptr);

bool get_meta(ENGINE_HANDLE* h,
              ENGINE_HANDLE_V1* h1,
              const char* key,
              cb::EngineErrorMetadataPair& out,
              const void* cookie = nullptr);

void set_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_set, uint32_t options = 0,
                   uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                   const void* cookie = nullptr,
                   const std::vector<char>& nmeta = {});

void add_with_meta(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *key,
                   const size_t keylen, const char *val, const size_t vallen,
                   const uint32_t vb, ItemMetaData *itemMeta,
                   uint64_t cas_for_add = 0, uint32_t options = 0,
                   uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                   const void* cookie = nullptr,
                   const std::vector<char>& nmeta = {});

void del_with_meta(ENGINE_HANDLE* h,
                   ENGINE_HANDLE_V1* h1,
                   const char* key,
                   const size_t keylen,
                   const uint32_t vb,
                   ItemMetaData* itemMeta,
                   uint64_t cas_for_delete = 0,
                   uint32_t options = 0,
                   const void* cookie = nullptr,
                   const std::vector<char>& nmeta = {},
                   protocol_binary_datatype_t datatype = 0,
                   const std::vector<char>& value = {} /*optional value*/);

// This version takes a RawItemMetaData allowing for 64-bit rev-seqno tests
void del_with_meta(ENGINE_HANDLE* h,
                   ENGINE_HANDLE_V1* h1,
                   const char* key,
                   const size_t keylen,
                   const uint32_t vb,
                   RawItemMetaData* itemMeta,
                   uint64_t cas_for_delete = 0,
                   uint32_t options = 0,
                   const void* cookie = nullptr,
                   const std::vector<char>& nmeta = {},
                   protocol_binary_datatype_t datatype = 0,
                   const std::vector<char>& value = {} /*optional value*/);

ENGINE_ERROR_CODE set_ret_meta(ENGINE_HANDLE* h,
                               ENGINE_HANDLE_V1* h1,
                               const char* key,
                               const size_t keylen,
                               const char* val,
                               const size_t vallen,
                               const uint32_t vb,
                               const uint64_t cas = 0,
                               const uint32_t flags = 0,
                               const uint32_t exp = 0,
                               uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                               const void* cookie = nullptr);

ENGINE_ERROR_CODE add_ret_meta(ENGINE_HANDLE* h,
                               ENGINE_HANDLE_V1* h1,
                               const char* key,
                               const size_t keylen,
                               const char* val,
                               const size_t vallen,
                               const uint32_t vb,
                               const uint64_t cas = 0,
                               const uint32_t flags = 0,
                               const uint32_t exp = 0,
                               uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                               const void* cookie = nullptr);

ENGINE_ERROR_CODE del_ret_meta(ENGINE_HANDLE* h,
                               ENGINE_HANDLE_V1* h1,
                               const char* key,
                               const size_t keylen,
                               const uint32_t vb,
                               const uint64_t cas = 0,
                               const void* cookie = nullptr);

void set_degraded_mode(ENGINE_HANDLE *h,
                       ENGINE_HANDLE_V1 *h1,
                       const void* cookie,
                       bool enable);

// Fetches the CAS of the specified key.
uint64_t get_CAS(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                 const std::string& key);

/**
 * Helper function to write unique "num_items" starting from {key_prefix}XX,
 * where XX is start_seqno.
 * @param num_items Number of items to write
 * @param start_seqno Sequence number to start from (inclusive).
 * @param key_prefix Prefix for key names
 * @param value Value for each item
 * @param expiry Expiration time for each item.
 * @param vb vbucket to use, default to 0
 * @param docState document state to write
 */
void write_items(ENGINE_HANDLE* h,
                 ENGINE_HANDLE_V1* h1,
                 int num_items,
                 int start_seqno = 0,
                 const char* key_prefix = "key",
                 const char* value = "data",
                 uint32_t expiry = 0,
                 uint16_t vb = 0,
                 DocumentState docState = DocumentState::Alive);

/* Helper function to write unique items starting from keyXX until memory usage
   hits "mem_thresh_perc" (XX is start_seqno) */
int write_items_upto_mem_perc(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int mem_thresh_perc, int start_seqno = 0,
                              const char *key_prefix = "key",
                              const char *value = "data");

template<typename T>
inline void wait_for_stat_change(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *stat, T initial,
                                 const char *stat_key,
                                 const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<T> accumulator("to change from", stat, stat_key,
                                         initial, max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, h1, stat, stat_key);
        if (current != initial) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

template<typename T>
void wait_for_stat_to_be(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                         const char *stat, T final, const char* stat_key,
                         const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<T> accumulator("to be", stat, stat_key, final,
                                       max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, h1, stat, stat_key);
        if (current == final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

template <typename T>
void wait_for_stat_to_be_lte(ENGINE_HANDLE* h,
                             ENGINE_HANDLE_V1* h1,
                             const char* stat,
                             T final,
                             const char* stat_key,
                             const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<T> accumulator("to be less than or equal to",
                                       stat,
                                       stat_key,
                                       final,
                                       max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, h1, stat, stat_key);
        if (current <= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

/**
 * Function that does an exponential wait for a 'val' to reach 'expected'
 *
 * @param val_description description for debug log purpose
 * @param val reference to the variable which is waited upon
 * @param expected final value of 'val'
 * @param max_wait_time_in_secs max wait time; default 60 seconds
 */
template <typename T>
void wait_for_val_to_be(const char* val_description,
                        T& val,
                        const T expected,
                        const time_t max_wait_time_in_secs = 60) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<T> accumulator(
            "to be", val_description, nullptr, expected, max_wait_time_in_secs);
    for (;;) {
        if (val == expected) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(val, sleepTime);
        decayingSleep(&sleepTime);
    }
}

/**
 * Check via the stats interface if full_eviction mode is enabled
 */
inline bool is_full_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return get_str_stat(h, h1, "ep_item_eviction_policy") == "full_eviction";
}

void reset_stats(gsl::not_null<ENGINE_HANDLE*> h);

#endif  // TESTS_EP_TEST_APIS_H_
