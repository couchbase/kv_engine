/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/protocol_binary.h>
#include <relaxed_atomic.h>

#include <folly/portability/SysStat.h>
#include <iostream>
#include <map>
#include <string>

#include "ep_request_utils.h"
#include "ep_types.h"
#include "ext_meta_parser.h"
#include "item.h"

#define check(expr, msg)                               \
    do {                                               \
        if (!(expr)) {                                 \
            abort_msg(#expr, msg, __FILE__, __LINE__); \
        }                                              \
    } while (0)

void abort_msg(const char* expr, const char* msg, const char* file, int line);

bool add_response(std::string_view key,
                  std::string_view extras,
                  std::string_view body,
                  uint8_t datatype,
                  cb::mcbp::Status status,
                  uint64_t cas,
                  const void* cookie);

void add_stats(std::string_view key,
               std::string_view value,
               gsl::not_null<const void*> cookie);

const uint8_t dcp_stream_end_resp_base_msg_bytes = 28;
const uint8_t dcp_snapshot_marker_base_msg_bytes = 44;
const uint8_t dcp_mutation_base_msg_bytes = 55;
const uint8_t dcp_deletion_base_msg_bytes = 42;
const uint8_t dcp_deletion_v2_base_msg_bytes = 45;
const uint8_t dcp_expiration_base_msg_bytes = 44;

extern std::atomic<cb::mcbp::Status> last_status;
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
extern ItemMetaData last_meta;

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
                        const char* stat_,
                        const char* stat_key,
                        const T final_,
                        const std::chrono::seconds wait_time_in_secs)
        : compareName(compare_name),
          stat(stat_),
          statKey(stat_key),
          final(final_),
          maxWaitTime(wait_time_in_secs),
          totalSleepTime(0) {
    }

    void incrementAndAbortIfLimitReached(
            T last_value, const std::chrono::microseconds sleep_time) {
        totalSleepTime += sleep_time;
        if (totalSleepTime >= maxWaitTime) {
            std::cerr << "Exceeded maximum wait time of " << maxWaitTime.count()
                      << "us waiting for stat '" << stat;
            if (statKey != nullptr) {
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
    const std::chrono::microseconds maxWaitTime;
    std::chrono::microseconds totalSleepTime;
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

/**
 * RAII-style class which marks the couchstore file in the given directory
 * inaccessible upon creation, restoring permissions back to the original value
 * when destroyed.
 */
class CouchstoreFileAccessGuard {
public:
    enum class Mode {
        ReadOnly,
        DenyAll,
    };

    explicit CouchstoreFileAccessGuard(std::string dbName,
                                       Mode mode = Mode::ReadOnly);

    ~CouchstoreFileAccessGuard();

private:
    std::string filename;
    struct stat originalStat;
#ifdef WIN32
    HANDLE hFile = nullptr;
#endif
};

void decayingSleep(std::chrono::microseconds* sleepTime);

// Basic Operations
cb::engine_errc del(EngineIface* h,
                    const char* key,
                    uint64_t cas,
                    Vbid vbucket,
                    CookieIface* cookie = nullptr);

cb::engine_errc del(EngineIface* h,
                    const char* key,
                    uint64_t* cas,
                    Vbid vbucket,
                    CookieIface* cookie,
                    mutation_descr_t* mut_info);

/** Simplified version of store for handling the common case of performing
 * a delete with a value.
 */
cb::engine_errc delete_with_value(
        EngineIface* h,
        CookieIface* cookie,
        uint64_t cas,
        const char* key,
        std::string_view value,
        cb::mcbp::Datatype datatype = cb::mcbp::Datatype::Raw);

void disable_traffic(EngineIface* h);
void enable_traffic(EngineIface* h);
void evict_key(EngineIface* h,
               const char* key,
               Vbid vbucketId = Vbid(0),
               const char* msg = nullptr,
               bool expectError = false);
cb::EngineErrorItemPair gat(EngineIface* h,
                            const char* key,
                            Vbid vb,
                            uint32_t exp);
bool get_item_info(EngineIface* h,
                   item_info* info,
                   const char* key,
                   Vbid vb = Vbid(0));

cb::EngineErrorItemPair getl(EngineIface* h,
                             CookieIface* cookie,
                             const char* key,
                             Vbid vb,
                             uint32_t lock_timeout);

[[nodiscard]] unique_request_ptr prepare_get_replica(
        EngineIface* h, vbucket_state_t state, bool makeinvalidkey = false);

void get_replica(EngineIface* h, const char* key, Vbid vb);
cb::engine_errc observe(EngineIface* h, std::map<std::string, Vbid> obskeys);
cb::engine_errc observe_seqno(EngineIface* h, Vbid vb_id, uint64_t uuid);

cb::engine_errc set_param(EngineIface* h,
                          EngineParamCategory paramtype,
                          const char* param,
                          const char* val,
                          Vbid vb = Vbid(0));

bool set_vbucket_state(EngineIface* h,
                       Vbid vb,
                       vbucket_state_t state,
                       std::string_view meta = {});

bool get_all_vb_seqnos(
        EngineIface* h,
        std::optional<RequestedVBState> state,
        CookieIface* cookie,
        std::optional<CollectionIDType> collection = {},
        cb::engine_errc expectedStatus = cb::engine_errc::success);
void verify_all_vb_seqnos(EngineIface* h,
                          int vb_start,
                          int vb_end,
                          std::optional<CollectionID> collection = {});
void start_persistence(EngineIface* h);
void stop_persistence(EngineIface* h);

/**
 * Store an item.
 *
 * @param outitem If non-null, address of the stored item is saved here.
 * @return
 */
cb::engine_errc store(
        EngineIface* h,
        CookieIface* cookie,
        StoreSemantics op,
        const char* key,
        const char* value,
        ItemIface** outitem = nullptr,
        uint64_t casIn = 0,
        Vbid vb = Vbid(0),
        uint32_t exp = 3600,
        uint8_t datatype = 0x00,
        DocumentState docState = DocumentState::Alive,
        const std::optional<cb::durability::Requirements>& durReqs = {});

cb::EngineErrorItemPair allocate(EngineIface* h,
                                 CookieIface* cookie,
                                 const std::string& key,
                                 size_t nbytes,
                                 int flags,
                                 rel_time_t exptime,
                                 uint8_t datatype,
                                 Vbid vb);

cb::EngineErrorItemPair get(
        EngineIface* h,
        CookieIface* cookie,
        const std::string& key,
        Vbid vb,
        DocStateFilter documentStateFilter = DocStateFilter::Alive);

/* Stores the specified document; returning the new CAS value via
 * {out_cas}.
 */
cb::engine_errc storeCasOut(EngineIface* h,
                            CookieIface* cookie,
                            Vbid vb,
                            const std::string& key,
                            const std::string& value,
                            protocol_binary_datatype_t datatype,
                            ItemIface*& out_item,
                            uint64_t& out_cas,
                            DocumentState docState = DocumentState::Alive);

cb::EngineErrorItemPair storeCasVb11(
        EngineIface* h,
        CookieIface* cookie,
        StoreSemantics op,
        const char* key,
        const char* value,
        size_t vlen,
        uint32_t flags,
        uint64_t casIn,
        Vbid vb,
        uint32_t exp = 3600,
        uint8_t datatype = 0x00,
        DocumentState docState = DocumentState::Alive,
        const std::optional<cb::durability::Requirements>& durReqs = {});

cb::engine_errc replace(EngineIface* h,
                        CookieIface* cookie,
                        const char* key,
                        const char* value,
                        uint32_t flags,
                        Vbid vb);

cb::engine_errc touch(EngineIface* h, const char* key, Vbid vb, uint32_t exp);
cb::engine_errc unl(EngineIface* h,
                    CookieIface* cookie,
                    const char* key,
                    Vbid vb,
                    uint64_t cas = 0);
cb::engine_errc verify_key(EngineIface* h,
                           const char* key,
                           Vbid vbucket = Vbid(0));

/**
 * Attempts to fetch the given key. On success returns cb::engine_errc::success
 * and the value, on failure returns the reason and an empty string.
 */
std::pair<cb::engine_errc, std::string> get_value(EngineIface* h,
                                                  CookieIface* cookie,
                                                  const char* key,
                                                  Vbid vbucket,
                                                  DocStateFilter state);

bool verify_vbucket_missing(EngineIface* h, Vbid vb);
bool verify_vbucket_state(EngineIface* h,
                          Vbid vb,
                          vbucket_state_t expected,
                          bool mute = false);

void sendDcpAck(EngineIface* h,
                const void* cookie,
                cb::mcbp::ClientOpcode opcode,
                cb::mcbp::Status status,
                uint32_t opaque);

void createCheckpoint(EngineIface* h);

cb::engine_errc seqnoPersistence(EngineIface* h,
                                 CookieIface* cookie,
                                 Vbid vbucket,
                                 uint64_t seqno);

// Stats Operations
int get_int_stat(EngineIface* h,
                 const char* statname,
                 const char* statkey = nullptr);
float get_float_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey = nullptr);
uint32_t get_ul_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey = nullptr);
uint64_t get_ull_stat(EngineIface* h,
                      const char* statname,
                      const char* statkey = nullptr);
std::string get_str_stat(EngineIface* h,
                         const char* statname,
                         const char* statkey = nullptr);
bool get_bool_stat(EngineIface* h,
                   const char* statname,
                   const char* statkey = nullptr);

cb::engine_errc get_stats(gsl::not_null<EngineIface*> h,
                          std::string_view key,
                          std::string_view value,
                          const AddStatFn& callback);

/* This is used to get stat info specified by 'histo_info' from histogram of
 * "statname" which is got by running stats on "statkey"
 */
uint64_t get_histo_stat(EngineIface* h,
                        const char* statname,
                        const char* statkey,
                        const Histo_stat_info histo_info);

using statistic_map = std::map<std::string, std::string>;

/* Returns a map of all statistics for the given statistic set.
 * @param statset The set of statistics to fetch. May be nullptr, in which case
 *                the default set will be returned.
 */
statistic_map get_all_stats(EngineIface* h, const char* statset = nullptr);

// Returns the value of the given stat, or the default value if the stat isn't
// present.
int get_int_stat_or_default(EngineIface* h,
                            int default_value,
                            const char* statname,
                            const char* statkey = nullptr);

/**
 * Templated function prototype to return a stat of the given type.
 * Should replace above uses of get_XXX_stat with this.
 */
template <typename T>
T get_stat(EngineIface* h,
           const char* statname,
           const char* statkey = nullptr);

// Explicit template instantiations declarations of get_stat<T>
template <>
std::string get_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey);
template <>
int get_stat(EngineIface* h,
             const char* statname,
             const char* statkey);

template <>
uint64_t get_stat(EngineIface* h,
                  const char* statname,
                  const char* statkey);

void verify_curr_items(EngineIface* h,
                       int exp,
                       const char* msg);
template <typename T>
void wait_for_stat_change(EngineIface* h,
                          const char* stat,
                          T initial,
                          const char* stat_key = nullptr,
                          const std::chrono::seconds max_wait_time_in_secs =
                                  std::chrono::seconds{60});

template <typename T>
void wait_for_stat_to_be(EngineIface* h,
                         const char* stat,
                         T final,
                         const char* stat_key = nullptr,
                         const std::chrono::seconds max_wait_time_in_secs =
                                 std::chrono::seconds{60});

void wait_for_stat_to_be_gte(EngineIface* h,
                             const char* stat,
                             int final,
                             const char* stat_key = nullptr,
                             const std::chrono::seconds max_wait_time_in_secs =
                                     std::chrono::seconds{60});

template <typename T>
void wait_for_stat_to_be_lte(EngineIface* h,
                             const char* stat,
                             T final,
                             const char* stat_key = nullptr,
                             const std::chrono::seconds max_wait_time_in_secs =
                                     std::chrono::seconds{60});

void wait_for_expired_items_to_be(
        EngineIface* h,
        int final,
        const std::chrono::seconds max_wait_time_in_secs = std::chrono::seconds{
                60});
bool wait_for_warmup_complete(EngineIface* h);
void wait_for_flusher_to_settle(EngineIface* h);
void wait_for_item_compressor_to_settle(EngineIface* h);
void wait_for_rollback_to_finish(EngineIface* h);
void wait_for_persisted_value(EngineIface* h,
                              const char* key,
                              const char* val,
                              Vbid vbucketId = Vbid(0));

void wait_for_memory_usage_below(
        EngineIface* h,
        int mem_threshold,
        const std::chrono::seconds max_wait_time_in_secs = std::chrono::seconds{
                60});

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
cb::engine_errc vbucketDelete(EngineIface* h,
                              Vbid vb,
                              const char* args = nullptr);

void compact_db(EngineIface* h,
                const Vbid db_file_id,
                const uint64_t purge_before_ts,
                const uint64_t purge_before_seq,
                const uint8_t drop_deletes);

bool get_meta(EngineIface* h, const char* key, CookieIface* cookie = nullptr);

bool get_meta(EngineIface* h,
              const char* key,
              cb::EngineErrorMetadataPair& out,
              CookieIface* cookie = nullptr);

cb::engine_errc set_with_meta(EngineIface* h,
                              const char* key,
                              const size_t keylen,
                              const char* val,
                              const size_t vallen,
                              const Vbid vb,
                              ItemMetaData* itemMeta,
                              uint64_t cas_for_set,
                              uint32_t options = 0,
                              uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                              CookieIface* cookie = nullptr,
                              const std::vector<char>& nmeta = {});

cb::engine_errc add_with_meta(EngineIface* h,
                              const char* key,
                              const size_t keylen,
                              const char* val,
                              const size_t vallen,
                              const Vbid vb,
                              ItemMetaData* itemMeta,
                              uint64_t cas_for_add = 0,
                              uint32_t options = 0,
                              uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                              CookieIface* cookie = nullptr,
                              const std::vector<char>& nmeta = {});

cb::engine_errc del_with_meta(
        EngineIface* h,
        const char* key,
        const size_t keylen,
        const Vbid vb,
        ItemMetaData* itemMeta,
        uint64_t cas_for_delete = 0,
        uint32_t options = 0,
        CookieIface* cookie = nullptr,
        const std::vector<char>& nmeta = {},
        protocol_binary_datatype_t datatype = 0,
        const std::vector<char>& value = {} /*optional value*/);

// This version takes a RawItemMetaData allowing for 64-bit rev-seqno tests
cb::engine_errc del_with_meta(
        EngineIface* h,
        const char* key,
        const size_t keylen,
        const Vbid vb,
        RawItemMetaData* itemMeta,
        uint64_t cas_for_delete = 0,
        uint32_t options = 0,
        CookieIface* cookie = nullptr,
        const std::vector<char>& nmeta = {},
        protocol_binary_datatype_t datatype = 0,
        const std::vector<char>& value = {} /*optional value*/);

cb::engine_errc set_ret_meta(EngineIface* h,
                             const char* key,
                             const size_t keylen,
                             const char* val,
                             const size_t vallen,
                             const Vbid vb,
                             const uint64_t cas = 0,
                             const uint32_t flags = 0,
                             const uint32_t exp = 0,
                             uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                             CookieIface* cookie = nullptr);

cb::engine_errc add_ret_meta(EngineIface* h,
                             const char* key,
                             const size_t keylen,
                             const char* val,
                             const size_t vallen,
                             const Vbid vb,
                             const uint64_t cas = 0,
                             const uint32_t flags = 0,
                             const uint32_t exp = 0,
                             uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                             CookieIface* cookie = nullptr);

cb::engine_errc del_ret_meta(EngineIface* h,
                             const char* key,
                             const size_t keylen,
                             const Vbid vb,
                             const uint64_t cas = 0,
                             CookieIface* cookie = nullptr);

// Fetches the CAS of the specified key.
uint64_t get_CAS(EngineIface* h, const std::string& key);

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
void write_items(EngineIface* h,
                 int num_items,
                 int start_seqno = 0,
                 const char* key_prefix = "key",
                 const char* value = "data",
                 uint32_t expiry = 0,
                 Vbid vb = Vbid(0),
                 DocumentState docState = DocumentState::Alive);

/* Helper function to write unique items starting from keyXX until memory usage
   hits "mem_thresh_perc" (XX is start_seqno) */
int write_items_upto_mem_perc(EngineIface* h,
                              int mem_thresh_perc,
                              int start_seqno = 0,
                              const char* key_prefix = "key",
                              const char* value = "data");

template <typename T>
inline void wait_for_stat_change(
        EngineIface* h,
        const char* stat,
        T initial,
        const char* stat_key,
        const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<T> accumulator("to change from", stat, stat_key,
                                         initial, max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, stat, stat_key);
        if (current != initial) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

template <typename T>
void wait_for_stat_to_be(EngineIface* h,
                         const char* stat,
                         T final,
                         const char* stat_key,
                         const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<T> accumulator("to be", stat, stat_key, final,
                                       max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, stat, stat_key);
        if (current == final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

template <typename T>
void wait_for_stat_to_be_lte(EngineIface* h,
                             const char* stat,
                             T final,
                             const char* stat_key,
                             const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<T> accumulator("to be less than or equal to",
                                       stat,
                                       stat_key,
                                       final,
                                       max_wait_time_in_secs);
    for (;;) {
        auto current = get_stat<T>(h, stat, stat_key);
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
                        const std::chrono::seconds max_wait_time_in_secs =
                                std::chrono::seconds{60}) {
    std::chrono::microseconds sleepTime{128};
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
inline bool is_full_eviction(EngineIface* h) {
    return get_str_stat(h, "ep_item_eviction_policy") == "full_eviction";
}

void reset_stats(gsl::not_null<EngineIface*> h);
