/*
 * Summary: Specification of the storage engine interface.
 *
 * Copy: See Copyright for the status of this software.
 *
 * Author: Trond Norbye <trond.norbye@sun.com>
 */
#pragma once

#include <memcached/engine.h>
#include <memcached/util.h>
#include <memcached/visibility.h>
#include <relaxed_atomic.h>

#include <atomic>
#include <mutex>

/** How long an object can reasonably be assumed to be locked before
    harvesting it on a low memory condition. */
#define TAIL_REPAIR_TIME (3 * 3600)


/* Forward decl */
struct default_engine;

#include "items.h"
#include "assoc.h"
#include "slabs.h"

   /* Flags */
#define ITEM_LINKED (1)

/* temp */
#define ITEM_SLABBED (2)

/** The item is deleted (may only be accessed if explicitly asked for) */
#define ITEM_ZOMBIE (4)

struct config {
   size_t verbose;
   rel_time_t oldest_live;
   bool evict_to_free;
   size_t maxbytes;
   bool preallocate;
   float factor;
   size_t chunk_size;
   size_t item_size_max;
   bool ignore_vbucket;
   bool vb0;
   char *uuid;
   bool keep_deleted;
   std::atomic<bool> xattr_enabled;
   std::atomic<BucketCompressionMode> compression_mode;
   std::atomic<float> min_compression_ratio;
};

/**
 * Statistic information collected by the default engine
 */
struct engine_stats {
    cb::RelaxedAtomic<uint64_t> evictions{0};
    cb::RelaxedAtomic<uint64_t> reclaimed{0};
    cb::RelaxedAtomic<uint64_t> curr_bytes{0};
    cb::RelaxedAtomic<uint64_t> curr_items{0};
    cb::RelaxedAtomic<uint64_t> total_items{0};
};

struct engine_scrubber {
    std::mutex lock;
    uint64_t visited;
    uint64_t cleaned;
    time_t started;
    time_t stopped;
    bool running;
    bool force_delete;
};

struct vbucket_info {
    int state : 2;
};

#define NUM_VBUCKETS 65536

/**
 * Definition of the private instance data used by the default engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct default_engine : public EngineIface {
    ENGINE_ERROR_CODE initialize(const char* config_str) override;
    void destroy(bool force) override;

    cb::EngineErrorItemPair allocate(gsl::not_null<const void*> cookie,
                                     const DocKey& key,
                                     size_t nbytes,
                                     int flags,
                                     rel_time_t exptime,
                                     uint8_t datatype,
                                     Vbid vbucket) override;
    std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override;

    ENGINE_ERROR_CODE remove(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;

    void release(gsl::not_null<item*> item) override;

    cb::EngineErrorItemPair get(gsl::not_null<const void*> cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override;

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*> cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override;

    ENGINE_ERROR_CODE unlock(gsl::not_null<const void*> cookie,
                             const DocKey& key,
                             Vbid vbucket,
                             uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expirytime,
            const std::optional<cb::durability::Requirements>& durability)
            override;

    ENGINE_ERROR_CODE store(
            gsl::not_null<const void*> cookie,
            gsl::not_null<item*> item,
            uint64_t& cas,
            ENGINE_STORE_OPERATION operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::EngineErrorCasPair store_if(
            gsl::not_null<const void*> cookie,
            gsl::not_null<item*> item,
            uint64_t cas,
            ENGINE_STORE_OPERATION operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    ENGINE_ERROR_CODE flush(gsl::not_null<const void*> cookie) override;

    ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*> cookie,
                                std::string_view key,
                                std::string_view value,
                                const AddStatFn& add_stat) override;

    void reset_stats(gsl::not_null<const void*> cookie) override;

    ENGINE_ERROR_CODE unknown_command(const void* cookie,
                                      const cb::mcbp::Request& request,
                                      const AddResponseFn& response) override;

    void item_set_cas(gsl::not_null<item*> item, uint64_t cas) override;

    void item_set_datatype(gsl::not_null<item*> item,
                           protocol_binary_datatype_t datatype) override;

    bool get_item_info(gsl::not_null<const item*> item,
                       gsl::not_null<item_info*> item_info) override;

    cb::engine::FeatureSet getFeatures() override;

    bool isXattrEnabled() override;

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override;

    BucketCompressionMode getCompressionMode() override;

    size_t getMaxItemSize() override;

    float getMinCompressionRatio() override;

    cb::engine_errc set_collection_manifest(gsl::not_null<const void*> cookie,
                                            std::string_view json) override;

    cb::engine_errc get_collection_manifest(
            gsl::not_null<const void*> cookie,
            const AddResponseFn& response) override;

    cb::EngineErrorGetCollectionIDResult get_collection_id(
            gsl::not_null<const void*> cookie, std::string_view path) override;

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const void*> cookie, std::string_view path) override;

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            std::optional<Vbid> vbid) const override;

    void generate_unknown_collection_response(const void* cookie) const;

    ServerApi server;
    GET_SERVER_API get_server_api;

    /**
     * Is the engine initalized or not
     */
    bool initialized;

    struct slabs slabs;
    struct items items;

    struct config config;
    struct engine_stats stats;
    struct engine_scrubber scrubber;

    char vbucket_infos[NUM_VBUCKETS];

    /* a unique bucket index, note this is not cluster wide and dies with the
     * process */
    bucket_id_t bucket_id;
};

char* item_get_data(const hash_item* item);
hash_key* item_get_key(const hash_item* item);
void item_set_cas(gsl::not_null<EngineIface*> handle,
                  gsl::not_null<item*> item,
                  uint64_t val);
void default_engine_constructor(struct default_engine* engine, bucket_id_t id);
void destroy_engine_instance(struct default_engine* engine);
