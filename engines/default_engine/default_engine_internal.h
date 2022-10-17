/*
 * Summary: Specification of the storage engine interface.
 *
 * Copy: See Copyright for the status of this software.
 *
 * Author: Trond Norbye <trond.norbye@sun.com>
 */
#pragma once

#include <folly/CancellationToken.h>
#include <memcached/engine.h>
#include <memcached/util.h>
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
   std::string uuid;
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

// Forward decls
namespace cb::prometheus {
enum class MetricGroup;
} // namespace cb::prometheus

class BucketStatCollector;
class StatCollector;

/**
 * Definition of the private instance data used by the default engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct default_engine : public EngineIface {
    cb::engine_errc initialize(std::string_view config_str) override;
    void destroy(bool force) override;

    cb::unique_item_ptr allocateItem(CookieIface& cookie,
                                     const DocKey& key,
                                     size_t nbytes,
                                     size_t priv_nbytes,
                                     int flags,
                                     rel_time_t exptime,
                                     uint8_t datatype,
                                     Vbid vbucket) override;

    cb::engine_errc remove(
            CookieIface& cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;

    void release(ItemIface& item) override;

    cb::EngineErrorItemPair get(CookieIface& cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(CookieIface& cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override;

    cb::EngineErrorItemPair get_locked(CookieIface& cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override;

    cb::engine_errc unlock(CookieIface& cookie,
                           const DocKey& key,
                           Vbid vbucket,
                           uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(
            CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expirytime,
            const std::optional<cb::durability::Requirements>& durability)
            override;

    cb::engine_errc store(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::EngineErrorCasPair store_if(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t cas,
            StoreSemantics operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::engine_errc flush(CookieIface& cookie) override;

    cb::engine_errc get_stats(CookieIface& cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat) override;

    cb::engine_errc get_prometheus_stats(
            const BucketStatCollector& collector,
            cb::prometheus::MetricGroup metricGroup) override;

    void reset_stats(CookieIface& cookie) override;

    cb::engine_errc unknown_command(CookieIface& cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) override;

    void item_set_cas(hash_item* item, uint64_t cas);

    void item_set_datatype(hash_item* item,
                           protocol_binary_datatype_t datatype);

    bool get_item_info(const hash_item* item,
                       gsl::not_null<item_info*> item_info);
    bool get_item_info(const ItemIface& item, item_info& item_info) override;

    cb::engine::FeatureSet getFeatures() override;

    bool isXattrEnabled() override;

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override;

    BucketCompressionMode getCompressionMode() override;

    size_t getMaxItemSize() override;

    float getMinCompressionRatio() override;

    cb::engine_errc set_collection_manifest(CookieIface& cookie,
                                            std::string_view json) override;

    cb::engine_errc get_collection_manifest(
            CookieIface& cookie, const AddResponseFn& response) override;

    cb::EngineErrorGetCollectionIDResult get_collection_id(
            CookieIface& cookie, std::string_view path) override;

    cb::EngineErrorGetScopeIDResult get_scope_id(
            CookieIface& cookie, std::string_view path) override;

    cb::EngineErrorGetCollectionMetaResult get_collection_meta(
            CookieIface& cookie,
            CollectionID cid,
            std::optional<Vbid> vbid) const override;

    cb::engine_errc setParameter(CookieIface& cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid) override;

    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(CookieIface& cookie,
                                                           Vbid vbid) override;
    cb::engine_errc setVBucket(CookieIface& cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override;
    cb::engine_errc deleteVBucket(CookieIface& cookie,
                                  Vbid vbid,
                                  bool sync) override;

    void generate_unknown_collection_response(CookieIface* cookie) const;

    cb::engine_errc pause(folly::CancellationToken cancellationToken) override {
        return cb::engine_errc::success;
    }

    cb::engine_errc resume() override {
        return cb::engine_errc::success;
    }

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

private:
    void do_engine_stats(const StatCollector& collector) const;
};

char* item_get_data(const hash_item* item);
hash_key* item_get_key(const hash_item* item);
void item_set_cas(gsl::not_null<EngineIface*> handle,
                  gsl::not_null<ItemIface*> item,
                  uint64_t val);
void default_engine_constructor(struct default_engine* engine, bucket_id_t id);
void destroy_engine_instance(struct default_engine* engine);
