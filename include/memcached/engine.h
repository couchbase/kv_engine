/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/thread_pool_config.h>
#include <memcached/types.h>
#include <memcached/vbucket.h>
#include <sys/types.h>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace cb {
struct EngineErrorGetCollectionIDResult;
struct EngineErrorGetScopeIDResult;
struct EngineErrorGetCollectionMetaResult;
} // namespace cb

namespace cb::durability {
class Requirements;
} // namespace cb::durability

namespace cb::mcbp {
class Request;
} // namespace cb::mcbp

namespace cb::prometheus {
enum class MetricGroup;
} // namespace cb::prometheus

namespace cb::rangescan {
struct SnapshotRequirements;
struct SamplingConfiguration;
} // namespace cb::rangescan

class ConnectionIface;
class CookieIface;
class BucketStatCollector;
class StatCollector;

namespace folly {
class CancellationToken;
}

/*! \mainpage memcached public API
 *
 * \section intro_sec Introduction
 *
 * The memcached project provides an API for providing engines as well
 * as data definitions for those implementing the protocol in C.  This
 * documentation will explain both to you.
 *
 * \section docs_sec API Documentation
 *
 * Jump right into <a href="modules.html">the modules docs</a> to get started.
 *
 * \example default_engine.cc
 */

/**
 * \defgroup Engine Storage Engine API
 * \defgroup Protex Protocol Extension API
 * \defgroup Protocol Binary Protocol Structures
 *
 * \addtogroup Engine
 * @{
 *
 * Most interesting here is to implement engine_interface_v1 for your
 * engine.
 */

struct DocKey;
struct ServerBucketIface;
struct ServerCoreIface;
struct ServerCookieIface;
struct ServerDocumentIface;
union protocol_binary_request_header;

struct ServerApi {
    ServerCoreIface* core = nullptr;
    ServerCookieIface* cookie = nullptr;
    ServerDocumentIface* document = nullptr;
    ServerBucketIface* bucket = nullptr;
};

using GET_SERVER_API = ServerApi* (*)();

namespace cb {
using EngineErrorItemPair = std::pair<cb::engine_errc, cb::unique_item_ptr>;

using EngineErrorMetadataPair = std::pair<engine_errc, item_info>;

enum class StoreIfStatus {
    Continue,
    Fail,
    GetItemInfo // please get me the item_info
};

using StoreIfPredicate = std::function<StoreIfStatus(
        const std::optional<item_info>&, cb::vbucket_info)>;

using EngineErrorCasPair = std::pair<engine_errc, uint64_t>;

/// Result of getVBucketHlcNow()
struct HlcTime {
    enum class Mode { Real, Logical };

    /// Seconds since Unix epoch.
    std::chrono::seconds now;
    Mode mode;
};
} // namespace cb

/**
 * The different compression modes that a bucket supports
 */
enum class BucketCompressionMode : uint8_t {
    Off,     //Data will be stored as uncompressed
    Passive, //Data will be stored as provided by the client
    Active   //Bucket will actively try to compress stored
             //data
};

/* The default minimum compression ratio */
static const float default_min_compression_ratio = 1.2f;

/* The default maximum size for a value */
static const size_t default_max_item_size = 20 * 1024 * 1024;

namespace cb::engine {
/**
 * Definition of the features that an engine can support
 */
enum class Feature : uint16_t {
    Collections = 1,
};

using FeatureSet = std::unordered_set<Feature>;
} // namespace cb::engine

namespace std {
template <>
struct hash<cb::engine::Feature> {
public:
    size_t operator()(const cb::engine::Feature& f) const {
        return static_cast<size_t>(f);
    }
};
} // namespace std

/**
 * Definition of the first version of the engine interface
 */
struct EngineIface {
    virtual ~EngineIface() = default;

    /**
     * Initialize an engine instance.
     * This is called *after* creation, but before the engine may be used.
     *
     * @param handle the engine handle
     * @param config_str configuration this engine needs to initialize itself.
     */
    virtual cb::engine_errc initialize(std::string_view config_str) {
        return cb::engine_errc::success;
    }

    /**
     * Tear down this engine.
     *
     * @param force the flag indicating the force shutdown or not.
     */
    virtual void destroy(bool force) = 0;

    /// Callback that a cookie will be disconnected (this method is
    /// deprecated and will go away once we change the internals of DCP
    /// to store the ConnectionIface and not the cookie
    virtual void disconnect(CookieIface& cookie){};

    /// Callback that a connection will be disconnected
    virtual void disconnect(const ConnectionIface& connection){};

    /**
     * Initiate the bucket shutdown logic (disconnect clients etc)
     */
    virtual void initiate_shutdown() {
        // empty
    }

    virtual void notify_num_writer_threads_changed() {
        // ignored
    }

    virtual void notify_num_auxio_threads_changed() {
        // ignored
    }

    // Set the number of storage threads
    virtual void set_num_storage_threads(
            ThreadPoolConfig::StorageThreadCount num) {
        // ignored
    }

    /**
     * Request the engine to cancel all of the ongoing requests which
     * may have cookies in an ewouldblock state.
     *
     * This method is to removed in CC when we'll tighten the logic for
     * bucket deletion to use the following logic:
     *
     * 1) Stop any new requests into the engine.
     * 2) Tell the engine to complete (cancel) anything currently in-flight
     *    (this notification holds a write lock to the engine so it won't
     *    race with any front end threads)
     * 3) wait for in-flight ops (i.e. step B) to finish then delete bucket.
     *
     * In Mad-Hatter initiate_shutdown may race with all of the frontend
     * worker threads, and could lead to operations being added after we've
     * inspected the vbucket. To work around that problem this new method
     * was introduced and will be called multiple times during bucket
     * deletion to work around potential race situations.
     */
    virtual void cancel_all_operations_in_ewb_state() {
        // empty
    }

    /*
     * Item operations.
     */

    /**
     * Allocate an item (extended API)
     *
     * @param cookie The cookie provided by the frontend
     * @param key the item's key
     * @param nbytes the number of bytes that will make up the
     *               value of this item.
     * @param priv_nbytes The number of bytes in nbytes containing
     *                    system data (and may exceed the item limit).
     * @param flags the item's flags
     * @param exptime the maximum lifetime of this item
     * @param vbucket virtual bucket to request allocation from
     * @return allocated item
     * @thows cb::engine_error with:
     *
     *   * `cb::engine_errc::no_bucket` The client is bound to the dummy
     *                                  `no bucket` which don't allow
     *                                  allocations.
     *
     *   * `cb::engine_errc::no_memory` The bucket is full
     *
     *   * `cb::engine_errc::too_big` The requested memory exceeds the
     *                                limit set for items in the bucket.
     *
     *   * `cb::engine_errc::disconnect` The client should be disconnected
     *
     *   * `cb::engine_errc::not_my_vbucket` The requested vbucket belongs
     *                                       to someone else
     *
     *   * `cb::engine_errc::temporary_failure` Temporary failure, the
     *                                          _client_ should try again
     *
     *   * `cb::engine_errc::too_busy` Too busy to serve the request,
     *                                 back off and try again.
     */
    virtual cb::unique_item_ptr allocateItem(CookieIface& cookie,
                                             const DocKey& key,
                                             size_t nbytes,
                                             size_t priv_nbytes,
                                             int flags,
                                             rel_time_t exptime,
                                             uint8_t datatype,
                                             Vbid vbucket) = 0;

    /**
     * Remove an item.
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key identifying the item to be removed
     * @param cas The cas to value to use for delete (or 0 as wildcard)
     * @param vbucket the virtual bucket id
     * @param durability the durability specifier for the command
     * @param mut_info On a successful remove write the mutation details to
     *                 this address.
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc remove(
            CookieIface& cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) = 0;

    /**
     * Indicate that a caller who received an item no longer needs
     * it.
     *
     * @param item the item to be released
     */
    virtual void release(ItemIface& item) = 0;

    /**
     * Retrieve an item.
     *
     * @param cookie The cookie provided by the frontend
     * @param item output variable that will receive the located item
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param documentStateFilter The document to return must be in any of
     *                            of these states. (If `Alive` is set, return
     *                            KEY_ENOENT if the document in the engine
     *                            is in another state)
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::EngineErrorItemPair get(CookieIface& cookie,
                                        const DocKey& key,
                                        Vbid vbucket,
                                        DocStateFilter documentStateFilter) = 0;

    /**
     * Optionally retrieve an item. Only non-deleted items may be fetched
     * through this interface (Documents in deleted state may be evicted
     * from memory and we don't want to go to disk in order to fetch these)
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param filter callback filter to see if the item should be returned
     *               or not. If filter returns false the item should be
     *               skipped.
     *               Note: the filter is applied only to the *metadata* of the
     *               item_info - i.e. the `value` should not be expected to be
     *               present when filter is invoked.
     * @return A pair of the error code and (optionally) the item
     */
    virtual cb::EngineErrorItemPair get_if(
            CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) = 0;

    /**
     * Retrieve metadata for a given item.
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     *
     * @return  Pair (cb::engine_errc::success, Metadata) if all goes well
     */
    virtual cb::EngineErrorMetadataPair get_meta(CookieIface& cookie,
                                                 const DocKey& key,
                                                 Vbid vbucket) = 0;

    /**
     * Lock and Retrieve an item.
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param lock_timeout the number of seconds to hold the lock
     *                     (0 == use the engines default lock time)
     *
     * @return A pair of the error code and (optionally) the item
     */
    virtual cb::EngineErrorItemPair get_locked(CookieIface& cookie,
                                               const DocKey& key,
                                               Vbid vbucket,
                                               uint32_t lock_timeout) = 0;

    /**
     * Unlock an item.
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param cas the cas value for the locked item
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc unlock(CookieIface& cookie,
                                   const DocKey& key,
                                   Vbid vbucket,
                                   uint64_t cas) = 0;

    /**
     * Get and update the expiry time for the document
     *
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param expirytime the new expiry time for the object
     * @param durability An optional durability requirement
     * @return A pair of the error code and (optionally) the item
     */
    virtual cb::EngineErrorItemPair get_and_touch(
            CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expirytime,
            const std::optional<cb::durability::Requirements>& durability) = 0;

    /**
     * Store an item into the underlying engine with the given
     * state. If the DocumentState is set to DocumentState::Deleted
     * the document shall not be returned unless explicitly asked for
     * documents in that state, and the underlying engine may choose to
     * purge it whenever it please.
     *
     * @param cookie The cookie provided by the frontend
     * @param item the item to store
     * @param cas the CAS value for conditional sets
     * @param semantics the semantics of the store operation
     * @param durability An optional durability requirement
     * @param document_state The state the document should have after
     *                       the update
     * @param preserveTtl if set to true the existing documents TTL should
     *                    be used.
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc store(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics semantics,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) = 0;

    /**
     * Store an item into the underlying engine with the given
     * state only if the predicate argument returns true when called against an
     * existing item.
     *
     * Optional interface; not supported by all engines.
     *
     * @param cookie The cookie provided by the frontend
     * @param item the item to store
     * @param cas the CAS value for conditional sets
     * @param semantics the semantics of the store operation
     * @param predicate a function that will be called from the engine the
     *                  result of which determines how the store behaves.
     *                  The function is given any existing item's item_info (as
     *                  a std::optional) and a cb::vbucket_info object. In the
     *                  case that the optional item_info is not initialised the
     *                  function can return cb::StoreIfStatus::GetInfo to
     *                  request that the engine tries to get the item_info, the
     *                  engine may ignore this return code if it knows better
     *                  i.e. a memory only engine and no item_info can be
     *                  fetched. The function can also return ::Fail if it
     *                  wishes to fail the store_if (returning predicate_failed)
     *                  or the predicate can return ::Continue and the rest of
     *                  the store_if will execute (and possibly fail for other
     *                  reasons).
     * @param durability An optional durability requirement
     * @param document_state The state the document should have after
     *                       the update
     * @param preserveTtl if set to true the existing documents TTL should
     *                    be used.
     *
     * @return a std::pair containing the engine_error code and new CAS
     */
    virtual cb::EngineErrorCasPair store_if(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t cas,
            StoreSemantics semantics,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) {
        return {cb::engine_errc::not_supported, 0};
    }

    /**
     * Flush the cache.
     *
     * Optional interface; not supported by all engines.
     *
     * @param cookie The cookie provided by the frontend
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc flush(CookieIface& cookie) {
        return cb::engine_errc::not_supported;
    }

    /*
     * Statistics
     */

    /**
     * Get statistics from the engine.
     *
     * @param cookie The cookie provided by the frontend
     * @param key optional argument to stats
     * @param value optional value for the given stat group
     * @param add_stat callback to feed results to the output
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc get_stats(CookieIface& cookie,
                                      std::string_view key,
                                      std::string_view value,
                                      const AddStatFn& add_stat) = 0;

    /**
     * Get statistics for Prometheus exposition from the engine.
     * Some engines may not support this.
     *
     * @param collector where the bucket may register stats
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc get_prometheus_stats(
            const BucketStatCollector& collector,
            cb::prometheus::MetricGroup metricGroup) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Reset the stats.
     *
     * @param cookie The cookie provided by the frontend
     */
    virtual void reset_stats(CookieIface& cookie) = 0;

    /**
     * Any unknown command will be considered engine specific.
     *
     * @param cookie The cookie provided by the frontend
     * @param request The request from the client
     * @param response function to transmit data
     *
     * @return cb::engine_errc::success if all goes well
     */
    virtual cb::engine_errc unknown_command(CookieIface& cookie,
                                            const cb::mcbp::Request& request,
                                            const AddResponseFn& response) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Get information about an item.
     *
     * The loader of the module may need the pointers to the actual data within
     * an item. Instead of having to create multiple functions to get each
     * individual item, this function will get all of them.
     *
     * @param item the item to request information about
     * @param item_info
     * @return true if successful
     */
    virtual bool get_item_info(const ItemIface& item, item_info& item_info) = 0;

    /**
     * The set of collections related functions. May be defined optionally by
     * the engine(s) that support them.
     */
    virtual cb::engine_errc set_collection_manifest(CookieIface& cookie,
                                                    std::string_view json) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Retrieve the last manifest set using set_manifest (a JSON document)
     */
    virtual cb::engine_errc get_collection_manifest(
            CookieIface& cookie, const AddResponseFn& response) {
        return cb::engine_errc::not_supported;
    }

    virtual cb::EngineErrorGetCollectionIDResult get_collection_id(
            CookieIface& cookie, std::string_view path);

    virtual cb::EngineErrorGetScopeIDResult get_scope_id(CookieIface& cookie,
                                                         std::string_view path);

    /**
     * Get the metadata for the given collection (scope and metering)
     *
     * @param cookie cookie object used to identify the request
     * @param cid The collection to lookup
     * @return pair with the manifest UID and if found the meta of the cid
     */
    virtual cb::EngineErrorGetCollectionMetaResult get_collection_meta(
            CookieIface& cookie,
            CollectionID cid,
            std::optional<Vbid> vbid = std::nullopt) const;

    /**
     * Ask the engine what features it supports.
     */
    virtual cb::engine::FeatureSet getFeatures() = 0;

    /**
     * @returns if XATTRs are enabled for this bucket
     */
    virtual bool isXattrEnabled() {
        return false;
    }

    /**
     * Get the "current" time and mode of the Hybrid Logical Clock for the
     * specified vBucket.
     * @returns seconds since unix epoch of the HLC, along with the current
     * HLC Mode (Real / Logical).
     */
    virtual cb::HlcTime getVBucketHlcNow(Vbid vbucket) = 0;

    /**
     * @returns the compression mode of the bucket
     */
    virtual BucketCompressionMode getCompressionMode() {
        return BucketCompressionMode::Off;
    }

    /**
     * @returns the maximum item size supported by the bucket
     */
    virtual size_t getMaxItemSize() {
        return default_max_item_size;
    };

    /**
     * @returns the minimum compression ratio defined in the bucket
     */
    virtual float getMinCompressionRatio() {
        return default_min_compression_ratio;
    }

    /**
     * Set a configuration parameter in the engine
     *
     * @param cookie The cookie identifying the request
     * @param category The parameter category
     * @param key The name of the parameter
     * @param value The value for the parameter
     * @param vbucket The vbucket specified in the request (only used for
     *                the vbucket sub group)
     * @return The standard engine codes
     */
    virtual cb::engine_errc setParameter(CookieIface& cookie,
                                         EngineParamCategory category,
                                         std::string_view key,
                                         std::string_view value,
                                         Vbid vbucket) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Compact a database
     *
     * @param cookie The cookie identifying the request
     * @param vbid The vbucket to compact
     * @param purge_before_ts The timestamp to purge items before
     * @param purge_before_seq The sequence number to purge items before
     * @param drop_deletes Set to true if deletes should be dropped
     * @return The standard engine codes
     */
    virtual cb::engine_errc compactDatabase(CookieIface& cookie,
                                            Vbid vbid,
                                            uint64_t purge_before_ts,
                                            uint64_t purge_before_seq,
                                            bool drop_deletes) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Get the state of a vbucket
     *
     * @param cookie The cookie identifying the request
     * @param vbid The vbucket to look up
     * @return A pair where the first entry is one of the standard engine codes
     *         and the second is the state when the status is success.
     */
    virtual std::pair<cb::engine_errc, vbucket_state_t> getVBucket(
            CookieIface& cookie, Vbid vbid) {
        return {cb::engine_errc::not_supported, vbucket_state_dead};
    }

    /**
     * Set the state of a vbucket
     *
     * @param cookie The cookie identifying the request
     * @param vbid The vbucket to update
     * @param cas The current value used for CAS swap
     * @param state The new vbucket state
     * @param meta The optional meta information for the state (nullptr if
     *             none is provided)
     * @return The standard engine codes
     */
    virtual cb::engine_errc setVBucket(CookieIface& cookie,
                                       Vbid vbid,
                                       uint64_t cas,
                                       vbucket_state_t state,
                                       nlohmann::json* meta) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Delete a vbucket
     *
     * @param cookie The cookie identifying the request
     * @param vbid The vbucket to delete
     * @param sync If the operation should block (return EWB and be signalled
     *             when the operation is done) or delete in "fire and forget"
     *             mode.
     * @return The standard engine codes
     */
    virtual cb::engine_errc deleteVBucket(CookieIface& cookie,
                                          Vbid vbid,
                                          bool sync) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Create a new range scan on a vbucket
     *
     * @param cookie The cookie identifying the request
     * @param vbid vbucket to create on
     * @param start key for the start of the range
     * @param end key for the end of the range
     * @param handler object that will receive callbacks when the scan continues
     * @param cookie connection cookie to notify when done
     * @param keyOnly key/value configuration of the scan
     * @param snapshotReqs optional requirements that the snapshot must satisfy
     * @param samplingConfig the parameters for the optional random sampling
     *
     * @return pair of status/cb::rangescan::Id - ID is valid on success
     */
    virtual std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            CookieIface& cookie,
            Vbid vbid,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig);

    /**
     * Continue the range scan with the given identifier.
     *
     * @param cookie The cookie identifying the request
     * @param vbid vbucket to find the scan on
     * @param uuid The identifier of the scan to continue
     * @param itemLimit The maximum number of items the continue can return
     *                  0 means no limit enforced
     * @param timeLimit The maximum duration the continue can return
     *                  0 means no limit enforced
     * @param byteLimit A trip wire value, when the number of bytes included in
     *                  the scan exceeds this value, the continue is complete.
     *                  Value of 0 disables this trigger.
     * @return would_block if the scan was found and successfully scheduled
     */
    virtual cb::engine_errc continueRangeScan(
            CookieIface& cookie,
            Vbid vbid,
            cb::rangescan::Id uuid,
            size_t itemLimit,
            std::chrono::milliseconds timeLimit,
            size_t byteLimit);

    /**
     * Cancel the range scan with the given identifier.
     *
     * @param cookie The cookie identifying the request
     * @param vbid vbucket to find the scan on
     * @param uuid The identifier of the scan to continue
     * @param schedule true if a task should be scheduled for the cancellation
     * @return would_block if the scan was found and successfully scheduled for
     *         cancellation
     */
    virtual cb::engine_errc cancelRangeScan(CookieIface& cookie,
                                            Vbid vbid,
                                            cb::rangescan::Id uuid);

    /**
     * Notify the engine it has been paused. Engine should perform any work
     * necessary to quiesce the on-disk bucket state, so the buckets' data
     * directory can be safely copied off the node as part of hibernating it.
     *
     * @param cancellationToken To allow cancellation of a pause() request
     *                          the caller can request cancellation via the
     *                          Cancellation Source this token was created from.
     * @return The standard engine codes
     */
    virtual cb::engine_errc pause(folly::CancellationToken cancellationToken);

    /**
     * Notify the engine it has been unpaused. Engine should perform any work
     * necessary to re-enable modification of on-disk bucket state so normal
     * mutations can be accepted again.
     *
     * @param cookie The cookie identifying the request
     * @return The standard engine codes
     */
    virtual cb::engine_errc resume() {
        return cb::engine_errc::not_supported;
    }
};

namespace cb {
class ItemDeleter {
public:
    ItemDeleter() : handle(nullptr) {}

    /**
     * Create a new instance of the item deleter.
     *
     * @param handle_ the handle to the the engine who owns the item
     */
    explicit ItemDeleter(EngineIface* handle_) : handle(handle_) {
        if (handle == nullptr) {
            throw std::invalid_argument(
                "cb::ItemDeleter: engine handle cannot be nil");
        }
    }

    void operator()(ItemIface* item) {
        if (handle) {
            handle->release(*item);
        } else {
            throw std::invalid_argument("cb::ItemDeleter: item attempted to be "
                                        "freed by null engine handle");
        }
    }

private:
    EngineIface* handle;
};

inline EngineErrorItemPair makeEngineErrorItemPair(cb::engine_errc err) {
    return {err, unique_item_ptr{nullptr, ItemDeleter{}}};
}

inline EngineErrorItemPair makeEngineErrorItemPair(cb::engine_errc err,
                                                   ItemIface* it,
                                                   EngineIface* handle) {
    return {err, unique_item_ptr{it, ItemDeleter{handle}}};
}
}

struct EngineDeletor {
    void operator()(EngineIface* engine) {
        engine->destroy(force);
    }

    bool force = false;
};

using unique_engine_ptr = std::unique_ptr<EngineIface, EngineDeletor>;

/**
 * @}
 */
