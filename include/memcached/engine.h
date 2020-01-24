/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once
#define MEMCACHED_ENGINE_H

#include <sys/types.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

#include <boost/optional/optional_fwd.hpp>
#include <gsl/gsl>

#include <mcbp/protocol/datatype.h>
#include <memcached/visibility.h>

#include "memcached/collections.h"
#include "memcached/config_parser.h"
#include "memcached/engine_common.h"
#include "memcached/thread_pool_config.h"
#include "memcached/types.h"
#include "memcached/vbucket.h"

namespace cb {
namespace durability {
class Requirements;
}
} // namespace cb

namespace cb {
namespace mcbp {
class Request;
}
} // namespace cb

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
struct ServerCallbackIface;
struct ServerLogIface;
struct ServerCookieIface;
struct ServerDocumentIface;
union protocol_binary_request_header;

/* This is typedefed in types.h */
struct server_handle_v1_t {
    ServerCoreIface* core;
    ServerCallbackIface* callback;
    ServerLogIface* log;
    ServerCookieIface* cookie;
    ServerDocumentIface* document;
    ServerBucketIface* bucket;
};

extern "C" {
typedef SERVER_HANDLE_V1* (*GET_SERVER_API)();
}

namespace cb {
using EngineErrorItemPair = std::pair<cb::engine_errc, cb::unique_item_ptr>;

using EngineErrorMetadataPair = std::pair<engine_errc, item_info>;

enum class StoreIfStatus {
    Continue,
    Fail,
    GetItemInfo // please get me the item_info
};

using StoreIfPredicate = std::function<StoreIfStatus(
        const boost::optional<item_info>&, cb::vbucket_info)>;

struct EngineErrorCasPair {
    engine_errc status;
    uint64_t cas;
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

namespace cb {
namespace engine {
/**
 * Definition of the features that an engine can support
 */
enum class Feature : uint16_t {
    Collections = 1,
};

typedef std::unordered_set<Feature> FeatureSet;
} // namespace engine
} // namespace cb

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
struct MEMCACHED_PUBLIC_CLASS EngineIface {
    virtual ~EngineIface() = default;

    /**
     * Initialize an engine instance.
     * This is called *after* creation, but before the engine may be used.
     *
     * @param handle the engine handle
     * @param config_str configuration this engine needs to initialize itself.
     */
    virtual ENGINE_ERROR_CODE initialize(const char* config_str) = 0;

    /**
     * Tear down this engine.
     *
     * @param force the flag indicating the force shutdown or not.
     */
    virtual void destroy(bool force) = 0;

    /// Callback that a cookie will be disconnected
    virtual void disconnect(gsl::not_null<const void*> cookie){};

    /**
     * Initiate the bucket shutdown logic (disconnect clients etc)
     */
    virtual void initiate_shutdown() {
        // empty
    }

    // Set the number or reader threads
    virtual void set_num_reader_threads(ThreadPoolConfig::ThreadCount num) {
        // ignored
    }

    // Set the number or writer threads
    virtual void set_num_writer_threads(ThreadPoolConfig::ThreadCount num) {
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
     * Allocate an item.
     *
     * @param cookie The cookie provided by the frontend
     * @param output variable that will receive the item
     * @param key the item's key
     * @param nbytes the number of bytes that will make up the
     *        value of this item.
     * @param flags the item's flags
     * @param exptime the maximum lifetime of this item
     * @param vbucket virtual bucket to request allocation from
     *
     * @return {cb::engine_errc::success, unique_item_ptr} if all goes well
     */
    virtual cb::EngineErrorItemPair allocate(gsl::not_null<const void*> cookie,
                                             const DocKey& key,
                                             size_t nbytes,
                                             int flags,
                                             rel_time_t exptime,
                                             uint8_t datatype,
                                             Vbid vbucket) = 0;

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
     * @return pair containing the item and the items information
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
    virtual std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
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
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE remove(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const boost::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) = 0;

    /**
     * Indicate that a caller who received an item no longer needs
     * it.
     *
     * @param item the item to be released
     */
    virtual void release(gsl::not_null<item*> item) = 0;

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
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual cb::EngineErrorItemPair get(gsl::not_null<const void*> cookie,
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
            gsl::not_null<const void*> cookie,
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
     * @return  Pair (ENGINE_SUCCESS, Metadata) if all goes well
     */
    virtual cb::EngineErrorMetadataPair get_meta(
            gsl::not_null<const void*> cookie,
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
    virtual cb::EngineErrorItemPair get_locked(
            gsl::not_null<const void*> cookie,
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
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE unlock(gsl::not_null<const void*> cookie,
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
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expirytime,
            const boost::optional<cb::durability::Requirements>&
                    durability) = 0;

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
     * @param operation the type of store operation to perform.
     * @param durability An optional durability requirement
     * @param document_state The state the document should have after
     *                       the update
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE store(
            gsl::not_null<const void*> cookie,
            gsl::not_null<item*> item,
            uint64_t& cas,
            ENGINE_STORE_OPERATION operation,
            const boost::optional<cb::durability::Requirements>& durability,
            DocumentState document_state) = 0;

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
     * @param operation the type of store operation to perform.
     * @param predicate a function that will be called from the engine the
     *                  result of which determines how the store behaves.
     *                  The function is given any existing item's item_info (as
     *                  a boost::optional) and a cb::vbucket_info object. In the
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
     *
     * @return a std::pair containing the engine_error code and new CAS
     */
    virtual cb::EngineErrorCasPair store_if(
            gsl::not_null<const void*> cookie,
            gsl::not_null<item*> item,
            uint64_t cas,
            ENGINE_STORE_OPERATION operation,
            const cb::StoreIfPredicate& predicate,
            const boost::optional<cb::durability::Requirements>& durability,
            DocumentState document_state) {
        return {cb::engine_errc::not_supported, 0};
    }

    /**
     * Flush the cache.
     *
     * Optional interface; not supported by all engines.
     *
     * @param cookie The cookie provided by the frontend
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE flush(gsl::not_null<const void*> cookie) {
        return ENGINE_ENOTSUP;
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
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*> cookie,
                                        cb::const_char_buffer key,
                                        cb::const_char_buffer value,
                                        const AddStatFn& add_stat) = 0;

    /**
     * Reset the stats.
     *
     * @param cookie The cookie provided by the frontend
     */
    virtual void reset_stats(gsl::not_null<const void*> cookie) = 0;

    /**
     * Any unknown command will be considered engine specific.
     *
     * @param cookie The cookie provided by the frontend
     * @param request The request from the client
     * @param response function to transmit data
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    virtual ENGINE_ERROR_CODE unknown_command(const void* cookie,
                                              const cb::mcbp::Request& request,
                                              const AddResponseFn& response) {
        return ENGINE_ENOTSUP;
    }

    /**
     * Set the CAS id on an item.
     */
    virtual void item_set_cas(gsl::not_null<item*> item, uint64_t cas) = 0;

    /**
     * Set the data type on an item.
     */
    virtual void item_set_datatype(gsl::not_null<item*> item,
                                   protocol_binary_datatype_t datatype) = 0;

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
    virtual bool get_item_info(gsl::not_null<const item*> item,
                               gsl::not_null<item_info*> item_info) = 0;

    /**
     * The set of collections related functions. May be defined optionally by
     * the engine(s) that support them.
     */
    virtual cb::engine_errc set_collection_manifest(
            gsl::not_null<const void*> cookie, cb::const_char_buffer json) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Retrieve the last manifest set using set_manifest (a JSON document)
     */
    virtual cb::engine_errc get_collection_manifest(
            gsl::not_null<const void*> cookie, const AddResponseFn& response) {
        return cb::engine_errc::not_supported;
    }

    virtual cb::EngineErrorGetCollectionIDResult get_collection_id(
            gsl::not_null<const void*> cookie, cb::const_char_buffer path) {
        return {cb::engine_errc::not_supported, {}, {}};
    }

    virtual cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const void*> cookie, cb::const_char_buffer path) {
        return {cb::engine_errc::not_supported, {}, {}};
    }

    /**
     * Get the scope for the provided key
     *
     * @param cookie cookie object used to identify the request
     * @param key the key to look up
     * @return pair with the manifest UID and if found the scope where the key
     *              belongs.
     */
    virtual std::pair<uint64_t, boost::optional<ScopeID>> get_scope_id(
            gsl::not_null<const void*> cookie, const DocKey& key) const {
        return {};
    }

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

    /**
     * Create a copy constructor to allow us to use std::move of the item
     */
    ItemDeleter(const ItemDeleter& other) = default;

    void operator()(item* item) {
        if (handle) {
            handle->release(item);
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
                                                   item* it,
                                                   EngineIface* handle) {
    return {err, unique_item_ptr{it, ItemDeleter{handle}}};
}
}

/**
 * @}
 */
