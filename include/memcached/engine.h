/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once
#define MEMCACHED_ENGINE_H

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <sys/types.h>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gsl/gsl>

#include "memcached/allocator_hooks.h"
#include "memcached/callback.h"
#include "memcached/collections.h"
#include "memcached/config_parser.h"
#include "memcached/dcp.h"
#include "memcached/dockey.h"
#include "memcached/engine_common.h"
#include "memcached/extension.h"
#include "memcached/protocol_binary.h"
#include "memcached/server_api.h"
#include "memcached/types.h"
#include "memcached/vbucket.h"

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

#define ENGINE_INTERFACE_VERSION 1


/**
 * Abstract interface to an engine.
 */
#ifdef WIN32
#undef interface
#endif

/* This is typedefed in types.h */
struct server_handle_v1_t {
    uint64_t interface; /**< The version number on the server structure */
    SERVER_CORE_API* core;
    SERVER_STAT_API* stat;
    SERVER_EXTENSION_API* extension;
    SERVER_CALLBACK_API* callback;
    SERVER_LOG_API* log;
    SERVER_COOKIE_API* cookie;
    ALLOCATOR_HOOKS_API* alloc_hooks;
    SERVER_DOCUMENT_API* document;
};

/**
 * The signature for the "create_instance" function exported from the module.
 *
 * This function should fill out an engine inteface structure according to
 * the interface parameter (Note: it is possible to return a lower version
 * number).
 *
 * @param interface The highest interface level the server supports
 * @param get_server_api function to get the server API from
 * @param Where to store the interface handle
 * @return See description of ENGINE_ERROR_CODE
 */
typedef ENGINE_ERROR_CODE (* CREATE_INSTANCE)(uint64_t interface,
                                              GET_SERVER_API get_server_api,
                                              ENGINE_HANDLE** handle);

/**
 * The signature for the "destroy_engine" function exported from the module.
 *
 * This function is called prior to closing of the module. This function should
 * free any globally allocated resources.
 *
 */
typedef void (* DESTROY_ENGINE)(void);

typedef enum {
    ENGINE_FEATURE_CAS, /**< has compare-and-set operation */
    ENGINE_FEATURE_PERSISTENT_STORAGE, /**< has persistent storage support*/
    ENGINE_FEATURE_SECONDARY_ENGINE, /**< performs as pseudo engine */
    ENGINE_FEATURE_ACCESS_CONTROL, /**< has access control feature */
    ENGINE_FEATURE_MULTI_TENANCY,
    ENGINE_FEATURE_LRU, /* Cache implements an LRU */
    ENGINE_FEATURE_VBUCKET, /* Cache implements virtual buckets */
    ENGINE_FEATURE_DATATYPE, /**< uses datatype field */
    /**
     * The engine supports storing the items value into multiple
     * chunks rather than a continous segment.
     */
    ENGINE_FEATURE_ITEM_IOVECTOR,

#define LAST_REGISTERED_ENGINE_FEATURE ENGINE_FEATURE_ITEM_IOVECTOR
} engine_feature_t;

typedef struct {
    /**
     * The identifier of this feature. All values with the most significant bit cleared is reserved
     * for "registered" features.
     */
    uint32_t feature;
    /**
     * A textual description of the feature. (null will print the registered name for the feature
     * (or "Unknown feature"))
     */
    const char* description;
} feature_info;

typedef struct {
    /**
     * Textual description of this engine
     */
    const char* description;
    /**
     * The number of features the server provides
     */
    uint32_t num_features;
    /**
     * An array containing all of the features the engine supports
     */
    feature_info features[1];
} engine_info;

/**
 * A unique_ptr to use with items returned from the engine interface.
 */
namespace cb {
class ItemDeleter;
typedef std::unique_ptr<item, ItemDeleter> unique_item_ptr;

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
}

/**
 * Definition of the first version of the engine interface
 */
typedef struct engine_interface_v1 {
    /**
     * Engine info.
     */
    struct engine_interface interface;

    /**
     * Get a description of this engine.
     *
     * @param handle the engine handle
     * @return a stringz description of this engine
     */
    const engine_info* (*get_info)(gsl::not_null<ENGINE_HANDLE*> handle);

    /**
     * Initialize an engine instance.
     * This is called *after* creation, but before the engine may be used.
     *
     * @param handle the engine handle
     * @param config_str configuration this engine needs to initialize itself.
     */
    ENGINE_ERROR_CODE(*initialize)
    (gsl::not_null<ENGINE_HANDLE*> handle, const char* config_str);

    /**
     * Tear down this engine.
     *
     * @param handle the engine handle
     * @param force the flag indicating the force shutdown or not.
     */
    void (*destroy)(gsl::not_null<ENGINE_HANDLE*> handle, const bool force);

    /*
     * Item operations.
     */

    /**
     * Allocate an item.
     *
     * @param handle the engine handle
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
    cb::EngineErrorItemPair (*allocate)(gsl::not_null<ENGINE_HANDLE*> handle,
                                        const void* cookie,
                                        const DocKey& key,
                                        const size_t nbytes,
                                        const int flags,
                                        const rel_time_t exptime,
                                        uint8_t datatype,
                                        uint16_t vbucket);

    /**
     * Allocate an item.
     *
     * @param handle the engine handle
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
    std::pair<cb::unique_item_ptr, item_info> (*allocate_ex)(
            gsl::not_null<ENGINE_HANDLE*> handle,
            const void* cookie,
            const DocKey& key,
            const size_t nbytes,
            const size_t priv_nbytes,
            const int flags,
            const rel_time_t exptime,
            uint8_t datatype,
            uint16_t vbucket);

    /**
     * Remove an item.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param key the key identifying the item to be removed
     * @param vbucket the virtual bucket id
     * @param mut_info On a successful remove write the mutation details to
     *                 this address.
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*remove)
    (gsl::not_null<ENGINE_HANDLE*> handle,
     const void* cookie,
     const DocKey& key,
     uint64_t* cas,
     uint16_t vbucket,
     mutation_descr_t* mut_info);

    /**
     * Indicate that a caller who received an item no longer needs
     * it.
     *
     * @param handle the engine handle
     * @param item the item to be released
     */
    void (*release)(gsl::not_null<ENGINE_HANDLE*> handle,
                    gsl::not_null<item*> item);

    /**
     * Retrieve an item.
     *
     * @param handle the engine handle
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
    cb::EngineErrorItemPair (*get)(gsl::not_null<ENGINE_HANDLE*> handle,
                                   const void* cookie,
                                   const DocKey& key,
                                   uint16_t vbucket,
                                   DocStateFilter documentStateFilter);

    /**
     * Retrieve metadata for a given item.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     *
     * @return  Pair (ENGINE_SUCCESS, Metadata) if all goes well
     */
    cb::EngineErrorMetadataPair (*get_meta)(
            gsl::not_null<ENGINE_HANDLE*> handle,
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint16_t vbucket);

    /**
     * Optionally retrieve an item. Only non-deleted items may be fetched
     * through this interface (Documents in deleted state may be evicted
     * from memory and we don't want to go to disk in order to fetch these)
     *
     * @param handle the engine handle
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
    cb::EngineErrorItemPair (*get_if)(
            gsl::not_null<ENGINE_HANDLE*> handle,
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint16_t vbucket,
            std::function<bool(const item_info&)> filter);

    /**
     * Lock and Retrieve an item.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param item output variable that will receive the located item
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param lock_timeout the number of seconds to hold the lock
     *                     (0 == use the engines default lock time)
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    cb::EngineErrorItemPair (*get_locked)(gsl::not_null<ENGINE_HANDLE*> handle,
                                          gsl::not_null<const void*> cookie,
                                          const DocKey& key,
                                          uint16_t vbucket,
                                          uint32_t lock_timeout);

    /**
     * Get and update the expiry time for the document
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param expirytime the new expiry time for the object
     * @return A pair of the error code and (optionally) the item
     */
    cb::EngineErrorItemPair (*get_and_touch)(
            gsl::not_null<ENGINE_HANDLE*> handle,
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint16_t vbucket,
            uint32_t expirytime);

    /**
     * Unlock an item.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param key the key to look up
     * @param vbucket the virtual bucket id
     * @param cas the cas value for the locked item
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*unlock)
    (gsl::not_null<ENGINE_HANDLE*> handle,
     gsl::not_null<const void*> cookie,
     const DocKey& key,
     uint16_t vbucket,
     uint64_t cas);

    /**
     * Store an item into the underlying engine with the given
     * state. If the DocumentState is set to DocumentState::Deleted
     * the document shall not be returned unless explicitly asked for
     * documents in that state, and the underlying engine may choose to
     * purge it whenever it please.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param item the item to store
     * @param cas the CAS value for conditional sets
     * @param operation the type of store operation to perform.
     * @param document_state The state the document should have after
     *                       the update
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*store)
    (gsl::not_null<ENGINE_HANDLE*> handle,
     const void* cookie,
     gsl::not_null<item*> item,
     gsl::not_null<uint64_t*> cas,
     ENGINE_STORE_OPERATION operation,
     DocumentState document_state);

    /**
     * Store an item into the underlying engine with the given
     * state only if the predicate argument returns true when called against an
     * existing item.
     *
     * @param handle the engine handle
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
     * @param document_state The state the document should have after
     *                       the update
     *
     * @return a std::pair containing the engine_error code and new CAS
     */
    cb::EngineErrorCasPair (*store_if)(gsl::not_null<ENGINE_HANDLE*> handle,
                                       gsl::not_null<const void*> cookie,
                                       gsl::not_null<item*> item,
                                       uint64_t cas,
                                       ENGINE_STORE_OPERATION operation,
                                       cb::StoreIfPredicate predicate,
                                       DocumentState document_state);

    /**
     * Flush the cache.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*flush)
    (gsl::not_null<ENGINE_HANDLE*> handle, gsl::not_null<const void*> cookie);

    /*
     * Statistics
     */

    /**
     * Get statistics from the engine.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param stat_key optional argument to stats
     * @param nkey the length of the stat_key
     * @param add_stat callback to feed results to the output
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*get_stats)
    (gsl::not_null<ENGINE_HANDLE*> handle,
     const void* cookie,
     const char* stat_key,
     int nkey,
     ADD_STAT add_stat);

    /**
     * Reset the stats.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     */
    void (*reset_stats)(gsl::not_null<ENGINE_HANDLE*> handle,
                        gsl::not_null<const void*> cookie);

    /**
     * Any unknown command will be considered engine specific.
     *
     * @param handle the engine handle
     * @param cookie The cookie provided by the frontend
     * @param request pointer to request header to be filled in
     * @param response function to transmit data
     * @param doc_namespace namespace the command applies to
     *
     * @return ENGINE_SUCCESS if all goes well
     */
    ENGINE_ERROR_CODE(*unknown_command)
    (gsl::not_null<ENGINE_HANDLE*> handle,
     const void* cookie,
     gsl::not_null<protocol_binary_request_header*> request,
     ADD_RESPONSE response,
     DocNamespace doc_namespace);

    /**
     * Set the CAS id on an item.
     */
    void (*item_set_cas)(gsl::not_null<ENGINE_HANDLE*> handle,
                         gsl::not_null<item*> item,
                         uint64_t cas);

    /**
     * Get information about an item.
     *
     * The loader of the module may need the pointers to the actual data within
     * an item. Instead of having to create multiple functions to get each
     * individual item, this function will get all of them.
     *
     * @param handle the engine that owns the object
     * @param cookie connection cookie for this item
     * @param item the item to request information about
     * @param item_info
     * @return true if successful
     */
    bool (*get_item_info)(gsl::not_null<ENGINE_HANDLE*> handle,
                          gsl::not_null<const item*> item,
                          gsl::not_null<item_info*> item_info);

    /**
     * Set information of an item.
     *
     * Set updated item information.
     *
     * @param handle the engine that owns the object
     * @param cookie connection cookie for this item
     * @param item the item who's information is to be updated
     * @param item_info
     * @return true if successful
     */
    bool (*set_item_info)(gsl::not_null<ENGINE_HANDLE*> handle,
                          gsl::not_null<item*> item,
                          gsl::not_null<const item_info*> itm_info);

    struct dcp_interface dcp;

    /**
     * Set the current log level
     *
     * @param handle the engine handle
     * @param level the current log level
     */
    void (*set_log_level)(gsl::not_null<ENGINE_HANDLE*> handle,
                          EXTENSION_LOG_LEVEL level);

    collections_interface collections;

    /**
     * @param handle the engine handle
     * @returns if XATTRs are enabled for this bucket
     */
    bool (*isXattrEnabled)(gsl::not_null<ENGINE_HANDLE*> handle);

} ENGINE_HANDLE_V1;

namespace cb {
class ItemDeleter {
public:
    ItemDeleter() : handle(nullptr) {}

    /**
     * Create a new instance of the item deleter.
     *
     * @param handle_ the handle to the the engine who owns the item
     */
    ItemDeleter(ENGINE_HANDLE* handle_)
        : handle(handle_) {
        if (handle == nullptr) {
            throw std::invalid_argument(
                "cb::ItemDeleter: engine handle cannot be nil");
        }
    }

    /**
     * Create a copy constructor to allow us to use std::move of the item
     */
    ItemDeleter(const ItemDeleter& other) : handle(other.handle) {
    }

    void operator()(item* item) {
        if (handle) {
            auto* v1 = reinterpret_cast<ENGINE_HANDLE_V1*>(handle);
            v1->release(handle, item);
        } else {
            throw std::invalid_argument("cb::ItemDeleter: item attempted to be "
                                        "freed by null engine handle");
        }
    }

private:
    ENGINE_HANDLE* handle;
};

inline EngineErrorItemPair makeEngineErrorItemPair(cb::engine_errc err) {
    return {err, unique_item_ptr{nullptr, ItemDeleter{}}};
}

inline EngineErrorItemPair makeEngineErrorItemPair(cb::engine_errc err,
                                                   item* it,
                                                   ENGINE_HANDLE* handle) {
    return {err, unique_item_ptr{it, ItemDeleter{handle}}};
}
}

/**
 * @}
 */
