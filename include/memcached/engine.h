/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_ENGINE_H
#define MEMCACHED_ENGINE_H

#ifndef __cplusplus
#include <stdbool.h>
#endif

#include <cstring>
#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>

#include "memcached/allocator_hooks.h"
#include "memcached/buffer.h"
#include "memcached/callback.h"
#include "memcached/config_parser.h"
#include "memcached/dcp.h"
#include "memcached/engine_common.h"
#include "memcached/extension.h"
#include "memcached/protocol_binary.h"
#include "memcached/server_api.h"
#include "memcached/types.h"
#include "memcached/vbucket.h"

#ifdef __cplusplus
extern "C" {
#endif

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
        SERVER_CORE_API *core;
        SERVER_STAT_API *stat;
        SERVER_EXTENSION_API *extension;
        SERVER_CALLBACK_API *callback;
        ENGINE_HANDLE *engine;
        SERVER_LOG_API *log;
        SERVER_COOKIE_API *cookie;
        ALLOCATOR_HOOKS_API *alloc_hooks;
    };

    typedef enum { TAP_MUTATION = 1,
                   TAP_DELETION,
                   TAP_FLUSH,
                   TAP_OPAQUE,
                   TAP_VBUCKET_SET,
                   TAP_ACK,
                   TAP_DISCONNECT,
                   TAP_NOOP,
                   TAP_PAUSE,
                   TAP_CHECKPOINT_START,
                   TAP_CHECKPOINT_END } tap_event_t;

    /**
     * An iterator for the tap stream.
     * The memcached core will keep on calling this function as long as a tap
     * client is connected to the server. Each event returned by the iterator
     * will be encoded in the binary protocol with the appropriate command opcode.
     *
     * If the engine needs to store extra information in the tap stream it should
     * do so by returning the data through the engine_specific pointer. This data
     * should be valid for the core to use (read only) until the next invocation
     * of the iterator, of if the connection is closed.
     *
     * @param handle the engine handle
     * @param cookie identification for the tap stream
     * @param item item to send returned here (check tap_event_t)
     * @param engine_specific engine specific data returned here
     * @param nengine_specific number of bytes of engine specific data
     * @param ttl ttl for this item (Tap stream hops)
     * @param flags tap flags for this object
     * @param seqno sequence number to send
     * @param vbucket the virtual bucket id
     * @return the tap event to send (or TAP_PAUSE if there isn't any events)
     */
    typedef tap_event_t (*TAP_ITERATOR)(ENGINE_HANDLE* handle,
                                        const void *cookie,
                                        item **item,
                                        void **engine_specific,
                                        uint16_t *nengine_specific,
                                        uint8_t *ttl,
                                        uint16_t *flags,
                                        uint32_t *seqno,
                                        uint16_t *vbucket);

    typedef ENGINE_ERROR_CODE (*engine_get_vb_map_cb)(const void *cookie,
                                                      const void *map,
                                                      size_t mapsize);

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
    typedef ENGINE_ERROR_CODE (*CREATE_INSTANCE)(uint64_t interface,
                                                 GET_SERVER_API get_server_api,
                                                 ENGINE_HANDLE** handle);

    /**
     * The signature for the "destroy_engine" function exported from the module.
     *
     * This function is called prior to closing of the module. This function should
     * free any globally allocated resources.
     *
     */
    typedef void (*DESTROY_ENGINE)(void);

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
        const char *description;
    } feature_info;

    typedef struct {
        /**
         * Textual description of this engine
         */
        const char *description;
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
     * DocNamespace "Document Namespace"
     * Meta-data that applies to every document stored in an engine.
     *
     * A document "key" with the flag DefaultCollection is not the same document
     * as "key" with the Collections flag and so on...
     *
     * DefaultCollection: describes "legacy" documents stored in a bucket by
     * clients that do not understand collections.
     *
     * Collections: describes documents that have a collection name as part of
     * the key. E.g. "planet::earth" and "planet::mars" are documents belonging
     * to a "planet" collection.
     *
     * System: describes documents that are created by the system for our own
     * uses. This is only planned for use with the collection feature where
     * special keys are interleaved in the users data stream to represent create
     * and delete events. In future more generic "system documents" maybe
     * created by the core but until such plans are more clear, ep-engine will
     * deny the core from performing operations in the System DocNamespace.
     * DocNamespace values are persisted ot the database and thus are fully
     * described now ready for future use.
     */
    enum class DocNamespace : uint8_t {
        DefaultCollection = 0,
        Collections = 1,
        System = 2
    };

    /**
     * DocKey is a non-owning structure used to describe a document keys over
     * the engine-API. All API commands working with "keys" must specify the
     * data, length and the namespace with which the document applies to.
     */
    struct DocKey : public cb::const_byte_buffer {

        /**
         * Standard constructor - creates a view onto key/nkey
         */
        DocKey(const uint8_t* key, size_t nkey, DocNamespace ins)
             : cb::const_byte_buffer(key, nkey),
               doc_namespace(ins) {}

        /**
         * C-string constructor - only for use with null terminated strings and
         * creates a view onto key/strlen(key).
         */
        DocKey(const char* key, DocNamespace ins)
             : DocKey(reinterpret_cast<const uint8_t*>(key),
               std::strlen(key), ins) {}

        /**
         * std::string constructor - creates a view onto the std::string internal
         * C-string buffer - i.e. c_str() for size() bytes
         */
        DocKey(const std::string& key, DocNamespace ins)
             : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                      key.size(),
                      ins) {}

        DocNamespace doc_namespace;
    };

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
        const engine_info* (*get_info)(ENGINE_HANDLE* handle);

        /**
         * Initialize an engine instance.
         * This is called *after* creation, but before the engine may be used.
         *
         * @param handle the engine handle
         * @param config_str configuration this engine needs to initialize itself.
         */
        ENGINE_ERROR_CODE (*initialize)(ENGINE_HANDLE* handle,
                                        const char* config_str);

        /**
         * Tear down this engine.
         *
         * @param handle the engine handle
         * @param force the flag indicating the force shutdown or not.
         */
        void (*destroy)(ENGINE_HANDLE* handle, const bool force);

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
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*allocate)(ENGINE_HANDLE* handle,
                                      const void* cookie,
                                      item **item,
                                      const DocKey& key,
                                      const size_t nbytes,
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
        ENGINE_ERROR_CODE (*remove)(ENGINE_HANDLE* handle,
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
         * @param cookie The cookie provided by the frontend
         * @param item the item to be released
         */
        void (*release)(ENGINE_HANDLE* handle, const
                        void *cookie,
                        item* item);

        /**
         * Retrieve an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item output variable that will receive the located item
         * @param key the key to look up
         * @param vbucket the virtual bucket id
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*get)(ENGINE_HANDLE* handle,
                                 const void* cookie,
                                 item** item,
                                 const DocKey& key,
                                 uint16_t vbucket);

        /**
         * Store an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item the item to store
         * @param cas the CAS value for conditional sets
         * @param operation the type of store operation to perform.
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*store)(ENGINE_HANDLE* handle,
                                   const void *cookie,
                                   item* item,
                                   uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation);

        /**
         * Flush the cache.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param when time at which the flush should take effect
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*flush)(ENGINE_HANDLE* handle,
                                   const void* cookie, time_t when);

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
        ENGINE_ERROR_CODE (*get_stats)(ENGINE_HANDLE* handle,
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
        void (*reset_stats)(ENGINE_HANDLE* handle, const void *cookie);

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
        ENGINE_ERROR_CODE (*unknown_command)(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response,
                                             DocNamespace doc_namespace);

        /* TAP operations */

        /**
         * Callback for all incoming TAP messages. It is up to the engine
         * to determine what to do with the event. The core will create and send
         * a TAP_ACK message if the flag section contains TAP_FLAG_SEND_ACK with
         * the status byte mapped from the return code.
         *
         * @param handle the engine handle
         * @param cookie identification for the tap stream
         * @param engine_specific pointer to engine specific data (received)
         * @param nengine_specific number of bytes of engine specific data
         * @param ttl ttl for this item (Tap stream hops)
         * @param tap_flags tap flags for this object
         * @param tap_event the tap event from over the wire
         * @param tap_seqno sequence number for this item
         * @param key the key in the message
         * @param nkey the number of bytes in the key
         * @param flags the flags for the item
         * @param exptime the expiry time for the object
         * @param cas the cas for the item
         * @param data the data for the item
         * @param ndata the number of bytes in the object
         * @param vbucket the virtual bucket for the object
         * @return ENGINE_SUCCESS for success
         */
        ENGINE_ERROR_CODE (*tap_notify)(ENGINE_HANDLE* handle,
                                        const void *cookie,
                                        void *engine_specific,
                                        uint16_t nengine,
                                        uint8_t ttl,
                                        uint16_t tap_flags,
                                        tap_event_t tap_event,
                                        uint32_t tap_seqno,
                                        const void *key,
                                        size_t nkey,
                                        uint32_t flags,
                                        uint32_t exptime,
                                        uint64_t cas,
                                        uint8_t datatype,
                                        const void *data,
                                        size_t ndata,
                                        uint16_t vbucket);

        /**
         * Get (or create) a Tap iterator for this connection.
         * @param handle the engine handle
         * @param cookie The connection cookie
         * @param client The "name" of the client
         * @param nclient The number of bytes in the client name
         * @param flags Tap connection flags
         * @param userdata Specific userdata the engine may know how to use
         * @param nuserdata The size of the userdata
         * @return a tap iterator to iterate through the event stream
         */
        TAP_ITERATOR (*get_tap_iterator)(ENGINE_HANDLE* handle, const void* cookie,
                                         const void* client, size_t nclient,
                                         uint32_t flags,
                                         const void* userdata, size_t nuserdata);

        /**
         * Set the CAS id on an item.
         */
        void (*item_set_cas)(ENGINE_HANDLE *handle, const void *cookie,
                             item *item, uint64_t cas);

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
        bool (*get_item_info)(ENGINE_HANDLE *handle,
                              const void *cookie,
                              const item* item,
                              item_info *item_info);

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
        bool (*set_item_info)(ENGINE_HANDLE *handle,
                              const void *cookie,
                              item* item,
                              const item_info *itm_info);

        /**
         * Get the vbucket map stored in the engine
         *
         * @param handle the engine handle
         * @param cookie The connection cookie
         * @param callback a function the engine may call to "return" the
         *                 buffer.
         * @return ENGINE_SUCCESS for success
         */
        ENGINE_ERROR_CODE (*get_engine_vb_map)(ENGINE_HANDLE* handle,
                                               const void * cookie,
                                               engine_get_vb_map_cb callback);

        struct dcp_interface dcp;

        /**
         * Set the current log level
         *
         * @param handle the engine handle
         * @param level the current log level
         */
        void (*set_log_level)(ENGINE_HANDLE* handle, EXTENSION_LOG_LEVEL level);
    } ENGINE_HANDLE_V1;

    /**
     * @}
     */
#ifdef __cplusplus
}
#endif

#endif
