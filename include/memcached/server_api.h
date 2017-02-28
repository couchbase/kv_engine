/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_SERVER_API_H
#define MEMCACHED_SERVER_API_H
#include <inttypes.h>

#include <memcached/types.h>
#include <memcached/config_parser.h>
#include <memcached/rbac.h>
#include <memcached/protocol_binary.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct {
        /**
         * The current time.
         */
        rel_time_t (*get_current_time)(void);

        /**
         * Get the relative time for the given time_t value.
         */
        rel_time_t (*realtime)(const time_t exptime);

        /**
         * Get the absolute time for the given rel_time_t value.
         */
        time_t (*abstime)(const rel_time_t exptime);

        /**
         * parser config options
         */
        int (*parse_config)(const char *str, struct config_item items[], FILE *error);

        /**
         * Request the server to start a shutdown sequence.
         */
        void (*shutdown)(void);

        /**
         * Get the maximum size of an iovec the core supports receiving
         * through the item_info structure. The underlying engine may
         * support using more entries to hold its data internally, but
         * when making the data available for the core it must fit
         * within these limits.
         */
        size_t (*get_max_item_iovec_size)(void);
    } SERVER_CORE_API;

    typedef struct {
        /**
         * Tell the server we've evicted an item.
         */
        void (*evicting)(const void *cookie,
                         const void *key,
                         int nkey);
    } SERVER_STAT_API;

    /**
     * Commands to operate on a specific cookie.
     */
    typedef struct {
        /**
         * Store engine-specific session data on the given cookie.
         *
         * The engine interface allows for a single item to be
         * attached to the connection that it can use to track
         * connection-specific data throughout duration of the
         * connection.
         *
         * @param cookie The cookie provided by the frontend
         * @param engine_data pointer to opaque data
         */
        void (*store_engine_specific)(const void *cookie, void *engine_data);

        /**
         * Retrieve engine-specific session data for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the data provied by store_engine_specific or NULL
         *         if none was provided
         */
        void *(*get_engine_specific)(const void *cookie);

        /**
         * Check if datatype is supported by the connection.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return true if supported or else false.
         */
        bool (*is_datatype_supported)(const void *cookie);

        /**
         * Check if mutation extras is supported by the connection.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return true if supported or else false.
         */
        bool (*is_mutation_extras_supported)(const void *cookie);

        /**
         * Check if xattrs is supported by the connection.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return true if supported or else false.
         */
        bool (*is_xattr_supported)(const void *cookie);

        /**
         * Retrieve the opcode of the connection, if
         * ewouldblock flag is set. Please note that the ewouldblock
         * flag for a connection is cleared before calling into
         * the engine interface, so this method only works in the
         * notify hooks.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the opcode from the binary_header saved in the
         * connection.
         */
        uint8_t (*get_opcode_if_ewouldblock_set)(const void *cookie);

        /**
         * Validate given ns_server's session cas token against
         * saved token in memached, and if so incrment the session
         * counter.
         *
         * @param cas The cas token from the request
         *
         * @return true if session cas matches the one saved in
         * memcached
         */
        bool (*validate_session_cas)(const uint64_t cas);

        /**
         * Decrement session_cas's counter everytime a control
         * command completes execution.
         */
        void (*decrement_session_ctr)(void);

        /**
         * Let a connection know that IO has completed.
         * @param cookie cookie representing the connection
         * @param status the status for the io operation
         */
        void (*notify_io_complete)(const void *cookie,
                                   ENGINE_ERROR_CODE status);

        /**
         * Notify the core that we're holding on to this cookie for
         * future use. (The core guarantees it will not invalidate the
         * memory until the cookie is invalidated by calling release())
         */
        ENGINE_ERROR_CODE (*reserve)(const void *cookie);

        /**
         * Notify the core that we're releasing the reference to the
         * The engine is not allowed to use the cookie (the core may invalidate
         * the memory)
         */
        ENGINE_ERROR_CODE (*release)(const void *cookie);

        /**
         * Set the priority for this connection
         */
        void (*set_priority)(const void *cookie, CONN_PRIORITY priority);

        /**
         * Get the bucket the connection is bound to
         *
         * @cookie The connection object
         * @return the bucket identifier for a cookie
         */
        bucket_id_t (*get_bucket_id)(const void *cookie);

        /**
         * Get connection id
         *
         * @param cookie the cookie sent to the engine for an operation
         * @return a unique identifier for a connection
         */
        uint64_t (*get_connection_id)(const void* cookie);

        /**
         * Check if the cookie have the specified privilege in it's
         * active set.
         *
         * @todo We should probably add the key we want to access as part
         *       of the API. We're going to need that when we're adding
         *       support for collections. For now let's assume that it
         *       won't be a big problem to fix that later on.
         * @param cookie the cookie sent to the engine for an operation
         * @param privilege the privilege to check for
         * @return true if the cookie have the privilege in its active set,
         *         false otherwise
         */
        cb::rbac::PrivilegeAccess (* check_privilege)(const void* cookie,
                                                      const cb::rbac::Privilege privilege);

        /**
         * Method to map an engine error code to the appropriate mcbp response
         * code (the client may not support all error codes so we may have
         * to remap some).
         *
         * @param cookie the client cookie (to look up the client connection)
         * @param code the engine error code to get the mcbp response code.
         * @return the mcbp response status to use
         * @throws std::engine_error if the error code results in being
         *                           ENGINE_DISCONNECT after remapping
         *         std::logic_error if the error code doesn't make sense
         *         std::invalid_argument if the code doesn't exist
         */
        protocol_binary_response_status (*engine_error2mcbp)(const void* cookie,
                                                             ENGINE_ERROR_CODE code);
    } SERVER_COOKIE_API;

    struct SERVER_DOCUMENT_API {
        /**
         * This callback is called from the underlying engine right before
         * it is linked into the list of available documents (it is currently
         * not visible to anyone). The engine should have validated all
         * properties set in the document by the client and the core, and
         * assigned a new CAS number for the document (and sequence number if
         * the underlying engine use those).
         *
         * The callback may at this time do post processing of the document
         * content (it is allowed to modify the content data, but not
         * reallocate or change the size of the data in any way).
         *
         * Given that the engine MAY HOLD LOCKS when calling this function
         * the core is *NOT* allowed to acquire *ANY* locks (except for doing
         * some sort of memory allocation for a temporary buffer).
         *
         * @param cookie The cookie provided to the engine for the storage
         *               command which may (which may hold more context)
         * @param info the items underlying data
         * @return ENGINE_SUCCESS means that the underlying engine should
         *                        proceed to link the item. All other
         *                        error codes means that the engine should
         *                        *NOT* link the item
         */
        ENGINE_ERROR_CODE (*pre_link)(const void* cookie, item_info& info);

        /**
         * This callback is called from the underlying engine right before
         * a particular document expires. The callback is responsible for
         * modifying the contents of the itm_info passed in. The updated
         * total size is available in itm_info.nbytes.
         *
         * @param itm_info info pertaining to the item that is to be expired.
         * @return true indicating that the info has been modified in itm_info.
         *         false indicating that there is no data available in itm_info.
         * @throws std::bad_alloc in case of memory allocation failure
         * @throws std::logic_error if the data has grown
         */
        bool (*pre_expiry)(item_info& itm_info);
    };

#ifdef WIN32
#undef interface
#endif

    typedef SERVER_HANDLE_V1* (*GET_SERVER_API)(void);

#ifdef __cplusplus
}
#endif

#endif
