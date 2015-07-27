/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_SERVER_API_H
#define MEMCACHED_SERVER_API_H
#include <inttypes.h>

#include <memcached/types.h>
#include <memcached/config_parser.h>

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
         * Get the server's version number.
         *
         * @return the server's version number
         */
        const char* (*server_version)(void);

        /**
         * Generate a simple hash value of a piece of data.
         *
         * @param data pointer to data to hash
         * @param size size of the data to generate the hash value of
         * @param seed an extra seed value for the hash function
         * @return hash value of the data.
         */
        uint32_t (*hash)(const void *data, size_t size, uint32_t seed);

        /**
         * parser config options
         */
        int (*parse_config)(const char *str, struct config_item items[], FILE *error);

        /**
         * Request the server to start a shutdown sequence.
         */
        void (*shutdown)(void);

        /**
         * Get the current configuration from the core..
         * See "stats settings" for a list of legal keywords
         */
        bool (*get_config)(struct config_item items[]);

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
         * See if this connection have admin access or not
         */
        bool (*is_admin)(const void *cookie);

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
    } SERVER_COOKIE_API;

#ifdef WIN32
#undef interface
#endif

    typedef SERVER_HANDLE_V1* (*GET_SERVER_API)(void);

#ifdef __cplusplus
}
#endif

#endif
