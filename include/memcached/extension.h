/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <memcached/engine_common.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_api.h>
#include <memcached/types.h>
#include <cstdint>

/**
 * \defgroup Extension Generic Extensions API
 * \addtogroup Extension
 * @{
 *
 * Definition of the generic extension API to memcached.
 */

/**
 * Response codes for extension operations.
 */
typedef enum {
    /** The command executed successfully */
    EXTENSION_SUCCESS = 0x00,
    /** A fatal error occurred, and the server should shut down as soon
     * as possible */
    EXTENSION_FATAL = 0xfe,
    /** Generic failure. */
    EXTENSION_FAILED = 0xff
} EXTENSION_ERROR_CODE;

typedef enum {
    /**
     * A generic extention that don't provide a functionality to the
     * memcached core, but lives in the memcached address space.
     */
    EXTENSION_DAEMON = 0x00,
    /**
     * A log consumer
     */
    EXTENSION_LOGGER = 0x01
} extension_type_t;

/**
 * Deamon extensions should provide the following descriptor when
 * they register themselves.
 */
typedef struct extension_daemon_descriptor {
    /**
     * Get the name of the descriptor. The memory area returned by this
     * function has to be valid until the descriptor is unregistered.
     */
    const char* (*get_name)(void);

    /**
     * Deamon descriptors are stored in a linked list in the memcached
     * core by using this pointer. Please do not modify this pointer
     * by yourself until you have unregistered the descriptor.
     * The <b>only</b> time it is safe for an extension to walk this
     * list is during initialization of the modules.
     */
    struct extension_daemon_descriptor* next;
    } EXTENSION_DAEMON_DESCRIPTOR;

    typedef enum {
        EXTENSION_LOG_DEBUG = 1,
        EXTENSION_LOG_INFO,
        EXTENSION_LOG_NOTICE,
        EXTENSION_LOG_WARNING,
        EXTENSION_LOG_FATAL
    } EXTENSION_LOG_LEVEL;

    /**
     * Log extensions should provide the following rescriptor when
     * they register themselves. Please note that if you register a log
     * extension it will <u>replace</u> old one. If you want to be nice to
     * the user you should allow your logger to be chained.
     *
     * Please note that the memcached server will <b>not</b> call the log
     * function if the verbosity level is too low. This is a perfomance
     * optimization from the core to avoid potential formatting of output
     * that may be thrown away.
     */
    typedef struct {
        /**
         * Get the name of the descriptor. The memory area returned by this
         * function has to be valid until the descriptor is unregistered.
         */
        const char* (*get_name)(void);

        /**
         * Add an entry to the log.
         * @param severity the severity for this log entry
         * @param client_cookie the client we're serving (may be NULL if not
         *                      known)
         * @param fmt format string to add to the log
         */
        void (*log)(EXTENSION_LOG_LEVEL severity,
                    const void* client_cookie,
                    const char *fmt, ...);
        /**
         * Tell the logger to shut down (flush buffers, close files etc)
         * @param force If true, attempt to forcefully shutdown as quickly as
         *              possible - don't assume any other code (e.g. background
         *              threads) will be run after this call.
         *              Note: This is designed for 'emergency' situations such
         *              as a fatal signal raised, where we want to try and get
         *              any pending log messages written before we die.
         */
        void (*shutdown)(bool force);

        /**
         * Tell the logger to flush it's buffers
         */
        void (*flush)();
    } EXTENSION_LOGGER_DESCRIPTOR;

    typedef struct {
        EXTENSION_LOGGER_DESCRIPTOR* (*get_logger)(void);
        EXTENSION_LOG_LEVEL (*get_level)(void);
        void (*set_level)(EXTENSION_LOG_LEVEL severity);
    } SERVER_LOG_API;

    typedef struct {
        char *value;
        size_t length;
    } mc_extension_token_t;

    /**
     * The signature for the "memcached_extensions_initialize" function
     * exported from the loadable module.
     *
     * @param config configuration for this extension
     * @param GET_SERVER_API pointer to a function to get a specific server
     *                       API from. server_extension_api contains functions
     *                       to register extensions.
     * @return one of the error codes above.
     */
    typedef EXTENSION_ERROR_CODE (*MEMCACHED_EXTENSIONS_INITIALIZE)(const char *config, GET_SERVER_API get_server_api);


    /**
     * The API provided by the server to manipulate the list of server
     * server extensions.
     */
    typedef struct {
        /**
         * Register an extension
         *
         * @param type The type of extension to register (ex: daemon, logger etc)
         * @param extension The extension to register
         * @return true if success, false otherwise
         */
        bool (*register_extension)(extension_type_t type, void *extension);

        /**
         * Unregister an extension
         *
         * @param type The type of extension to unregister
         * @param extension The extension to unregister
         */
        void (*unregister_extension)(extension_type_t type, void *extension);

        /**
         * Get the registered extension for a certain type. This is useful
         * if you would like to replace one of the handlers with your own
         * extension to proxy functionality.
         *
         * @param type The type of extension to get
         * @param extension Pointer to the registered event. Please note that
         *        if the extension allows for multiple instances of the
         *        extension there will be a "next" pointer inside the element
         *        that can be used for object traversal.
         */
        void *(*get_extension)(extension_type_t type);
    } SERVER_EXTENSION_API;

    /**
     * @}
     */
