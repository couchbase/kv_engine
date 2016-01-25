/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_CALLBACK_H
#define MEMCACHED_CALLBACK_H

#include "memcached/engine_common.h"

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Event types for callbacks to the engine indicating state
     * changes in the server.
     */
    typedef enum {
        ON_DISCONNECT = 0, /**< A connection was terminated. */
        ON_LOG_LEVEL = 1, /**< Changed log level */
        ON_DELETE_BUCKET = 2 /**< Deletion of the bucket is initiated */
    } ENGINE_EVENT_TYPE;

    #define MAX_ENGINE_EVENT_TYPE 3

    /**
     * Callback for server events.
     *
     * @param cookie The cookie provided by the frontend
     * @param type the type of event
     * @param event_data additional event-specific data.
     * @param cb_data data as registered
     */
    typedef void (*EVENT_CALLBACK)(const void *cookie,
                                   ENGINE_EVENT_TYPE type,
                                   const void *event_data,
                                   const void *cb_data);

    /**
     * The API provided by the server to manipulate callbacks
     */
    typedef struct {
        /**
         * Register an event callback.
         *
         * @param type the type of event to register
         * @param cb the callback to fire when the event occurs
         * @param cb_data opaque data to be given back to the caller
         *        on event
         */
        void (*register_callback)(ENGINE_HANDLE *engine,
                                  ENGINE_EVENT_TYPE type,
                                  EVENT_CALLBACK cb,
                                  const void *cb_data);

        /**
         * Fire callbacks
         */
        void (*perform_callbacks)(ENGINE_EVENT_TYPE type,
                                  const void *data,
                                  const void *cookie);
    } SERVER_CALLBACK_API;

#ifdef __cplusplus
}
#endif

#endif
