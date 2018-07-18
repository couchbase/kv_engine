/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "memcached/engine_common.h"

#pragma once

/**
 * Event types for callbacks to the engine indicating state
 * changes in the server.
 */
typedef enum {
    ON_DISCONNECT = 0, /**< A connection was terminated. */
    ON_LOG_LEVEL = 1, /**< Changed log level */
    ON_DELETE_BUCKET = 2, /**< Deletion of the bucket is initiated */
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
typedef void (*EVENT_CALLBACK)(const void* cookie,
                               ENGINE_EVENT_TYPE type,
                               const void* event_data,
                               const void* cb_data);

/**
 * The API provided by the server to manipulate callbacks
 */
struct ServerCallbackIface {
    virtual ~ServerCallbackIface() = default;
    /**
     * Register an event callback.
     *
     * @param type the type of event to register
     * @param cb the callback to fire when the event occurs
     * @param cb_data opaque data to be given back to the caller
     *        on event
     */
    virtual void register_callback(ENGINE_HANDLE* engine,
                                   ENGINE_EVENT_TYPE type,
                                   EVENT_CALLBACK cb,
                                   const void* cb_data) = 0;

    /**
     * Fire callbacks
     */
    virtual void perform_callbacks(ENGINE_EVENT_TYPE type,
                                   const void* data,
                                   const void* cookie) = 0;
};
