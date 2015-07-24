/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#pragma once

#include <memcached/engine.h>
#include <cstring>
#include "timings.h"
#include "topkeys.h"

enum class BucketState : uint8_t {
    /** This bucket entry is not used */
    None,
    /** The bucket is currently being created (may not be used yet) */
    Creating,
    /** The bucket is currently initializing itself */
    Initializing,
    /** The bucket is ready for use */
    Ready,
    /**
     * The bucket is currently being stopped. Awaiting clients to
     * be disconnected.
     */
    Stopping,
    /** The bucket is currently being destroyed. */
    Destroying
};

enum class BucketType : uint8_t {
    Unknown,
    NoBucket,
    Memcached,
    Couchstore,
    EWouldBlock
};

#define MAX_BUCKET_NAME_LENGTH 100

struct engine_event_handler;

class Bucket {
public:
    Bucket()
        : clients(0),
          state(BucketState::None),
          type(BucketType::Unknown),
          stats(nullptr),
          topkeys(nullptr)
    {
        std::memset(name, 0, sizeof(name));
        std::memset(engine_event_handlers, 0, sizeof(engine_event_handlers));
        cb_mutex_initialize(&mutex);
        cb_cond_initialize(&cond);
    }

    ~Bucket() {
        cb_mutex_destroy(&mutex);
        cb_cond_destroy(&cond);
    }

    /**
     * Mutex protecting the state and refcount. (@todo move to std::mutex)
     */
    cb_mutex_t mutex;
    cb_cond_t cond;

    /**
     * The number of clients currently connected to the bucket (performed
     * a SASL_AUTH to the bucket.
     */
    uint32_t clients;

    /**
     * The current state of the bucket.
     */
    BucketState state;

    /**
     * The type of bucket
     */
    BucketType type;

    /**
     * The name of the bucket (and space for the '\0')
     */
    char name[MAX_BUCKET_NAME_LENGTH + 1];

    /**
     * The array of registered event handlers
     */
    struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

    /**
     * @todo add properties!
     */

    /**
     * Pointer to the actual engine serving the request
     */
    ENGINE_HANDLE_V1 *engine; /* Pointer to the bucket */

    /**
     * Statistics array
     */
    struct thread_stats *stats;

    /**
     * Timing data
     */
    Timings timings;

    /**
     * Topkeys
     */
    TopKeys *topkeys;
};

class Connection;
/**
 * Get the name of the associated bucket. Note that this function must
 * only be called while the current connection is being served (otherwise
 * a race may occur causing the data to be returned to be modified).
 *
 * The client should not try to modify (or release) the returned pointer
 * as it points into the static area of the bucket array. The entry
 * is valid as long as the current connection is being served (unless it
 * tries to switch bucket/delete bucket, then it is invalidated).
 */
extern const char* getBucketName(const Connection* c);