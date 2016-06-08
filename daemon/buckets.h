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
#include <array>
#include <cstring>
#include <vector>
#include <platform/thread.h>

#include "timings.h"
#include "topkeys.h"
#include "task.h"

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

inline const char *to_string(const BucketType &type) {
    switch (type) {
    case BucketType::Memcached:
        return "Memcached";
    case BucketType::Couchstore:
        return "Couchstore";
    case BucketType::EWouldBlock:
        return "EWouldBlock";
    case BucketType::NoBucket:
        return "No Bucket";
    case BucketType::Unknown:
        return "Uknown";
    }
    throw std::logic_error("Invalid bucket type: " + std::to_string(int(type)));
}

#define MAX_BUCKET_NAME_LENGTH 100

struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
};

// Set of engine event handlers, one per event type.
typedef std::array<std::vector<struct engine_event_handler>,
                   MAX_ENGINE_EVENT_TYPE + 1> engine_event_handler_array_t;

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
        cb_mutex_initialize(&mutex);
        cb_cond_initialize(&cond);
    }

    /* Copy-construct. Note acquires the lock of `other` ensuring a
     * consistent state is copied.
     */
    Bucket(const Bucket& other);

    ~Bucket() {
        cb_mutex_destroy(&mutex);
        cb_cond_destroy(&cond);
    }

    /**
     * Mutex protecting the state and refcount. (@todo move to std::mutex).
     */
    mutable cb_mutex_t mutex;
    mutable cb_cond_t cond;

    /**
     * The number of clients currently connected to the bucket (performed
     * a SASL_AUTH to the bucket.
     */
    uint32_t clients;

    /**
     * The current state of the bucket. Atomic as we permit it to be
     * read without acquiring the mutex, for example in
     * is_bucket_dying().
     */
    std::atomic<BucketState> state;

    /**
     * The type of bucket
     */
    BucketType type;

    /**
     * The name of the bucket (and space for the '\0')
     */
    char name[MAX_BUCKET_NAME_LENGTH + 1];

    /**
     * An array of registered event handler vectors, one for each type.
     */
    engine_event_handler_array_t engine_event_handlers;

    /**
     * @todo add properties!
     */

    /**
     * Pointer to the actual engine serving the request
     */
    ENGINE_HANDLE_V1 *engine; /* Pointer to the bucket */

    /**
     * Statistics array, one per front-end thread.
     */
    struct thread_stats *stats;

    /**
     * Command timing data
     */
    Timings timings;

    /**
     *  Sub-document JSON parser (subjson) operation execution time histogram.
     */
    TimingHistogram subjson_operation_times;

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

/**
 * All of the buckets are stored in the following vector. It is to be
 * treated as an array whos size is set at runtime. Once set it never
 * changes
 */
extern std::vector<Bucket> all_buckets;

cJSON *get_bucket_details(int idx);

/**
 * Is the connected bucket currently dying?
 *
 * If the bucket is dying (being deleted) the connection object will be
 * disconnected (for MCBP connections this means closed)
 *
 * @param c the connection to query
 * @return true if it is dying, false otherwise
 */
bool is_bucket_dying(Connection *c);
bucket_id_t get_bucket_id(const void *cookie);


namespace BucketValidator {
    class InvalidBucketName : public std::invalid_argument {
    public:
        InvalidBucketName(const std::string &msg)
            : std::invalid_argument(msg) {
            // empty
        }
    };

    class InvalidBucketType : public std::invalid_argument {
    public:
        InvalidBucketType(const std::string &msg)
            : std::invalid_argument(msg) {
            // empty
        }
    };

    /**
     * Validate that a bucket name confirms to the restrictions for bucket
     * names.
     *
     * @param name the name to validate
     * @param errors where to store a textual description of the problems
     * @return true if the bucket name is valid, false otherwise
     */
    bool validateBucketName(const std::string& name, std::string& errors);

    /**
     * Validate that a bucket type is one of the supported types
     *
     * @param type the type to validate
     * @param errors where to store a textual description of the problems
     * @return true if the bucket type is valid and supported, false otherwise
     */
    bool validateBucketType(const BucketType& type, std::string& errors);
}

/**
 * The CreateBucketThread is as the name implies a thread who creates a new
 * bucket.
 */
class CreateBucketThread : public Couchbase::Thread {
public:
    /**
     * Initialize this bucket creation thread.
     *
     * @param name_ the name of the bucket to create
     * @param config_ the buckets configuration
     * @param type_ the type of bucket to create
     * @param connection_ the connection that requested the operation (and
     *                    should be signalled when the creation is complete)
     *
     * @throws std::illegal_arguments if bucket name contains illegal
     *                                characters
     */
    CreateBucketThread(const std::string& name_,
                       const std::string& config_,
                       const BucketType& type_,
                       Connection& connection_,
                       Task* task_)
        : Couchbase::Thread("mc:bucket_add"),
          name(name_),
          config(config_),
          type(type_),
          connection(connection_),
          task(task_),
          result(ENGINE_DISCONNECT) {
        // Empty
    }

    ~CreateBucketThread() {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    Connection& getConnection() const {
        return connection;
    }

    ENGINE_ERROR_CODE getResult() const {
        return result;
    }

    const std::string& getErrorMessage() const {
        return error;
    }

protected:
    virtual void run() override;

private:
    /**
     * The actual implementation of the bucket creation.
     */
    void create();

    std::string name;
    std::string config;
    BucketType type;
    Connection& connection;
    Task* task;
    ENGINE_ERROR_CODE result;
    std::string error;
};

/**
 * The DestroyBucketThread is as the name implies a thread is responsible for
 * deleting a bucket.
 */
class DestroyBucketThread : public Couchbase::Thread {
public:
    /**
     * Initialize this bucket creation task.
     *
     * @param name_ the name of the bucket to delete
     * @param force_ should the bucket be forcibly shut down or should it
     *               try to perform a clean shutdown
     * @param connection_ the connection that requested the operation
     * @param task_ the task to notify when deletion is complete
     */
    DestroyBucketThread(const std::string& name_,
                        bool force_,
                        Connection* connection_,
                        Task* task_)
        : Couchbase::Thread("mc:bucket_del"),
          name(name_),
          force(force_),
          connection(connection_),
          task(task_),
          result(ENGINE_DISCONNECT) {
    }

    ~DestroyBucketThread() {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    Connection* getConnection() const {
        return connection;
    }

    ENGINE_ERROR_CODE getResult() const {
        return result;
    }

protected:
    virtual void run() override;

private:

    /**
     * The actual implementation of the bucket deletion.
     */
    void destroy();

    std::string name;
    bool force;
    Connection* connection;
    Task* task;
    ENGINE_ERROR_CODE result;
};
