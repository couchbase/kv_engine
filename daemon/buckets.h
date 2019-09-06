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

#include "bucket_type.h"
#include "cluster_config.h"
#include "mcbp_validators.h"
#include "timings.h"

#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/server_callback_iface.h>
#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>
#include <utilities/hdrhistogram.h>

#include <condition_variable>
#include <memory>

struct thread_stats;
struct DcpIface;
class TopKeys;
class Connection;

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
    enum class State : uint8_t {
        /// This bucket entry is not used
        None,
        /// The bucket is currently being created (may not be used yet)
        Creating,
        /// The bucket is currently initializing itself
        Initializing,
        /// The bucket is ready for use
        Ready,
        /// The bucket is currently being stopped. Awaiting clients to
        /// be disconnected.
        Stopping,
        /// The bucket is currently being destroyed.
        Destroying
    };

    Bucket();

    /// The bucket contains pointers to other objects and we don't want to
    /// make a deep copy of them (or "share" the pointers). No one should need
    /// to create a copy of the bucket anyway, so it's most likely a bug
    /// if people ended up making a copy.
    Bucket(const Bucket& other) = delete;

    /// @returns a pointer to the actual engine serving the request
    EngineIface* getEngine() const;

    void setEngine(EngineIface* engine);

    /**
     * @returns the DCP interface for the connected bucket, or nullptr if the
     *          conencted bucket doesn't implement DCP.
     */
    DcpIface* getDcpIface() const;

    /**
     * Mutex protecting the state and refcount. (@todo move to std::mutex).
     */
    mutable std::mutex mutex;
    mutable std::condition_variable cond;

    /**
     * The number of clients currently connected to the bucket (performed
     * a SASL_AUTH to the bucket.
     */
    uint32_t clients{0};

    /**
     * The current state of the bucket. Atomic as we permit it to be
     * read without acquiring the mutex, for example in
     * is_bucket_dying().
     */
    std::atomic<State> state{State::None};

    /**
     * The type of bucket
     */
    BucketType type{BucketType::Unknown};

    /**
     * The name of the bucket (and space for the '\0')
     */
    char name[MAX_BUCKET_NAME_LENGTH + 1]{};

    /**
     * Topkeys
     */
    std::unique_ptr<TopKeys> topkeys;

    /**
     * An array of registered event handler vectors, one for each type.
     */
    engine_event_handler_array_t engine_event_handlers;

    /**
     * @todo add properties!
     */

    /**
     * Statistics vector, one per front-end thread.
     */
    std::vector<thread_stats> stats;

    /**
     * Command timing data
     */
    Timings timings;

    /**
     *  Sub-document JSON parser (subjson) operation execution time histogram.
     */
    Hdr1sfMicroSecHistogram subjson_operation_times;

    using ResponseCounter = cb::RelaxedAtomic<uint64_t>;

    /**
     * Response counters that count the number of times a specific response
     * status is sent
     */
    std::array<ResponseCounter, size_t(cb::mcbp::Status::COUNT)> responseCounters;

    /**
     * The cluster configuration for this bucket
     */
    ClusterConfiguration clusterConfiguration;

    /**
     * The maximum document size for this bucket
     */
    size_t max_document_size = default_max_item_size;

    /**
     * The set of features that the bucket supports
     */
    cb::engine::FeatureSet supportedFeatures{};

    /**
     * Convenience function to check if the bucket supports the feature by
     * searching for it in the supportedFeatures set.
     */
    bool supports(cb::engine::Feature feature);

    /**
     * Reset the bucket back to NoBucket state
     */
    void reset();

protected:
    EngineIface* engine{nullptr};

    /**
     * The dcp interface for the connected bucket. May be null if the
     * connected bucket doesn't support DCP.
     */
    DcpIface* bucketDcp{nullptr};
};

std::string to_string(Bucket::State state);

/**
 * All of the buckets are stored in the following array. Index 0 is reserved
 * for the "no bucket" where all connections start off (unless there is a
 * bucket named "default", and there is a username named "default"
 * with an empty password.).
 */
extern std::array<Bucket, cb::limits::TotalBuckets + 1> all_buckets;

/**
 * Call a function on each ready bucket.
 * @param fn Function to call for each bucket. Should return false if iteration
 * should stop.
 * @param arg argument passed to each invocation
 * @note Buckets which are not yet in a ready state will not be passed to
 * the function.
 *
 */
void bucketsForEach(std::function<bool(Bucket&, void*)> fn, void *arg);

nlohmann::json get_bucket_details(size_t idx);

/**
 * Is the connected bucket currently dying?
 *
 * If the bucket is dying (being deleted) the connection object will be
 * disconnected (for MCBP connections this means closed)
 *
 * @param c the connection to query
 * @return true if it is dying, false otherwise
 */
bool is_bucket_dying(Connection& c);

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

