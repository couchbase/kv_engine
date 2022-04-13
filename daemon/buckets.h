/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "cluster_config.h"
#include "mcbp_validators.h"
#include "stat_timings.h"
#include "timings.h"

#include <memcached/bucket_type.h>
#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>
#include <hdrhistogram/hdrhistogram.h>

#include <condition_variable>
#include <memory>

struct thread_stats;
struct DcpIface;
class Connection;

#define MAX_BUCKET_NAME_LENGTH 100

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
    EngineIface& getEngine() const;

    void setEngine(unique_engine_ptr engine_);

    /// Destroy the underlying engine
    void destroyEngine(bool force);

    /**
     * @returns the DCP interface for the connected bucket, or nullptr if the
     *          conencted bucket doesn't implement DCP.
     */
    DcpIface* getDcpIface() const;

    /**
     * Mutex protecting the state and refcount.
     */
    mutable std::mutex mutex;
    mutable std::condition_variable cond;

    /**
     * The number of clients currently connected to the bucket (performed
     * a SASL_AUTH to the bucket.
     */
    std::atomic<uint32_t> clients{0};

    /**
     * The current state of the bucket. Atomic as we permit it to be
     * read without acquiring the mutex, for example in
     * is_bucket_dying().
     */
    std::atomic<State> state{State::None};

    /// The number of items in flight from the bucket (which keeps a reference
    /// inside the bucket so we cannot kill the bucket until they're gone.
    /// The atomic is _ALWAYS_ incremented while the connection holds
    /// a reference in the "clients" member. When the clients member is 0
    /// we'll just wait for this member to become 0 before we can drop
    /// the bucket.
    std::atomic<uint32_t> items_in_transit;

    /**
     * The type of bucket
     */
    BucketType type{BucketType::Unknown};

    /**
     * The name of the bucket (and space for the '\0')
     */
    char name[MAX_BUCKET_NAME_LENGTH + 1]{};

    /**
     * Statistics vector, one per front-end thread.
     */
    std::vector<thread_stats> stats;

    /**
     * Command timing data
     */
    Timings timings;

    /**
     * Stat request timing data
     */
    StatTimings statTimings;

    /**
     *  Sub-document JSON parser (subjson) operation execution time histogram.
     */
    Hdr1sfMicroSecHistogram subjson_operation_times;

    /// JSON validation time histogram.
    Hdr1sfMicroSecHistogram jsonValidateTimes;

    /// Snappy decompression time histogram.
    Hdr1sfMicroSecHistogram snappyDecompressionTimes;

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

    /// Get a JSON representation of the bucket
    nlohmann::json to_json() const;

    /**
     * Reset the bucket back to NoBucket state
     */
    void reset();

    /// Notify the bucket that the provided command completed execution in
    /// the bucket
    void commandExecuted(const Cookie& cookie);

protected:
    unique_engine_ptr engine;

    /**
     * The dcp interface for the connected bucket. May be null if the
     * connected bucket doesn't support DCP.
     */
    DcpIface* bucketDcp{nullptr};

    /// The number of RCUs being used in this bucket
    std::atomic<std::size_t> read_compute_units_used{0};

    /// The number of WCUs being used in this bucket
    std::atomic<std::size_t> write_compute_units_used{0};
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
/**
 * Validate that a bucket name confirms to the restrictions for bucket
 * names.
 *
 * @param name the name to validate
 * @return An error message if there was something wrong with the name
 *         (empty string == everything OK)
 */
std::string validateBucketName(std::string_view name);
}

/// May the user connected to this cookie access the specified bucket?
bool mayAccessBucket(Cookie& cookie, const std::string& bucket);

/// The bucket manager is a singleton with provides the ability to
/// perform bucket management tasks like create and delete buckets,
/// but also iterate over all buckets
class BucketManager {
public:
    static BucketManager& instance();

    /**
     * Create a bucket
     *
     * @param cookie The cookie requested bucket creation
     * @param name The name of the bucket
     * @param config The configuration for the bucket
     * @param type The type of bucket to create
     * @return Status for the operation
     */
    cb::engine_errc create(Cookie& cookie,
                           const std::string name,
                           const std::string config,
                           BucketType type);

    /**
     * Destroy a bucket
     *
     * @param cookie The cookie requested bucket deletion
     * @param name The name of the bucket to delete
     * @param force If set to true the underlying engine should not try to
     *              persist pending items etc
     * @return Status for the operation
     */
    cb::engine_errc destroy(Cookie* cookie, const std::string name, bool force);

    /// Destroy all of the buckets
    void destroyAll();

    /// Get the bucket with the given index
    Bucket& at(size_t idx);

    /**
     * Call a function on each ready bucket.
     * @param fn Function to call for each bucket. Should return false if
     * iteration should stop.
     * @note Buckets which are not yet in a ready state will not be passed to
     * the function.
     *
     */
    void forEach(std::function<bool(Bucket&)> fn);

protected:
    BucketManager();
};
