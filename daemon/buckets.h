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
#include "sloppy_gauge.h"
#include "stat_timings.h"
#include "timings.h"

#include <folly/CancellationToken.h>
#include <hdrhistogram/hdrhistogram.h>
#include <mcbp/protocol/status.h>
#include <memcached/bucket_type.h>
#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

struct thread_stats;
class BucketStatCollector;
struct DcpIface;
class Connection;
struct FrontEndThread;
class Cookie;
class BucketManager;

constexpr static const size_t MaxBucketNameLength = 100;

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
        /// The bucket is currently pausing itself
        Pausing,
        /// The bucket has been paused
        Paused,
        /// The bucket is currently being destroyed. It might be awaiting
        /// clients to be disconnected.
        Destroying
    };

    Bucket();

    /// The bucket contains pointers to other objects and we don't want to
    /// make a deep copy of them (or "share" the pointers). No one should need
    /// to create a copy of the bucket anyway, so it's most likely a bug
    /// if people ended up making a copy.
    Bucket(const Bucket& other) = delete;

    /// Does this bucket have an engine or not (for unit tests)
    bool hasEngine() const;

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
     * Mutex protecting the state and refcount, and pause_cancellation_source
     * initialization / reset.
     */
    mutable std::mutex mutex;
    mutable std::condition_variable cond;

    /// The number of clients currently connected to the bucket
    std::atomic<uint32_t> clients{0};

    /**
     * The current state of the bucket. Atomic as we permit it to be
     * read without acquiring the mutex, for example in
     * is_bucket_dying().
     */
    std::atomic<State> state{State::None};

    /**
     * A ClusterConfigOnly bucket may be "upgraded" to a real bucket "in place"
     * which makes it possible for two ns_server connections to concurrently
     * perform that operation (and we would have a race touching the internal
     * members). We don't want to fully serialize bucket creation/deletion
     * as creating / deletion of some buckets _could_ take some time,
     * and we would like to be able to run such commands in parallel.
     * The management operations should look at this member in order
     * to check if there is already and ongoing management operation on
     * the bucket before trying to start a new operation. The member is put
     * here as we had an available padding byte here, another alternative
     * would have been to put a member within the BucketManager itself.
     */
    bool management_operation_in_progress;

    /// The number of items in flight from the bucket (which keeps a reference
    /// inside the bucket so we cannot kill the bucket until they're gone.
    /// The atomic is _ALWAYS_ incremented while the connection holds
    /// a reference in the "clients" member. When the clients member is 0
    /// we'll just wait for this member to become 0 before we can drop
    /// the bucket.
    std::atomic<uint32_t> items_in_transit;

    /// The type of bucket.
    ///
    /// The type of bucket would normally not change after it is created
    /// except for the special case where it starts off as a "config-only"
    /// bucket which later gets replaced with a real instance of a Couchbase
    /// bucket. Due to that special case it needs to be marked as atomic
    /// so that we can read it without having to lock the bucket.
    std::atomic<BucketType> type{BucketType::Unknown};

    /**
     * The name of the bucket, should never be greater in size than
     * MaxBucketNameLength
     */
    std::string name;

    /// Is the bucket quota exceeded or not
    std::atomic_bool bucket_quota_exceeded;

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
     * A CancellationSource object which allows an in-progress pause() request
     * to be cancelled if a resume() request occurs during the pause.
     * Guarded via Bucket::mutex, to avoid races between checking for an
     * in-progress pause and that pause finishing.
     * Note that once CancellationTokens are created from the source (under
     * Bucket::mutex) we _can_ safely manipulate those tokens without the
     * Bucket::mutex being held as CancellationToken performs it's own
     * synchronisation.
     */
    folly::CancellationSource pause_cancellation_source;

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
     * Add all per-bucket metering related metrics to a stat collector.
     *
     * Used to generate `_metering` metrics endpoint.
     */
    void addMeteringMetrics(const BucketStatCollector& collector) const;

    /**
     * Reset the bucket back to NoBucket state
     */
    void reset();

    /// Notify the bucket that the provided command completed execution in
    /// the bucket
    void commandExecuted(const Cookie& cookie);

    /// The number of commands we rected to execute
    void rejectCommand(const Cookie& cookie);

    /// Update the bucket metering data that we've read (used when pushing
    /// DCP messages)
    void recordDcpMeteringReadBytes(const Connection& conn, std::size_t nread);

    /// A document expired in the bucket
    void documentExpired(size_t nbytes);

    /**
     * Check to see if execution of the provided cookie should be throttled
     * or not
     *
     * @param cookie The cookie to throttle
     * @param addConnectionToThrottleList  If set to true the connection should
     *         be added to the list of connections containing a throttled
     *         command
     * @param pendingBytes The throttle check will include this value in the
     *        check, it is not yet added to all metering/throttling variables.
     * @return True if the cookie should be throttled
     */
    bool shouldThrottle(const Cookie& cookie,
                        bool addConnectionToThrottleList,
                        size_t pendingBytes);

    /**
     * Check to see if this DCP connection should be throttled or not
     *
     * @param connection The connection to check
     * @return true if the DCP should be throttled, false otherwise
     */
    bool shouldThrottleDcp(const Connection& connection);

    /// move the clock forwards in this bucket
    void tick();

    /**
     * Removes the throttled connection entries as the Bucket is no longer in a
     * ready state and resets all throttled cookies
     */
    void deleteThrottledCommands();

    /**
     * Set the throttle limit for the bucket
     *
     * @param limit Number of units (RU+WU) to use before throttle commands
     */
    void setThrottleLimit(std::size_t limit);

    bool isCollectionCapable() {
        return type != BucketType::NoBucket &&
               supports(cb::engine::Feature::Collections);
    }

protected:
    unique_engine_ptr engine;

    /**
     * The dcp interface for the connected bucket. May be null if the
     * connected bucket doesn't support DCP.
     */
    DcpIface* bucketDcp{nullptr};

    /// The number of RUs being used in this bucket
    std::atomic<std::size_t> read_units_used{0};

    /// The number of WUs being used in this bucket
    std::atomic<std::size_t> write_units_used{0};

    /// The gauge to use for throttling of commands.
    SloppyGauge throttle_gauge;

    /// The number of units (RU+WU) consumed before we should start throttle
    std::atomic<std::size_t> throttle_limit{0};

    /// The number of times we've throttled due to reaching the throttle limit
    std::atomic<std::size_t> num_throttled{0};

    /// The total time (in usec) spent in a throttled state
    std::atomic<uint64_t> throttle_wait_time{0};

    /// The total number of commands executed within the bucket
    std::atomic<uint64_t> num_commands{0};

    /// The total number of commands using metered units within the bucket
    std::atomic<uint64_t> num_commands_with_metered_units{0};

    /// The total number of metered DCP messages
    std::atomic<uint64_t> num_metered_dcp_messages{0};

    /// The number of commands we rejected to start executing
    std::atomic<uint64_t> num_rejected{0};

    /// A deque per front end thread containing all of the connections
    /// which have one or more cookies throttled. It should _only_ be
    /// accessed in the context of the front end worker threads, and each
    /// front end thread should _ONLY_ operate at its own deque stored
    /// at its thread-index. Doing so allows a lock free datastructure.
    std::vector<std::deque<Connection*>> throttledConnections;
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
     * @param type Only delete the bucket if it is of the given type
     * @return Status for the operation
     */
    cb::engine_errc destroy(Cookie& cookie,
                            const std::string name,
                            bool force,
                            std::optional<BucketType> type);

    /// Set the cluster configuration for the named bucket
    cb::engine_errc setClusterConfig(
            const std::string& name,
            std::unique_ptr<ClusterConfiguration::Configuration> configuration);

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

    /**
     * Registers an internal client with the bucket. The bucket must be in the
     * Ready state to be able to associate with it.
     *
     * The bucket will not be shutdown until all clients are unregistered by
     * calling disassociateBucket. Associated buckets can move to other states
     * like the Destroying state, which indicates that the bucket is waiting
     * for clients to disconnect.
     *
     * @param engine A pointer to the engine. This is allowed to be a dangling
     *               pointer (in which case this operation will fail).
     * @return A pointer to the bucket that we associate with, or nullptr on
     *         failure.
     */
    Bucket* tryAssociateBucket(EngineIface* engine);

    /**
     * Disassociates from a bucket for which tryAssociateBucket has previously
     * succeeded.
     */
    void disassociateBucket(Bucket* bucket);

    /// move the clock forwards in all buckets
    void tick();

    /**
     * Pause a bucket
     * @param cookie The cookie requesting bucket pause.
     * @param name The name of the bucket to pause.
     * @return Status for operation.
     */
    cb::engine_errc pause(Cookie& cookie, std::string_view name);

    /**
     * Resume a bucket
     *
     * @param cookie The cookie requesting bucket resume.
     * @param name The name of the bucket to resume.
     * @return Status for operation.
     */
    cb::engine_errc resume(Cookie& cookie, std::string_view name);

protected:
    /**
     * Create a bucket
     *
     * @param cid The client identifier (for logging)
     * @param name The name of the bucket
     * @param config The configuration for the bucket
     * @param type The type of bucket to create
     * @return Status for the operation
     */
    cb::engine_errc create(uint32_t cid,
                           const std::string name,
                           const std::string config,
                           BucketType type);

    /**
     * Destroy a bucket
     *
     * @param cid The client identifier (for logging)
     * @param name The name of the bucket to delete
     * @param force If set to true the underlying engine should not try to
     *              persist pending items etc
     * @param type Only delete the bucket if it is of the given type
     * @return Status for the operation
     */
    cb::engine_errc destroy(std::string_view cid,
                            const std::string name,
                            bool force,
                            std::optional<BucketType> type);

    /**
     * iterate over all the buckets, lock the bucket and call the provided
     * function. If the callback returns false iteration stops.
     *
     * The member function is intended to be used within the bucket manager
     * to try to locate a bucket bucket. The caller typically holds global
     * lock to make sure that no one else will try to create/delete buckets
     * while iterating over the buckets. (Note that it is possible to call
     * the method _without_ holding the lock, but then you're not guaranteed
     * that buckets will come or go during the iteration).
     */
    void iterateBuckets(std::function<bool(Bucket&)> fn);

    /**
     * Allocate a bucket for the given name and set its state to creating.
     *
     * @param name The name of the bucket to create
     * @return success if a slot was successfully allocated
     *         key_already_exists if a bucket with the same name already exists
     *         too_big if all bucket slots are in use
     *         temporary_failure if there is an ongoing management command on
     *                           the bucket represented by the provided name
     */
    std::pair<cb::engine_errc, Bucket*> allocateBucket(std::string_view name);

    /**
     * Create an engine instance of the specified type for the provided bucket
     *
     * @param bucket The bucket to get the engine instance
     * @param type The type of engine to create
     * @param name The name of the bucket (for logging)
     * @param config The configuration to initialize the engine
     * @param cid The client identifier (for logging)
     */
    void createEngineInstance(Bucket& bucket,
                              BucketType type,
                              std::string_view name,
                              std::string_view config,
                              uint32_t cid);

    // Called right before changing the bucket type (to use for testing)
    virtual void bucketTypeChangeListener(Bucket& bucket, BucketType type) {
    }

    // Called right before changing the bucket state (to use for testing)
    virtual void bucketStateChangeListener(Bucket& bucket,
                                           Bucket::State state) {
    }

    /**
     * Called after transitioning to the Pausing state. It is called in multiple
     * places as pause() progressed, with the 'phase' argument specifying
     * at what point this callback is called.
     * Bucket object is *not* locked at this point, so we only pass its name.
     * @param bucket Name of the bucket being paused.
     * @param phase Position within the pause() process where this callback is
     *              called from.
     */
    virtual void bucketPausingListener(std::string_view bucket,
                                       std::string_view phase) {
    }

    /**
     * Wait for all clients to disconnect from the provided bucket
     *
     * @param bucket The bucket to wait for
     * @param operation The kind of operation requesting the wait (used for
     *                  logging)
     * @param id The connection identifier requested the wait (used for
     *           logging and is typically cookie.getConnectionId(), but in
     *           the case where there are no client context it should be
     *           set to <none>)
     * @param cancellationToken A cancellation token which can be used to
     *        cancel waiting for all clients to disconnect.
     */
    void waitForEveryoneToDisconnect(
            Bucket& bucket,
            std::string_view operation,
            std::string_view id,
            folly::CancellationToken cancellationToken = {});

    /**
     * Pause a bucket
     * @param cid The client identifier (for logging)
     * @param name The name of the bucket to pause.
     * @return Status for operation.
     */
    cb::engine_errc pause(std::string_view cid, std::string_view name);

    /**
     * Resume a bucket
     *
     * @param cid The client identifier (for logging)
     * @param name The name of the bucket to resume.
     * @return Status for operation.
     */
    cb::engine_errc resume(std::string_view cid, std::string_view name);

    BucketManager();
};
