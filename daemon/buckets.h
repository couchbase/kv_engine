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
class BucketDestroyer;
struct FrontEndThread;
class Cookie;
class BucketManager;
enum class ResourceAllocationDomain : uint8_t;

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
    bool management_operation_in_progress = false;

    /// The number of items in flight from the bucket (which keeps a reference
    /// inside the bucket so we cannot kill the bucket until they're gone.
    /// The atomic is _ALWAYS_ incremented while the connection holds
    /// a reference in the "clients" member. When the clients member is 0
    /// we'll just wait for this member to become 0 before we can drop
    /// the bucket.
    std::atomic<uint32_t> items_in_transit{0};

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

    /// If data_ingress_stats != Success, data ingress is disallowed
    /// and its status is returned for the operation.
    std::atomic<cb::mcbp::Status> data_ingress_status{cb::mcbp::Status::Success};

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

    /**
     * Update the bucket metering data that we've read (used when pushing DCP
     * messages)
     *
     * @param conn The connection sending the data
     * @param nread The number of bytes data read
     * @param domain Where the allocation for sending data came from
     */
    void recordDcpMeteringReadBytes(const Connection& conn,
                                    std::size_t nread,
                                    ResourceAllocationDomain domain);

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
    bool shouldThrottle(Cookie& cookie,
                        bool addConnectionToThrottleList,
                        size_t pendingBytes);

    /**
     * Check to see if this DCP connection should be throttled or not
     *
     * @param connection The connection to check
     * @return true if the DCP should be throttled, false otherwise.
     *         if false; the domain is set to where it allocated from.
     */
    std::pair<bool, ResourceAllocationDomain> shouldThrottleDcp(
            const Connection& connection);

    /// move the clock forwards in this bucket
    void tick();

    /**
     * Removes the throttled connection entries as the Bucket is no longer in a
     * ready state and resets all throttled cookies
     */
    void deleteThrottledCommands();

    /**
     * Set the throttle limits for the bucket. See Throttling.md for a
     * description on how the various limits work.
     *
     * @param reserved The reserved number of units (RU+WU) the bucket gets
     * @param hard_limit The maximum number of units to use before throttling
     *                   commands
     */
    void setThrottleLimits(std::size_t reserved, size_t hard_limit);

    /**
     * Get the throttle limits specified for this bucket
     */
    std::pair<std::size_t, std::size_t> getThrottleLimits() const {
        return {throttle_reserved.load(), throttle_hard_limit.load()};
    }

    bool isCollectionCapable() {
        return type != BucketType::NoBucket &&
               supports(cb::engine::Feature::Collections);
    }

protected:
    unique_engine_ptr engine;

    /**
     * Update the appropriate throttle gauge with the provided number of
     * units
     *
     * @param units The number of units
     * @param domain The domain the units was allocated from
     */
    void consumedUnits(std::size_t units, ResourceAllocationDomain domain);

    /**
     * May the connection perform an operation consuming the provided number
     * of units, or should it be throttled
     *
     * @return {true, None} if the connection should be throttled
     *         {false, domain} if the connection may perform the operation
     *                         and the domain contains where it allocated
     *                         the resource from
     */
    std::pair<bool, ResourceAllocationDomain> shouldThrottle(
            const Connection& connection, std::size_t units);

    /**
     * The dcp interface for the connected bucket. May be null if the
     * connected bucket doesn't support DCP.
     */
    DcpIface* bucketDcp{nullptr};

    /// The number of RUs being used in this bucket
    /// Only maintained for serverless profile.
    std::atomic<std::size_t> read_units_used{0};

    /// The number of WUs being used in this bucket
    /// Only maintained for serverless profile.
    std::atomic<std::size_t> write_units_used{0};

    /// The gauge to use for throttling of commands.
    /// Only maintained for serverless profile.
    SloppyGauge throttle_gauge;

    /// The reserved number of units/s (RU+WU) consumed before we're subject
    /// to throttling
    std::atomic<std::size_t> throttle_reserved{0};

    /// The maxium number of units consumed before we should start throttle
    std::atomic<std::size_t> throttle_hard_limit{0};

    /// The number of times we've throttled due to reaching the throttle limit
    std::atomic<std::size_t> num_throttled{0};

    /// The total time (in usec) spent in a throttled state
    /// Only maintained for serverless profile.
    std::atomic<uint64_t> throttle_wait_time{0};

    /// The total number of commands executed within the bucket.
    /// Only maintained for serverless profile.
    std::atomic<uint64_t> num_commands{0};

    /// The total number of commands using metered units within the bucket
    /// Only maintained for serverless profile.
    std::atomic<uint64_t> num_commands_with_metered_units{0};

    /// The total number of metered DCP messages
    /// Only maintained for serverless profile.
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
 * for the "no bucket" where all connections start off.
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
    cb::engine_errc create(CookieIface& cookie,
                           std::string_view name,
                           std::string_view config,
                           BucketType type);

    /// Set the cluster configuration for the named bucket
    std::pair<cb::engine_errc, Bucket::State> setClusterConfig(
            std::string_view name,
            std::shared_ptr<ClusterConfiguration::Configuration> configuration);

    /**
     * Start bucket destruction.
     *
     * If the destruction needs to wait for some condition (e.g., waiting
     * for all remaining connections to disconnect), this may return
     * would_block and a BucketDestroyer which should be driven to continue
     * the destruction.
     *
     *
     * @param cid The client identifier (for logging)
     * @param name The name of the bucket to delete
     * @param force If set to true the underlying engine should not try to
     *              persist pending items etc
     * @return Status of the operation, and optional BucketDestroyer which can
     *         be used to continue the deletion if status=would_block
     */
    std::pair<cb::engine_errc, std::optional<BucketDestroyer>> startDestroy(
            std::string_view cid,
            std::string_view name,
            bool force,
            std::optional<BucketType> type);

    /**
     * Destroy a bucket (will block until connections are closed and operations
     * in flight are complete).
     *
     * Wraps startDestroy in a while loop; use startDestroy directly if the
     * caller needs to do anything else while bucket deletion is in progress
     * (e.g., needs to snooze a task rather than blocking a thread).
     *
     * @param cookie The cookie requested bucket deletion
     * @param name The name of the bucket to delete
     * @param force If set to true the underlying engine should not try to
     *              persist pending items etc
     * @param type Only delete the bucket if it is of the given type
     * @return Status for the operation
     */
    cb::engine_errc doBlockingDestroy(CookieIface& cookie,
                                      std::string_view name,
                                      bool force,
                                      std::optional<BucketType> type);

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
    void forEach(const std::function<bool(Bucket&)>& fn);

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

    /** Move the clock forwards in all buckets. Expected to be called
     * periodically, once per second.
     */
    void tick();

    /**
     * Pause a bucket
     * @param cookie The cookie requesting bucket pause.
     * @param name The name of the bucket to pause.
     * @return Status for operation.
     */
    cb::engine_errc pause(CookieIface& cookie, std::string_view name);

    /**
     * Resume a bucket
     *
     * @param cookie The cookie requesting bucket resume.
     * @param name The name of the bucket to resume.
     * @return Status for operation.
     */
    cb::engine_errc resume(CookieIface& cookie, std::string_view name);

    /**
     * Check to see if the provided number of units is available in the
     * global free pool
     *
     * @param units the number of units needed
     * @return true if the requested number of units is available,
     *         false otherwise
     */
    bool isUnassignedResourcesAvailable(std::size_t units) {
        return unassigned_resources_gauge.isBelow(unassigned_resources_limit,
                                                  units);
    }

    /**
     * Mark the provided number of units as used
     */
    void consumedResources(std::size_t units) {
        unassigned_resources_gauge.increment(units);
    }

    /// Aggregated timings for all buckets
    Timings aggregatedTimings;

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
                           std::string_view name,
                           std::string_view config,
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
                            std::string_view name,
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
    void iterateBuckets(const std::function<bool(Bucket&)>& fn);

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

    /// The "unassigned resources" gauge to use for throttling of commands.
    SloppyGauge unassigned_resources_gauge;

    /// The maximum number of unassigned resources to use (updated every tick
    /// from the node capacity - all buckets reserved). See Throttling.md
    std::atomic<std::size_t> unassigned_resources_limit{0};
};
