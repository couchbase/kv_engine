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
#include "thread_stats.h"
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

struct HighResolutionThreadStats;
struct LowResolutionThreadStats;
class BucketStatCollector;
struct DcpIface;
class Connection;
struct FrontEndThread;
class Cookie;
enum class ResourceAllocationDomain : uint8_t;

constexpr static size_t MaxBucketNameLength = 100;

class Bucket : public std::enable_shared_from_this<Bucket> {
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

    const std::size_t index;
    Bucket(std::size_t idx);

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

    /// The number of rererences currently held to the bucket
    std::atomic<uint32_t> references{0};

    std::atomic_size_t curr_conn_closing{0};

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

    /// Statistics vector for the high resolution stats, one per front-end
    /// thread.
    std::vector<HighResolutionThreadStats> high_resolution_stats;

    /// Statistics vector for the low resolution stats, one per front-end
    /// thread.
    std::vector<LowResolutionThreadStats> low_resolution_stats;

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

    void addHighResolutionStats(const BucketStatCollector& collector) const;

    /**
     * Add all per-bucket high cardinality metrics to a stat collector.
     */
    void addLowResolutionStats(const BucketStatCollector& collector) const;

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
     * @return cb::engine_errc::success if the limits were set successfully,
     */
    cb::engine_errc setThrottleLimits(std::size_t reserved, size_t hard_limit);

    /**
     * Get the throttle reserved limit specified for this bucket
     */
    std::size_t getThrottleReservedLimit() const {
        return throttle_reserved.load();
    }

    /**
     * Get the throttle hard limit specified for this bucket
     */
    std::size_t getThrottleHardLimit() const {
        return throttle_hard_limit.load();
    }

    bool isCollectionCapable() {
        return type != BucketType::NoBucket &&
               supports(cb::engine::Feature::Collections);
    }

    /**
     * A file chunk has been read (for updating counter tracking bytes read)
     *
     * @param nread The number of bytes read
     */
    void readFileChunkComplete(std::size_t nread) {
        file_chunk_read_bytes.fetch_add(nread);
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
    std::atomic<std::size_t> read_units_used{0};

    /// The number of WUs being used in this bucket
    std::atomic<std::size_t> write_units_used{0};

    /// The gauge to use for throttling of commands.
    SloppyGauge throttle_gauge;

    /// The reserved number of units/s (RU+WU) consumed before we're subject
    /// to throttling
    std::atomic<std::size_t> throttle_reserved{0};

    /// The maxium number of units consumed before we should start throttle
    std::atomic<std::size_t> throttle_hard_limit{0};

    /// The number of times we've throttled due to reaching the throttle limit
    std::atomic<std::size_t> num_throttled{0};

    /// The total number of commands executed within the bucket.
    std::atomic<uint64_t> num_commands{0};

    /// The total number of commands using metered units within the bucket
    std::atomic<uint64_t> num_commands_with_metered_units{0};

    /// The total number of metered DCP messages
    std::atomic<uint64_t> num_metered_dcp_messages{0};

    /// The number of commands we rejected to start executing
    std::atomic<uint64_t> num_rejected{0};

    /// The total number of bytes read from the bucket
    cb::RelaxedAtomic<uint64_t> file_chunk_read_bytes{0};

    /// A deque per front end thread containing all of the connections
    /// which have one or more cookies throttled. It should _only_ be
    /// accessed in the context of the front end worker threads, and each
    /// front end thread should _ONLY_ operate at its own deque stored
    /// at its thread-index. Doing so allows a lock free datastructure.
    std::vector<std::deque<Connection*>> throttledConnections;
};

void to_json(nlohmann::json& json, const Bucket& bucket);
std::string format_as(Bucket::State state);

template <typename BasicJsonType>
void to_json(BasicJsonType& j, Bucket::State state) {
    j = format_as(state);
}

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
} // namespace BucketValidator
