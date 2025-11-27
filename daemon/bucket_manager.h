/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "buckets.h"
#include "cluster_config.h"
#include "sloppy_gauge.h"
#include "timings.h"

#include <folly/CancellationToken.h>
#include <memcached/bucket_type.h>
#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/types.h>
#include <nlohmann/json_fwd.hpp>
#include <memory>
#include <mutex>
#include <optional>

class Connection;
class BucketDestroyer;
class Cookie;

/// The bucket manager is a singleton with provides the ability to
/// perform bucket management tasks like create and delete buckets,
/// but also iterate over all buckets
class BucketManager {
public:
    static BucketManager& instance();

    /// Get information about a bucket as a JSON object
    /// If name is empty, return information about all buckets
    nlohmann::json getBucketInfo(std::string_view name);

    /**
     * Wait for all buckets to be in a quiescent state, and then
     * destroy all of the underlying engines.
     */
    void shutdown();

    /// Associate provided connection withthe initial bucket (no bucket)
    void associateInitialBucket(Connection& connection);

    bool associateBucket(Cookie& cookie, const std::string_view name);
    bool associateBucket(Connection& connection,
                         const std::string_view name,
                         Cookie* cookie = nullptr);
    void disassociateBucket(Connection& connection, Cookie* cookie = nullptr);
    void disconnectBucket(Bucket& bucket, Cookie* cookie);

    /**
     * Create a bucket
     *
     * @param cookie The cookie requested bucket creation
     * @param name The name of the bucket
     * @param config The configuration for the bucket
     * @param type The type of bucket to create
     * @return Status for the operation
     */
    cb::engine_errc create(const CookieIface& cookie,
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
    cb::engine_errc doBlockingDestroy(const CookieIface& cookie,
                                      std::string_view name,
                                      bool force,
                                      std::optional<BucketType> type);

    /// Destroy all of the buckets
    void destroyAll();

    /// Get the bucket with the given index
    Bucket& at(size_t idx);
    const Bucket& at(size_t idx) const;

    Bucket& getNoBucket() {
        return at(0);
    }

    /// Get the name of the bucket with the given index
    std::string getName(size_t idx) const;

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
    cb::engine_errc pause(const CookieIface& cookie, std::string_view name);

    /**
     * Resume a bucket
     *
     * @param cookie The cookie requesting bucket resume.
     * @param name The name of the bucket to resume.
     * @return Status for operation.
     */
    cb::engine_errc resume(const CookieIface& cookie, std::string_view name);

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
    /// Try to destroy all "ready" buckets in parallel
    void destroyBucketsInParallel();

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

    /**
     * All of the buckets are stored in the following array. Index 0 is reserved
     * for the "no bucket" where all connections start off.
     */
    static std::mutex buckets_lock;
    static std::array<std::shared_ptr<Bucket>, cb::limits::TotalBuckets + 1>
            all_buckets_ptr;

    /// The "unassigned resources" gauge to use for throttling of commands.
    SloppyGauge unassigned_resources_gauge;

    /// The maximum number of unassigned resources to use (updated every tick
    /// from the node capacity - all buckets reserved). See Throttling.md
    std::atomic<std::size_t> unassigned_resources_limit{0};
};
