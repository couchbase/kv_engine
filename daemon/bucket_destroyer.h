/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "buckets.h"

#include <string>

/**
 * Helper for destroying a bucket.
 *
 * Bucket destruction may take some time, and should not block a
 * thread pool thread for the full duration (as this can starve other NonIO
 * tasks, including ones _required_ for the bucket deletion to proceed...).
 *
 * This helper allows a caller to attempt to progress the bucket deletion
 * without blocking, allowing the calling task to snooze while waiting
 * for e.g., connection cleanup
 *
 */
class BucketDestroyer {
public:
    /**
     * Create a BucketDestroyer which will attempt to progress bucket deletion
     * via repeated calls to drive()
     * @param bucket reference to the bucket to be destroyed
     * @param connectionId the id of the connection issuing the bucket delete
     * @param force whether to forcibly delete the bucket (see
     *              Bucket::destroyEngine)
     */
    BucketDestroyer(Bucket& bucket, std::string connectionId, bool force);

    /**
     * Attempt to progress bucket deletion.
     *
     * May return would_block, to indicate bucket deletion is not yet
     * complete and that the destroyer should be called again "soon".
     *
     * returns success if the bucket is deleted, or an indicative errc
     * if something fails.
     *
     * Once any errc other than would_block is returned, the destroyer
     * should not be called again; whether successful or not, the bucket
     * deletion attempt is done.
     */
    cb::engine_errc drive();

private:
    /**
     * Start shutting down the engine.
     */
    cb::engine_errc start();

    /**
     * Check for any lingering connections associated with the bucket.
     *
     * If any remain, poke them and return would_block. Else, success.
     */
    cb::engine_errc waitForConnections();

    /**
     * Wait for items which have already been buffered to send.
     */
    cb::engine_errc waitForItemsInTransit();

    /**
     * Finish destroying the engine and reset the bucket.
     */
    cb::engine_errc cleanup();

    enum class State {
        Starting,
        WaitingForConnectionsToClose,
        WaitingForItemsInTransit,
        Cleanup,
        Done,
    };

    State state = State::Starting;

    const std::string connectionId;
    Bucket& bucket;
    const std::string name;
    const bool force;

    // time after which destroyer should log remaining connections
    // again.
    std::chrono::steady_clock::time_point nextLogConnections;
    // how many times items_in_transit has been checked
    // (to log periodically rather than each check).
    int itemsInTransitCheckCounter = 0;
};
