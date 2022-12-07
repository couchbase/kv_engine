/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <memcached/server_bucket_iface.h>
#include <mutex>
#include <unordered_set>

class EventuallyPersistentEngine;

/**
 * A set of ep-engine instances which share some common characteristics.
 *
 * We use this to group quota sharing engines for example.
 */
class EPEngineGroup {
public:
    /**
     * Remap the handle type to EventuallyPersistentEngine.
     */
    using Handle = std::unique_ptr<EventuallyPersistentEngine,
                                   AssociatedBucketHandle::deleter_type>;

    explicit EPEngineGroup(ServerBucketIface& bucketApi);

    /**
     * Add an engine instance to the group. Remember to remove the engine by
     * calling EPEngineGroup::remove.
     */
    void add(EventuallyPersistentEngine& engine);

    /**
     * Remove an engine instance from the group.
     */
    void remove(EventuallyPersistentEngine& engine);

    /**
     * Returns handles to the active engines in the group. Active in this case
     * means an engine whose bucket is in the Ready state.
     *
     * An engine instance will not be destroyed or paused while there are active
     * handles on it. To achieve this, each handle object increments a
     * per-engine reference count, which gets decremented when the handle goes
     * out of scope or is reset.
     *
     * For those reasons, the returned Handles should not be stored for
     * prolonged periods of time.
     */
    std::vector<Handle> getActive() const;

private:
    ServerBucketIface& bucketApi;

    /**
     * The list of engines registered with this instance.
     */
    folly::Synchronized<std::unordered_set<EventuallyPersistentEngine*>,
                        std::mutex>
            engines;
};
