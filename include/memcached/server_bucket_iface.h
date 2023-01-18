/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "engine.h"

#include <functional>
#include <memory>
#include <string>

using AssociatedBucketHandle =
        std::unique_ptr<EngineIface, std::function<void(EngineIface*)>>;

/**
 * The ServerBucketIface allows the EWB engine to create buckets without
 * having to load the shared object (and have to worry about when to release
 * it).
 */
struct ServerBucketIface {
    virtual ~ServerBucketIface() = default;

    /**
     * Associates with the bucket with the given engine. The bucket must be in
     * the Ready state (otherwise, it will fail to associate). The bucket will
     * not be destroyed until all handles obtained by calling this method have
     * been released.
     *
     * The bucket might still change state however. In particular, in the case
     * where the bucket is prevented from being destroyed (by a handle or other
     * reason), the state will be Destroying.
     *
     * @param engine The bucket to associate with. This pointer is explicitly
     *               allowed to be dangling.
     * @return If the bucket is not active, nullopt is returned.
     */
    virtual std::optional<AssociatedBucketHandle> tryAssociateBucket(
            EngineIface* engine) const = 0;
};
