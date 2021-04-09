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

#include <memory>
#include <string>

/**
 * The ServerBucketIface allows the EWB engine to create buckets without
 * having to load the shared object (and have to worry about when to release
 * it).
 */
struct ServerBucketIface {
    virtual ~ServerBucketIface() = default;

    /**
     * Create a new bucket
     *
     * @param module the name of the shared object containing the bucket
     * @param get_server_api the method to provide to the instance
     * @return the newly created engine, or {} if not found
     */
    virtual unique_engine_ptr createBucket(
            const std::string& module,
            ServerApi* (*get_server_api)()) const = 0;
};
