/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "buckets.h"
#include <memcached/engine.h>

class ConfigurationIface;

/**
 * Create a new instance of the bucket configuration.
 */
std::unique_ptr<ConfigurationIface> create_bucket_configuration(
        BucketType type);

/**
 * Create a new instance of the given bucket type
 *
 * @param type the type of engine to create an instance of
 * @param get_server_api A function to get the server API (passed to the
 *                       underlying engine)
 * @return the allocated handle if successful
 * @throws std::bad_alloc for memory allocation failures,
 *         cb::engine_error for engine related errors
 */
unique_engine_ptr new_engine_instance(BucketType type,
                                      GET_SERVER_API get_server_api);

/**
 *  Create and initialize an instance of the crash engine which is used for
 *  breakpad testing
 */
void create_crash_instance();

/**
 * Call the engine shutdown function for all valid BucketTypes
 */
void shutdown_all_engines();
