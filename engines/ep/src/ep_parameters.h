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

#include <memcached/types.h>
#include <string_view>
#include <unordered_set>

/**
 * Check if the key is a bucket configuration parameter settable using
 * the SetParameter API (in the specified category).
 * @param key the key to check
 * @param category the category of the parameter
 * @return true if the key is a config parameter in the category
 */

bool checkSetParameterCategory(std::string_view key,
                               EngineParamCategory category);

/**
 * Get all the keys of the bucket configuration parameters settable
 * using the SetParameter API. Added for testing.
 */
std::unordered_set<std::string_view> getSetParameterKeys();
