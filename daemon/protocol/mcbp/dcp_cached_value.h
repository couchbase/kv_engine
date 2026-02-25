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

#include "../../memcached.h"

/**
 * Executor for the DCP_CACHED_VALUE message.
 */
void dcp_cached_value_executor(Cookie& cookie);

/**
 * Executor for the DCP_CACHED_KEY_META message.
 */
void dcp_cached_key_meta_executor(Cookie& cookie);

/**
 * Executor for the DCP_CACHE_TRANSFER_END message.
 */
void dcp_cache_transfer_end_executor(Cookie& cookie);