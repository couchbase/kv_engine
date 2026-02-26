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
 * Handler for the DCP_CACHED_VALUE message.
 */
cb::engine_errc dcp_cached_value(Cookie& cookie);

/**
 * Handler for the DCP_CACHED_KEY_META message.
 */
cb::engine_errc dcp_cached_key_meta(Cookie& cookie);