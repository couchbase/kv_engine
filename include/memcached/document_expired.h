/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstddef>

class CookieIface;
struct EngineIface;

namespace cb::server {
/**
 * Notify the core that the engine expired the document
 *
 * @param engine The engine which expired a document
 * @param size The size of the expired document (This is the size
 *             after the pre-expiry hook was run to prune the value
 *             and user xattrs)
 */
void document_expired(const EngineIface& engine, size_t nbytes);
} // namespace cb::server
