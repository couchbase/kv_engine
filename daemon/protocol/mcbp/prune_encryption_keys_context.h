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

#include "background_thread_command_context.h"

namespace cb::dek {
enum class Entity;
}

/**
 * The SetActiveEncryptionKeysContext is a state machine used by the memcached
 * core to implement the "PruneEncryptionKeys" operation
 */
class PruneEncryptionKeysContext : public BackgroundThreadCommandContext {
public:
    explicit PruneEncryptionKeysContext(Cookie& cookie);

protected:
    // Execute the operation when running on the executor
    cb::engine_errc execute() override;

    void pruneLog();

    /// The keys to drop
    const std::vector<std::string> keys;
    /// The entity we're updating the keys for
    const cb::dek::Entity entity;
};
