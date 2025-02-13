/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "background_thread_command_context.h"
#include "steppable_command_context.h"

/**
 * The SetActiveEncryptionKeysContext is a state machine used by the memcached
 * core to implement the "SetActiveEncryptionKeys" operation
 */
class SetActiveEncryptionKeysContext : public BackgroundThreadCommandContext {
public:
    explicit SetActiveEncryptionKeysContext(Cookie& cookie);

protected:
    // Execute the operation when running on the executor
    cb::engine_errc execute() override;

    /// The JSON payload from the client containing the new key information
    /// (active key, identifiers, key data etc)
    const nlohmann::json json;
    /// The JSON payload from the client containing the new keys, but with
    /// the actual key data removed so it won't appear in the logs
    const nlohmann::json loggable_json;
    /// The entity we're updating the keys for
    const std::string entity;
};
