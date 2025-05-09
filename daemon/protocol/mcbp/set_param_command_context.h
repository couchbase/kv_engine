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

#include "steppable_command_context.h"

/// Command context for executing SetParam requests
class SetParamCommandContext : public SteppableCommandContext {
public:
    explicit SetParamCommandContext(Cookie& cookie);

protected:
    static EngineParamCategory getParamCategory(Cookie& cookie);

    cb::engine_errc step() override;
    const EngineParamCategory category;
    const std::string key;
    const std::string value;
    const Vbid vbid;
};
