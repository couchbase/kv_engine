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

/**
 * The UnknownPacketCommandContext is used to handle unknown commands
 * (commands not implemented by the front end core, but may be implemented
 * in the underlying engine)
 */
class UnknownPacketCommandContext : public SteppableCommandContext {
public:
    explicit UnknownPacketCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
};
