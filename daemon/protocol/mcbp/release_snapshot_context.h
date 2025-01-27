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

/**
 * Implementation of the "ReleaseSnapshot" command.
 *
 * The command takes the UUID or VB for the snapshot and removes
 * the on-disk knowledge of the snapshot. Given that we don't want
 * file IO from the worker threads it'll schedule a task to run
 * in the thread pool to perform the actual IO before being rescheduled
 * to send the reply back to the client.
 */
class ReleaseSnapshotContext : public BackgroundThreadCommandContext {
public:
    explicit ReleaseSnapshotContext(Cookie& cookie);

protected:
    cb::engine_errc execute() override;
    const std::string uuid;
    const Vbid vbid;
};
