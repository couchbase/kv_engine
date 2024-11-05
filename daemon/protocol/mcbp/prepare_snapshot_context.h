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

#include "steppable_command_context.h"
#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>

/**
 * Implementation of the "OpenVbSnapshot" command.
 *
 * The command use the VBucket id in the command to open a snapshot
 * and return the manifest back to the client
 *
 *     {
 *       "uuid" : "1023",
 *       "files" : [
 *         {
 *           "id" : 0,
 *           "size": "1234",
 *           "path": 1023.couch.1;
 *         }
 *       ]
 *     }
 *
 * Given that we don't want file IO from the worker threads it'll schedule a
 * task to run in the thread pool to perform the actual IO to build up
 * the snapshot and write the manifest file before being rescheduled to send
 * the manifest back to the client.
 */
class PrepareSnapshotContext : public SteppableCommandContext {
public:
    enum class State : uint8_t { Initialize, Done };

    explicit PrepareSnapshotContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    cb::engine_errc initialize();
    cb::engine_errc done();
    cb::engine_errc doCreateSnapshot();
    const Vbid vb;
    folly::Synchronized<nlohmann::json> snapshot;
    State state = State::Initialize;
};
