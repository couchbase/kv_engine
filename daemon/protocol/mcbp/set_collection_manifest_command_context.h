/*
 *     Copyright 2026-Present Couchbase, Inc.
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
 * SetCollectionManifestCommandContext is responsible for handling the
 * CollectionsSetManifest command. The operation involves updating the
 * collections manifest which may perform IO, so it is offloaded to a
 * background thread to avoid blocking frontend threads.
 */
class SetCollectionManifestCommandContext
    : public BackgroundThreadCommandContext {
public:
    explicit SetCollectionManifestCommandContext(Cookie& cookie);

protected:
    cb::engine_errc execute() override;

private:
    /// Copy of the manifest JSON from the request
    std::string json;
};
