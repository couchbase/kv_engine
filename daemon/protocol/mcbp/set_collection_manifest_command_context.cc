/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "set_collection_manifest_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>

SetCollectionManifestCommandContext::SetCollectionManifestCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_SetCollectionManifestTask,
              "SetCollectionManifest",
              ConcurrencySemaphores::instance().set_collections_manifest),
      json(cookie.getRequest().getValueString()) {
}

cb::engine_errc SetCollectionManifestCommandContext::execute() {
    return cookie.getConnection().getBucketEngine().set_collection_manifest(
            cookie, json);
}
