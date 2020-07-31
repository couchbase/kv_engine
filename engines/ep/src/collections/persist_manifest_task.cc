/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "collections/persist_manifest_task.h"
#include "bucket_logger.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "ep_bucket.h"
#include "ep_engine.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

#include <fstream>
#include <iostream>

namespace Collections {

PersistManifestTask::PersistManifestTask(
        EPBucket& bucket,
        std::unique_ptr<Collections::Manifest> manifest,
        const void* cookie)
    : ::GlobalTask(&bucket.getEPEngine(),
                   TaskId::PersistCollectionsManifest,
                   0,
                   true),
      manifest(std::move(manifest)),
      cookie(cookie) {
}

bool PersistManifestTask::run() {
    std::string fname = engine->getConfiguration().getDbname() +
                        cb::io::DirectorySeparator +
                        std::string(ManifestFileName);
    // @todo: more advanced read/write (flatbuffers + crc32c)
    std::ofstream writer(fname, std::ofstream::trunc);
    writer << manifest->toJson(
            [](ScopeID, std::optional<CollectionID>) -> bool { return true; });
    writer.close();

    ENGINE_ERROR_CODE status = ENGINE_SUCCESS;
    if (!writer.good()) {
        status = ENGINE_ERROR_CODE(
                cb::engine_errc::cannot_apply_collections_manifest);
        // log the bad, the fail and the eof.
        EP_LOG_INFO(
                "PersistManifestTask::run writer error bad:{} fail:{} eof:{}",
                writer.bad(),
                writer.fail(),
                writer.eof());
        // failure, when this task goes away the manifest will be destroyed
    } else {
        // Success, release the manifest back to set_collections
        manifest.release();
    }

    engine->notifyIOComplete(cookie, status);
    return false;
}

std::unique_ptr<Manifest> PersistManifestTask::tryAndLoad(
        const std::string& dbname) {
    std::string fname =
            dbname + cb::io::DirectorySeparator + std::string(ManifestFileName);
    if (!cb::io::isFile(fname)) {
        return {};
    }

    std::unique_ptr<Manifest> rv;
    try {
        // @todo: read and validate crc - fail warm-up?
        return std::make_unique<Manifest>(cb::io::loadFile(fname));
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL("PersistManifestTask::tryAndLoad failed {}", e.what());
    }
    // @todo: should we stop warm-up?
    return {};
}

} // namespace Collections
