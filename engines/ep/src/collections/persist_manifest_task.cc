/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/persist_manifest_task.h"
#include "bucket_logger.h"
#include "collections/collections_types.h"
#include "collections/manager.h"
#include "collections/manifest.h"
#include "collections/manifest_generated.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_task.h"

#include <nlohmann/json.hpp>
#include <platform/crc32c.h>
#include <platform/dirutils.h>

namespace Collections {

PersistManifestTask::PersistManifestTask(EPBucket& bucket,
                                         Collections::Manifest&& manifest,
                                         CookieIface* cookie)
    : EpTask(bucket.getEPEngine(), TaskId::PersistCollectionsManifest, 0, true),
      manifest(std::move(manifest)),
      cookie(cookie) {
}

std::string PersistManifestTask::getDescription() const {
    return "PersistManifestTask for " + engine->getName();
}

bool PersistManifestTask::run() {
    auto status = doTaskCore();
    engine->getKVBucket()
            ->getCollectionsManager()
            .updatePersistManifestTaskDone(*engine, cookie, status);
    engine->notifyIOComplete(cookie, status);
    return false;
}

cb::engine_errc PersistManifestTask::doTaskCore() {
    std::string finalFile = engine->getConfiguration().getDbname();

    if (!cb::io::isDirectory(finalFile)) {
        EP_LOG_WARN("PersistManifestTask::run fail isDirectory {}", finalFile);
        return cb::engine_errc::cannot_apply_collections_manifest;
    }

    finalFile += cb::io::DirectorySeparator + std::string(ManifestFileName);

    auto fbData = manifest.toFlatbuffer();

    // Now wrap with a CRC
    flatbuffers::FlatBufferBuilder builder;
    auto fbManifest = builder.CreateVector(fbData.data(), fbData.size());
    auto toWrite = Collections::Persist::CreateManifestWithCrc(
            builder, crc32c(fbData.data(), fbData.size(), 0), fbManifest);
    builder.Finish(toWrite);

    // The data must be synced to disk before it is renamed into place, or a
    // crash could leave an empty manifest behind (which prevents warmup).
    std::error_code ec;
    if (!cb::io::saveFileAtomic(
                finalFile,
                {reinterpret_cast<const char*>(builder.GetBufferPointer()),
                 builder.GetSize()},
                ec)) {
        // failure, when this task goes away the manifest will be destroyed
        EP_LOG_WARN("PersistManifestTask::run failed to save {}: {}",
                    finalFile,
                    ec.message());
        return cb::engine_errc::cannot_apply_collections_manifest;
    }

    return cb::engine_errc::success;
}

std::optional<Manifest> PersistManifestTask::tryAndLoad(
        std::string_view dbname) {
    std::string fname{dbname};
    fname += cb::io::DirectorySeparator + std::string(ManifestFileName);

    if (!cb::io::isFile(fname)) {
        return Manifest{};
    }

    try {
        auto manifestRaw = cb::io::loadFile(fname);

        // First do a verification with FlatBuffers - this does a basic check
        // that the data appears to be of the correct schema, but does not
        // detect values that changed in-place.
        flatbuffers::Verifier v(
                reinterpret_cast<const uint8_t*>(manifestRaw.data()),
                manifestRaw.size());
        if (!v.VerifyBuffer<Collections::Persist::ManifestWithCrc>(nullptr)) {
            EP_LOG_CRITICAL_RAW(
                    "PersistManifestTask::tryAndLoad failed VerifyBuffer");
            return std::nullopt;
        }

        auto fbData =
                flatbuffers::GetRoot<Collections::Persist::ManifestWithCrc>(
                        manifestRaw.data());
        uint32_t storedCrc = fbData->crc();
        uint32_t crc = crc32c(
                fbData->manifest()->data(), fbData->manifest()->size(), 0);
        if (crc != storedCrc) {
            EP_LOG_CRITICAL(
                    "PersistManifestTask::tryAndLoad failed crc mismatch "
                    "storedCrc:{}, crc:{} ",
                    storedCrc,
                    crc);
            return std::nullopt;
        }

        std::string_view view(
                reinterpret_cast<const char*>(fbData->manifest()->data()),
                fbData->manifest()->size());
        return Manifest{view, Manifest::FlatBuffers{}};
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL("PersistManifestTask::tryAndLoad failed {}", e.what());
    }
    return std::nullopt;
}

} // namespace Collections
