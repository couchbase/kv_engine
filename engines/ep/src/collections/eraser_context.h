/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "collections/vbucket_manifest.h"

#pragma once

namespace Collections {
namespace VB {

/**
 * The EraserContext holds a reference to the manifest which is used by the
 * erasing process of a collection, keys will be tested for isLogicallyDeleted
 * with this object's manifest.
 *
 * Additionally the class tracks how many collections were fully erased.
 *
 */
class EraserContext {
public:
    EraserContext(Manifest& manifest) : manifest(manifest) {
    }

    /**
     * Write lock the manifest which is referenced by the erase context
     */
    Collections::VB::Manifest::WriteHandle wlockCollections() {
        return manifest.wlock();
    }

    /**
     * Lock the manifest which is referenced by the erase context
     */
    Collections::VB::Manifest::CachingReadHandle lockCollections(
            const ::DocKey& key, bool allowSystem) const {
        return manifest.lock(key, allowSystem);
    }

    bool needToUpdateCollectionsManifest() const {
        return collectionsErased > 0;
    }

    void incrementErasedCount() {
        collectionsErased++;
    }

    void finaliseCollectionsManifest(
            std::function<void(cb::const_byte_buffer)> saveManifestCb);

private:
    size_t collectionsErased = 0;

    /// The manifest which collection erasing can compare keys.
    Manifest& manifest;
};
} // namespace VB
} // namespace Collections