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

#include "collections/collections_types.h"
#include "collections/vbucket_manifest.h"

#pragma once

struct DocKey;

namespace Collections {
namespace VB {

/**
 * The ScanContext holds data relevant to performing a scan of the disk index
 * e.g. collection erasing may iterate the index and use data with the
 * ScanContext for choosing which keys to erase.
 */
class ScanContext {
public:
    ScanContext(const PersistedManifest& data) : manifest(data) {
    }

    /**
     * Lock the manifest which is owned by the scan context
     */
    Collections::VB::Manifest::CachingReadHandle lockCollections(
            const ::DocKey& key, bool allowSystem) const {
        return manifest.lock(key, allowSystem);
    }

protected:
    /// The manifest which collection scan can compare keys.
    Manifest manifest;
};
} // namespace VB
} // namespace Collections