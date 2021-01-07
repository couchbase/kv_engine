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

#pragma once

#include "globaltask.h"

#include <optional>

class EPBucket;

namespace Collections {
class Manifest;

/**
 * A task for storing the Collection::Manifest into the bucket's data directory
 */
class PersistManifestTask : public ::GlobalTask {
public:
    PersistManifestTask(EPBucket& bucket,
                        std::unique_ptr<Collections::Manifest> manifest,
                        const void* cookie);

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(1);
    }

    bool run() noexcept override;

    /**
     * Load back what this task writes.
     *
     * This returns an optional unique_ptr. If the optional has no value, then
     * tryAndLoad failed - something went wrong (logged as CRITICAL).
     *
     * If the returned optional has a value, it could be a nullptr. This occurs
     * for when there is no previous state to load (first warmup).
     */
    static std::optional<Manifest> tryAndLoad(std::string_view dbpath);

private:
    /**
     * The task is given ownership whilst scheduled and running of the manifest
     * to store. The task releases ownership on successful store or keeps it
     * for destruction on failure.
     */
    std::unique_ptr<Collections::Manifest> manifest;
    const void* cookie;
};

} // namespace Collections
