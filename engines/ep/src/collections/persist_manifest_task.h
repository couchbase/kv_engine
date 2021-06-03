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

#pragma once

#include <executor/globaltask.h>
#include <memcached/engine_error.h>

#include <optional>

class CookieIface;
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
                        const CookieIface* cookie);

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(1);
    }

    bool run() override;

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
    cb::engine_errc doTaskCore();

    /**
     * The task is given ownership whilst scheduled and running of the manifest
     * to store. The task releases ownership on successful store or keeps it
     * for destruction on failure.
     */
    std::unique_ptr<Collections::Manifest> manifest;
    const CookieIface* cookie;
};

} // namespace Collections
