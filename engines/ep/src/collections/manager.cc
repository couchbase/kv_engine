/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/filter.h"
#include "collections/manager.h"
#include "collections/manifest.h"
#include "kv_bucket.h"
#include "vbucket.h"

Collections::Manager::Manager() : current(std::make_unique<Manifest>()) {
}

cb::engine_error Collections::Manager::update(KVBucket& bucket,
                                              const std::string& json) {
    std::unique_lock<std::mutex> ul(lock, std::try_to_lock);
    if (!ul.owns_lock()) {
        // Make concurrent updates fail, in realiy there should only be one
        // admin connection making changes.
        return cb::engine_error(cb::engine_errc::temporary_failure,
                                "Collections::Manager::update already locked");
    }

    std::unique_ptr<Manifest> newManifest;
    // Construct a newManifest (will throw if JSON was illegal)
    try {
        newManifest = std::make_unique<Manifest>(json);
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_NOTICE,
            "Collections::Manager::update can't construct manifest e.what:%s",
            e.what());
        return cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest json invalid:" + json);
    }

    // Validate manifest revision is increasing
    if (newManifest->getRevision() <= current->getRevision()) {
        return cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest revision:" +
                        std::to_string(current->getRevision()) + " json:" +
                        json);
    }

    current = std::move(newManifest);

    for (int i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(i);

        if (vb && vb->getState() == vbucket_state_active) {
            vb->updateFromManifest(*current);
        }
    }

    return cb::engine_error(cb::engine_errc::success,
                            "Collections::Manager::update");
}

void Collections::Manager::update(VBucket& vb) const {
    // Lock manager updates
    std::lock_guard<std::mutex> ul(lock);
    vb.updateFromManifest(*current);
}

std::unique_ptr<Collections::Filter> Collections::Manager::makeFilter(
        bool collectionsEnabled, const std::string& json) const {
    // Lock manager updates
    std::lock_guard<std::mutex> lg(lock);
    boost::optional<const std::string&> jsonFilter;
    if (collectionsEnabled) {
        jsonFilter = json;
    }
    return std::make_unique<Collections::Filter>(jsonFilter, *current);
}
