/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_engine_group.h"

#include "ep_engine.h"
#include <gsl/gsl-lite.hpp>
#include <memcached/server_bucket_iface.h>

EPEngineGroup::EPEngineGroup(ServerBucketIface& bucketApi)
    : bucketApi(bucketApi) {
}

void EPEngineGroup::add(EventuallyPersistentEngine& engine) {
    Expects(&bucketApi == engine.getServerApi()->bucket);
    bool inserted = engines.lock()->emplace(&engine).second;
    if (!inserted) {
        throw std::invalid_argument("Engine is already a member of the group.");
    }
}

void EPEngineGroup::remove(EventuallyPersistentEngine& engine) {
    Expects(&bucketApi == engine.getServerApi()->bucket);
    bool removed = static_cast<bool>(engines.lock()->extract(&engine));
    if (!removed) {
        throw std::invalid_argument("Engine is not a member of the group.");
    }
}

std::vector<EPEngineGroup::Handle> EPEngineGroup::getActive() const {
    std::vector<Handle> handles;
    auto lockedEngines = engines.lock();

    for (auto* engine : *lockedEngines) {
        // Assist ASAN in catching dangling engine pointers.
        folly::compiler_must_not_elide(engine->getConfiguration());
        if (auto handle = bucketApi.tryAssociateBucket(engine)) {
            handles.push_back(Handle(
                    static_cast<EventuallyPersistentEngine*>(handle->release()),
                    handle->get_deleter()));
        }
    }

    return handles;
}
