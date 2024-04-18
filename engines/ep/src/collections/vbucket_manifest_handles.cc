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

#include "ep_engine.h"
#include "vbucket_manifest_handles.h"
#include "collections/collection_persisted_stats.h"

#include <iostream>

namespace Collections::VB {

Metered ReadHandle::isMetered(CollectionID cid) const {
    return manifest->getManifestEntry(cid).isMetered();
}

PersistedStats StatsReadHandle::getPersistedStats() const {
    return {itr->second.getItemCount(),
            itr->second.getPersistedHighSeqno(),
            itr->second.getDiskSize()};
}

uint64_t StatsReadHandle::getHighSeqno() const {
    return itr->second.getHighSeqno();
}

size_t StatsReadHandle::getItemCount() const {
    return itr->second.getItemCount();
}

void ReadHandle::dump() const {
    std::cerr << *manifest << std::endl;
}

cb::engine_errc CachingReadHandle::handleWriteStatus(
        EventuallyPersistentEngine& engine, CookieIface* cookie) {
    // Collection not found
    if (!valid()) {
        engine.setUnknownCollectionErrorContext(*cookie, getManifestUid());
        return cb::engine_errc::unknown_collection;
    }
    return cb::engine_errc::success;
}

void CachingReadHandle::dump() {
    std::cerr << *manifest << std::endl;
}

void StatsReadHandle::dump() {
    std::cerr << *manifest << std::endl;
}

CanDeduplicate WriteHandle::getCanDeduplicate(CollectionID cid) const {
    return manifest.getCanDeduplicate(cid);
}

void WriteHandle::dump() {
    std::cerr << manifest << std::endl;
}

} // namespace Collections::VB
