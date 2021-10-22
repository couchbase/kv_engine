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

#include "vbucket_manifest_handles.h"
#include "collections/collection_persisted_stats.h"

#include <iostream>

namespace Collections::VB {

size_t ReadHandle::getDataSize(ScopeID sid) const {
    return manifest->getDataSize(sid);
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

size_t StatsReadHandle::getOpsStore() const {
    return itr->second.getOpsStore();
}

size_t StatsReadHandle::getOpsDelete() const {
    return itr->second.getOpsDelete();
}

size_t StatsReadHandle::getOpsGet() const {
    return itr->second.getOpsGet();
}

void ReadHandle::dump() const {
    std::cerr << *manifest << std::endl;
}

void CachingReadHandle::dump() {
    std::cerr << *manifest << std::endl;
}

void StatsReadHandle::dump() {
    std::cerr << *manifest << std::endl;
}

void WriteHandle::dump() {
    std::cerr << manifest << std::endl;
}

} // namespace Collections::VB
