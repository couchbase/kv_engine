/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "vbucket_manifest_handles.h"
#include "collections/collection_persisted_stats.h"

#include <iostream>

namespace Collections::VB {

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
