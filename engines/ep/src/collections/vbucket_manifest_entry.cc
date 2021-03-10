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

#include "collections/vbucket_manifest_entry.h"
#include "bucket_logger.h"

#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>

#include <sstream>

bool Collections::VB::ManifestEntry::operator==(
        const ManifestEntry& other) const {
    return startSeqno == other.startSeqno && highSeqno == other.highSeqno &&
           itemCount == other.itemCount && diskSize == other.diskSize &&
           persistedHighSeqno == other.persistedHighSeqno &&
           numOpsGet == other.numOpsGet && numOpsDelete == other.numOpsDelete &&
           numOpsStore == other.numOpsStore && meta == other.meta;
}

std::string Collections::VB::ManifestEntry::getExceptionString(
        const std::string& thrower, const std::string& error) const {
    std::stringstream ss;
    ss << "VB::ManifestEntry::" << thrower << ": " << error
       << ", this:" << *this;
    return ss.str();
}

bool Collections::VB::ManifestEntry::addStats(
        const std::string& cid,
        Vbid vbid,
        const StatCollector& collector) const {
    fmt::memory_buffer key;
    format_to(key, "vb_{}:{}:", vbid.get(), cid);
    const auto prefixLen = key.size();

    const auto addStat = [&key, prefixLen, &collector](std::string_view statKey,
                                                       auto statValue) {
        // resize the buffer back down to just the prefix.
        // this saves reformatting the prefix for each call.
        key.resize(prefixLen);
        key.append(statKey.data(), statKey.data() + statKey.size());
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };
    using namespace std::string_view_literals;
    addStat("name"sv, getName());
    addStat("scope"sv, getScopeID().to_string().c_str());
    addStat("start_seqno"sv, getStartSeqno());
    addStat("high_seqno"sv, getHighSeqno());
    addStat("persisted_high_seqno"sv, getPersistedHighSeqno());
    addStat("items"sv, getItemCount());
    addStat("disk_size"sv, getDiskSize());
    addStat("ops_get"sv, getOpsGet());
    addStat("ops_store"sv, getOpsStore());
    addStat("ops_delete"sv, getOpsDelete());
    if (getMaxTtl()) {
        addStat("maxTTL"sv, getMaxTtl().value().count());
    }

    return true;
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os,
        const Collections::VB::ManifestEntry& manifestEntry) {
    os << "ManifestEntry:"
       << " name:" << manifestEntry.getName()
       << ", scope:" << manifestEntry.getScopeID() << std::dec
       << ", startSeqno:" << manifestEntry.getStartSeqno()
       << ", highSeqno:" << manifestEntry.getHighSeqno()
       << ", persistedHighSeqno:" << manifestEntry.getPersistedHighSeqno()
       << ", itemCount:" << manifestEntry.getItemCount()
       << ", diskSize:" << manifestEntry.getDiskSize()
       << ", r/w/d:" << manifestEntry.getOpsGet() << "/"
       << manifestEntry.getOpsStore() << "/" << manifestEntry.getOpsDelete();

    if (manifestEntry.getMaxTtl()) {
        os << ", maxTtl:" << manifestEntry.getMaxTtl().value().count();
    }
    return os;
}
