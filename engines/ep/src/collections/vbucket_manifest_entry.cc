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
#include "statwriter.h"

#include <platform/checked_snprintf.h>

Collections::VB::ManifestEntry::ManifestEntry(
        const Collections::VB::ManifestEntry& other) {
    *this = other;
}

Collections::VB::ManifestEntry& Collections::VB::ManifestEntry::operator=(
        const ManifestEntry& other) {
    startSeqno = other.startSeqno;
    scopeID = other.scopeID;
    maxTtl = other.maxTtl;
    diskCount = other.diskCount;
    highSeqno.reset(other.highSeqno);
    persistedHighSeqno.store(other.persistedHighSeqno,
                             std::memory_order_relaxed);
    return *this;
}

bool Collections::VB::ManifestEntry::operator==(
        const ManifestEntry& other) const {
    if (scopeID == other.scopeID && startSeqno == other.startSeqno &&
        maxTtl == other.maxTtl && highSeqno == other.highSeqno &&
        persistedHighSeqno == other.persistedHighSeqno) {
        return true;
    }
    return false;
}

std::string Collections::VB::ManifestEntry::getExceptionString(
        const std::string& thrower, const std::string& error) const {
    std::stringstream ss;
    ss << "VB::ManifestEntry::" << thrower << ": " << error
       << ", this:" << *this;
    return ss.str();
}

bool Collections::VB::ManifestEntry::addStats(const std::string& cid,
                                              Vbid vbid,
                                              const void* cookie,
                                              const AddStatFn& add_stat) const {
    try {
        const int bsize = 512;
        char buffer[bsize];
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:scope",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getScopeID().to_string(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:start_seqno",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getStartSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:high_seqno",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:persisted_high_seqno",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getPersistedHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:collection:%s:entry:items",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getDiskCount(), add_stat, cookie);

        if (getMaxTtl()) {
            checked_snprintf(buffer,
                             bsize,
                             "vb_%d:collection:%s:entry:maxTTL",
                             vbid.get(),
                             cid.c_str());
            add_casted_stat(buffer, getMaxTtl().value().count(), add_stat, cookie);
        }
        return true;
    } catch (const std::exception& error) {
        EP_LOG_WARN(
                "VB::ManifestEntry::addStats {}, failed to build stats, "
                "exception:{}",
                vbid,
                error.what());
    }
    return false;
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os,
        const Collections::VB::ManifestEntry& manifestEntry) {
    os << "ManifestEntry: scope:" << manifestEntry.getScopeID()
       << ", startSeqno:" << manifestEntry.getStartSeqno()
       << ", highSeqno:" << manifestEntry.getHighSeqno()
       << ", persistedHighSeqno:" << manifestEntry.getPersistedHighSeqno()
       << ", diskCount:" << manifestEntry.getDiskCount();

    if (manifestEntry.getMaxTtl()) {
        os << ", maxTtl:" << manifestEntry.getMaxTtl().value().count();
    }
    return os;
}
