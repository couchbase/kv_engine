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
#include "statistics/collector.h"

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
    itemCount = other.itemCount;
    diskSize = other.diskSize;
    highSeqno.reset(other.highSeqno);
    persistedHighSeqno.store(other.persistedHighSeqno,
                             std::memory_order_relaxed);
    numOpsStore = other.numOpsStore;
    numOpsDelete = other.numOpsDelete;
    numOpsGet = other.numOpsGet;
    return *this;
}

bool Collections::VB::ManifestEntry::operator==(
        const ManifestEntry& other) const {
    return scopeID == other.scopeID && startSeqno == other.startSeqno &&
           maxTtl == other.maxTtl && highSeqno == other.highSeqno &&
           itemCount == other.itemCount && diskSize == other.diskSize &&
           persistedHighSeqno == other.persistedHighSeqno &&
           numOpsGet == other.numOpsGet && numOpsDelete == other.numOpsDelete &&
           numOpsStore == other.numOpsStore;
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
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:scope", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getScopeID().to_string(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:start_seqno", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getStartSeqno(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:high_seqno", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:%s:persisted_high_seqno",
                         vbid.get(),
                         cid.c_str());
        add_casted_stat(buffer, getPersistedHighSeqno(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:items", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getItemCount(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:disk_size", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getDiskSize(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:ops_get", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getOpsGet(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:ops_store", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getOpsStore(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:%s:ops_delete", vbid.get(), cid.c_str());
        add_casted_stat(buffer, getOpsDelete(), add_stat, cookie);

        if (getMaxTtl()) {
            checked_snprintf(
                    buffer, bsize, "vb_%d:%s:maxTTL", vbid.get(), cid.c_str());
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
       << ", itemCount:" << manifestEntry.getItemCount()
       << ", diskSize:" << manifestEntry.getDiskSize()
       << ", r/w/d:" << manifestEntry.getOpsGet() << "/"
       << manifestEntry.getOpsStore() << "/" << manifestEntry.getOpsDelete();

    if (manifestEntry.getMaxTtl()) {
        os << ", maxTtl:" << manifestEntry.getMaxTtl().value().count();
    }
    return os;
}
