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

SystemEvent Collections::VB::ManifestEntry::completeDeletion() {
    if (isExclusiveDeleting()) {
        // All items from any generation of the collection gone, so delete the
        // collection metadata
        return SystemEvent::DeleteCollectionHard;
    } else if (isOpenAndDeleting()) {
        resetEndSeqno(); // reset the end to the special seqno and return soft
        return SystemEvent::DeleteCollectionSoft;
    }
    throwException<std::logic_error>(__FUNCTION__, "invalid state");
}

std::string Collections::VB::ManifestEntry::getExceptionString(
        const std::string& thrower, const std::string& error) const {
    std::stringstream ss;
    ss << "VB::ManifestEntry::" << thrower << ": " << error
       << ", this:" << *this;
    return ss.str();
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os,
        const Collections::VB::ManifestEntry& manifestEntry) {
    os << "ManifestEntry: startSeqno:" << manifestEntry.getStartSeqno()
       << ", endSeqno:" << manifestEntry.getEndSeqno();
    return os;
}