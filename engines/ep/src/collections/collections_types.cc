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

#include "collections/collections_types.h"
#include "collections/vbucket_serialised_manifest_entry_generated.h"

#include <cctype>
#include <cstring>
#include <iostream>

namespace Collections {

ManifestUid makeUid(const char* uid, size_t len) {
    if (std::strlen(uid) == 0 || std::strlen(uid) > len) {
        throw std::invalid_argument(
                "Collections::makeUid uid must be > 0 and <=" +
                std::to_string(len) +
                " characters: "
                "strlen(uid):" +
                std::to_string(std::strlen(uid)));
    }

    // verify that the input characters satisfy isxdigit
    for (size_t ii = 0; ii < std::strlen(uid); ii++) {
        if (uid[ii] == 0) {
            break;
        } else if (!std::isxdigit(uid[ii])) {
            throw std::invalid_argument("Collections::makeUid: uid:" +
                                        std::string(uid) + ", index:" +
                                        std::to_string(ii) + " fails isxdigit");
        }
    }

    return std::strtoul(uid, nullptr, 16);
}
} // end namespace Collections

std::ostream& operator<<(std::ostream& os,
                         const Collections::VB::PersistedManifest& data) {
    os << "PersistedManifest:";
    if (data.empty()) {
        os << "empty";
    } else {
        auto manifest =
                flatbuffers::GetRoot<Collections::VB::SerialisedManifest>(
                        reinterpret_cast<const uint8_t*>(data.data()));

        auto entries = manifest->entries();
        os << "uid:" << manifest->uid()
           << ", entries:" << entries->size() << std::endl;

        for (const auto& entry : *entries) {
            os << "scope:0x" << std::hex << entry->scopeId() << ", cid:0x"
               << std::hex << entry->collectionId()
               << ", startSeqno:" << entry->startSeqno()
               << ", endSeqno:" << entry->endSeqno() << std::endl;
        }
    }
    return os;
}