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

#pragma once

#include <memcached/dockey.h>
#include <platform/sized_buffer.h>
#include <gsl/gsl>

namespace Collections {

// The reserved name of the system owned, default collection.
const char* const _DefaultCollectionIdentifier = "_default";
static cb::const_char_buffer DefaultCollectionIdentifier(
        _DefaultCollectionIdentifier);

// SystemEvent keys or parts which will be made into keys
const char* const SystemSeparator = ":"; // Note this never changes
const char* const SystemEventPrefix = "_collections";
const char* const SystemEventPrefixWithSeparator = "_collections:";
const char* const DeleteKey = "_collections_delete:";

// Couchstore private file name for manifest data
const char CouchstoreManifest[] = "_local/collections_manifest";

// Length of the string excluding the zero terminator (i.e. strlen)
const size_t CouchstoreManifestLen = sizeof(CouchstoreManifest) - 1;

using uid_t = uint64_t;

/**
 * Return a uid_t from a C-string.
 * A valid uid_t is a C-string where each character satisfies std::isxdigit
 * and can be converted to a uid_t by std::strtoull.
 *
 * @param uid C-string uid
 * @param len a length for validation purposes
 * @throws std::invalid_argument if uid is invalid
 */
uid_t makeUid(const char* uid, size_t len = 16);

/**
 * Return a uid_t from a std::string
 * A valid uid_t is a std::string where each character satisfies std::isxdigit
 * and can be converted to a uid_t by std::strtoull.
 *
 * @param uid std::string
 * @throws std::invalid_argument if uid is invalid
 */
static inline uid_t makeUid(const std::string& uid) {
    return makeUid(uid.c_str());
}

/**
 * Return a CollectionID from a C-string.
 * A valid CollectionID is a C-string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul.
 *
 * @param uid C-string uid
 * @throws std::invalid_argument if uid is invalid
 */
static inline CollectionID makeCollectionID(const char* uid) {
    // CollectionID is 8 characters max and smaller than a uid_t
    return gsl::narrow_cast<CollectionID>(makeUid(uid, 8));
}

/**
 * Return a CollectionID from a std::string
 * A valid CollectionID is a std::string where each character satisfies
 * std::isxdigit and can be converted to a CollectionID by std::strtoul
 *
 * @param uid std::string
 * @throws std::invalid_argument if uid is invalid
 */
static inline CollectionID makeCollectionID(const std::string& uid) {
    return makeCollectionID(uid.c_str());
}

/**
 * All of the data a DCP system event message will carry (for create/delete).
 * This is the layout of such data and the collection name is the key of the
 * event packet.
 */
struct SystemEventDcpData {
    /// The manifest uid stored in network byte order ready for sending
    Collections::uid_t manifestUid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
};


} // end namespace Collections
