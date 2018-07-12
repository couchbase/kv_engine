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

#include "collections/vbucket_filter.h"
#include "collections/filter.h"
#include "collections/vbucket_manifest.h"
#include "dcp/response.h"
#include "statwriter.h"

#include <platform/checked_snprintf.h>
#include <memory>

Collections::VB::Filter::Filter(const Collections::Filter& filter,
                                const Collections::VB::Manifest& manifest)
    : defaultAllowed(false),
      passthrough(filter.isPassthrough()),
      systemEventsAllowed(filter.allowSystemEvents()) {
    // Don't build a filter if all documents are allowed
    if (passthrough) {
        defaultAllowed = true;
        return;
    }

    // Lock for reading and create a VB filter
    auto rh = manifest.lock();
    if (filter.allowDefaultCollection()) {
        if (rh.doesDefaultCollectionExist()) {
            defaultAllowed = true;
        } else {
            // The VB::Manifest no longer has $default so don't filter it
            LOG(EXTENSION_LOG_NOTICE,
                "VB::Filter::Filter: dropping $default as it's not in the "
                "VB::Manifest");
        }
    }

    for (const auto& collection : filter.getFilter()) {
        // lookup by ID
        if (rh.isCollectionOpen(collection)) {
            this->filter.insert(collection);
        } else {
            // The VB::Manifest doesn't have the collection, or the collection
            // is deleted
            LOG(EXTENSION_LOG_NOTICE,
                "VB::Filter::Filter: dropping collection:%" PRIx32
                "as it's not open",
                uint32_t(collection));
        }
    }
}

bool Collections::VB::Filter::checkAndUpdateSlow(const Item& item) {
    bool allowed = false;

    if (item.getKey().getCollectionID() == DocNamespace::System) {
        allowed = allowSystemEvent(item);

        if (item.isDeleted()) {
            remove(item);
        }
    } else {
        allowed = filter.count(item.getKey().getCollectionID());
    }

    return allowed;
}

void Collections::VB::Filter::remove(const Item& item) {
    if (passthrough) {
        return;
    }

    CollectionID collection =
            VB::Manifest::getCollectionIDFromKey(item.getKey());
    if (collection == CollectionID::DefaultCollection) {
        defaultAllowed = false;
    } else {
        filter.erase(collection);
    }
}

bool Collections::VB::Filter::empty() const {
    return filter.empty() && !defaultAllowed;
}

bool Collections::VB::Filter::allowSystemEvent(const Item& item) const {
    if (!systemEventsAllowed) {
        return false;
    }
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection: {
        CollectionID collection =
                VB::Manifest::getCollectionIDFromKey(item.getKey());
        if ((collection == CollectionID::DefaultCollection && defaultAllowed) ||
            passthrough) {
            return true;
        } else {
            // These events are sent only if they relate to a collection in the
            // filter
            return filter.count(collection) > 0;
        }
    }
    case SystemEvent::DeleteCollectionHard: {
        return false;
    }
    default: {
        throw std::invalid_argument(
                "VB::Filter::allowSystemEvent:: event unknown:" +
                std::to_string(int(item.getFlags())));
    }
    }
}

void Collections::VB::Filter::addStats(ADD_STAT add_stat,
                                       const void* c,
                                       const std::string& prefix,
                                       uint16_t vb) const {
    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(
                buffer, bsize, "%s:filter_%d_passthrough", prefix.c_str(), vb);
        add_casted_stat(buffer, passthrough, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:filter_%d_default_allowed",
                         prefix.c_str(),
                         vb);
        add_casted_stat(buffer, defaultAllowed, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:filter_%d_system_allowed",
                         prefix.c_str(),
                         vb);
        add_casted_stat(buffer, systemEventsAllowed, add_stat, c);

        checked_snprintf(
                buffer, bsize, "%s:filter_%d_size", prefix.c_str(), vb);
        add_casted_stat(buffer, filter.size(), add_stat, c);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "Collections::VB::Filter::addStats: %s:vb:%" PRIu16
            " exception.what:%s",
            prefix.c_str(),
            vb,
            error.what());
    }
}

void Collections::VB::Filter::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os, const Collections::VB::Filter& filter) {
    os << "VBucket::Filter"
       << ": defaultAllowed:" << filter.defaultAllowed
       << ", passthrough:" << filter.passthrough
       << ", systemEventsAllowed:" << filter.systemEventsAllowed;

    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (auto& cid : filter.filter) {
        os << std::hex << cid << "," << std::endl;
    }
    return os;
}
