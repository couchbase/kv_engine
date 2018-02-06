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
#include "collections/collections_dockey.h"
#include "collections/filter.h"
#include "collections/vbucket_manifest.h"
#include "dcp/response.h"
#include "statwriter.h"

#include <platform/checked_snprintf.h>
#include <platform/make_unique.h>

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
    separator = rh.getSeparator();
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

    for (const auto& c : filter.getFilter()) {
        bool validFilterEntry = false;
        if (c.second.is_initialized()) {
            // strict name/uid lookup
            validFilterEntry = rh.isCollectionOpen(Identifier{
                    {c.first.data(), c.first.size()}, c.second.get()});
        } else {
            // just name lookup
            validFilterEntry = rh.isCollectionOpen(
                    cb::const_char_buffer{c.first.data(), c.first.size()});
        }

        if (validFilterEntry) {
            auto m = std::make_unique<std::string>(c.first);
            cb::const_char_buffer b{m->data(), m->size()};
            this->filter.emplace(b, std::move(m));
        } else {
            // The VB::Manifest doesn't have the collection, or the collection
            // is deleted
            if (c.second.is_initialized()) {
                LOG(EXTENSION_LOG_NOTICE,
                    "VB::Filter::Filter: dropping collection:%s:%" PRIu64
                    " as it's not open",
                    c.first.c_str(),
                    c.second.get());
            } else {
                LOG(EXTENSION_LOG_NOTICE,
                    "VB::Filter::Filter: dropping collection:%s as it's not "
                    "open",
                    c.first.c_str());
            }
        }
    }
}

bool Collections::VB::Filter::checkAndUpdate(const Item& item) {
    // passthrough, everything is allowed.
    if (passthrough) {
        return true;
    }

    bool allowed = false;
    // The presence of $default is a simple check against defaultAllowed
    if (item.getKey().getDocNamespace() == DocNamespace::DefaultCollection &&
        defaultAllowed) {
        allowed = true;
    } else if (item.getKey().getDocNamespace() == DocNamespace::Collections &&
               !filter.empty()) {
        // Collections require a look up in the filter
        const auto cKey = Collections::DocKey::make(item.getKey(), separator);
        allowed = filter.count(cKey.getCollection());
    } else if (item.getKey().getDocNamespace() == DocNamespace::System) {
        allowed = allowSystemEvent(item);

        if (item.isDeleted()) {
            remove(item);
        }
    }
    return allowed;
}

void Collections::VB::Filter::remove(const Item& item) {
    if (passthrough) {
        return;
    }

    const auto cKey = Collections::DocKey::make(item.getKey());
    if (cKey.getKey() == DefaultCollectionIdentifier) {
        defaultAllowed = false;
    } else {
        filter.erase(cKey.getKey());
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
        const auto cKey = Collections::DocKey::make(item.getKey());
        if ((cKey.getKey() == DefaultCollectionIdentifier && defaultAllowed) ||
            passthrough) {
            return true;
        } else {
            // These events are sent only if they relate to a collection in the
            // filter
            return filter.count(cKey.getKey()) > 0;
        }
    }
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft: {
        return false;
    }
    case SystemEvent::CollectionsSeparatorChanged: {
        // The separator changed event is sent if system events are allowed
        return systemEventsAllowed;
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

    if (filter.separator.empty()) {
        os << ", separator empty";
    } else {
        os << ", separator:" << filter.separator;
    }

    os << ", filter.size:" << filter.filter.size() << std::endl;
    for (auto& m : filter.filter) {
        os << *m.second << std::endl;
    }
    return os;
}