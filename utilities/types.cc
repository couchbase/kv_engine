/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <memcached/types.h>

std::string to_string(const DocumentState& ds) {
    switch (ds) {
    case DocumentState::Deleted:
        return "DocumentState::Deleted";
    case DocumentState::Alive:
        return "DocumentState::Alive";
    }
    return "Invalid DocumentState";
}

std::ostream& operator<<(std::ostream& os, const DocumentState& ds) {
    return os << to_string(ds);
}

std::string to_string(const DocStateFilter& filter) {
    switch (filter) {
    case DocStateFilter::Deleted:
        return "Deleted";
    case DocStateFilter::Alive:
        return "Alive";
    case DocStateFilter::AliveOrDeleted:
        return "Alive|Deleted";
    }
    return "Invalid DocStateFilter:" + std::to_string(int(filter));
}

std::ostream& operator<<(std::ostream& os, const DocStateFilter& filter) {
    return os << to_string(filter);
}

std::string to_string(DeleteSource deleteSource) {
    switch (deleteSource) {
    case DeleteSource::Explicit:
        return "Explicit";
    case DeleteSource::TTL:
        return "TTL";
    }
    return "Invalid DeleteSource";
}

std::string to_string(ConnectionPriority cp) {
    switch (cp) {
    case ConnectionPriority::High:
        return "High";
    case ConnectionPriority::Medium:
        return "Medium";
    case ConnectionPriority::Low:
        return "Low";
    }
    throw std::invalid_argument("Invalid ConnectionPriority: " +
                                std::to_string(int(cp)));
}

std::ostream& operator<<(std::ostream& os, const ConnectionPriority& cp) {
    return os << to_string(cp);
}

std::string to_string(StoreSemantics ss) {
    switch (ss) {
    case StoreSemantics::Add:
        return "Add";
    case StoreSemantics::Set:
        return "Set";
    case StoreSemantics::Replace:
        return "Replace";
    case StoreSemantics::CAS:
        return "CAS";
    }

    return "to_string(StoreSemantics semantics): Invalid semantics: " +
           std::to_string(int(ss));
}

std::ostream& operator<<(std::ostream& os, const StoreSemantics& ss) {
    os << to_string(ss);
    return os;
}
