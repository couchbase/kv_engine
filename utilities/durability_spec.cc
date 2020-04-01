/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>

#include <string>
#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

#include <folly/lang/Assume.h>

namespace cb {
namespace durability {

bool operator==(const Timeout& lhs, const Timeout& rhs) {
    return lhs.get() == rhs.get();
}

std::string to_string(Timeout t) {
    if (t.isDefault()) {
        return "default";
    }
    if (t.isInfinite()) {
        return "infinite";
    }
    return std::to_string(t.get());
}

bool operator==(const Requirements& lhs, const Requirements& rhs) {
    return (lhs.getLevel() == rhs.getLevel()) &&
           (lhs.getTimeout() == rhs.getTimeout());
}

std::string to_string(Requirements r) {
    std::string desc = "{" + to_string(r.getLevel()) + ", ";
    desc += "timeout=" + to_string(r.getTimeout()) + "}";
    return desc;
}

// Note: level<->string representation defined in the EP configuration

std::string to_string(Level l) {
    switch (l) {
    case Level::None:
        return "none";
    case Level::Majority:
        return "majority";
    case Level::MajorityAndPersistOnMaster:
        return "majority_and_persist_on_master";
    case Level::PersistToMajority:
        return "persist_to_majority";
    }
    folly::assume_unreachable();
}

Level to_level(const std::string& s) {
    if (s == "none") {
        return Level::None;
    } else if (s == "majority") {
        return Level::Majority;
    } else if (s == "majority_and_persist_on_master") {
        return Level::MajorityAndPersistOnMaster;
    } else if (s == "persist_to_majority") {
        return Level::PersistToMajority;
    }
    throw std::invalid_argument("cb::durability::to_level: unknown level " + s);
}

Requirements::Requirements(cb::const_byte_buffer buffer) {
    if (buffer.size() != 1 && buffer.size() != 3) {
        throw std::invalid_argument(
                "Requirements(): Invalid sized buffer provided: " +
                std::to_string(buffer.size()));
    }
    level = Level(buffer.front());
    if (buffer.size() == 3) {
        timeout = ntohs(*reinterpret_cast<const uint16_t*>(buffer.data() + 1));
    }
    if (!isValid()) {
        throw std::runtime_error(
                "Requirements(): Content represents an invalid requirement "
                "specification");
    }
}

nlohmann::json Requirements::to_json() const {
    nlohmann::json obj;
    switch (getLevel()) {
    case durability::Level::None:
        obj["level"] = "None";
        break;
    case durability::Level::Majority:
        obj["level"] = "Majority";
        break;
    case durability::Level::MajorityAndPersistOnMaster:
        obj["level"] = "MajorityAndPersistOnMaster";
        break;
    case durability::Level::PersistToMajority:
        obj["level"] = "PersistToMajority";
        break;
    }
    auto tmo = getTimeout();
    if (tmo.isDefault()) {
        obj["timeout"] = "Default";
        ;
    } else if (tmo.isInfinite()) {
        obj["timeout"] = "Infinite";
    } else {
        obj["timeout"] = tmo.get();
    }

    return obj;
}

} // namespace durability
} // namespace cb
