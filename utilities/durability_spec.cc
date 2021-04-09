/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/durability_spec.h>

#include <folly/lang/Assume.h>
#include <nlohmann/json.hpp>

#include <string>
#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

namespace cb::durability {

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
        timeout = Timeout(
                ntohs(*reinterpret_cast<const uint16_t*>(buffer.data() + 1)));
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

} // namespace cb::durability
