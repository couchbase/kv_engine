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

#include <string>
#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

namespace cb {
namespace durability {

std::string to_string(Requirements r) {
    std::string desc = "{";
    switch (r.getLevel()) {
    case Level::None:
        desc += "None, ";
        break;
    case Level::Majority:
        desc += "Majority, ";
        break;
    case Level::MajorityAndPersistOnMaster:
        desc += "MajorityAndPersistOnMaster, ";
        break;
    case Level::PersistToMajority:
        desc += "PersistToMajority, ";
        break;
    }
    desc += "timeout=" + std::to_string(r.getTimeout()) + "}";
    return desc;
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

} // namespace durability
} // namespace cb
