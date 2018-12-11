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

std::string cb::durability::to_string(Requirements r) {
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
