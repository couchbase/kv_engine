/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <libgreenstack/Opcodes.h>

#include <stdexcept>
#include <strings.h>
#include <map>

const std::map<Greenstack::Opcode, std::string> mappings = {
    // Generic
    {Greenstack::Opcode::Hello,        "Hello"},
    {Greenstack::Opcode::SaslAuth,     "SaslAuth"},
    {Greenstack::Opcode::Noop,         "Noop"},

    // Memcached
    {Greenstack::Opcode::SelectBucket, "SelectBucket"},
    {Greenstack::Opcode::ListBuckets,  "ListBuckets"},
    {Greenstack::Opcode::CreateBucket, "CreateBucket"},
    {Greenstack::Opcode::DeleteBucket, "DeleteBucket"},
    {Greenstack::Opcode::AssumeRole,   "AssumeRole"},
    {Greenstack::Opcode::Mutation,     "Mutation"},
    {Greenstack::Opcode::Get,          "Get"}
};

std::string Greenstack::to_string(const Greenstack::Opcode& opcode) {
    const auto iter = mappings.find(opcode);
    if (iter == mappings.end()) {
        return std::to_string(uint16_t(opcode));
    } else {
        return iter->second;
    }
}

Greenstack::Opcode Greenstack::from_string(const std::string& val) {
    for (auto iter : mappings) {
        if (strcasecmp(val.c_str(), iter.second.c_str()) == 0) {
            return iter.first;
        }
    }

    std::string msg = "Unknown command [";
    msg.append(val);
    msg.append("]");
    throw std::runtime_error(msg);
}

Greenstack::Opcode Greenstack::to_opcode(uint16_t opcode) {
    Opcode op = Opcode(opcode);
    const auto iter = mappings.find(op);
    if (iter == mappings.cend()) {
        return Opcode::InvalidOpcode;
    } else {
        return iter->first;
    }
}
