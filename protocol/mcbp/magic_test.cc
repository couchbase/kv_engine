/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <gtest/gtest.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/protocol_binary.h>
#include <algorithm>
#include <cctype>
#include <map>
#include <stdexcept>

const std::map<cb::mcbp::Magic, std::string> blueprint = {
        {{cb::mcbp::Magic::ClientRequest, "ClientRequest"},
         {cb::mcbp::Magic::AltClientRequest, "AltClientRequest"},
         {cb::mcbp::Magic::ClientResponse, "ClientResponse"},
         {cb::mcbp::Magic::AltClientResponse, "AltClientResponse"},
         {cb::mcbp::Magic::ServerRequest, "ServerRequest"},
         {cb::mcbp::Magic::ServerResponse, "ServerResponse"}}};

TEST(Magic, to_string) {
    for (int ii = 0; ii < 0x100; ++ii) {
        auto magic = cb::mcbp::Magic(ii);
        auto entry = blueprint.find(magic);
        if (entry == blueprint.end()) {
            EXPECT_THROW(to_string(magic), std::invalid_argument);
        } else {
            EXPECT_EQ(entry->second, to_string(entry->first));
        }
    }
}

TEST(Magic, is_legal) {
    for (int ii = 0; ii < 0x100; ++ii) {
        auto magic = cb::mcbp::Magic(ii);
        // If it isn't in the map it shouldn't be legal
        EXPECT_NE(blueprint.find(magic) == blueprint.end(),
                  cb::mcbp::is_legal(magic));
    }
}
