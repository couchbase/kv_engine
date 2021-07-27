/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>
#include <map>
#include <stdexcept>

const std::map<cb::mcbp::Magic, std::string> magicBlueprint = {
        {{cb::mcbp::Magic::ClientRequest, "ClientRequest"},
         {cb::mcbp::Magic::AltClientRequest, "AltClientRequest"},
         {cb::mcbp::Magic::ClientResponse, "ClientResponse"},
         {cb::mcbp::Magic::AltClientResponse, "AltClientResponse"},
         {cb::mcbp::Magic::ServerRequest, "ServerRequest"},
         {cb::mcbp::Magic::ServerResponse, "ServerResponse"}}};

TEST(Magic, to_string) {
    for (int ii = 0; ii < 0x100; ++ii) {
        auto magic = cb::mcbp::Magic(ii);
        auto entry = magicBlueprint.find(magic);
        if (entry == magicBlueprint.end()) {
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
        EXPECT_NE(magicBlueprint.find(magic) == magicBlueprint.end(),
                  cb::mcbp::is_legal(magic));
    }
}
