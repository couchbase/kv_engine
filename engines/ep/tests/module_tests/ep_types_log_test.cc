/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "mcbp/protocol/status.h"
#include "memcached/engine_error.h"
#include "memcached/vbucket.h"
#include <platform/json_log.h>

using namespace std::string_view_literals;

TEST(EpTypesLogTest, McbpStatus) {
    cb::logger::Json j(cb::mcbp::Status::Success);
    EXPECT_EQ("\"Success\""sv, j.dump());
}

TEST(EpTypesLogTest, EngineErrc) {
    cb::logger::Json j(cb::engine_errc::success);
    EXPECT_EQ("\"success\""sv, j.dump());
}

TEST(EpTypesLogTest, Vbid) {
    cb::logger::Json j(Vbid(0));
    EXPECT_EQ("\"vb:0\""sv, j.dump());
}
