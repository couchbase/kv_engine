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
#include <folly/portability/GTest.h>
#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

TEST(McbpStatusTest, VerifyErrorMap) {
    const auto filename = cb::io::sanitizePath(
            SOURCE_ROOT "/etc/couchbase/kv/error_maps/error_map_v1.json");
    auto json = nlohmann::json::parse(cb::io::loadFile(filename));
    ASSERT_EQ(1, json["version"].get<int>());
    ASSERT_EQ(4, json["revision"].get<int>());

    // Iterate over all status codes and check that they're defined
    for (uint16_t ii = 0; ii < uint16_t(cb::mcbp::Status::COUNT); ++ii) {
        try {
            const auto status = to_string(cb::mcbp::Status(ii));
            // This error is defined... verify that we've got it in the
            // json
            char buffer[10];
            snprintf(buffer, sizeof(buffer), "%x", ii);
            try {
                json["errors"][buffer]["name"].get<std::string>();
            } catch (const nlohmann::json::exception&) {
                FAIL() << "Missing entry for [" << status << "]: " << buffer;
            }
        } catch (const std::invalid_argument&) {
            // not defined
        }
    }
}
