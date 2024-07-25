/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include <fmt/format.h>
#include <nlohmann/json.hpp>

class ErrmapTest : public TestappClientTest {
protected:
    static void checkVersion(uint16_t ver, uint16_t result) {
        const auto rsp =
                userConnection->execute(BinprotGetErrorMapCommand{ver});
        if (ver > 0) {
            ASSERT_TRUE(rsp.isSuccess());
            const auto json = rsp.getDataJson();
            auto version = json.find("version");
            ASSERT_NE(json.end(), version);
            EXPECT_EQ(result, version->get<int>());
        } else {
            ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
        }
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         ErrmapTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(ErrmapTest, GetErrmapV0) {
    checkVersion(0, 0);
}

TEST_P(ErrmapTest, GetErrmapV1) {
    checkVersion(1, 1);
}

TEST_P(ErrmapTest, GetErrmapV2) {
    checkVersion(2, 2);
}

TEST_P(ErrmapTest, GetErrmapNewer) {
    // Only two versions exist: 1 and 2.
    for (uint16_t ii = 3; ii < 10; ++ii) {
        checkVersion(ii, 2);
    }
}

TEST_P(ErrmapTest, AllErrorsDocumented) {
    const auto rsp = userConnection->execute(BinprotGetErrorMapCommand{2});
    ASSERT_TRUE(rsp.isSuccess());
    const auto json = rsp.getDataJson();
    using namespace cb::mcbp;

    auto& errors = json.at("errors");

    for (int ii = 0; ii < int(Status::COUNT); ++ii) {
        const auto status = Status(ii);
        if (is_known(status)) {
            EXPECT_TRUE(errors.contains(fmt::format("{0:x}", ii)))
                    << "Missing entry for : " << status;
        }
    }
}
