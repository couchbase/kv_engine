/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include "xattr/blob.h"
#include "xattr/utils.h"

#include <nlohmann/json.hpp>
#include <algorithm>

/**
 * Class to test GetRandomDocument. We want it to be a single test suite
 * to create a bucket containing just a single document (so that we know
 * what to expect upon return) and so that we can verify that the server
 * strips of the correct part of the message as we go along
 */
class GetRandomKeyTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }

    // Verify that the document contains the correct value and system xattrs
    static void validateDocument(Document doc,
                                 bool withSystemXattrs,
                                 const nlohmann::json& json) {
        auto datatype = static_cast<uint8_t>(doc.info.datatype);
        if (cb::mcbp::datatype::is_snappy(datatype)) {
            doc.uncompress();
        }

        std::string_view value;
        if (cb::mcbp::datatype::is_xattr(datatype)) {
            value = cb::xattr::get_body(doc.value);
            std::string xattr;
            xattr = {doc.value.data(), cb::xattr::get_body_offset(doc.value)};
            cb::xattr::Blob blob(xattr);
            const auto xattr_json = nlohmann::json::parse(blob.to_string());
            if (withSystemXattrs) {
                EXPECT_EQ(R"({"_sys":true,"xattr":"X-value"})"_json,
                          xattr_json);
            } else {
                EXPECT_EQ(R"({"xattr":"X-value"})"_json, xattr_json);
            }
        } else {
            value = doc.value;
        }

        EXPECT_EQ(json, nlohmann::json::parse(value));
    }

    static void SetUpTestCase() {
        TestappXattrClientTest::SetUpTestCase();
    }
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        GetRandomKeyTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

TEST_P(GetRandomKeyTest, GetRandomKeyTest) {
    if (!mcd_env->getTestBucket().supportsOp(
                cb::mcbp::ClientOpcode::SetWithMeta)) {
        GTEST_SKIP() << "bucket does not support setWithMeta";
    }

    nlohmann::json json;
    json["foo"] = std::string(1024, 'a');

    setBodyAndXattr(
            json.dump(), {{"xattr", "\"X-value\""}, {"_sys", "true"}}, true);

    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool found = false;
    do {
        try {
            auto doc = userConnection->getRandomKey(Vbid{0});
            found = true;
        } catch (const ConnectionError& e) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    } while (!found && std::chrono::steady_clock::now() < timeout);
    if (!found) {
        throw std::runtime_error("Failed to get random key");
    }

    // Get the random document (there is only one in there). It should just
    // be JSON as the server stripped off the XATTRS (and had to inflate it
    // to do so)
    auto doc = userConnection->getRandomKey(Vbid{0});
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON,
              static_cast<uint8_t>(doc.info.datatype));
    validateDocument(doc, false, json);

    // Enable GetRandomKeyIncludeXattr (which require SnappyEverywhere and
    // collections as the server wants to do a minimum of job)
    userConnection->setFeature(cb::mcbp::Feature::Collections, true);
    userConnection->setFeature(cb::mcbp::Feature::SnappyEverywhere, true);
    userConnection->setFeature(cb::mcbp::Feature::GetRandomKeyIncludeXattr,
                               true);
    doc = userConnection->getRandomKey(Vbid{0});

    // It should be returned compressed and all the XATTRS intact
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR |
                      PROTOCOL_BINARY_DATATYPE_SNAPPY,
              static_cast<uint8_t>(doc.info.datatype));
    validateDocument(doc, true, json);

    // Drop the system xattr read privilege, and the server would need to
    // inflate the document to strip it off
    userConnection->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    doc = userConnection->getRandomKey(Vbid{0});
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              static_cast<uint8_t>(doc.info.datatype));
    validateDocument(doc, false, json);
}
