/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <include/memcached/protocol_binary.h>
#include <platform/compress.h>
#include <protocol/connection/frameinfo.h>
#include <algorithm>

/// Tests to verify that we're able to preserve the TTL for mutation
/// commands when the "Preserve TTL" frame info is present.
class TtlTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        ttl = store(getConnection(), name, 32, MutationType::Set, true);
    }

protected:
    /// We need support JSON as we'll be operating with subdoc.
    MemcachedConnection& getConnection() override {
        auto& ret = TestappClientTest::getConnection();
        ret.setFeature(cb::mcbp::Feature::JSON, true);
        return ret;
    }

    /// Helper method to store a fixed document with a given TTL (or preserve
    /// the old one if preserveTtl is set to true).
    /// Returns the TTL on the document on the server
    time_t store(MemcachedConnection& connection,
                 const std::string& key,
                 time_t ttl,
                 MutationType type,
                 bool preserveTtl);

    /// Helper method to modify a fixed document with a given TTL (or preserve
    /// the old one if preserveTtl is set to true) by using subdoc.
    /// Returns the TTL on the document on the server
    time_t subdoc_modify(MemcachedConnection& connection,
                         const std::string& key,
                         time_t ttl,
                         bool preserveTtl);

    /// Get the TTL for the document on the server
    time_t getTtl(MemcachedConnection& connection, std::string key);

    void test(MutationType type);

    /// The ttl for the initial version of the document stored on the server
    time_t ttl{0};
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TtlTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

time_t TtlTest::getTtl(MemcachedConnection& connection, std::string key) {
    BinprotSubdocCommand cmd(cb::mcbp::ClientOpcode::SubdocGet,
                             name,
                             R"($document.exptime)",
                             {},
                             SUBDOC_FLAG_XATTR_PATH);
    auto rsp = connection.execute(cmd);
    if (!rsp.isSuccess()) {
        throw std::runtime_error("TtlTest::getTtl: Failed to get exptime: " +
                                 rsp.getDataString());
    }
    auto json = nlohmann::json::parse(rsp.getDataString());
    return json.get<time_t>();
}

time_t TtlTest::store(MemcachedConnection& connection,
                      const std::string& key,
                      time_t exptime,
                      MutationType type,
                      bool preserveTtl) {
    Document document;
    document.info.cas = mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = gsl::narrow_cast<uint32_t>(exptime);
    document.info.datatype = cb::mcbp::Datatype::JSON;
    document.value = R"({ "foo" : "bar" })";
    connection.mutate(
            document, Vbid{0}, type, [preserveTtl]() -> FrameInfoVector {
                FrameInfoVector ret;
                if (preserveTtl) {
                    ret.emplace_back(std::make_unique<PreserveTtlFrameInfo>());
                }
                return ret;
            });
    return getTtl(connection, key);
}

time_t TtlTest::subdoc_modify(MemcachedConnection& connection,
                              const std::string& key,
                              time_t exptime,
                              bool preserveTtl) {
    BinprotSubdocCommand cmd(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             name,
                             R"(foo)",
                             R"("subdoc_modify")");
    cmd.setExpiry(exptime);
    if (preserveTtl) {
        cmd.addFrameInfo(PreserveTtlFrameInfo{});
    }

    auto rsp = connection.execute(cmd);
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                "TtlTest::subdoc_modify: SubdocDictAdd failed: " +
                rsp.getDataString());
    }

    return getTtl(connection, key);
}

void TtlTest::test(MutationType type) {
    auto& conn = getConnection();
    ASSERT_NE(0, ttl) << "A relative expiry was specified";

    // Try to call set again (with a completely different TTL) and verify that
    // it don't change
    ASSERT_EQ(ttl, store(conn, name, 0, type, true));

    // Try to call set again (with a completely different TTL) and verify that
    // it does change if we don't ask for it to be preserved
    ASSERT_EQ(0, store(conn, name, 0, type, false));
}

TEST_P(TtlTest, Set) {
    test(MutationType::Set);
}

TEST_P(TtlTest, Replace) {
    test(MutationType::Replace);
}

TEST_P(TtlTest, Subdoc) {
    // There isn't a need for testing all of the various ways to use subdoc to
    // mutate a document as all of the subdoc methods work in the same
    // way: "read, modify, cas swap" and it is the final swap which contains
    // the logic for storing the new document (with the new / preserved expiry
    // time.
    auto& conn = getConnection();
    ASSERT_NE(0, ttl) << "A relative expiry was specified";

    // Try to call set again (with a completely different TTL) and verify that
    // it don't change
    ASSERT_EQ(ttl, subdoc_modify(conn, name, 0, true));

    // Try to call set again (with a completely different TTL) and verify that
    // it does change if we don't ask for it to be preserved
    ASSERT_EQ(0, subdoc_modify(conn, name, 0, false));
}
