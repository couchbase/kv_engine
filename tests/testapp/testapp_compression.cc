/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"
#include "testapp_environment.h"
#include <protocol/mcbp/ewb_encode.h>
#include <cctype>
#include <limits>
#include <thread>

using namespace std::string_literals;

/**
 * The CompressionTest batch performs tests on a bucket with active
 * compression set (and allows for testing operations where the data
 * on the server is compressed).
 */
class CompressionTest : public TestappXattrClientTest {
public:
    static void SetUpTestCase() {
        TestappXattrClientTest::SetUpTestCase();
        xattr_value.resize(4096, 'z');
        xattr_value.front() = '"';
        xattr_value.back() = '"';
    }

protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        mcd_env->getTestBucket().setCompressionMode(
                *adminConnection, bucketName, "active");
    }

    void waitForCompression() {
        bool compressed = false;
        using std::chrono::steady_clock;
        const auto timeout = steady_clock::now() + std::chrono::seconds{10};
        while (steady_clock::now() < timeout) {
            userConnection->stats(
                    [&compressed](auto& k, auto& v) {
                        if (k == "key_datatype"s) {
                            if (v.find("snappy") != std::string::npos) {
                                compressed = true;
                            }
                            if (k == "key_valid"s) {
                                EXPECT_EQ("valid"s, v);
                            }
                        }
                    },
                    fmt::format("vkey {} 0", name));
            if (compressed) {
                break;
            } else {
                // Don't busy-wait. back off and try again
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
        ASSERT_TRUE(compressed)
                << "Timed out waiting for the document to be compressed";
    }

    /**
     * Upsert a document
     *
     * Helper function to update or insert a document with
     * extended attributes and wait for the document to be compressed
     *
     * @param value The documents value
     */
    void upsert(std::string value) {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.setVBucket(Vbid{0});
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        cb::mcbp::subdoc::PathFlag::XattrPath,
                        xattr_key,
                        xattr_value);
        cmd.addMutation(cb::mcbp::ClientOpcode::Set, {}, "", value);
        cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);
        auto rsp = userConnection->execute(cmd);
        if (!rsp.isSuccess()) {
            throw ConnectionError("Failed to upsert document", rsp);
        }
        waitForCompression();
    }

    void validateJsonField(std::string_view key,
                           std::string_view value,
                           bool xattr) {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath(std::string{key});
        if (xattr) {
            cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath);
        }
        userConnection->sendCommand(cmd);
        BinprotSubdocResponse resp;
        userConnection->recvResponse(resp);
        ASSERT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
        EXPECT_EQ(value, resp.getValue());
    }

    void validateXAttrs() {
        validateJsonField(xattr_key, xattr_value, true);
    }

    static const std::string xattr_key;
    static std::string xattr_value;
};

const std::string CompressionTest::xattr_key = "meta.value";
std::string CompressionTest::xattr_value;

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        CompressionTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

/// Verify that increment works on a compressed document which contains extended
/// attributes
TEST_P(CompressionTest, MB57640) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    upsert("0");

    // Increment the counter
    EXPECT_EQ(1, userConnection->increment(name, 1));

    // Validate that the XATTRs is still intact
    validateXAttrs();
}

/// Verify that Subdoc operations work works on a compressed document
/// with extended attributes
TEST_P(CompressionTest, SubdocLookupXattrOnCompressedDocument) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    upsert("0");

    // The document should be compressed, verify that we can read the
    // attributes
    validateXAttrs();
}

/// Verify that Subdoc operations work works on a compressed document
/// with extended attributes
TEST_P(CompressionTest, SubdocLookupFieldOnCompressedDocument) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    upsert(R"({"foo" : "bar"})");

    // The document should be compressed, verify that we can read the
    // document fields by using subdoc
    validateJsonField("foo", R"("bar")", false);
}

/// Verify that get works correctly:
/// * If the document contains xattrs, the document needs to be inflated
///   before being returned.
/// * If there is no xattrs, the document should be returned compressed
TEST_P(CompressionTest, GetReturnsCompressedData) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    Document doc;
    doc.info.id = name;
    doc.value.resize(4096, 'a');
    doc.compress();
    // By storing the document compressed, the server shouldn't need to compress
    // it and just reuse the provided blob
    userConnection->mutate(doc, Vbid{0}, MutationType::Set);
    waitForCompression();

    auto document = userConnection->get(name, Vbid{0});
    EXPECT_EQ(cb::mcbp::Datatype::Snappy, document.info.datatype);
    EXPECT_EQ(doc.value, document.value);

    // Verify that if the document contains xattrs it gets inflated (and
    // the xattrs stripped off)
    std::string value(R"({"foo" : "bar"})");
    upsert(value);
    document = userConnection->get(name, Vbid{0});
    EXPECT_EQ(cb::mcbp::Datatype::JSON, document.info.datatype);
    EXPECT_EQ(value, document.value);
}

/// Verify that append work correctly on compressed document
TEST_P(CompressionTest, AppendPrepend) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    upsert(R"(1234)");
    Document document;
    document.value = "{}";
    document.info.id = name;
    userConnection->mutate(document, Vbid{0}, MutationType::Append);
    userConnection->mutate(document, Vbid{0}, MutationType::Prepend);
    waitForCompression();
    auto doc = userConnection->get(name, Vbid{0});
    // The document contains XATTRS so it should be inflated before returned
    EXPECT_EQ(cb::mcbp::Datatype::Raw, doc.info.datatype);
    EXPECT_EQ("{}1234{}"s, doc.value);

    // Validate that we didn't mess up the xattr's
    validateXAttrs();
}

TEST_P(CompressionTest, GatReturnsCompressedData) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    Document doc;
    doc.info.id = name;
    doc.value.resize(4096, 'a');
    doc.compress();
    // By storing the document compressed, the server shouldn't need to compress
    // it and just reuse the provided blob
    userConnection->mutate(doc, Vbid{0}, MutationType::Set);
    waitForCompression();
    auto rsp = userConnection->execute(
            BinprotGetAndTouchCommand{name, Vbid{0}, 0});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_EQ(cb::mcbp::Datatype::Snappy,
              cb::mcbp::Datatype(rsp.getDatatype()));
    EXPECT_EQ(doc.value, rsp.getDataView());

    // MB-57672 Incorrect datatype returned when the document contains
    //          xattrs
    std::string value(R"({"foo" : "bar"})");
    upsert(value);
    rsp = userConnection->execute(BinprotGetAndTouchCommand{name, Vbid{0}, 0});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_EQ(cb::mcbp::Datatype::JSON, cb::mcbp::Datatype(rsp.getDatatype()));
    EXPECT_EQ(value, rsp.getDataView());
}

TEST_P(CompressionTest, GetLockedReturnsCompressedData) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    Document doc;
    doc.info.id = name;
    doc.value.resize(4096, 'a');
    doc.compress();
    // By storing the document compressed, the server shouldn't need to compress
    // it and just reuse the provided blob
    userConnection->mutate(doc, Vbid{0}, MutationType::Set);
    waitForCompression();
    auto locked = userConnection->get_and_lock(name, Vbid{0}, 60);
    EXPECT_EQ(cb::mcbp::Datatype::Snappy, locked.info.datatype);
    EXPECT_EQ(doc.value, locked.value);
    userConnection->unlock(name, Vbid{0}, locked.info.cas);

    // MB-57672 Incorrect datatype returned when the document contains
    //          xattrs
    std::string value(R"({"foo" : "bar"})");
    upsert(value);
    locked = userConnection->get_and_lock(name, Vbid{0}, 60);
    EXPECT_EQ(cb::mcbp::Datatype::JSON, locked.info.datatype);
    EXPECT_EQ(value, locked.value);
    userConnection->unlock(name, Vbid{0}, locked.info.cas);
}
