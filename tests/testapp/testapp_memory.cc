/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <folly/portability/GMock.h>

class MemTrackingBucketTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        // Note: Important to set the env BEFORE starting memcached,
        // env vars wouldn't be passed to the memcached process otherwise
        // (unless testapp is run in -e (embedded) mode)
        if (!getenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT")) {
            ASSERT_EQ(0,
                      setenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT", "1", 0));
        }

        auto config = generate_config();
        config["threads"] = 1;
        TestappTest::doSetUpTestCaseWithConfiguration(config);
    }

    static void TearDownTestCase() {
        EXPECT_EQ(0, unsetenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT"));
        TestappTest::TearDownTestCase();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         MemTrackingBucketTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(MemTrackingBucketTest, MB_68823) {
    // Note: Not using adminConnection as the connection in the test is
    // forcibly disconnected and we need adminConnection at TearDown.
    auto& conn = getConnection();
    conn.authenticate("@admin");
    conn.selectBucket(bucketName);
    conn.setFeature(cb::mcbp::Feature::JSON, true);
    conn.setFeature(cb::mcbp::Feature::Collections, true);
    conn.dcpOpenProducer("dcp-conn_invalid-stream-req-filter");
    conn.dcpControl("enable_noop", "true");

    // Invalid StreamReq filter (with cid duplicate) throws in Filter::ctor.
    // Before the fix the test fails by:
    //
    // ===ERROR===: JeArenaMalloc deallocation mismatch
    //     Memory freed by client:100 domain:None which is assigned arena:0,
    //     but memory was previously allocated from arena:2 (client-specific
    //     arena).
    //     Allocation address:0x10b1b1080 size:192
    try {
        conn.dcpStreamRequest(Vbid(0),
                              cb::mcbp::DcpAddStreamFlag::None,
                              0, // startSeq
                              ~0ULL, // endSeq,
                              0, // vbUuid
                              0, // snapStart
                              0, // snapEnd
                              R"({"collections":["0", "0"]})"_json); // filter
    } catch (const std::exception&) {
        const auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds{10};
        const auto line =
                "EventuallyPersistentEngine::stream_req: Exception GSL: "
                "Precondition failure: 'emplaced'";
        constexpr auto expectedLogInstances = 1;
        do {
            if (mcd_env->verifyLogLine(line) == expectedLogInstances) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        } while (std::chrono::steady_clock::now() < timeout);

        FAIL() << "Timeout before the log line was dumped to the file";
    }
    FAIL() << "StreamRequest should have failed";
}

TEST_P(MemTrackingBucketTest, MB_71836) {
    auto& conn = getConnection();
    conn.authenticate("@admin");
    conn.selectBucket(bucketName);

    // Craft an invalid SetWithMeta payload
    Document doc{};
    const auto key = "key";
    doc.info.id = key;
    doc.info.cas = 123;
    doc.info.expiration = 0;
    doc.info.flags = 0xaabbccdd;
    doc.value = "value";
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    // 1-byte meta
    const std::vector<uint8_t> meta = {0xff};

    class InvalidSetWithMetaCommand : public BinprotSetWithMetaCommand {
    public:
        InvalidSetWithMetaCommand(const Document& doc,
                                  Vbid vbid,
                                  uint64_t cas,
                                  uint64_t seqno,
                                  uint32_t options,
                                  const std::vector<uint8_t>& meta)
            : BinprotSetWithMetaCommand(doc, vbid, cas, seqno, options, meta) {
        }

        void encode(std::vector<uint8_t>& buf) const override {
            BinprotSetWithMetaCommand::encode(buf);
            // Poison the meta_size with its max value (uint16_t, 0xffff).
            // Before the fix, that triggers broken code that underflows the
            // internal doc value representation by making its size huge.
            buf.at(48) = static_cast<uint16_t>(0xff);
            buf.at(49) = static_cast<uint16_t>(0xff);
        }
    };

    InvalidSetWithMetaCommand cmd(
            doc, Vbid(0), cb::mcbp::cas::Wildcard, 1, 0, meta);
    // We want to validate the invalid request and force disconnection.
    // Before the fix the invalid payload goes through, sign is that we try to
    // allocate a huge blob by that, the connection stays alive we just return
    // E2big.
    try {
        conn.execute(cmd);
        FAIL() << "Server should force disconnection";
    } catch (const std::system_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr("AsyncSocketException: Network error"));
    }
}
