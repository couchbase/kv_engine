/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp_client_test.h"

/// Tests how a single worker handles multiple "concurrent" connections
/// performing operations.
class WorkerConcurrencyTest : public TestappTest {
public:
    static void SetUpTestCase() {
        // Change the number of worker threads to one so we guarantee that
        // multiple connections are handled by a single worker.
        auto cfg = generate_config();
        cfg["threads"] = 1;
        doSetUpTestCaseWithConfiguration(cfg);
    }
};

/// "Concurrently" add to two different array documents, using two
/// connections. In this thread try to send data as fast as possible
/// mixing on two sockets. Hopefully this is faster than the other
/// end may consume the packets so that we'll end up with a "pipeline"
/// of data on both connections. (the default number of requests per
/// event cycle is 20 before we'll back off)
TEST_F(WorkerConcurrencyTest, SubdocArrayPushLast_Concurrent) {
    rebuildUserConnection(false);

    userConnection->store("a", Vbid{0}, "[]");
    userConnection->store("b", Vbid{0}, "[]");

    auto c1 = userConnection->clone();
    c1->authenticate("Luke", mcd_env->getPassword("Luke"));
    c1->selectBucket(bucketName);

    auto c2 = userConnection->clone();
    c2->authenticate("Luke", mcd_env->getPassword("Luke"));
    c2->selectBucket(bucketName);

    const size_t push_count = 200;

    std::vector<BinprotSubdocCommand> docA;
    std::vector<BinprotSubdocCommand> docB;

    // Build pipeline for the even commands.
    std::string expected_a;
    std::string expected_b;

    for (unsigned int i = 0; i < push_count; i++) {
        if ((i & 1) == 0) {
            expected_a += std::to_string(i) + ",";
            docA.emplace_back(BinprotSubdocCommand{
                    cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                    "a",
                    "",
                    std::to_string(i)});
        } else {
            expected_b += std::to_string(i) + ",";
            docB.emplace_back(BinprotSubdocCommand{
                    cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                    "b",
                    "",
                    std::to_string(i)});
        }
    }

    for (unsigned int i = 0; i < push_count / 2; i++) {
        c1->sendCommand(docA[i]);
        c2->sendCommand(docB[i]);
    }

    // Fixup the expected values - remove the trailing comma and bookend with
    // [ ].
    expected_a.insert(0, "[");
    expected_a.replace(expected_a.size() - 1, 1, "]");
    expected_b.insert(0, "[");
    expected_b.replace(expected_b.size() - 1, 1, "]");

    // Consume all the responses we should be expecting back.
    for (unsigned int i = 0; i < push_count / 2; i++) {
        BinprotResponse rsp;
        c1->recvResponse(rsp);
        ASSERT_TRUE(rsp.isSuccess());
        c2->recvResponse(rsp);
        ASSERT_TRUE(rsp.isSuccess());
    }

    // Validate correct data was written.
    validate_json_document("a", expected_a);
    validate_json_document("b", expected_b);

    userConnection->remove("a", Vbid{0});
    userConnection->remove("b", Vbid{0});
}
