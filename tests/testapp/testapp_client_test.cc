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
#include <fmt/format.h>
#include <mcbp/codec/frameinfo.h>
#include <platform/random.h>
#include <xattr/blob.h>

void TestappClientTest::SetUpTestCase() {
    TestappTest::SetUpTestCase();
    createUserConnection = true;
}

void TestappXattrClientTest::SetUpTestCase() {
    TestappTest::SetUpTestCase();
    createUserConnection = true;
}

bool TestappClientTest::isTlsEnabled() const {
    switch (GetParam()) {
    case TransportProtocols::McbpPlain:
        return false;
    case TransportProtocols::McbpSsl:
        return true;
    }
    throw std::logic_error("isTlsEnabled(): unknown transport");
}

size_t TestappClientTest::populateData(double limit) {
    Document doc;
    doc.value.resize(10 * 1024 * 1024);
    cb::RandomGenerator rand;
    rand.getBytes(doc.value.data(), doc.value.size());
    bool done = false;
    int total = 0;
    constexpr int batch = 10;
    do {
        for (int ii = 0; ii < batch; ++ii) {
            doc.info.id = fmt::format("mykey-{}", total + ii);
            userConnection->mutate(doc, Vbid(0), MutationType::Set);
        }
        total += batch;
        adminConnection->executeInBucket(
                bucketName, [&done, limit](auto& conn) {
                    conn.stats(
                            [&done, limit](auto& k, auto& v) {
                                if (k == "vb_active_perc_mem_resident") {
                                    done = std::stod(v) < limit;
                                }
                            },
                            "");
                });
    } while (!done);
    return total;
}

void TestappClientTest::rerunAccessScanner() {
    auto num_runs = waitForSnoozedAccessScanner();
    // we're about to schedule 1 access scanner per shard, so fetch
    // the current number of runs and add the number of shards so that
    // we know how many runs we need to wait for
    auto needed_runs = getAggregatedAccessScannerCounts() + getNumShards();

    BinprotResponse response;
    adminConnection->executeInBucket(bucketName, [&response](auto& conn) {
        response = conn.execute(BinprotSetParamCommand{
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "access_scanner_run",
                "true"});
    });
    if (!response.isSuccess()) {
        throw ConnectionError("rerunAccessScanner: SetParam failed", response);
    }

    while (num_runs == waitForSnoozedAccessScanner() ||
           getAggregatedAccessScannerCounts() < needed_runs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

nlohmann::json TestappClientTest::getAccessScannerStats() {
    nlohmann::json stats;
    auto key = fmt::format("ep_tasks:tasks:{}", bucketName);
    adminConnection->executeInBucket(bucketName, [&stats, &key](auto& conn) {
        conn.stats(
                [&stats, &key](auto& k, auto& v) {
                    if (k == key) {
                        stats = nlohmann::json::parse(v);
                    }
                },
                "tasks");
    });

    for (const auto& task : stats) {
        if (task.contains("name") && task["name"] == "AccessScannerVisitor") {
            return {};
        }
    }

    for (const auto& task : stats) {
        if (task.contains("name") && task["name"] == "AccessScanner") {
            return task;
        }
    }
    return {};
}

int TestappClientTest::waitForSnoozedAccessScanner() {
    do {
        auto stats = getAccessScannerStats();
        if (!stats.empty() && stats.value("state", "") == "SNOOZED") {
            return stats.value("num_runs", 0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);
}

std::pair<int, int> TestappClientTest::getNumAccessScannerCounts() {
    int skips = 0;
    int runs = 0;
    adminConnection->executeInBucket(bucketName, [&skips, &runs](auto& conn) {
        conn.stats(
                [&skips, &runs](auto& k, auto& v) {
                    if (k == "ep_num_access_scanner_skips") {
                        skips = std::stoi(v);
                    } else if (k == "ep_num_access_scanner_runs") {
                        runs = std::stoi(v);
                    }
                },
                "");
    });
    return {runs, skips};
}

int TestappClientTest::getNumShards() {
    int shards = 0;
    adminConnection->executeInBucket(bucketName, [&shards](auto& conn) {
        conn.stats(
                [&shards](auto& k, auto& v) {
                    if (k == "ep_workload:num_shards") {
                        shards = std::stoi(v);
                    }
                },
                "workload");
    });
    return shards;
}

void TestappClientTest::verifyAccessLogFiles(int num_shards,
                                             bool encrypted,
                                             bool expect_old) {
    std::filesystem::path directory =
            mcd_env->getTestBucket().getDbPath() / bucketName;

    for (int ii = 0; ii < num_shards; ++ii) {
        auto prefix = fmt::format("access_log.{}", ii);
        if (encrypted) {
            // The unencrypted versions should not exist!
            EXPECT_FALSE(exists(directory / prefix))
                    << "Did not expect access log file to exist at "
                    << (directory / prefix).string();
            EXPECT_FALSE(exists(directory / (prefix + ".old")))
                    << "Did not expect access log file to exist at "
                    << (directory / (prefix + ".old")).string();

            // The encrypted version should exist
            EXPECT_TRUE(exists(directory / (prefix + ".cef")))
                    << "Expected access log file to exist at "
                    << (directory / (prefix + ".cef")).string();

            if (expect_old) {
                EXPECT_TRUE(exists(directory / (prefix + ".old.cef")))
                        << "Expected access log file to exist at "
                        << (directory / (prefix + ".old.cef")).string();
            } else {
                EXPECT_FALSE(exists(directory / (prefix + ".old.cef")))
                        << "Did not expect access log file to exist at "
                        << (directory / (prefix + ".old.cef")).string();
            }
        } else {
            // The encrypted versions should not exist!
            EXPECT_FALSE(exists(directory / (prefix + ".cef")))
                    << "Did not expect access log file to exist at "
                    << (directory / (prefix + ".cef")).string();
            EXPECT_FALSE(exists(directory / (prefix + ".old.cef")))
                    << "Did not expect access log file to exist at "
                    << (directory / (prefix + ".old.cef")).string();

            // The unencrypted version should exist
            EXPECT_TRUE(exists(directory / prefix))
                    << "Expected access log file to exist at "
                    << (directory / prefix).string();

            if (expect_old) {
                EXPECT_TRUE(exists(directory / (prefix + ".old")))
                        << "Expected access log file to exist at "
                        << (directory / (prefix + ".old")).string();
            } else {
                EXPECT_FALSE(exists(directory / (prefix + ".old")))
                        << "Did not expect access log file to exist at "
                        << (directory / (prefix + ".old")).string();
            }
        }
    }
}

void TestappClientTest::verifyNoAccessLogFiles(int num_shards) {
    std::filesystem::path directory =
            mcd_env->getTestBucket().getDbPath() / bucketName;

    for (int ii = 0; ii < num_shards; ++ii) {
        auto prefix = fmt::format("access_log.{}", ii);
        for (const auto* ext : {".cef", ".old.cef", "", ".old"}) {
            EXPECT_FALSE(exists(directory / (prefix + ext)))
                    << "Did not expect access log file to exist at "
                    << (directory / (prefix + ext)).string();
        }
    }
}

void TestappXattrClientTest::setBodyAndXattr(
        const std::string& startValue,
        std::initializer_list<std::pair<const std::string, std::string>>
                xattrList,
        bool compressValue) {
    document.info.flags = 0xcaffee;
    document.info.id = name;

    // Combine the body and Extended Attribute into a single value -
    // this allows us to store already compressed documents which
    // have XATTRs.
    document.info.cas = 10; // withMeta requires a non-zero CAS.
    document.info.datatype = cb::mcbp::Datatype::Xattr;
    document.value = cb::xattr::make_wire_encoded_string(startValue, xattrList);
    if (compressValue) {
        // Compress the complete body.
        document.compress();
    }

    // As we are using setWithMeta; we need to explicitly set JSON
    // if our connection supports it.
    if (hasJSONSupport() == ClientJSONSupport::Yes) {
        document.info.datatype = cb::mcbp::Datatype(
                int(document.info.datatype) | int(cb::mcbp::Datatype::JSON));
    }
    userConnection->mutateWithMeta(document,
                                   Vbid(0),
                                   cb::mcbp::cas::Wildcard,
                                   /*seqno*/ 1,
                                   FORCE_WITH_META_OP | REGENERATE_CAS |
                                           SKIP_CONFLICT_RESOLUTION_FLAG);
}

void TestappXattrClientTest::setBodyAndXattr(
        const std::string& value,
        std::initializer_list<std::pair<const std::string, std::string>>
                xattrList) {
    setBodyAndXattr(
            value, xattrList, hasSnappySupport() == ClientSnappySupport::Yes);
}

BinprotSubdocResponse TestappXattrClientTest::subdoc(
        cb::mcbp::ClientOpcode opcode,
        const std::string& key,
        const std::string& path,
        const std::string& value,
        cb::mcbp::subdoc::PathFlag flag,
        cb::mcbp::subdoc::DocFlag docFlag,
        const std::optional<cb::durability::Requirements>& durReqs) {
    BinprotSubdocCommand cmd;
    cmd.setOp(opcode);
    cmd.setKey(key);
    cmd.setPath(path);
    cmd.setValue(value);
    cmd.addPathFlags(flag);
    cmd.addDocFlags(docFlag);

    if (durReqs) {
        cmd.addFrameInfo(cb::mcbp::request::DurabilityFrameInfo(
                durReqs->getLevel(), durReqs->getTimeout()));
    }

    return BinprotSubdocResponse(userConnection->execute(cmd));
}

BinprotSubdocResponse TestappXattrClientTest::subdocMultiMutation(
        const BinprotSubdocMultiMutationCommand& cmd) {
    return BinprotSubdocResponse(userConnection->execute(cmd));
}

cb::mcbp::Status TestappXattrClientTest::xattr_upsert(
        const std::string& path,
        const std::string& value) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       path,
                       value,
                       cb::mcbp::subdoc::PathFlag::XattrPath |
                               cb::mcbp::subdoc::PathFlag::Mkdir_p,
                       cb::mcbp::subdoc::DocFlag::Mkdoc);
    return resp.getStatus();
}

void TestappXattrClientTest::SetUp() {
    TestappTest::SetUp();

    mcd_env->getTestBucket().setXattrEnabled(
            *adminConnection,
            bucketName,
            ::testing::get<1>(GetParam()) == XattrSupport::Yes);
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        xattrOperationStatus = cb::mcbp::Status::NotSupported;
    }

    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 0;
    document.value = memcached_cfg.dump();
    if (hasJSONSupport() == ClientJSONSupport::Yes) {
        document.info.datatype = cb::mcbp::Datatype::JSON;
    }

    // If the client has Snappy support, enable passive compression
    // on the bucket and compress our initial document we work with.
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        setCompressionMode("passive");
        document.compress();
    }

    setMinCompressionRatio(0);
}

void TestappXattrClientTest::createXattr(const std::string& path,
                                         const std::string& value,
                                         bool macro) {
    runCreateXattr(path, value, macro, xattrOperationStatus);
}

bool TestappXattrClientTest::isTlsEnabled() const {
    switch (::testing::get<0>(GetParam())) {
    case TransportProtocols::McbpPlain:
        return false;
    case TransportProtocols::McbpSsl:
        return true;
    }
    throw std::logic_error("isTlsEnabled(): Unknown transport");
}

ClientJSONSupport TestappXattrClientTest::hasJSONSupport() const {
    return ::testing::get<2>(GetParam());
}

ClientSnappySupport TestappXattrClientTest::hasSnappySupport() const {
    return ::testing::get<3>(GetParam());
}

cb::mcbp::Datatype TestappXattrClientTest::expectedJSONDatatype() const {
    return hasJSONSupport() == ClientJSONSupport::Yes ? cb::mcbp::Datatype::JSON
                                                      : cb::mcbp::Datatype::Raw;
}

cb::mcbp::Datatype TestappXattrClientTest::expectedJSONSnappyDatatype() const {
    cb::mcbp::Datatype datatype = expectedJSONDatatype();
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        datatype = cb::mcbp::Datatype(int(datatype) |
                                      int(cb::mcbp::Datatype::Snappy));
    }
    return datatype;
}

/**
 * Helper function to check datatype is what we expect for this test config,
 * and if datatype says JSON; validate the value /is/ JSON.
 */
::testing::AssertionResult TestappXattrClientTest::hasCorrectDatatype(
        const Document& doc, cb::mcbp::Datatype expectedType) {
    return hasCorrectDatatype(expectedType,
                              doc.info.datatype,
                              {doc.value.data(), doc.value.size()});
}

::testing::AssertionResult TestappXattrClientTest::hasCorrectDatatype(
        cb::mcbp::Datatype expectedType,
        cb::mcbp::Datatype actualDatatype,
        std::string_view value) {
    using namespace cb::mcbp::datatype;
    if (actualDatatype != expectedType) {
        return ::testing::AssertionFailure()
               << "Datatype mismatch - expected:"
               << to_string(protocol_binary_datatype_t(expectedType))
               << " actual:"
               << to_string(protocol_binary_datatype_t(actualDatatype));
    }

    if (actualDatatype == cb::mcbp::Datatype::JSON) {
        if (!isJSON(value)) {
            return ::testing::AssertionFailure()
                   << "JSON validation failed for response data:'" << value
                   << "''";
        }
    }
    return ::testing::AssertionSuccess();
}

void TestappXattrClientTest::runCreateXattr(std::string path,
                                            std::string value,
                                            bool macro,
                                            cb::mcbp::Status expectedStatus) {
    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictAdd);
    cmd.setKey(name);
    cmd.setPath(std::move(path));
    cmd.setValue(std::move(value));
    if (macro) {
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath |
                         cb::mcbp::subdoc::PathFlag::ExpandMacros |
                         cb::mcbp::subdoc::PathFlag::Mkdir_p);
    } else {
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath |
                         cb::mcbp::subdoc::PathFlag::Mkdir_p);
    }

    auto resp = userConnection->execute(cmd);
    EXPECT_EQ(expectedStatus, resp.getStatus());
}

BinprotSubdocResponse TestappXattrClientTest::runGetXattr(
        std::string path,
        bool deleted,
        cb::mcbp::Status expectedStatus) {
    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
    cmd.setKey(name);
    cmd.setPath(std::move(path));
    if (deleted) {
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath);
        cmd.addDocFlags(cb::mcbp::subdoc::DocFlag::AccessDeleted);
    } else {
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath);
    }

    auto resp = BinprotSubdocResponse(userConnection->execute(cmd));
    auto status = resp.getStatus();
    if (deleted && status == cb::mcbp::Status::SubdocSuccessDeleted) {
        status = cb::mcbp::Status::Success;
    }

    if (status != expectedStatus) {
        throw ConnectionError("runGetXattr() failed: ", resp);
    }
    return resp;
}

BinprotSubdocResponse TestappXattrClientTest::getXattr(const std::string& path,
                                                       bool deleted) {
    return runGetXattr(path, deleted, xattrOperationStatus);
}

std::ostream& operator<<(std::ostream& os, const XattrSupport& xattrSupport) {
    os << to_string(xattrSupport);
    return os;
}

std::string to_string(const XattrSupport& xattrSupport) {
    switch (xattrSupport) {
    case XattrSupport::Yes:
        return "XattrYes";
    case XattrSupport::No:
        return "XattrNo";
    }
    throw std::logic_error("Unknown xattr support");
}

size_t TestappClientTest::get_cmd_counter(std::string_view name) {
    std::optional<std::size_t> value;
    userConnection->stats([&value, name](const auto& key, const auto& val) {
        if (key == name) {
            value = std::stoi(val);
        }
    });

    if (!value.has_value()) {
        throw std::runtime_error(fmt::format(
                "TestappClientTest::get_cmd_counter: No entry for: {}", name));
    }
    return value.value();
}

std::string PrintToStringCombinedName::operator()(
        const ::testing::TestParamInfo<::testing::tuple<TransportProtocols,
                                                        XattrSupport,
                                                        ClientJSONSupport,
                                                        ClientSnappySupport>>&
                info) const {
    std::string rv = to_string(::testing::get<0>(info.param)) + "_" +
                     to_string(::testing::get<1>(info.param)) + "_" +
                     to_string(::testing::get<2>(info.param)) + "_" +
                     to_string(::testing::get<3>(info.param));
    return rv;
}
