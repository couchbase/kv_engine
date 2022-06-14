/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/EventBase.h>
#include <protocol/connection/async_client_connection.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <serverless/config.h>
#include <deque>
#include <filesystem>
#include <vector>

namespace cb::test {
constexpr size_t MaxConnectionsPerBucket = 16;

std::unique_ptr<Cluster> ServerlessTest::cluster;

void ServerlessTest::StartCluster() {
    cluster = Cluster::create(
            3, {}, [](std::string_view, nlohmann::json& config) {
                config["deployment_model"] = "serverless";
                auto file =
                        std::filesystem::path{
                                config["root"].get<std::string>()} /
                        "etc" / "couchbase" / "kv" / "serverless" /
                        "configuration.json";
                create_directories(file.parent_path());
                nlohmann::json json;
                json["max_connections_per_bucket"] = MaxConnectionsPerBucket;
                FILE* fp = fopen(file.generic_string().c_str(), "w");
                fprintf(fp, "%s\n", json.dump(2).c_str());
                fclose(fp);
            });
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void ServerlessTest::SetUpTestCase() {
    if (!cluster) {
        std::cerr << "Cluster not running" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    try {
        for (int ii = 0; ii < 5; ++ii) {
            const auto name = "bucket-" + std::to_string(ii);
            std::string rbac = R"({
"buckets": {
  "bucket-@": {
    "privileges": [
      "Read",
      "SimpleStats",
      "Insert",
      "Delete",
      "Upsert",
      "DcpProducer",
      "DcpStream"
    ]
  }
},
"privileges": [],
"domain": "external"
})";
            rbac[rbac.find('@')] = '0' + ii;
            cluster->getAuthProviderService().upsertUser(
                    {name, name, nlohmann::json::parse(rbac)});

            auto bucket = cluster->createBucket(
                    name, {{"replicas", 2}, {"max_vbuckets", 8}});
            if (!bucket) {
                throw std::runtime_error("Failed to create bucket: " + name);
            }

            // Running under sanitizers slow down the system a lot so
            // lets use a lower limit to ensure that operations actually
            // gets throttled.
            bucket->setThrottleLimit(folly::kIsSanitize ? 256 : 1024);

            // @todo add collections and scopes
        }
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void ServerlessTest::TearDownTestCase() {
    // @todo iterate over the buckets and delete all of them
}

void ServerlessTest::ShutdownCluster() {
    cluster.reset();
}

void ServerlessTest::SetUp() {
    Test::SetUp();
}

void ServerlessTest::TearDown() {
    Test::TearDown();
}

TEST_F(ServerlessTest, TestDefaultThrottleLimit) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto bucket = cluster->createBucket("TestDefaultThrottleLimit",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    if (!bucket) {
        throw std::runtime_error(
                "Failed to create bucket: TestDefaultThrottleLimit");
    }
    std::size_t limit;
    admin->stats(
            [&limit](const auto& k, const auto& v) {
                nlohmann::json json = nlohmann::json::parse(v);
                limit = json["throttle_limit"].get<size_t>();
            },
            "bucket_details TestDefaultThrottleLimit");
    cluster->deleteBucket("TestDefaultThrottleLimit");
    EXPECT_EQ(cb::serverless::DefaultThrottleLimit, limit);
}

TEST_F(ServerlessTest, MaxConnectionPerBucket) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto getNumClients = [&admin]() -> std::size_t {
        size_t num_clients = 0;
        admin->stats(
                [&num_clients](const auto& k, const auto& v) {
                    nlohmann::json json = nlohmann::json::parse(v);
                    num_clients = json["clients"].get<size_t>();
                },
                "bucket_details bucket-0");
        return num_clients;
    };

    std::deque<std::unique_ptr<MemcachedConnection>> connections;
    bool done = false;
    BinprotResponse rsp;
    do {
        auto conn = cluster->getConnection(0);
        conn->authenticate("bucket-0", "bucket-0");
        rsp = conn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
        if (rsp.isSuccess()) {
            connections.emplace_back(std::move(conn));
            ASSERT_LE(getNumClients(), MaxConnectionsPerBucket);
        } else {
            ASSERT_EQ(cb::mcbp::Status::RateLimitedMaxConnections,
                      rsp.getStatus());
            // Without XERROR E2BIG should be returned
            conn->setXerrorSupport(false);
            rsp = conn->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::SelectBucket, "bucket-0"});
            ASSERT_FALSE(rsp.isSuccess());
            ASSERT_EQ(cb::mcbp::Status::E2big, rsp.getStatus());
            done = true;
        }
    } while (!done);

    // But we should be allowed to connect internal users
    for (int ii = 0; ii < 5; ++ii) {
        auto conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("bucket-0");
        connections.emplace_back(std::move(conn));
    }
    EXPECT_EQ(MaxConnectionsPerBucket + 5, getNumClients());
}

TEST_F(ServerlessTest, OpsAreThrottled) {
    auto func = [this](const std::string& name) {
        auto conn = cluster->getConnection(0);
        conn->authenticate(name, name);
        conn->selectBucket(name);
        conn->setReadTimeout(std::chrono::seconds{3});

        Document document;
        document.info.id = "OpsAreThrottled";
        document.value = "This is the awesome document";

        // store a document
        conn->mutate(document, Vbid{0}, MutationType::Set);

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < 4096; ++i) { // Run 4k mutations
            conn->get(document.info.id, Vbid{0});
        }
        auto end = std::chrono::steady_clock::now();
        EXPECT_LT(
                std::chrono::seconds{2},
                std::chrono::duration_cast<std::chrono::seconds>(end - start));

        nlohmann::json stats;
        conn->authenticate("@admin", "password");
        conn->stats(
                [&stats](const auto& k, const auto& v) {
                    stats = nlohmann::json::parse(v);
                },
                std::string{"bucket_details "} + name);
        ASSERT_FALSE(stats.empty());
        ASSERT_LE(3, stats["num_throttled"]);
        // it's hard to compare this with a "real value"; but it should at
        // least be non-zero
        ASSERT_NE(0, stats["throttle_wait_time"]);
    };

    std::vector<std::thread> threads;
    for (int ii = 0; ii < 5; ++ii) {
        threads.emplace_back(
                std::thread{[func, name = "bucket-" + std::to_string(ii)]() {
                    func(name);
                }});
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(ServerlessTest, ComputeUnitsReported) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("bucket-0", "bucket-0");
    conn->selectBucket("bucket-0");
    conn->setFeature(cb::mcbp::Feature::ReportComputeUnitUsage, true);
    conn->setReadTimeout(std::chrono::seconds{3});

    DocumentInfo info;
    info.id = "ComputeUnitsReported";

    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("This is a document");
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    ASSERT_TRUE(rsp.isSuccess());

    auto rcu = rsp.getReadComputeUnits();
    auto wcu = rsp.getWriteComputeUnits();

    ASSERT_FALSE(rcu.has_value()) << "mutate should not use RCU";
    ASSERT_TRUE(wcu.has_value()) << "mutate should use WCU";
    ASSERT_EQ(1, *wcu) << "The value should be 1 WCU";
    wcu.reset();

    rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, info.id});

    rcu = rsp.getReadComputeUnits();
    wcu = rsp.getWriteComputeUnits();

    ASSERT_TRUE(rcu.has_value()) << "get should use RCU";
    ASSERT_FALSE(wcu.has_value()) << "get should not use WCU";
    ASSERT_EQ(1, *rcu) << "The value should be 1 RCU";
}

TEST_F(ServerlessTest, AllConnectionsAreMetered) {
    auto admin = cluster->getConnection(0);
    auto conn = admin->clone();
    admin->authenticate("@admin", "password");
    conn->authenticate("bucket-0", "bucket-0");
    admin->selectBucket("bucket-0");
    conn->selectBucket("bucket-0");

    auto getStats = [&admin]() -> nlohmann::json {
        nlohmann::json ret;
        admin->stats([&ret](const auto& k,
                            const auto& v) { ret = nlohmann::json::parse(v); },
                     "bucket_details bucket-0");
        return ret;
    };

    auto readDoc = [stat = getStats](MemcachedConnection& conn) {
        auto initial = stat();
        conn.get("mydoc", Vbid{0});
        auto after = stat();
        EXPECT_EQ(initial["rcu"].get<std::size_t>() + 1,
                  after["rcu"].get<std::size_t>());
        // Read should not update wcu
        EXPECT_EQ(initial["wcu"].get<std::size_t>(),
                  after["wcu"].get<std::size_t>());
    };

    auto writeDoc = [stat = getStats](MemcachedConnection& conn) {
        auto initial = stat();
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
        auto after = stat();
        EXPECT_EQ(initial["wcu"].get<std::size_t>() + 1,
                  after["wcu"].get<std::size_t>());

        // write should not update rcu
        EXPECT_EQ(initial["rcu"].get<std::size_t>(),
                  after["rcu"].get<std::size_t>());
    };

    writeDoc(*admin);
    writeDoc(*conn);
    readDoc(*admin);
    readDoc(*conn);
}

TEST_F(ServerlessTest, StopClientDataIngress) {
    auto writeDoc = [](MemcachedConnection& conn) {
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    };

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("bucket-0");

    auto bucket0 = admin->clone();
    bucket0->authenticate("bucket-0", "bucket-0");
    bucket0->selectBucket("bucket-0");

    // store a document
    writeDoc(*bucket0);

    // Disable client ingress
    auto rsp =
            admin->execute(SetBucketDataLimitExceededCommand{"bucket-0", true});
    EXPECT_TRUE(rsp.isSuccess());

    // fail to store a document
    try {
        writeDoc(*bucket0);
        FAIL() << "Should not be able to store a document";
    } catch (ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::BucketSizeLimitExceeded, error.getReason());
    }
    // Succeeds to store a document in bucket-1
    auto bucket1 = admin->clone();
    bucket1->authenticate("bucket-1", "bucket-1");
    bucket1->selectBucket("bucket-1");
    writeDoc(*bucket1);

    // enable client ingress
    rsp = admin->execute(SetBucketDataLimitExceededCommand{"bucket-0", false});
    EXPECT_TRUE(rsp.isSuccess());

    // succeed to store a document
    writeDoc(*bucket0);
}

TEST_F(ServerlessTest, MemcachedBucketNotSupported) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    auto rsp = admin->execute(BinprotCreateBucketCommand{
            "NotSupported", "default_engine.so", ""});
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

class DcpDrain {
public:
    DcpDrain(const std::string host,
             const std::string port,
             const std::string username,
             const std::string password,
             const std::string bucketname)
        : host(std::move(host)),
          port(std::move(port)),
          username(std::move(username)),
          password(std::move(password)),
          bucketname(std::move(bucketname)),
          connection(AsyncClientConnection::create(base)) {
        connection->setIoErrorListener(
                [this](AsyncClientConnection::Direction dir,
                       const folly::AsyncSocketException& ex) {
                    error = ex.what();
                    base.terminateLoopSoon();
                });
    }

    void drain() {
        connect();
        // We need to use PLAIN auth as we're using the external auth
        // service
        connection->authenticate(username, password, "PLAIN");
        setFeatures();
        selectBucket();

        openDcp();

        setControlMessages();

        sendStreamRequest();

        connection->setFrameReceivedListener(
                [this](const auto& header) { onFrameReceived(header); });

        // Now loop until we're done
        base.loopForever();
        if (error) {
            throw std::runtime_error(*error);
        }
    }

    size_t getNumMutations() const {
        return num_mutations;
    }

    size_t getRcu() const {
        return rcu;
    }

protected:
    void onFrameReceived(const cb::mcbp::Header& header) {
        if (header.isRequest()) {
            onRequest(header.getRequest());
        } else {
            onResponse(header.getResponse());
        }
    }

    void onResponse(const cb::mcbp::Response& res) {
        if (res.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamReq) {
            if (!cb::mcbp::isStatusSuccess(res.getStatus())) {
                error = "onResponse::DcpStreamReq returned error: " +
                        ::to_string(res.getStatus());
                base.terminateLoopSoon();
            }
        } else {
            error = "onResponse(): Unexpected message received: " +
                    res.toJSON(false).dump();
            base.terminateLoopSoon();
        }
    }

    std::size_t calcRcu(std::size_t size) {
        return (size + 1023) / 1024;
    }

    void onRequest(const cb::mcbp::Request& req) {
        if (req.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamEnd) {
            base.terminateLoopSoon();
        }

        switch (req.getClientOpcode()) {
        case cb::mcbp::ClientOpcode::DcpStreamEnd:
            base.terminateLoopSoon();
            break;
        case cb::mcbp::ClientOpcode::DcpNoop:
            handleDcpNoop(req);
            break;
        case cb::mcbp::ClientOpcode::DcpMutation:
            ++num_mutations;
            rcu += calcRcu(req.getValue().size() + req.getKey().size());
            break;
        case cb::mcbp::ClientOpcode::DcpDeletion:
            ++num_deletions;
            rcu += calcRcu(req.getValue().size() + req.getKey().size());
            break;
        case cb::mcbp::ClientOpcode::DcpExpiration:
            ++num_expirations;
            rcu += calcRcu(req.getValue().size() + req.getKey().size());
            break;

        case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
            break;

        case cb::mcbp::ClientOpcode::DcpAddStream:
        case cb::mcbp::ClientOpcode::DcpCloseStream:
        case cb::mcbp::ClientOpcode::DcpStreamReq:
        case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
        case cb::mcbp::ClientOpcode::DcpFlush_Unsupported:
        case cb::mcbp::ClientOpcode::DcpSetVbucketState:
        case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
        case cb::mcbp::ClientOpcode::DcpControl:
        case cb::mcbp::ClientOpcode::DcpSystemEvent:
        case cb::mcbp::ClientOpcode::DcpPrepare:
        case cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged:
        case cb::mcbp::ClientOpcode::DcpCommit:
        case cb::mcbp::ClientOpcode::DcpAbort:
        case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
        case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
            // fallthrough
        default:
            error = "Received unexpected message: " + req.toJSON(false).dump();
            base.terminateLoopSoon();
        }
    }

    void connect() {
        connection->setConnectListener([this]() { base.terminateLoopSoon(); });
        connection->connect(host, port);
        base.loopForever();
        if (error) {
            throw std::runtime_error("DcpDrain::connect: " + *error);
        }
    }

    void setFeatures() {
        using cb::mcbp::Feature;

        const std::vector<Feature> requested{{Feature::MUTATION_SEQNO,
                                              Feature::XATTR,
                                              Feature::XERROR,
                                              Feature::SNAPPY,
                                              Feature::JSON,
                                              Feature::Tracing,
                                              Feature::Collections,
                                              Feature::ReportComputeUnitUsage}};

        auto enabled = connection->hello("serverless", "MeterDCP", requested);
        if (enabled != requested) {
            throw std::runtime_error(
                    "DcpDrain::setFeatures(): Failed to enable the "
                    "requested "
                    "features");
        }
    }

    void selectBucket() {
        const auto rsp = connection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SelectBucket, bucketname});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    "DcpDrain::selectBucket: " + ::to_string(rsp.getStatus()) +
                    " " + rsp.getDataString());
        }
    }

    void openDcp() {
        const auto rsp = connection->execute(BinprotDcpOpenCommand{
                "MeterDcpName", cb::mcbp::request::DcpOpenPayload::Producer});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    "DcpDrain::openDcp: " + ::to_string(rsp.getStatus()) + " " +
                    rsp.getDataString());
        }
    }

    void setControlMessages() {
        auto setCtrlMessage = [this](const std::string& key,
                                     const std::string& value) {
            const auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::DcpControl, key, value});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        };
        std::vector<std::pair<std::string, std::string>> controls{
                {"set_priority", "high"},
                {"supports_cursor_dropping_vulcan", "true"},
                {"supports_hifi_MFU", "true"},
                {"send_stream_end_on_client_close_stream", "true"},
                {"enable_expiry_opcode", "true"},
                {"set_noop_interval", "1"},
                {"enable_noop", "true"}};
        for (const auto& [k, v] : controls) {
            setCtrlMessage(k, v);
        }
    }

    void sendStreamRequest() {
        BinprotDcpStreamRequestCommand cmd;
        cmd.setDcpFlags(DCP_ADD_STREAM_FLAG_TO_LATEST);
        cmd.setDcpReserved(0);
        cmd.setDcpStartSeqno(0);
        cmd.setDcpEndSeqno(~0);
        cmd.setDcpVbucketUuid(0);
        cmd.setDcpSnapStartSeqno(0);
        cmd.setDcpSnapEndSeqno(0);
        cmd.setVBucket(Vbid(0));

        connection->send(cmd);
    }

    void handleDcpNoop(const cb::mcbp::Request& header) {
        cb::mcbp::Response resp = {};
        resp.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.setOpaque(header.getOpaque());
        resp.setOpcode(header.getClientOpcode());

        auto iob = folly::IOBuf::createCombined(sizeof(resp));
        std::memcpy(iob->writableData(), &resp, sizeof(resp));
        iob->append(sizeof(resp));
        connection->send(std::move(iob));
    }

    const std::string host;
    const std::string port;
    const std::string username;
    const std::string password;
    const std::string bucketname;
    folly::EventBase base;
    std::unique_ptr<AsyncClientConnection> connection;
    std::optional<std::string> error;
    std::size_t num_mutations = 0;
    std::size_t num_deletions = 0;
    std::size_t num_expirations = 0;
    std::size_t rcu = 0;
};

/// Test that we meter all operations according to their spec (well, there
/// is no spec at the moment ;)
///
/// To make sure that we don't sneak in a new opcode without considering if
/// it should be metered or not the code loops over all available opcodes
/// and call a function which performs a switch (so the compiler will barf
/// out if we don't handle the case). By doing so one must explicitly think
/// if the new opcode needs to be metered or not.
TEST_F(ServerlessTest, OpsMetered) {
    using namespace cb::mcbp;
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");

    auto executeWithExpectedCU = [&admin](std::function<void()> func,
                                          size_t rcu,
                                          size_t wcu) {
        nlohmann::json before;
        admin->stats([&before](auto k,
                               auto v) { before = nlohmann::json::parse(v); },
                     "bucket_details bucket-0");
        func();
        nlohmann::json after;
        admin->stats(
                [&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details bucket-0");
        EXPECT_EQ(rcu,
                  after["rcu"].get<size_t>() - before["rcu"].get<size_t>());
        EXPECT_EQ(wcu,
                  after["wcu"].get<size_t>() - before["wcu"].get<size_t>());
    };

    auto testOpcode = [&executeWithExpectedCU](MemcachedConnection& conn,
                                               ClientOpcode opcode) {
        auto createDocument = [&conn](std::string key,
                                      std::string value,
                                      MutationType op = MutationType::Set,
                                      uint64_t cas = 0) {
            Document doc;
            doc.info.id = std::move(key);
            doc.info.cas = cas;
            doc.value = std::move(value);
            return conn.mutate(doc, Vbid{0}, op);
        };

        BinprotResponse rsp;
        switch (opcode) {
        case ClientOpcode::Flush:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq:
        case ClientOpcode::Getq:
        case ClientOpcode::Getk:
        case ClientOpcode::Getkq:
        case ClientOpcode::Gatq:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::GetqMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelqWithMeta:
        case ClientOpcode::Rget_Unsupported:
        case ClientOpcode::Rset_Unsupported:
        case ClientOpcode::Rsetq_Unsupported:
        case ClientOpcode::Rappend_Unsupported:
        case ClientOpcode::Rappendq_Unsupported:
        case ClientOpcode::Rprepend_Unsupported:
        case ClientOpcode::Rprependq_Unsupported:
        case ClientOpcode::Rdelete_Unsupported:
        case ClientOpcode::Rdeleteq_Unsupported:
        case ClientOpcode::Rincr_Unsupported:
        case ClientOpcode::Rincrq_Unsupported:
        case ClientOpcode::Rdecr_Unsupported:
        case ClientOpcode::Rdecrq_Unsupported:
        case ClientOpcode::TapConnect_Unsupported:
        case ClientOpcode::TapMutation_Unsupported:
        case ClientOpcode::TapDelete_Unsupported:
        case ClientOpcode::TapFlush_Unsupported:
        case ClientOpcode::TapOpaque_Unsupported:
        case ClientOpcode::TapVbucketSet_Unsupported:
        case ClientOpcode::TapCheckpointStart_Unsupported:
        case ClientOpcode::TapCheckpointEnd_Unsupported:
        case ClientOpcode::ResetReplicationChain_Unsupported:
        case ClientOpcode::SnapshotVbStates_Unsupported:
        case ClientOpcode::VbucketBatchCount_Unsupported:
        case ClientOpcode::NotifyVbucketUpdate_Unsupported:
        case ClientOpcode::ChangeVbFilter_Unsupported:
        case ClientOpcode::CheckpointPersistence_Unsupported:
        case ClientOpcode::SetDriftCounterState_Unsupported:
        case ClientOpcode::GetAdjustedTime_Unsupported:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DeregisterTapClient_Unsupported:
            // Just verify that we don't support them
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_EQ(Status::NotSupported, rsp.getStatus()) << opcode;

            // SASL commands aren't being metered (and not necessairly bound
            // to a bucket so its hard to check as we don't know where it'll
        // go
        case ClientOpcode::SaslListMechs:
        case ClientOpcode::SaslAuth:
        case ClientOpcode::SaslStep:
            break;

        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::SelectBucket:
            break;

            // Quit close the connection so its hard to test (and it would
        // be weird if someone updated the code to start collecting data)
        case ClientOpcode::Quit:
            executeWithExpectedCU(
                    [&conn]() {
                        conn.sendCommand(
                                BinprotGenericCommand{ClientOpcode::Quit});
                        // Allow some time for the connection to disconnect
                        std::this_thread::sleep_for(
                                std::chrono::milliseconds{500});
                    },
                    0,
                    0);
            conn.reconnect();
            conn.authenticate("@admin", "password");
            conn.selectBucket("bucket-0");
            conn.setFeature(cb::mcbp::Feature::ReportComputeUnitUsage, true);
            conn.setReadTimeout(std::chrono::seconds{3});
            break;

        case ClientOpcode::ListBuckets:
        case ClientOpcode::Version:
        case ClientOpcode::Noop:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::GetFailoverLog:
        case ClientOpcode::CollectionsGetManifest:
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_TRUE(rsp.isSuccess()) << opcode;
            EXPECT_FALSE(rsp.getReadComputeUnits()) << opcode;
            EXPECT_FALSE(rsp.getWriteComputeUnits()) << opcode;
            break;

        case ClientOpcode::Get:
            // Get of a non-existing document should not cost anything
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::Get"});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());

            createDocument("ClientOpcode::Get", "Hello World");
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::Get"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Set:
            // Writing a document should cost
            executeWithExpectedCU(
                    [&createDocument]() {
                        createDocument("ClientOpcode::Set",
                                       "Hello",
                                       MutationType::Set);
                    },
                    0,
                    1);
            break;
        case ClientOpcode::Add:
            // Writing a document should cost
            executeWithExpectedCU(
                    [&createDocument]() {
                        createDocument("ClientOpcode::Add",
                                       "Hello",
                                       MutationType::Add);
                    },
                    0,
                    1);
            // add failure shouldn't cost anything
            executeWithExpectedCU(
                    [&createDocument]() {
                        try {
                            createDocument("ClientOpcode::Add",
                                           "Hello",
                                           MutationType::Add);
                            FAIL() << "Add of existing document should fail";
                        } catch (const ConnectionError&) {
                        }
                    },
                    0,
                    0);
            break;
        case ClientOpcode::Replace:
            // Replace failure shouldn't cost anything
            executeWithExpectedCU(
                    [&createDocument]() {
                        try {
                            createDocument("ClientOpcode::Replace",
                                           "Hello",
                                           MutationType::Replace);
                            FAIL() << "Add of existing document should fail";
                        } catch (const ConnectionError&) {
                        }
                    },
                    0,
                    0);
            createDocument("ClientOpcode::Replace", "Hello");
            executeWithExpectedCU(
                    [&createDocument]() {
                        createDocument("ClientOpcode::Replace",
                                       "World",
                                       MutationType::Replace);
                    },
                    0,
                    1);
            break;
        case ClientOpcode::Delete:
            // Delete of a document should cost
            createDocument("ClientOpcode::Delete", "Hello");
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::Delete"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            EXPECT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            break;
        case ClientOpcode::Increment:
            createDocument("ClientOpcode::Increment", "foo");
            executeWithExpectedCU(
                    [&conn]() {
                        try {
                            conn.increment("ClientOpcode::Increment", 1);
                            FAIL() << "Document not a number";
                        } catch (const ConnectionError&) {
                        }
                    },
                    1,
                    0);
            createDocument("ClientOpcode::Increment", "0");
            executeWithExpectedCU(
                    [&conn]() { conn.increment("ClientOpcode::Increment", 1); },
                    1,
                    1);
            break;
        case ClientOpcode::Decrement:
            createDocument("ClientOpcode::Decrement", "foo");
            executeWithExpectedCU(
                    [&conn]() {
                        try {
                            conn.decrement("ClientOpcode::Decrement", 1);
                            FAIL() << "Document not a number";
                        } catch (const ConnectionError&) {
                        }
                    },
                    1,
                    0);
            createDocument("ClientOpcode::Decrement", "1");
            executeWithExpectedCU(
                    [&conn]() { conn.decrement("ClientOpcode::Decrement", 1); },
                    1,
                    1);
            break;
        case ClientOpcode::Append:
            // Append on non-existing document should fail and be free
            executeWithExpectedCU(
                    [&conn]() {
                        auto r = conn.execute(
                                BinprotGenericCommand{ClientOpcode::Append,
                                                      "ClientOpcode::Append",
                                                      "world"});
                        EXPECT_FALSE(r.isSuccess());
                        EXPECT_FALSE(r.getReadComputeUnits());
                        EXPECT_FALSE(r.getWriteComputeUnits());
                    },
                    0,
                    0);
            createDocument("ClientOpcode::Append", "hello");
            rsp = conn.execute(BinprotGenericCommand{
                    ClientOpcode::Append, "ClientOpcode::Append", "world"});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Prepend:
            // Append on non-existing document should fail and be free
            executeWithExpectedCU(
                    [&conn]() {
                        auto r = conn.execute(
                                BinprotGenericCommand{ClientOpcode::Prepend,
                                                      "ClientOpcode::Prepend",
                                                      "hello"});
                        EXPECT_FALSE(r.isSuccess());
                        EXPECT_FALSE(r.getReadComputeUnits());
                        EXPECT_FALSE(r.getWriteComputeUnits());
                    },
                    0,
                    0);
            createDocument("ClientOpcode::Prepend", "world");
            rsp = conn.execute(BinprotGenericCommand{
                    ClientOpcode::Append, "ClientOpcode::Prepend", "hello"});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Stat:
            executeWithExpectedCU([&conn]() { conn.stats(""); }, 0, 0);
            break;
        case ClientOpcode::Verbosity:
            rsp = conn.execute(BinprotVerbosityCommand{0});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Touch:
            // Touch of non-existing document should fail and is free
            rsp = conn.execute(BinprotTouchCommand{"ClientOpcode::Touch", 0});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            createDocument("ClientOpcode::Touch", "Hello World");
            rsp = conn.execute(BinprotTouchCommand{"ClientOpcode::Touch", 0});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Gat:
            // Touch of non-existing document should fail and is free
            rsp = conn.execute(
                    BinprotGetAndTouchCommand{"ClientOpcode::Gat", Vbid{0}, 0});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            createDocument("ClientOpcode::Gat", "Hello World");
            rsp = conn.execute(
                    BinprotGetAndTouchCommand{"ClientOpcode::Gat", Vbid{0}, 0});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::Hello:
            executeWithExpectedCU(
                    [&conn]() {
                        conn.setFeature(Feature::AltRequestSupport, true);
                    },
                    0,
                    0);
            break;

        case ClientOpcode::GetReplica:
            do {
                auto bucket = cluster->getBucket("bucket-0");
                auto rcon = bucket->getConnection(
                        Vbid(0), vbucket_state_replica, 1);
                rcon->authenticate("@admin", "password");
                rcon->selectBucket("bucket-0");
                rcon->setFeature(cb::mcbp::Feature::ReportComputeUnitUsage,
                                 true);
                rcon->setReadTimeout(std::chrono::seconds{3});
                createDocument("ClientOpcode::GetReplica", "value");
                std::this_thread::sleep_for(std::chrono::milliseconds{100});
                do {
                    rsp = rcon->execute(
                            BinprotGenericCommand{ClientOpcode::GetReplica,
                                                  "ClientOpcode::GetReplica"});
                } while (rsp.getStatus() == Status::KeyEnoent);
                EXPECT_TRUE(rsp.isSuccess());
            } while (false);
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::GetLocked:
            rsp = conn.execute(
                    BinprotGetAndLockCommand{"ClientOpcode::GetLocked"});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            createDocument("ClientOpcode::GetLocked", "value");
            rsp = conn.execute(
                    BinprotGetAndLockCommand{"ClientOpcode::GetLocked"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::UnlockKey:
            do {
                createDocument("ClientOpcode::UnlockKey", "value");
                auto doc = conn.get_and_lock(
                        "ClientOpcode::UnlockKey", Vbid{0}, 15);
                executeWithExpectedCU(
                        [&conn, cas = doc.info.cas]() {
                            conn.unlock(
                                    "ClientOpcode::UnlockKey", Vbid{0}, cas);
                        },
                        0,
                        0);
            } while (false);
            break;
        case ClientOpcode::ObserveSeqno:
            do {
                uint64_t uuid = 0;
                conn.stats(
                        [&uuid](auto k, auto v) {
                            if (k == "vb_0:uuid") {
                                uuid = std::stoull(v);
                            }
                        },
                        "vbucket-details 0");
                rsp = conn.execute(BinprotObserveSeqnoCommand{Vbid{0}, uuid});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadComputeUnits());
                EXPECT_FALSE(rsp.getWriteComputeUnits());
            } while (false);
            break;
        case ClientOpcode::Observe:
            do {
                createDocument("ClientOpcode::Observe", "myvalue");
                std::vector<std::pair<Vbid, std::string>> keys;
                keys.emplace_back(std::make_pair<Vbid, std::string>(
                        Vbid{0}, "ClientOpcode::Observe"));
                rsp = conn.execute(BinprotObserveCommand{std::move(keys)});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadComputeUnits());
                EXPECT_FALSE(rsp.getWriteComputeUnits());
            } while (false);
            break;
        case ClientOpcode::GetMeta:
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::GetMeta"});
            EXPECT_EQ(Status::KeyEnoent, rsp.getStatus()) << rsp.getStatus();
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            createDocument("ClientOpcode::GetMeta", "myvalue");
            rsp = conn.execute(
                    BinprotGenericCommand{opcode, "ClientOpcode::GetMeta"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::GetRandomKey:
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_NE(0, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SeqnoPersistence:
            break;
        case ClientOpcode::GetKeys:
            break;
        case ClientOpcode::CollectionsGetID:
            break;
        case ClientOpcode::CollectionsGetScopeID:
            break;

        case ClientOpcode::SubdocGet:
            createDocument("ClientOpcode::SubdocGet",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocGet", "hello"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocExists:
            createDocument("ClientOpcode::SubdocExists",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocExists", "hello"});
            EXPECT_TRUE(rsp.isSuccess());
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocDictAdd:
        case ClientOpcode::SubdocDictUpsert:
            createDocument("ClientOpcode::SubdocDictAdd",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocDictAdd", "add", "true"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocDelete:
            createDocument("ClientOpcode::SubdocDelete",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocDelete", "hello"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocReplace:
            createDocument("ClientOpcode::SubdocReplace",
                           R"({ "hello" : "world"})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocReplace",
                                         "hello",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocArrayPushLast:
        case ClientOpcode::SubdocArrayPushFirst:
        case ClientOpcode::SubdocArrayAddUnique:
            createDocument("ClientOpcode::SubdocArrayPush",
                           R"({ "hello" : ["world"]})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocArrayPush",
                                         "hello",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocArrayInsert:
            createDocument("ClientOpcode::SubdocArrayPush",
                           R"({ "hello" : ["world"]})");
            rsp = conn.execute(
                    BinprotSubdocCommand{opcode,
                                         "ClientOpcode::SubdocArrayPush",
                                         "hello.[0]",
                                         R"("couchbase")"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocCounter:
            createDocument("ClientOpcode::SubdocCounter",
                           R"({ "counter" : 0})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocCounter", "counter", "1"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            ASSERT_TRUE(rsp.getWriteComputeUnits());
            EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocGetCount:
            createDocument("ClientOpcode::SubdocGetCount",
                           R"({ "array" : [0,1,2,3,4]})");
            rsp = conn.execute(BinprotSubdocCommand{
                    opcode, "ClientOpcode::SubdocGetCount", "array"});
            EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
            ASSERT_TRUE(rsp.getReadComputeUnits());
            EXPECT_EQ(1, *rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;
        case ClientOpcode::SubdocMultiLookup:
            do {
                createDocument(
                        "ClientOpcode::SubdocMultiLookup",
                        R"({ "array" : [0,1,2,3,4], "hello" : "world"})");

                rsp = conn.execute(BinprotSubdocMultiLookupCommand{
                        "ClientOpcode::SubdocMultiLookup",
                        {
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array.[0]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array.[1]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "array[4]"},
                                {ClientOpcode::SubdocGet,
                                 SUBDOC_FLAG_NONE,
                                 "hello"},
                        },
                        ::mcbp::subdoc::doc_flag::None});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                ASSERT_TRUE(rsp.getReadComputeUnits());
                EXPECT_EQ(1, *rsp.getReadComputeUnits());
                EXPECT_FALSE(rsp.getWriteComputeUnits());
            } while (false);
            break;
        case ClientOpcode::SubdocMultiMutation:
            do {
                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocMultiMutation",
                        {
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo",
                                 "true"},
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo1",
                                 "true"},
                                {ClientOpcode::SubdocDictUpsert,
                                 SUBDOC_FLAG_MKDIR_P,
                                 "foo2",
                                 "true"},
                        },
                        ::mcbp::subdoc::doc_flag::Mkdoc});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadComputeUnits());
                ASSERT_TRUE(rsp.getWriteComputeUnits());
                EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            } while (false);
            break;
        case ClientOpcode::SubdocReplaceBodyWithXattr:
            do {
                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocReplaceBodyWithXattr",
                        {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                          SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                          "tnx.op.staged",
                          R"({"couchbase": {"version": "cheshire-cat", "next_version": "unknown"}})"},
                         {cb::mcbp::ClientOpcode::SubdocDictUpsert,
                          SUBDOC_FLAG_NONE,
                          "couchbase",
                          R"({"version": "mad-hatter", "next_version": "cheshire-cat"})"}},
                        ::mcbp::subdoc::doc_flag::Mkdoc});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadComputeUnits());
                ASSERT_TRUE(rsp.getWriteComputeUnits());
                EXPECT_EQ(1, *rsp.getWriteComputeUnits());

                rsp = conn.execute(BinprotSubdocMultiMutationCommand{
                        "ClientOpcode::SubdocReplaceBodyWithXattr",
                        {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
                          SUBDOC_FLAG_XATTR_PATH,
                          "tnx.op.staged",
                          {}},
                         {cb::mcbp::ClientOpcode::SubdocDelete,
                          SUBDOC_FLAG_XATTR_PATH,
                          "tnx.op.staged",
                          {}}},
                        ::mcbp::subdoc::doc_flag::None});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                ASSERT_TRUE(rsp.getReadComputeUnits());
                EXPECT_EQ(1, *rsp.getReadComputeUnits());
                ASSERT_TRUE(rsp.getWriteComputeUnits());
                EXPECT_EQ(1, *rsp.getWriteComputeUnits());
            } while (false);
            break;

        case ClientOpcode::GetCmdTimer:
            rsp = conn.execute(
                    BinprotGetCmdTimerCommand{"bucket-0", ClientOpcode::Noop});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;

        case ClientOpcode::GetErrorMap:
            rsp = conn.execute(BinprotGetErrorMapCommand{});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadComputeUnits());
            EXPECT_FALSE(rsp.getWriteComputeUnits());
            break;

            // MetaWrite ops require meta write privilege... probably not
        // something we'll need initially...
        case ClientOpcode::SetWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::ReturnMeta:
            break;

            // @todo create a test case for range scans
        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::RangeScanContinue:
        case ClientOpcode::RangeScanCancel:
            break;

            // We need a special unit test for DCP
        case ClientOpcode::DcpOpen:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::DcpPrepare:
        case ClientOpcode::DcpSeqnoAcknowledged:
        case ClientOpcode::DcpCommit:
        case ClientOpcode::DcpAbort:
        case ClientOpcode::DcpSeqnoAdvanced:
        case ClientOpcode::DcpOsoSnapshot:
            break;

            // The following are "internal"/advanced commands not intended
        // for the average users. We may add unit tests at a later time for
        // them
        case ClientOpcode::IoctlGet:
        case ClientOpcode::IoctlSet:
        case ClientOpcode::ConfigValidate:
        case ClientOpcode::ConfigReload:
        case ClientOpcode::AuditPut:
        case ClientOpcode::AuditConfigReload:
        case ClientOpcode::Shutdown:
        case ClientOpcode::SetBucketComputeUnitThrottleLimits:
        case ClientOpcode::SetBucketDataLimitExceeded:
        case ClientOpcode::SetVbucket:
        case ClientOpcode::GetVbucket:
        case ClientOpcode::DelVbucket:
        case ClientOpcode::GetAllVbSeqnos:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::SetParam:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::Ifconfig:
        case ClientOpcode::CreateCheckpoint:
        case ClientOpcode::LastClosedCheckpoint:
        case ClientOpcode::CompactDb:
        case ClientOpcode::SetClusterConfig:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::EvictKey:
        case ClientOpcode::Scrub:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh:
        case ClientOpcode::SetCtrlToken:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::UpdateExternalUserPermissions:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::AuthProvider:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::Invalid:
            break;
        }
    };

    auto connection = cluster->getConnection(0);
    connection->authenticate("@admin", "password");
    connection->selectBucket("bucket-0");
    connection->setFeature(cb::mcbp::Feature::ReportComputeUnitUsage, true);
    connection->setReadTimeout(std::chrono::seconds{3});

    for (int ii = 0; ii < 0x100; ++ii) {
        auto opcode = ClientOpcode(ii);
        if (is_valid_opcode(opcode)) {
            testOpcode(*connection, opcode);
        }
    }

    executeWithExpectedCU(
            [&connection]() {
                DcpDrain instance("127.0.0.1",
                                  std::to_string(connection->getPort()),
                                  "@admin",
                                  "password",
                                  "bucket-0");
                instance.drain();
                EXPECT_NE(0, instance.getNumMutations());
                EXPECT_NE(0, instance.getRcu());
            },
            0,
            0);

    /// but when running as another user it should meter
    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details bucket-0");
    DcpDrain instance("127.0.0.1",
                      std::to_string(connection->getPort()),
                      "bucket-0",
                      "bucket-0",
                      "bucket-0");
    instance.drain();
    EXPECT_NE(0, instance.getNumMutations());
    EXPECT_NE(0, instance.getRcu());

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details bucket-0");
    EXPECT_EQ(instance.getRcu(),
              after["rcu"].get<size_t>() - before["rcu"].get<size_t>());
    EXPECT_EQ(0, after["wcu"].get<size_t>() - before["wcu"].get<size_t>());
}

} // namespace cb::test
