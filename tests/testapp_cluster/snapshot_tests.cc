/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <boost/filesystem/operations.hpp>
#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <mcbp/codec/crc_sink.h>
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <snapshot/download_properties.h>
#include <snapshot/manifest.h>
#include <tests/testapp/testapp_subdoc_common.h>

#include <string>

using cb::mcbp::ClientOpcode;
using cb::mcbp::Status;
using cb::mcbp::subdoc::DocFlag;
using cb::mcbp::subdoc::PathFlag;

using cb::test::Bucket;

static FrameInfoVector GetMajorityDurabilityFrameInfoVector() {
    FrameInfoVector ret;
    ret.emplace_back(std::make_unique<cb::mcbp::request::DurabilityFrameInfo>(
            cb::durability::Level::MajorityAndPersistOnMaster));
    return ret;
}

class SnapshotClusterTest : public cb::test::ClusterTest,
                            public ::testing::WithParamInterface<size_t> {
protected:
    static void SetUpTestCase();
    static void TearDownTestCase() {
        bucket.reset();
        if (cluster->getBucket(bucket_name)) {
            cluster->deleteBucket(bucket_name);
        }
    }

    void populate_docs_on_source() {
        // Create some documents so that we know we can verify something once
        // we're done
        for (int ii = 0; ii < 100; ++ii) {
            source_node->arithmetic(fmt::format("key:{}", ii), 1, ii);
        }
        // And finally one we wait for disk
        source_node->arithmetic(fmt::format("key:{}", 1000),
                                1,
                                1000,
                                0,
                                nullptr,
                                GetMajorityDurabilityFrameInfoVector);
    }

    void validate_docs_on_destination() {
        for (int ii = 0; ii < 100; ++ii) {
            ii = destination_node->arithmetic(
                    fmt::format("key:{}", ii), 0, 1000);
        }
    }

    void validate_snapshot(const std::filesystem::path& root,
                           const cb::snapshot::Manifest& manifest);

    void do_snapshot_status(
            std::string_view context,
            Vbid vbid,
            MemcachedConnection& requestConnection,
            const std::vector<std::string_view>& failStates,
            const std::vector<std::string_view>& successStates,
            std::chrono::seconds waitFor = std::chrono::seconds(0));

    static std::unique_ptr<MemcachedConnection> source_node;
    static std::filesystem::path source_path;
    static std::unique_ptr<MemcachedConnection> destination_node;
    static std::filesystem::path destination_path;
    static std::shared_ptr<Bucket> bucket;
    static cb::snapshot::DownloadProperties download_properties;
    static const std::string bucket_name;
};

std::unique_ptr<MemcachedConnection> SnapshotClusterTest::source_node;
std::filesystem::path SnapshotClusterTest::source_path;
std::unique_ptr<MemcachedConnection> SnapshotClusterTest::destination_node;
std::filesystem::path SnapshotClusterTest::destination_path;
std::shared_ptr<Bucket> SnapshotClusterTest::bucket;
cb::snapshot::DownloadProperties SnapshotClusterTest::download_properties;
const std::string SnapshotClusterTest::bucket_name = "snapshot_test";

void SnapshotClusterTest::SetUpTestCase() {
    if (!cluster) {
        std::cerr << "Cluster not running" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    try {
        bucket = cluster->createBucket(
                bucket_name,
                {{"replicas", 0},
                 {"max_vbuckets", 8},
                 {"item_eviction_policy", "full_eviction"}},
                {},
                false);

        cluster->iterateNodes([](auto& node) {
            auto conn = node.getConnection();
            conn->authenticate("@admin");
            conn->selectBucket(bucket_name);
            auto state = conn->getVbucket(Vbid(0));
            if (state == vbucket_state_active) {
                conn->ifconfig(
                        "tls",
                        {{"private key",
                          OBJECT_ROOT "/tests/cert/servers/node1.key"},
                         {"certificate chain",
                          OBJECT_ROOT "/tests/cert/servers/chain.cert"},
                         {"CA file",
                          OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
                         {"minimum version", "TLS 1.2"},
                         {"cipher list",
                          {{"TLS 1.2", "HIGH"},
                           {"TLS 1.3",
                            "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_"
                            "SHA256:TLS_AES_128_GCM_SHA256:TLS_AES_128_CCM_8_"
                            "SHA256:TLS_AES_128_CCM_SHA256"}}},
                         {"cipher order", true},
                         {"client cert auth", "disabled"}});

                // create a TLS port
                auto spec = conn->ifconfig("define",
                                           {{"host", "::1"},
                                            {"port", 0},
                                            {"family", "inet6"},
                                            {"system", true},
                                            {"type", "mcbp"},
                                            {"tls", true},
                                            {"tag", "tls"}});

                download_properties.hostname = "::1";
                download_properties.port = spec["ports"].front()["port"];
                download_properties.bucket = bucket_name;
                download_properties.sasl = {"PLAIN", "@admin", "password"};
                download_properties.tls = nlohmann::json{
                        {"cert",
                         OBJECT_ROOT "/tests/cert/clients/internal.cert"},
                        {"key", OBJECT_ROOT "/tests/cert/clients/internal.key"},
                        {"ca_store",
                         OBJECT_ROOT "/tests/cert/root/ca_root.cert"}};

                source_node = std::move(conn);
                source_path = node.directory;
            }
            if (state == vbucket_state_dead) {
                destination_node = std::move(conn);
                destination_path = node.directory;
            }
        });
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void SnapshotClusterTest::validate_snapshot(
        const std::filesystem::path& root,
        const cb::snapshot::Manifest& manifest) {
    ASSERT_TRUE(exists(root));

    cb::snapshot::Manifest manifestOnDisk =
            nlohmann::json::parse(cb::io::loadFile(root / "manifest.json"));
    ASSERT_EQ(manifest, manifestOnDisk);

    size_t filesLargerThanMaxReadSize = 0;

    for (auto& file : manifest.files) {
        ASSERT_TRUE(exists(root / file.path));
        ASSERT_LE(file.size, file_size(root / file.path));
        if (file.size > GetParam()) {
            ++filesLargerThanMaxReadSize;
        }
    }
    for (auto& file : manifest.deks) {
        ASSERT_TRUE(exists(root / file.path));
        ASSERT_LE(file.size, file_size(root / file.path));
    }

    if (GetParam() != 2_GiB) {
        ASSERT_NE(0, filesLargerThanMaxReadSize)
                << "The snapshot doesn't have any file which will exercise "
                   "chunking and test MB-69369. "
                << "file_fragment_max_read_size: " << GetParam();
    }
}

void SnapshotClusterTest::do_snapshot_status(
        std::string_view context,
        Vbid vbid,
        MemcachedConnection& requestConnection,
        const std::vector<std::string_view>& failStates,
        const std::vector<std::string_view>& successStates,
        std::chrono::seconds waitFor) {
    bool done = false;
    const auto timeout = std::chrono::steady_clock::now() + waitFor;
    std::string value = "unintialised";
    do {
        std::string key;
        requestConnection.stats(
                [&key, &value](auto k, auto v) {
                    key = k;
                    value = v;
                },
                fmt::format("snapshot-status {}", vbid.get()));

        ASSERT_EQ(key, fmt::format("vb_{}:status", vbid.get()));
        if (std::ranges::find(failStates, value) != failStates.end()) {
            ASSERT_FALSE(true)
                    << "called from " << context
                    << " snapshot-status returned a failing state " << key
                    << " " << value << " while waiting for "
                    << nlohmann::json(successStates).dump()
                    << " snapshot-details: "
                    << requestConnection.stats("snapshot-details").dump();
        }

        if (std::ranges::find(successStates, value) != successStates.end()) {
            done = true;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    } while (!done && std::chrono::steady_clock::now() < timeout);
    ASSERT_TRUE(done) << "called from " << context
                      << ". Timeout waiting for snapshot-status to reach a "
                         "desired state. waited for:"
                      << waitFor.count()
                      << "s with last observed state: " << value;
}

TEST_P(SnapshotClusterTest, Snapshots) {
    cluster->changeConfig([](nlohmann::json& config) {
        config["file_fragment_max_read_size"] = GetParam();
        // Run with a small checksum length to help improve test coverage.
        config["file_fragment_checksum_length"] = 128;
    });

    populate_docs_on_source();
    BinprotGenericCommand download(ClientOpcode::DownloadSnapshot,
                                   {},
                                   nlohmann::json(download_properties).dump());
    download.setVBucket(Vbid{0});
    download.setDatatype(cb::mcbp::Datatype::JSON);

    auto rsp = destination_node->execute(download);
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << std::endl
                                 << rsp.getDataView();

    // ns_server don't want a blocking call to download the snapshot so
    // we need to poll the state until it is done
    using namespace std::chrono_literals;
    std::optional<cb::snapshot::Manifest> manifest;
    std::string error;

    // check but with a timeout as we're downloading on this node
    do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                       Vbid{0},
                       *destination_node,
                       {"failed", "incomplete", "none"},
                       {"available"},
                       30s);
    bool found = false;
    destination_node->stats(
            [&manifest, &error, &found](auto k, auto v) {
                if (k == "vb_0:download") {
                    found = true;
                    auto json = nlohmann::json::parse(v);
                    EXPECT_EQ(json["state"], "Finished");
                    if (json.contains("manifest")) {
                        manifest = json["manifest"];
                    }
                    if (json.contains("error")) {
                        error = v;
                    }
                }
            },
            "snapshot-details");
    ASSERT_TRUE(found) << "Failed to find snapshot-details";
    found = false;
    destination_node->stats(
            [&found](auto k, auto v) {
                if (k == "ep_snapshot_read_bytes") {
                    found = true;
                    EXPECT_NE(v, "0");
                }
            },
            "");
    ASSERT_TRUE(found) << "Failed to find ep_snapshot_read_bytes";
    ASSERT_TRUE(error.empty()) << "Failed to download snapshot: " << error;
    EXPECT_TRUE(manifest.has_value());

    // The snapshot should exist on both nodes:
    validate_snapshot(source_path / bucket_name / "snapshots" / manifest->uuid,
                      *manifest);
    validate_snapshot(
            destination_path / bucket_name / "snapshots" / manifest->uuid,
            *manifest);

    // Release the snapshot on the source node (not needed anymore)
    do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                       Vbid{0},
                       *source_node,
                       {"failed", "running", "none", "incomplete"},
                       {"available"});
    EXPECT_TRUE(
            exists(source_path / bucket_name / "snapshots" / manifest->uuid));
    rsp = source_node->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseSnapshot, manifest->uuid});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
            << "Failed to release on source node";
    // Expect immediate "none"
    do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                       Vbid{0},
                       *source_node,
                       {"failed", "running", "available", "incomplete"},
                       {"none"});
    EXPECT_FALSE(
            exists(source_path / bucket_name / "snapshots" / manifest->uuid));

    // Install the deks in the snapshot on the destination node
    cb::crypto::KeyStore keystore;
    for (const auto& p : std::filesystem::directory_iterator(
                 destination_path / bucket_name / "deks")) {
        auto json = nlohmann::json::parse(cb::io::loadFile(p.path()));
        auto key = std::make_shared<cb::crypto::KeyDerivationKey>();
        *key = json;
        keystore.setActiveKey(key);
    }

    for (const auto& file : manifest->deks) {
        auto src = destination_path / bucket_name / "snapshots" /
                   manifest->uuid / file.path;
        auto dst = destination_path / bucket_name / file.path;
        std::filesystem::copy_file(
                src, dst, std::filesystem::copy_options::overwrite_existing);
        std::filesystem::resize_file(dst, file.size);

        auto json = nlohmann::json::parse(cb::io::loadFile(dst));
        auto key = std::make_shared<cb::crypto::KeyDerivationKey>();
        *key = json;
        keystore.add(key);
    }

    nlohmann::json json = keystore;
    rsp = destination_node->execute(
            BinprotSetActiveEncryptionKeysCommand{bucket_name, json});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
            << "Failed to set the active encryption keys";

    // We know the snapshot is correctly located on the destination node.
    // lets make it active on the destination node
    destination_node->setVbucket(Vbid{0},
                                 vbucket_state_active,
                                 nlohmann::json{{"use_snapshot", "fbr"}});

    // Release and delete the snapshot on the destination node
    do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                       Vbid{0},
                       *destination_node,
                       {"failed", "running", "none", "incomplete"},
                       {"available"});
    EXPECT_TRUE(exists(destination_path / bucket_name / "snapshots" /
                       manifest->uuid));
    rsp = destination_node->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseSnapshot, manifest->uuid});

    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
            << "Failed to release on destination node";
    do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                       Vbid{0},
                       *destination_node,
                       {"failed", "running", "available", "incomplete"},
                       {"none"});
    EXPECT_FALSE(exists(destination_path / bucket_name / "snapshots" /
                        manifest->uuid));

    // We should be able to retrieve the same documents on the destination
    // node
    validate_docs_on_destination();

    destination_node->delVbucket(Vbid{0});
}

TEST_P(SnapshotClusterTest, PrepareSnapshot) {
    cluster->changeConfig([](nlohmann::json& config) {
        config["file_fragment_max_read_size"] = GetParam();
    });

    populate_docs_on_source();
    BinprotGenericCommand prepare(ClientOpcode::PrepareSnapshot);
    prepare.setVBucket(Vbid{0});
    auto rsp = source_node->execute(prepare);
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    // Must be able to construct a Manifest from the response
    cb::snapshot::Manifest manifest{nlohmann::json::parse(rsp.getDataView())};
    rsp = source_node->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseSnapshot, manifest.uuid});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

class GetFileFragmentTestSink : public cb::io::Sink {
public:
    void sink(std::string_view data) override {
        bytes_written += data.size();
    }

    std::size_t fsync() override {
        return 0;
    }
    std::size_t close() override {
        return 0;
    }
    std::size_t getBytesWritten() const override {
        return bytes_written;
    }
    size_t bytes_written = 0;
};

TEST_P(SnapshotClusterTest, GetFileFragment) {
    cluster->changeConfig([](nlohmann::json& config) {
        config["file_fragment_max_read_size"] = GetParam();
    });

    populate_docs_on_source();
    BinprotGenericCommand prepare(ClientOpcode::PrepareSnapshot);
    prepare.setVBucket(Vbid{0});
    auto rsp = source_node->execute(prepare);
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    cb::snapshot::Manifest manifest{nlohmann::json::parse(rsp.getDataView())};
    for (auto& file : manifest.files) {
        size_t offset = 0;
        while (file.size > 0) {
            auto sink = std::make_unique<GetFileFragmentTestSink>();
            source_node->getFileFragment(manifest.uuid,
                                         file.id,
                                         offset,
                                         file.size,
                                         0,
                                         sink.get(),
                                         [](std::size_t size) {});
            if (GetParam() == 2_GiB) {
                // When the server is configured with 2GiB max_read_size we will
                // get the file in one go and can expect bytes written to match
                // the file size.
                EXPECT_EQ(sink->getBytesWritten(), file.size);
            } // in the 1024 case the output will be trimmed
            file.size -= sink->getBytesWritten();
            offset += sink->getBytesWritten();
        }
    }

    rsp = source_node->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseSnapshot, manifest.uuid});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

TEST_P(SnapshotClusterTest, GetFileFragmentWithChecksum) {
    cluster->changeConfig([](nlohmann::json& config) {
        config["file_fragment_max_read_size"] = GetParam();
    });

    populate_docs_on_source();
    BinprotGenericCommand prepare(ClientOpcode::PrepareSnapshot);
    prepare.setVBucket(Vbid{0});
    auto rsp = source_node->execute(prepare);
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    cb::snapshot::Manifest manifest{nlohmann::json::parse(rsp.getDataView())};
    for (auto& file : manifest.files) {
        size_t offset = 0;
        auto sink = std::make_unique<GetFileFragmentTestSink>();
        size_t bytes_written = 0;
        size_t expected_bytes_written = file.size;
        while (file.size > 0) {
            const auto fragment_bytes_written =
                    source_node->getFileFragment(manifest.uuid,
                                                 file.id,
                                                 offset,
                                                 file.size,
                                                 15,
                                                 sink.get(),
                                                 [](std::size_t size) {});

            bytes_written += fragment_bytes_written;
            file.size -= fragment_bytes_written;
            offset += fragment_bytes_written;
        }
        EXPECT_EQ(bytes_written, expected_bytes_written);
        EXPECT_EQ(sink->getBytesWritten(), expected_bytes_written);
    }

    rsp = source_node->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseSnapshot, manifest.uuid});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

INSTANTIATE_TEST_SUITE_P(SnapshotClusterTest,
                         SnapshotClusterTest,
                         ::testing::Values(1024, 2_GiB),
                         ::testing::PrintToStringParamName());