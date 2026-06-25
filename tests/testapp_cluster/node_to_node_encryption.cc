/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// Integration tests for node-to-node TLS certificate enforcement during
// snapshot downloads (MB-72054). Each test sets the CRL configuration
// directly in download_properties.tls->crl_config and verifies that
// DownloadSnapshot reaches the expected snapshot-status state.
//
// Certificate relationships used by these tests:
//   node1.cert  — server cert for the source node (active vbucket)
//                 Serial 1, Issuer: "KV engine signing certificate"
//   intermediate_ca_jane_revoked.crl
//               — CRL from "KV engine signing certificate" that revokes serial
//                 01, which covers node1.cert (same issuer and serial)

#include "clustertest.h"
#include "test_utilities.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <snapshot/download_properties.h>
#include <snapshot/manifest.h>

using cb::mcbp::ClientOpcode;
using cb::test::Bucket;

class NodeToNodeEncryptionTest : public cb::test::ClusterTest {
protected:
    static void SetUpTestCase();
    static void TearDownTestCase() {
        bucket.reset();
        if (cluster->getBucket(bucket_name)) {
            cluster->deleteBucket(bucket_name);
        }
    }

    void TearDown() override {
        // After failure tests the destination vbucket stays dead and no
        // snapshot is active on the source (TLS handshake fails before
        // PrepareSnapshot is issued), so there is nothing to release.
        // Clear the CRL config so the next test starts from a consistent state.
        download_properties.tls->crl_config = std::nullopt;
    }

    void populate_docs_on_source() {
        for (int ii = 0; ii < 10; ++ii) {
            source_node->arithmetic(fmt::format("key:{}", ii), 1, ii);
        }
    }

    // Initiate an async snapshot download from source to destination.
    void initiate_download() {
        BinprotGenericCommand cmd(ClientOpcode::DownloadSnapshot,
                                  {},
                                  nlohmann::json(download_properties).dump());
        cmd.setVBucket(Vbid{0});
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        auto rsp = destination_node->execute(cmd);
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << rsp.getDataView();
    }

    static std::unique_ptr<MemcachedConnection> source_node;
    static std::unique_ptr<MemcachedConnection> destination_node;
    static std::shared_ptr<Bucket> bucket;
    static cb::snapshot::DownloadProperties download_properties;
    static const std::string bucket_name;
};

std::unique_ptr<MemcachedConnection> NodeToNodeEncryptionTest::source_node;
std::unique_ptr<MemcachedConnection> NodeToNodeEncryptionTest::destination_node;
std::shared_ptr<Bucket> NodeToNodeEncryptionTest::bucket;
cb::snapshot::DownloadProperties NodeToNodeEncryptionTest::download_properties;
const std::string NodeToNodeEncryptionTest::bucket_name =
        "node_to_node_enc_test";

void NodeToNodeEncryptionTest::SetUpTestCase() {
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
                // Configure TLS on the source node (the one that will serve
                // the snapshot).  A separate TLS port is created so that the
                // destination can reach it over an encrypted channel.
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
                        {"ssl_peer_verify", true},
                        {"ca_store",
                         OBJECT_ROOT "/tests/cert/root/ca_root.cert"}};

                source_node = std::move(conn);
            }
            if (state == vbucket_state_dead) {
                destination_node = std::move(conn);
            }
        });
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

// With CRL policy "Require" and a CRL that revokes the source node's server
// certificate (node1.cert, serial 01 from "KV engine signing certificate"),
// the TLS handshake is rejected and the download reaches "failed".
TEST_F(NodeToNodeEncryptionTest, CrlRequire_RevokedServerCert) {
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    download_properties.tls->crl_config = CrlConfiguration{
            {CrlPolicy::Require, CrlPolicy::Disabled}, {revokedCrl}, false};

    populate_docs_on_source();
    initiate_download();

    using namespace std::chrono_literals;
    cb::test::do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                                 Vbid{0},
                                 *destination_node,
                                 {"available", "incomplete"},
                                 {"failed"},
                                 30s);
}

// "Strict" behaves the same as "Require" for a revoked certificate: the CRL
// lists the cert as revoked, so the connection must be rejected.
TEST_F(NodeToNodeEncryptionTest, CrlStrict_RevokedServerCert) {
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    download_properties.tls->crl_config = CrlConfiguration{
            {CrlPolicy::Strict, CrlPolicy::Disabled}, {revokedCrl}, false};

    populate_docs_on_source();
    initiate_download();

    using namespace std::chrono_literals;
    cb::test::do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                                 Vbid{0},
                                 *destination_node,
                                 {"available", "incomplete"},
                                 {"failed"},
                                 30s);
}

// "Permissive" still rejects certificates that are explicitly listed as
// revoked in the CRL — it only tolerates a missing or expired CRL.
TEST_F(NodeToNodeEncryptionTest, CrlPermissive_RevokedServerCert) {
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    download_properties.tls->crl_config = CrlConfiguration{
            {CrlPolicy::Permissive, CrlPolicy::Disabled}, {revokedCrl}, false};

    populate_docs_on_source();
    initiate_download();

    using namespace std::chrono_literals;
    cb::test::do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                                 Vbid{0},
                                 *destination_node,
                                 {"available", "incomplete"},
                                 {"failed"},
                                 30s);
}

// With policy "Require" and no CRL files configured, OpenSSL cannot find a
// CRL for the server certificate's issuer (UNABLE_TO_GET_CRL). "Require"
// treats a missing CRL as a hard error, so the connection is rejected.
TEST_F(NodeToNodeEncryptionTest, CrlRequire_NoCrlFiles) {
    download_properties.tls->crl_config = CrlConfiguration{
            {CrlPolicy::Require, CrlPolicy::Disabled}, {}, false};

    populate_docs_on_source();
    initiate_download();

    using namespace std::chrono_literals;
    cb::test::do_snapshot_status(fmt::format("{}:{}", __func__, __LINE__),
                                 Vbid{0},
                                 *destination_node,
                                 {"available", "incomplete"},
                                 {"failed"},
                                 30s);
}
