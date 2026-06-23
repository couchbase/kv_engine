/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "platform/dirutils.h"
#include "testapp.h"
#include "testapp_client_test.h"

#include <auditd/couchbase_audit_events.h>
#include <cbcrypto/digest.h>
#include <folly/portability/GMock.h>
#include <openssl/opensslv.h>
#include <protocol/mcbp/ewb_encode.h>
#include <algorithm>
#include <filesystem>

class TlsTests : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        std::vector<std::string> crlfiles;

        tls = {{"private key", OBJECT_ROOT "/tests/cert/root/ca_root.key"},
               {"certificate chain",
                OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
               {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
               {"minimum version", "TLS 1.2"},
               {"cipher list",
                {{"TLS 1.2", "HIGH"},
                 {"TLS 1.3",
                  "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
                  "AES_"
                  "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
                  "SHA256"}}},
               {"cipher order", true},
               {"client cert auth", "disabled"},
               {"crl_policies",
                {{"node_to_node", "Disabled"}, {"client_auth", "Disabled"}}},
               {"crl_files", crlfiles}};

        reconfigure_client_cert_auth("disabled", "", "", "");
        reloadConfig();
        connection =
                connectionMap
                        .getConnection(true,
                                       mcd_env->haveIPv4() ? AF_INET : AF_INET6)
                        .clone();
        initialCertVerificationProblems = getTlsCertVerificationProblems();
        prepare_audit_trail();
    }

    void TearDown() override {
        auto& json = mcd_env->getAuditConfig();
        json["auditd_enabled"] = false;
        updateAuditConfig();

        reconfigure_client_cert_auth("disabled", "", "", "");
        TestappTest::TearDown();
    }

    void prepare_audit_trail() {
        auto logdir = mcd_env->getAuditLogDir();
        cb::io::remove_with_retry(logdir);
        std::filesystem::create_directories(logdir);
        auto& json = mcd_env->getAuditConfig();
        json["auditd_enabled"] = true;
        json["filtering_enabled"] = false;
        json["event_states"][std::to_string(
                MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM)] =
                "enabled";
        json["event_states"][std::to_string(
                MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY)] =
                "enabled";
        updateAuditConfig();
    }

    void updateAuditConfig() {
        try {
            mcd_env->rewriteAuditConfig();
        } catch (std::exception& e) {
            FAIL() << "Failed to toggle audit state: " << e.what();
        }
        adminConnection->reloadAuditConfiguration();
    }

    void waitForAuditEvent(
            uint32_t event,
            const std::function<void(const nlohmann::json&)>& callback = {}) {
        auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);

        do {
            if (mcd_env->iterateAuditEvents(
                        [event, &callback](const nlohmann::json& entry) {
                            if (entry.value("id", 0u) == event) {
                                if (callback) {
                                    callback(entry);
                                }
                                return true;
                            }
                            return false;
                        })) {
                return;
            }
            // Avoid busy-loop by backing off
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (std::chrono::steady_clock::now() < timeout);

        FAIL() << "Timed out waiting for audit event " << event;
    }

    void waitForAuditEvent(uint32_t event, long code) {
        const std::string reason = X509_verify_cert_error_string(code);
        waitForAuditEvent(event, [&reason, this](const auto& entry) {
            if (entry.value("reason", "") != reason) {
                throw std::runtime_error(fmt::format(
                        "Unexpected reason. Expected: \"{}\". Audit event: {}",
                        reason,
                        entry.dump()));
            }
            if (expected_peer_cert != entry["peer_certificate"]) {
                throw std::runtime_error(fmt::format(
                        "Unexpected peer cert detected: Expected: {} got: {}",
                        expected_peer_cert.dump(),
                        entry["peer_certificate"].dump()));
            }
        });
    }

    uint64_t getTlsCertVerificationProblems() {
        std::optional<uint64_t> problems;
        adminConnection->stats(
                [&problems](auto key, auto value) {
                    if (key == "tls_certificate_verification_problems") {
                        problems = std::stoull(value);
                    }
                },
                {});
        if (!problems.has_value()) {
            throw std::runtime_error(
                    "Failed to get tls_certificate_verification_problems stat");
        }
        return *problems;
    }

    void reloadConfig() {
        auto rsp = adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::Ifconfig, "tls", tls.dump()});
        ASSERT_TRUE(rsp.isSuccess())
                << "Failed to set TLS properties: " << rsp.getStatus()
                << std::endl
                << rsp.getDataView();
    }

    BinprotResponse tryReloadConfig() {
        return adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::Ifconfig, "tls", tls.dump()});
    }

    void setTlsMinimumSpec(const std::string& version) {
        tls["minimum version"] = version;
        reloadConfig();
    }

    void shouldPass(TlsVersion version) {
        try {
            connection->setTlsProtocol(version);
            connection->reconnect();
            const auto rsp = connection->execute(BinprotGenericCommand(
                    cb::mcbp::ClientOpcode::SaslListMechs));
            ASSERT_TRUE(rsp.isSuccess()) << "Failed with version " << version;
        } catch (const std::exception& e) {
            FAIL() << "Failed with version \"" << version << "\": " << e.what();
        }
    }

    void shouldFail(TlsVersion version) {
        try {
            connection->setTlsProtocol(version);
            connection->reconnect();
            FAIL() << "Should fail with " << version;
        } catch (const std::exception&) {
        }
    }

    nlohmann::json tls;
    std::unique_ptr<MemcachedConnection> connection;
    uint64_t initialCertVerificationProblems{0};
    nlohmann::json expected_peer_cert = {
            {"issuer",
             "/C=NO/O=Couchbase Inc/OU=kv engine/CN=KV engine signing "
             "certificate"},
            {"serial", "01"},
            {"subject",
             "/C=NO/O=Couchbase Inc/OU=kv "
             "engine/GN=Jane/SN=Doe/CN=Jane Doe"}};
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TlsTests,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(TlsTests, Minimum_Tls1_2) {
    setTlsMinimumSpec("TLS 1.2");

    shouldPass(TlsVersion::Any);
    shouldPass(TlsVersion::V1_2);
    shouldPass(TlsVersion::V1_3);
}

TEST_P(TlsTests, Minimum_Tls1_3) {
    setTlsMinimumSpec("TLS 1.3");

    shouldPass(TlsVersion::Any);
    shouldFail(TlsVersion::V1_2);
    shouldPass(TlsVersion::V1_3);
}

TEST_P(TlsTests, TLS12_Ciphers) {
    // Disable all TLS v1 ciphers
    tls["cipher list"]["TLS 1.2"] = "HIGH:!TLSv1";
    reloadConfig();

    shouldPass(TlsVersion::V1_2);

    // But all should fail if we set that we only want TLSv1 ciphers
    connection->setTls12Ciphers("TLSv1");
    shouldFail(TlsVersion::V1_2);

    // TLS 1.1. did not introduce any new ciphers
    tls["cipher list"]["TLS 1.2"] = "HIGH:!TLSv1:!TLSv1.2";
    reloadConfig();
    connection->setTls12Ciphers("TLSv1.2");
    shouldFail(TlsVersion::V1_2);
}

TEST_P(TlsTests, TLS13_Ciphers) {
    tls["cipher list"]["TLS 1.3"] = "TLS_AES_256_GCM_SHA384";
    reloadConfig();

    shouldPass(TlsVersion::V1_3);

    connection->setTls13Ciphers("TLS_AES_128_GCM_SHA256");
    shouldFail(TlsVersion::V1_3);
}

// In the weird case you've configured the system to not accept any ciphers
// at all
TEST_P(TlsTests, No_Ciphers) {
    tls["cipher list"]["TLS 1.2"] = "";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();

    shouldFail(TlsVersion::Any);
    shouldFail(TlsVersion::V1_2);
    shouldFail(TlsVersion::V1_3);
}

TEST_P(TlsTests, ECDHE_RSA_AES256_GCM_SHA384) {
    tls["cipher list"]["TLS 1.2"] = "ECDHE-RSA-AES256-GCM-SHA384";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();
    shouldPass(TlsVersion::V1_2);
}

TEST_P(TlsTests, DHE_RSA_AES256_SHA256) {
    tls["cipher list"]["TLS 1.2"] = "DHE-RSA-AES256-SHA256";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();
    shouldPass(TlsVersion::V1_2);
}

TEST_P(TlsTests, CrlFileVersions_NonExistingFile) {
    tls["crl_files"] = std::vector<std::string>{
            {"/nonexistent/path/crl_does_not_exist.crl"}};
    auto rsp = tryReloadConfig();
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_THAT(rsp.getErrorContext(),
                testing::HasSubstr("cb::io::loadFile("
                                   "/nonexistent/path/crl_does_not_exist.crl"
                                   ") failed"));
}

TEST_P(TlsTests, CrlFileVersions_EmptyFile) {
    auto tmpFile = cb::io::mktemp("crl_test");
    cb::io::saveFile(tmpFile, "");
    tls["crl_files"] = std::vector<std::string>{{tmpFile}};
    auto rsp = tryReloadConfig();
    std::filesystem::remove(tmpFile);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ("PEM data must not be empty", rsp.getErrorContext());
}

TEST_P(TlsTests, CrlFileVersions_InvalidPem) {
    auto tmpFile = cb::io::mktemp("crl_test");
    const std::string garbage = "this is not a valid PEM CRL file";
    cb::io::saveFile(tmpFile, garbage);
    tls["crl_files"] = std::vector{{tmpFile}};
    auto rsp = tryReloadConfig();
    std::filesystem::remove(tmpFile);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ("Failed to load any CRLs from the file", rsp.getErrorContext());
}

TEST_P(TlsTests, CrlFileVersions_SingleValidFile) {
    const std::string crlPath = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    tls["crl_files"] = std::vector{{crlPath}};
    reloadConfig();
}

TEST_P(TlsTests, CrlFileVersions_MultipleValidFiles) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    tls["crl_files"] = std::vector{{rootCrl, intermediateCrl}};
    reloadConfig();
}

TEST_P(TlsTests, CrlPolicyPerScope_AllValidCombinations) {
    for (const auto* nodePolicy :
         {"Disabled", "Permissive", "Strict", "Require"}) {
        for (const auto* clientPolicy :
             {"Disabled", "Permissive", "Strict", "Require"}) {
            tls["crl_policies"] = {{"node_to_node", nodePolicy},
                                   {"client_auth", clientPolicy}};
            reloadConfig();
        }
    }
}

TEST_P(TlsTests, CrlPolicyPerScope_InvalidValue) {
    tls["crl_policies"] = {{"node_to_node", "Invalid"},
                           {"client_auth", "Disabled"}};
    auto rsp = tryReloadConfig();
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(
            R"(CRL policy must be one of "Disabled", "Permissive", "Strict" or "Require")",
            rsp.getErrorContext());
}

// With both scopes Disabled the X509_STORE CRL flags are never set, so
// loaded CRL files are ignored entirely.  Jane's cert is in the revoked
// list yet she must still connect successfully.
TEST_P(TlsTests, CrlClientAuthPolicy_Disabled) {
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Disabled"}};
    tls["crl_files"] = std::vector<std::string>{{revokedCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);
}

// With Permissive, a missing CRL (UNABLE_TO_GET_CRL) is tolerated.  No
// CRL is loaded for the intermediate CA, so OpenSSL cannot find one for
// jane's issuer — but Permissive allows the connection to proceed.
TEST_P(TlsTests, CrlClientAuthPolicy_Permissive) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_UNABLE_TO_GET_CRL);
}

// With Strict and no CRL loaded, a missing CRL is tolerated (allowed with a
// warning) — same permissive behavior as Permissive for this case.
TEST_P(TlsTests, CrlClientAuthPolicy_Strict_MissingCrl) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_UNABLE_TO_GET_CRL);
}

// With Strict and an explicitly revoked certificate, the connection must be
// rejected — same hard-drop behaviour as Require for this case.
TEST_P(TlsTests, CrlClientAuthPolicy_Strict_RevokedCert) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, revokedCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: jane's certificate is "
                  "explicitly revoked";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CERT_REVOKED);
}

// With Require, an explicitly revoked certificate must be rejected.  We
// load root_ca.crl (so the intermediate CA cert passes its own CRL check)
// and intermediate_ca_jane_revoked.crl (which marks jane as revoked).
TEST_P(TlsTests, CrlClientAuthPolicy_Require) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, revokedCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: jane's certificate is "
                  "explicitly revoked";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CERT_REVOKED);
}

// With Require and no CRL loaded, OpenSSL cannot find a CRL for jane's issuer
// (UNABLE_TO_GET_CRL).  Unlike Strict, Require treats a missing CRL as a hard
// failure so the connection must be rejected.
TEST_P(TlsTests, CrlClientAuthPolicy_Require_MissingCrl) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Require rejects missing CRL";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_UNABLE_TO_GET_CRL);
}

// With Permissive and an explicitly revoked certificate, the connection must
// be rejected.  Permissive only tolerates missing/expired CRLs; CERT_REVOKED
// falls through to OpenSSL's native judgment (preverify_ok = 0).
TEST_P(TlsTests, CrlClientAuthPolicy_Permissive_RevokedCert) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string revokedCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_jane_revoked.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, revokedCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: jane's certificate is "
                  "explicitly revoked";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CERT_REVOKED);
}

// With Permissive and an expired CRL, CRL_HAS_EXPIRED is tolerated and the
// connection is allowed.  We load root_ca.crl (valid) for the root CA level
// and intermediate_ca_expired.crl (expired at build time) for jane's issuer.
TEST_P(TlsTests, CrlClientAuthPolicy_Permissive_ExpiredCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string expiredCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_expired.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, expiredCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_CRL_HAS_EXPIRED);
}

// With Strict and an expired CRL, CRL_HAS_EXPIRED is treated as a hard
// failure and the connection must be rejected.
TEST_P(TlsTests, CrlClientAuthPolicy_Strict_ExpiredCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string expiredCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_expired.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, expiredCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Strict rejects expired CRL";
    } catch (const std::exception&) {
    }
    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CRL_HAS_EXPIRED);
}

// With Require and an expired CRL, CRL_HAS_EXPIRED is treated as a hard
// failure and the connection must be rejected.
TEST_P(TlsTests, CrlClientAuthPolicy_Require_ExpiredCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string expiredCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_expired.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, expiredCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Require rejects expired CRL";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CRL_HAS_EXPIRED);
}

// With Permissive and a not-yet-valid CRL, CRL_NOT_YET_VALID is tolerated and
// the connection is allowed.  We load root_ca.crl (valid) for the root CA
// level and intermediate_ca_not_yet_valid.crl (thisUpdate pinned to 2049) for
// jane's issuer.
TEST_P(TlsTests, CrlClientAuthPolicy_Permissive_NotYetValidCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string notYetValidCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_not_yet_valid.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, notYetValidCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_CRL_NOT_YET_VALID);
}

// With Strict and a not-yet-valid CRL, CRL_NOT_YET_VALID is treated as a hard
// failure and the connection must be rejected.
TEST_P(TlsTests, CrlClientAuthPolicy_Strict_NotYetValidCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string notYetValidCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_not_yet_valid.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, notYetValidCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Strict rejects "
                  "not-yet-valid CRL";
    } catch (const std::exception&) {
    }
    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CRL_NOT_YET_VALID);
}

// With Require and a not-yet-valid CRL, CRL_NOT_YET_VALID is treated as a hard
// failure and the connection must be rejected.
TEST_P(TlsTests, CrlClientAuthPolicy_Require_NotYetValidCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string notYetValidCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_not_yet_valid.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, notYetValidCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Require rejects "
                  "not-yet-valid CRL";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CRL_NOT_YET_VALID);
}

// With Permissive and a CRL whose signature is corrupted
// (X509_V_ERR_CRL_SIGNATURE_FAILURE), the policy falls through to OpenSSL's
// native preverify_ok=0, so the connection is rejected.  Unlike missing/expired
// CRLs, a bad signature is NOT tolerated by Permissive — and because no
// errorCallback is invoked for this case the stat counter must stay unchanged.
TEST_P(TlsTests, CrlClientAuthPolicy_Permissive_BadSigCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string badSigCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_bad_signature.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, badSigCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Permissive rejects bad CRL "
                  "signature";
    } catch (const std::exception&) {
    }

    // Permissive does not call errorCallback for CRL_SIGNATURE_FAILURE —
    // the error falls through to preverify_ok (0) without incrementing the stat
    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems);
}

// With Strict and a CRL whose signature is corrupted, the connection is
// rejected via preverify_ok=0 (same silent fall-through as Permissive).
// The stat counter must stay unchanged because no errorCallback is invoked.
TEST_P(TlsTests, CrlClientAuthPolicy_Strict_BadSigCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string badSigCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_bad_signature.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, badSigCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Strict rejects bad CRL "
                  "signature";
    } catch (const std::exception&) {
    }

    // Strict does not call errorCallback for CRL_SIGNATURE_FAILURE —
    // same silent fall-through as Permissive; stat must stay unchanged
    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems);
}

// With Require and a CRL whose signature is corrupted, the connection is
// rejected and errorCallback IS invoked (Require explicitly handles
// CRL_SIGNATURE_FAILURE before falling through to preverify_ok=0).
// The stat counter must be incremented by exactly 1.
TEST_P(TlsTests, CrlClientAuthPolicy_Require_BadSigCrl) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string badSigCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca_bad_signature.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, badSigCrl}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: Require rejects bad CRL "
                  "signature";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);

    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_CRL_SIGNATURE_FAILURE);
}

// Without crl_check_intermediate (the default) the CRL check covers only the
// end-entity certificate.  Loading only the intermediate CA's CRL (which
// covers jane's cert) is therefore sufficient for Require to accept the
// connection — the intermediate CA cert itself is not required to have a CRL
// entry.
TEST_P(TlsTests, CrlCheckIntermediate_False) {
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{intermediateCrl}};
    tls["crl_check_intermediate"] = false;
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems);
}

// With crl_check_intermediate=true (X509_V_FLAG_CRL_CHECK_ALL) OpenSSL also
// requires a CRL for every intermediate CA in the chain.
// client_intermediate_ca.pem includes both the intermediate cert and the root
// CA cert, so the chain has three levels.  Loading only the intermediate CA's
// own CRL leaves the root CA's CRL absent; OpenSSL returns UNABLE_TO_GET_CRL
// for the intermediate CA cert and Require rejects the connection.
TEST_P(TlsTests, CrlCheckIntermediate_True_MissingRootCrl) {
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{intermediateCrl}};
    tls["crl_check_intermediate"] = true;
    reloadConfig();

    expected_peer_cert = {{"issuer", "/C=NO/O=Couchbase Inc/CN=Root CA"},
                          {"serial", "01"},
                          {"subject",
                           "/C=NO/O=Couchbase Inc/OU=kv engine/CN=KV engine "
                           "signing certificate"}};

    setClientCertData(*connection, "jane");
    try {
        connection->reconnect();
        connection->setFeature(cb::mcbp::Feature::JSON, true);
        FAIL() << "Expected TLS handshake to fail: root CA CRL absent, "
                  "Require rejects the intermediate CA cert";
    } catch (const std::exception&) {
    }

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 1);
    waitForAuditEvent(
            MEMCACHED_AUDIT_TLS_CLIENT_DISCONNECTED___CERTIFICATE_REJECTED_BY_CRL_POLICY,
            X509_V_ERR_UNABLE_TO_GET_CRL);
}

// With crl_check_intermediate=true and both CRLs present — the root CA CRL
// (covering the intermediate CA cert) and the intermediate CA CRL (covering
// jane's cert) — the full chain passes CRL verification and the connection
// is accepted.
TEST_P(TlsTests, CrlCheckIntermediate_True_AllCrlsPresent) {
    const std::string rootCrl = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Require"}};
    tls["crl_files"] = std::vector<std::string>{{rootCrl, intermediateCrl}};
    tls["crl_check_intermediate"] = true;
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems);
}

// With crl_check_intermediate=true and Permissive policy, a missing root CA
// CRL (needed to check the intermediate CA cert) is tolerated — Permissive
// allows UNABLE_TO_GET_CRL with a warning.  The connection succeeds, but the
// stat counter is incremented and a CERTIFICATE_VERIFICATION_PROBLEM audit
// event is emitted.
TEST_P(TlsTests, CrlCheckIntermediate_True_MissingRootCrl_Permissive) {
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Permissive"}};
    tls["crl_files"] = std::vector<std::string>{{intermediateCrl}};
    tls["crl_check_intermediate"] = true;
    reloadConfig();
    expected_peer_cert = {{"issuer", "/C=NO/O=Couchbase Inc/CN=Root CA"},
                          {"serial", "01"},
                          {"subject",
                           "/C=NO/O=Couchbase Inc/OU=kv engine/CN=KV engine "
                           "signing certificate"}};

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 2);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_UNABLE_TO_GET_CRL);
}

// With crl_check_intermediate=true and Strict policy, a missing root CA CRL
// is also tolerated — Strict allows UNABLE_TO_GET_CRL with a warning, same
// as Permissive for this case.  The connection succeeds, the stat counter is
// incremented, and a CERTIFICATE_VERIFICATION_PROBLEM audit event is emitted.
TEST_P(TlsTests, CrlCheckIntermediate_True_MissingRootCrl_Strict) {
    const std::string intermediateCrl =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";
    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{intermediateCrl}};
    tls["crl_check_intermediate"] = true;
    reloadConfig();
    expected_peer_cert = {{"issuer", "/C=NO/O=Couchbase Inc/CN=Root CA"},
                          {"serial", "01"},
                          {"subject",
                           "/C=NO/O=Couchbase Inc/OU=kv engine/CN=KV engine "
                           "signing certificate"}};

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    EXPECT_EQ(getTlsCertVerificationProblems(),
              initialCertVerificationProblems + 2);
    waitForAuditEvent(MEMCACHED_AUDIT_TLS_CERTIFICATE_VERIFICATION_PROBLEM,
                      X509_V_ERR_UNABLE_TO_GET_CRL);
}

// Verify that multiple CRL PEM blocks concatenated into a single file are
// all loaded correctly.  We write root_ca.crl and intermediate_ca.crl into
// one temporary file and confirm that a client with a valid cert can connect
// with Strict policy (both CRLs must be found for the chain to verify).
TEST_P(TlsTests, CrlFileVersions_MultipleInSingleFile) {
    const std::string rootCrlPath = OBJECT_ROOT "/tests/cert/crl/root_ca.crl";
    const std::string intermediateCrlPath =
            OBJECT_ROOT "/tests/cert/crl/intermediate_ca.crl";

    auto rootPem = cb::io::loadFile(rootCrlPath);
    auto intermediatePem = cb::io::loadFile(intermediateCrlPath);

    auto tmpFile = cb::io::mktemp("crl_test");
    cb::io::saveFile(tmpFile, rootPem + intermediatePem);

    reconfigure_client_cert_auth("mandatory", "subject.cn", "", " ");
    tls["CA file"] =
            OBJECT_ROOT "/tests/cert/intermediate/client_intermediate_ca.pem";
    tls["client cert auth"] = "mandatory";
    tls["crl_policies"] = {{"node_to_node", "Disabled"},
                           {"client_auth", "Strict"}};
    tls["crl_files"] = std::vector<std::string>{{tmpFile}};
    reloadConfig();

    setClientCertData(*connection, "jane");
    connection->reconnect();
    connection->setFeature(cb::mcbp::Feature::JSON, true);

    std::filesystem::remove(tmpFile);
}
