/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "testapp_environment.h"
#include "memcached_audit_events.h"
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/openssl_utils.h>
#include <fstream>
#include <memory>

void TestBucketImpl::createEwbBucket(const std::string& name,
                                     const std::string& plugin,
                                     const std::string& config,
                                     MemcachedConnection& conn) {
    std::string cfg(plugin);
    if (!config.empty()) {
        cfg += ";" + config;
    }
    conn.createBucket(name, cfg, BucketType::EWouldBlock);
}

// Both memcache and ep-engine buckets support set_param for xattr on/off
void TestBucketImpl::setXattrEnabled(MemcachedConnection& conn,
                                     const std::string& bucketName,
                                     bool value) {
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SetParam);
    cmd.setKey("xattr_enabled");
    cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
            cb::mcbp::request::SetParamPayload::Type::Flush)));
    if (value) {
        cmd.setValue("true");
    } else {
        cmd.setValue("false");
    }

    const auto resp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

void TestBucketImpl::setCompressionMode(MemcachedConnection& conn,
                                        const std::string& bucketName,
                                        const std::string& value) {
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SetParam);
    cmd.setKey("compression_mode");
    cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
            cb::mcbp::request::SetParamPayload::Type::Flush)));
    cmd.setValue(value);

    const auto resp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

void TestBucketImpl::setMinCompressionRatio(MemcachedConnection& conn,
                                            const std::string& bucketName,
                                            const std::string& value) {
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SetParam);
    cmd.setKey("min_compression_ratio");
    cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
            cb::mcbp::request::SetParamPayload::Type::Flush)));
    cmd.setValue(value);

    const auto resp = conn.execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

class DefaultBucketImpl : public TestBucketImpl {
public:
    explicit DefaultBucketImpl(std::string extraConfig = {})
        : TestBucketImpl(extraConfig) {
    }

    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn) override {
        std::string settings = config;
        if (!extraConfig.empty()) {
            if (!settings.empty()) {
                settings += ';';
            }
            settings += extraConfig;
        }
        createEwbBucket(name, "default_engine.so", settings, conn);
    }

    std::string getName() const override {
        return "default_engine";
    }

    bool supportsOp(cb::mcbp::ClientOpcode cmd) const override {
        switch (cmd) {
        case cb::mcbp::ClientOpcode::DcpOpen:
        case cb::mcbp::ClientOpcode::DcpAddStream:
        case cb::mcbp::ClientOpcode::DcpCloseStream:
        case cb::mcbp::ClientOpcode::DcpStreamReq:
        case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
        case cb::mcbp::ClientOpcode::DcpStreamEnd:
        case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
        case cb::mcbp::ClientOpcode::DcpMutation:
        case cb::mcbp::ClientOpcode::DcpDeletion:
        case cb::mcbp::ClientOpcode::DcpExpiration:
        case cb::mcbp::ClientOpcode::DcpSetVbucketState:
        case cb::mcbp::ClientOpcode::DcpNoop:
        case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
        case cb::mcbp::ClientOpcode::DcpControl:
        case cb::mcbp::ClientOpcode::DcpSystemEvent:
        case cb::mcbp::ClientOpcode::SetWithMeta:
        case cb::mcbp::ClientOpcode::SetqWithMeta:
        case cb::mcbp::ClientOpcode::AddWithMeta:
        case cb::mcbp::ClientOpcode::AddqWithMeta:
        case cb::mcbp::ClientOpcode::DelWithMeta:
        case cb::mcbp::ClientOpcode::DelqWithMeta:
        case cb::mcbp::ClientOpcode::EnableTraffic:
        case cb::mcbp::ClientOpcode::DisableTraffic:
        case cb::mcbp::ClientOpcode::GetFailoverLog:
        case cb::mcbp::ClientOpcode::GetRandomKey:
            return false;
        default:
            return true;
        }
    }

    bool supportsPrivilegedBytes() const override {
        return false;
    }

    size_t getMaximumDocSize() const override {
        return 1024 * 1024;
    }

    bool supportsLastModifiedVattr() const override {
        return false;
    }

    bool supportsPersistence() const override {
        return false;
    }

    bool supportsSyncWrites() const override {
        return false;
    }

    bool supportsCollections() const override {
        return true;
    }
};

class EpBucketImpl : public TestBucketImpl {
public:
    explicit EpBucketImpl(std::string extraConfig = {})
        : TestBucketImpl(extraConfig), dbPath(cb::io::mkdtemp("mc_testapp")) {
        // Cleanup any files from a previous run still on disk.
        try {
            cb::io::rmrf(dbPath);
        } catch (...) { /* nothing exists */
        }
    }

    ~EpBucketImpl() override {
        // Cleanup any files created.
        try {
            cb::io::rmrf(dbPath);
        } catch (...) { /* nothing exists */
        }
    }

    void setBucketCreateMode(BucketCreateMode mode) override {
        bucketCreateMode = mode;
    }

    BucketCreateMode bucketCreateMode = BucketCreateMode::Clean;

    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn) override {
        const auto dbdir = cb::io::sanitizePath(dbPath + "/" + name);
        if (cb::io::isDirectory(dbdir) &&
            bucketCreateMode == BucketCreateMode::Clean) {
            cb::io::rmrf(dbdir);
        }

        std::string settings = "dbname=" + dbPath + "/" + name;
        // Increase bucket quota from 100MB to 200MB as there are some
        // testapp tests requiring more than the default.
        settings += ";max_size=200000000";
        // Disable bloom_filters - for all memcahed testapp tests we want
        // to see things like gets of tombstones going to disk and not
        // getting skipped (to ensure correct EWOULDBLOCK handling etc).
        settings += ";bfilter_enabled=false";

        if (!config.empty()) {
            settings += ";" + config;
        }
        if (!extraConfig.empty()) {
            settings += ";" + extraConfig;
        }
        createEwbBucket(name, "ep.so", settings, conn);

        BinprotGenericCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SelectBucket);
        cmd.setKey(name);
        auto resp = conn.execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        cmd.clear();
        resp.clear();

        // Set the vBucket state. Set a single replica so that any SyncWrites
        // can be completed.
        conn.setDatatypeJson(true);
        nlohmann::json meta;
        meta["topology"] = nlohmann::json::array({{"active"}});
        conn.setVbucket(Vbid(0), vbucket_state_active, meta);

        // Clear the JSON data type (just in case)
        conn.setDatatypeJson(false);

        auto auto_retry_tmpfail = conn.getAutoRetryTmpfail();
        conn.setAutoRetryTmpfail(true);
        resp = conn.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::EnableTraffic});
        conn.setAutoRetryTmpfail(auto_retry_tmpfail);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    std::string getName() const override {
        return "ep_engine";
    }

    bool supportsOp(cb::mcbp::ClientOpcode cmd) const override {
        switch (cmd) {
        case cb::mcbp::ClientOpcode::Flush:
        case cb::mcbp::ClientOpcode::Flushq:
            // TODO: Flush *is* supported by ep-engine, but it needs traffic
            // disabling before it's permitted.
        case cb::mcbp::ClientOpcode::Scrub:
            return false;

        default:
            return true;
        }
    }

    bool supportsPrivilegedBytes() const override {
        return true;
    }

    size_t getMaximumDocSize() const override {
        return 20 * 1024 * 1024;
    }

    bool supportsLastModifiedVattr() const override {
        return true;
    }

    bool supportsPersistence() const override {
        return true;
    }

    bool supportsSyncWrites() const override {
        return true;
    }

    bool supportsCollections() const override {
        return true;
    }

    /// Directory for any database files.
    const std::string dbPath;
};

McdEnvironment::McdEnvironment(bool manageSSL_,
                               std::string engineName,
                               std::string engineConfig)
    : manageSSL(manageSSL_) {
    if (manageSSL) {
        initialize_openssl();
    }

    if (engineName == "default") {
        std::string config = "keep_deleted=true";
        if (!engineConfig.empty()) {
            config += ";" + engineConfig;
        }
        testBucket = std::make_unique<DefaultBucketImpl>(config);
    } else if (engineName == "ep") {
        testBucket = std::make_unique<EpBucketImpl>(engineConfig);
    } else {
        throw std::invalid_argument("Unknown engine '" + engineName +
                                    "' "
                                    "Options are 'default' and 'ep'");
    }
}

McdEnvironment::~McdEnvironment() {
   if (manageSSL) {
       shutdown_openssl();
   }
}

void McdEnvironment::SetUp() {
    cwd = cb::io::getcwd();
    SetupAuditFile();
    SetupIsaslPw();
    SetupRbacFile();
}

void McdEnvironment::SetupIsaslPw() {
    isasl_file_name = SOURCE_ROOT;
    isasl_file_name.append("/tests/testapp/cbsaslpw.json");
    std::replace(isasl_file_name.begin(), isasl_file_name.end(), '\\', '/');

    // Add the file to the exec environment
    snprintf(isasl_env_var, sizeof(isasl_env_var), "CBSASL_PWFILE=%s",
             isasl_file_name.c_str());
    putenv(isasl_env_var);
}

void McdEnvironment::SetupAuditFile() {
    try {
        audit_file_name = cwd + "/" + cb::io::mktemp("audit.cfg");
        audit_log_dir = cwd + "/" + cb::io::mktemp("audit.log");
        const std::string descriptor = cwd + "/auditd";
        EXPECT_NO_THROW(cb::io::rmrf(audit_log_dir));
        cb::io::mkdirp(audit_log_dir);

        // Generate the auditd config file.
        audit_config = {};
        audit_config["version"] = 2;
        audit_config["uuid"] = "this_is_the_uuid";
        audit_config["auditd_enabled"] = false;
        audit_config["rotate_interval"] = 1440;
        audit_config["rotate_size"] = 20971520;
        audit_config["buffered"] = false;
        audit_config["log_path"] = audit_log_dir;
        audit_config["descriptors_path"] = descriptor;
        audit_config["sync"] = nlohmann::json::array();
        audit_config["disabled"] = nlohmann::json::array();
        audit_config["event_states"]
                    [std::to_string(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED)] =
                            "enabled";
        audit_config["filtering_enabled"] = false;
        audit_config["disabled_userids"] = nlohmann::json::array();
    } catch (const std::exception& e) {
        FAIL() << "Failed to generate audit configuration: " << e.what();
    }

    rewriteAuditConfig();
}

void McdEnvironment::TearDown() {
    // Cleanup Audit config file
    if (!audit_file_name.empty()) {
        cb::io::rmrf(audit_file_name);
    }

    // Cleanup Audit log directory
    if (!audit_log_dir.empty()) {
        cb::io::rmrf(audit_log_dir);
    }

    // Cleanup RBAC configuration
    if (!rbac_file_name.empty()) {
        cb::io::rmrf(rbac_file_name);
    }
}

void McdEnvironment::rewriteAuditConfig() {
    try {
        std::string audit_text = audit_config.dump();
        std::ofstream out(audit_file_name);
        out.write(audit_text.c_str(), audit_text.size());
        out.close();
    } catch (std::exception& e) {
        FAIL() << "Failed to store audit configuration: " << e.what();
    }
}

void McdEnvironment::SetupRbacFile() {
    const auto input_file =
            cb::io::sanitizePath(SOURCE_ROOT "/tests/testapp/rbac.json");
    rbac_data = nlohmann::json::parse(cb::io::loadFile(input_file));
    rbac_file_name = cwd + "/" + cb::io::mktemp("rbac.json.XXXXXX");
    rewriteRbacFile();
}

void McdEnvironment::rewriteRbacFile() {
    try {
        std::ofstream out(rbac_file_name);
        out << rbac_data.dump(2) << std::endl;
        out.close();
    } catch (std::exception& e) {
        FAIL() << "Failed to store rbac configuration: " << e.what();
    }
}

const std::string& McdEnvironment::getDbPath() const {
    return static_cast<EpBucketImpl*>(testBucket.get())->dbPath;
}

char McdEnvironment::isasl_env_var[256];
