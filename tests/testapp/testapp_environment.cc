/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_environment.h"
#include "memcached_audit_events.h"
#include <boost/filesystem.hpp>
#include <cbsasl/password_database.h>
#include <cbsasl/user.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/openssl_utils.h>
#include <utilities/string_utilities.h>
#include <fstream>
#include <memory>
#include <thread>

std::string TestBucketImpl::mergeConfigString(const std::string& next) {
    if (next.empty()) {
        return extraConfig;
    }

    if (extraConfig.empty()) {
        return next;
    }

    std::string settings = next;
    auto tokens = split_string(extraConfig, ";");
    auto key = settings;
    auto idx = key.find("=");
    if (idx != std::string::npos) {
        key.resize(idx);
    }
    for (auto& entry : tokens) {
        if (split_string(entry, "=")[0] != key) {
            settings += ";";
            settings += entry;
        }
    }

    return settings;
}

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
    conn.executeInBucket(bucketName, [&](auto& connection) {
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

        const auto resp = connection.execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

void TestBucketImpl::setCompressionMode(MemcachedConnection& conn,
                                        const std::string& bucketName,
                                        const std::string& value) {
    conn.executeInBucket(bucketName, [&](auto& connection) {
        // Encode a set_flush_param (like cbepctl)
        BinprotGenericCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SetParam);
        cmd.setKey("compression_mode");
        cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
                cb::mcbp::request::SetParamPayload::Type::Flush)));
        cmd.setValue(value);

        const auto resp = connection.execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

void TestBucketImpl::setMinCompressionRatio(MemcachedConnection& conn,
                                            const std::string& bucketName,
                                            const std::string& value) {
    conn.executeInBucket(bucketName, [&](auto& connection) {
        // Encode a set_flush_param (like cbepctl)
        BinprotGenericCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SetParam);
        cmd.setKey("min_compression_ratio");
        cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
                cb::mcbp::request::SetParamPayload::Type::Flush)));
        cmd.setValue(value);

        const auto resp = connection.execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

class DefaultBucketImpl : public TestBucketImpl {
public:
    explicit DefaultBucketImpl(std::string extraConfig)
        : TestBucketImpl(std::move(extraConfig)) {
    }

    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn) override {
        createEwbBucket(
                name, "default_engine.so", mergeConfigString(config), conn);
    }

    void createBucket(const std::string& name,
                      const std::string& config,
                      MemcachedConnection& conn) override {
        conn.createBucket(name, config, BucketType::Memcached);
    }

    std::string getName() const override {
        return "default_engine";
    }

    BucketType getType() const override {
        return BucketType::Memcached;
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
    EpBucketImpl(const boost::filesystem::path& test_directory,
                 std::string extraConfig)
        : TestBucketImpl(std::move(extraConfig)),
          dbPath(test_directory / "dbase") {
    }

    void setBucketCreateMode(BucketCreateMode mode) override {
        bucketCreateMode = mode;
    }

    BucketCreateMode bucketCreateMode = BucketCreateMode::Clean;

    void removeDbDir(const boost::filesystem::path& path) {
        if (exists(path)) {
            boost::filesystem::remove_all(path);
        }
    }

    void createBucket(const std::string& name,
                      const std::string& config,
                      MemcachedConnection& conn) override {
        const auto dbdir = dbPath / name;
        removeDbDir(dbdir);

        std::string settings = "dbname=" + dbdir.generic_string();
        if (!config.empty()) {
            settings += ";" + config;
        }

        conn.createBucket(name, settings, BucketType::Couchbase);
        conn.executeInBucket(name, [](auto& connection) {
            // Set the vBucket state. Set a single replica so that any
            // SyncWrites can be completed.
            nlohmann::json meta;
            meta["topology"] = nlohmann::json::array({{"active"}});
            connection.setVbucket(Vbid(0), vbucket_state_active, meta);

            auto auto_retry_tmpfail = connection.getAutoRetryTmpfail();
            connection.setAutoRetryTmpfail(true);
            auto resp = connection.execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::EnableTraffic});
            connection.setAutoRetryTmpfail(auto_retry_tmpfail);
            ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        });
    }

    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn) override {
        const auto dbdir = dbPath / name;
        if (bucketCreateMode == BucketCreateMode::Clean) {
            removeDbDir(dbdir);
        }

        std::string settings = "dbname=" + dbdir.generic_string();
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

        createEwbBucket(name, "ep.so", mergeConfigString(settings), conn);
        conn.executeInBucket(name, [](auto& connection) {
            // Set the vBucket state. Set a single replica so that any
            // SyncWrites can be completed.
            nlohmann::json meta;
            meta["topology"] = nlohmann::json::array({{"active"}});
            connection.setVbucket(Vbid(0), vbucket_state_active, meta);

            auto auto_retry_tmpfail = connection.getAutoRetryTmpfail();
            connection.setAutoRetryTmpfail(true);
            const auto resp = connection.execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::EnableTraffic});
            connection.setAutoRetryTmpfail(auto_retry_tmpfail);
            ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        });
    }

    std::string getName() const override {
        return "ep_engine";
    }

    BucketType getType() const override {
        return BucketType::Couchbase;
    }

    bool supportsOp(cb::mcbp::ClientOpcode cmd) const override {
        switch (cmd) {
        case cb::mcbp::ClientOpcode::Flush:
        case cb::mcbp::ClientOpcode::Flushq:
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
    const boost::filesystem::path dbPath;
};

class McdEnvironmentImpl : public McdEnvironment {
public:
    McdEnvironmentImpl(bool manageSSL_,
                       const std::string& engineName,
                       const std::string& engineConfig)
        : test_directory(absolute(boost::filesystem::path(
                  cb::io::mkdtemp("memcached_testapp.")))),
          isasl_file_name(test_directory / "cbsaslpw.json"),
          configuration_file(test_directory / "memcached.json"),
          portnumber_file(test_directory / "ports.json"),
          rbac_file_name(test_directory / "rbac.json"),
          audit_file_name(test_directory / "audit.cfg"),
          audit_log_dir(test_directory / "audittrail"),
          minidump_dir(test_directory / "crash"),
          log_dir(test_directory / "log"),
          manageSSL(manageSSL_),
          ipaddresses(cb::net::getIpAddresses(false)) {
        create_directories(minidump_dir);
        create_directories(log_dir);

        if (manageSSL) {
            initialize_openssl();
        }

        // We need to set MEMCACHED_UNIT_TESTS to enable the use of
        // the ewouldblock engine..
        setenv("MEMCACHED_UNIT_TESTS", "true", 1);
        setenv("MEMCACHED_TOP_KEYS", "10", 1);

        if (engineName == "default") {
            std::string config = "keep_deleted=true";
            if (!engineConfig.empty()) {
                config += ";" + engineConfig;
            }
            testBucket = std::make_unique<DefaultBucketImpl>(config);
        } else if (engineName == "ep") {
            testBucket = std::make_unique<EpBucketImpl>(test_directory,
                                                        engineConfig);
        } else {
            throw std::invalid_argument("Unknown engine '" + engineName +
                                        "' Options are 'default' and 'ep'");
        }
        // I shouldn't need to use string() here, but it fails to compile on
        // windows without it:
        //
        // cannot convert argument 2 from
        // 'const boost::filesystem::path::value_type *' to 'const char *'
        setenv("CBSASL_PWFILE", isasl_file_name.generic_string().c_str(), 1);
        setupPasswordDatabase();

        // Some of the tests wants to modify the RBAC file so we need
        // to make a local copy they can operate on
        const auto input_file =
                cb::io::sanitizePath(SOURCE_ROOT "/tests/testapp/rbac.json");
        rbac_data = nlohmann::json::parse(cb::io::loadFile(input_file));
        rewriteRbacFileImpl();

        // And we need an audit daemon configuration
        const auto descr =
                (test_directory.parent_path() / "auditd").generic_string();
        audit_config = {
                {"version", 2},
                {"uuid", "this_is_the_uuid"},
                {"auditd_enabled", false},
                {"rotate_interval", 1440},
                {"rotate_size", 20971520},
                {"buffered", false},
                {"log_path", audit_log_dir.generic_string()},
                {"descriptors_path", descr},
                {"sync", nlohmann::json::array()},
                {"disabled", nlohmann::json::array()},
                {"event_states",
                 {{std::to_string(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED),
                   "enabled"}}},
                {"filtering_enabled", false},
                {"disabled_userids", nlohmann::json::array()}};
        rewriteAuditConfigImpl();
    }

    std::string getPassword(std::string_view user) const override {
        auto iter = users.find(std::string{user});
        if (iter == users.end()) {
            throw std::runtime_error("getPassword(): Unknown user: " +
                                     std::string{user});
        }
        return iter->second;
    }

    const std::unordered_map<std::string, std::string> users{
            {"@admin", "password"},
            {"@fts", "<#w`?D4QwY/x%j8M"},
            {"bucket-1", "1S|=,%#x1"},
            {"bucket-2", "secret"},
            {"bucket-3", "1S|=,%#x1"},
            {"smith", "smithpassword"},
            {"larry", "larrypassword"},
            {"legacy", "new"},
            {"jones", "jonespassword"},
            {"Luke", "Skywalker"}};

    void setupPasswordDatabase() {
        // Write the initial password database:
        using cb::sasl::pwdb::User;
        using cb::sasl::pwdb::UserFactory;
        using cb::sasl::pwdb::user::Limits;

        // Reduce the iteration count to speed up the unit tests
        UserFactory::setDefaultHmacIterationCount(10);

        for (const auto& [u, p] : users) {
            if (u != "jones") {
                passwordDatabase.upsert(UserFactory::create(u, p));
            }
        }

        auto jones = UserFactory::create("jones", "jonespassword");
        Limits limits;
        limits.egress_mib_per_min = 1;
        limits.ingress_mib_per_min = 1;
        limits.num_ops_per_min = 100;
        limits.num_connections = 10;
        jones.setLimits(limits);
        passwordDatabase.upsert(std::move(jones));

        std::ofstream cbsasldb(isasl_file_name.generic_string());
        cbsasldb << passwordDatabase.to_json() << std::endl;
    }

    void terminate(int exitcode) override {
        if (manageSSL) {
            shutdown_openssl();
        }

        unsetenv("MEMCACHED_UNIT_TESTS");
        unsetenv("MEMCACHED_TOP_KEYS");
        unsetenv("CBSASL_PWFILE");

        // If exit-code != EXIT_SUCCESS it means that we had at least one
        // failure. When running on the CV machines we might have information
        // in the log files so lets dump the last 8k of the log file to give
        // the user more information
        if (exitcode != EXIT_SUCCESS) {
            // We've set the cycle size to be 200M so we should expect
            // only a single log file (but for simplicity just iterate
            // over them all and print the last 8k of each file
            std::cerr << "Last 8k of the log files" << std::endl
                      << "========================" << std::endl;
            for (const auto& p :
                 boost::filesystem::directory_iterator(log_dir)) {
                if (is_regular_file(p)) {
                    auto content = cb::io::loadFile(p.path().generic_string());
                    if (content.size() > 8192) {
                        content = content.substr(
                                content.find('\n', content.size() - 8192));
                    }
                    std::cerr << p.path().generic_string() << std::endl
                              << content << std::endl
                              << "-----------------------------" << std::endl;
                }
            }
        }

        bool cleanup = true;
        for (const auto& p :
             boost::filesystem::directory_iterator(minidump_dir)) {
            if (is_regular_file(p)) {
                cleanup = false;
                break;
            }
        }

        if (cleanup) {
            for (int ii = 0; ii < 100; ++ii) {
                try {
                    boost::filesystem::remove_all(test_directory);
                    break;
                } catch (const std::exception& e) {
                    std::cerr << "Failed to remove: "
                              << test_directory.generic_string() << ": "
                              << e.what() << std::endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds{20});
                }
            }
        } else {
            std::cerr << "Test directory " << test_directory.generic_string()
                      << " not removed as minidump files exists" << std::endl;
        }

        std::exit(exitcode);
    }

    std::string getAuditFilename() const override {
        return audit_file_name.generic_string();
    }

    std::string getAuditLogDir() const override {
        return audit_log_dir.generic_string();
    }

    nlohmann::json& getAuditConfig() override {
        return audit_config;
    }

    void rewriteAuditConfigImpl() {
        std::ofstream out(audit_file_name.generic_string());
        out << audit_config.dump(2) << std::endl;
        out.close();
    }

    void rewriteAuditConfig() override {
        rewriteAuditConfigImpl();
    }

    std::string getRbacFilename() const override {
        return rbac_file_name.generic_string();
    }

    nlohmann::json& getRbacConfig() override {
        return rbac_data;
    }

    void rewriteRbacFileImpl() {
        std::ofstream out(rbac_file_name.generic_string());
        out << rbac_data.dump(2) << std::endl;
        out.close();
    }

    void rewriteRbacFile() override {
        rewriteRbacFileImpl();
    }

    TestBucketImpl& getTestBucket() override {
        return *testBucket;
    }

    std::string getTestDir() const override {
        return test_directory.generic_string();
    }

    std::string getDbPath() const override {
        return static_cast<EpBucketImpl*>(testBucket.get())
                ->dbPath.generic_string();
    }

    std::string getConfigurationFile() const override {
        return configuration_file.generic_string();
    }

    std::string getPortnumberFile() const override {
        return portnumber_file.generic_string();
    }

    std::string getMinidumpDir() const override {
        return minidump_dir.generic_string();
    }

    std::string getLogDir() const override {
        return log_dir.generic_string();
    }

    std::string getLogFilePattern() const override {
        return (log_dir / "memcached").generic_string();
    }

    cb::sasl::pwdb::MutablePasswordDatabase& getPasswordDatabase() override {
        return passwordDatabase;
    }

    bool haveIPv4() const override {
        return !ipaddresses.first.empty();
    }

    bool haveIPv6() const override {
        return !ipaddresses.second.empty();
    }

    void refreshPasswordDatabase(MemcachedConnection& connection) override {
        std::ofstream cbsasldb(isasl_file_name.generic_string());
        cbsasldb << passwordDatabase.to_json() << std::endl;
        cbsasldb.close();
        auto rsp = connection.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::IsaslRefresh});
        if (!rsp.isSuccess()) {
            throw ConnectionError(
                    "refreshPasswordDatabase: Failed to reload database", rsp);
        }
    }

private:
    const boost::filesystem::path test_directory;
    const boost::filesystem::path isasl_file_name;
    const boost::filesystem::path configuration_file;
    const boost::filesystem::path portnumber_file;
    const boost::filesystem::path rbac_file_name;
    const boost::filesystem::path audit_file_name;
    const boost::filesystem::path audit_log_dir;
    const boost::filesystem::path minidump_dir;
    const boost::filesystem::path log_dir;
    const bool manageSSL;
    /// first entry is IPv4 addresses, second is IPv6
    /// (see cb::net::getIPAdresses)
    const std::pair<std::vector<std::string>, std::vector<std::string>>
            ipaddresses;

    nlohmann::json audit_config;
    nlohmann::json rbac_data;
    std::unique_ptr<TestBucketImpl> testBucket;
    cb::sasl::pwdb::MutablePasswordDatabase passwordDatabase;
};

McdEnvironment* McdEnvironment::create(bool manageSSL_,
                                       const std::string& engineName,
                                       const std::string& engineConfig) {
    return new McdEnvironmentImpl(manageSSL_, engineName, engineConfig);
}
