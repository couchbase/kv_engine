/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "dek/manager.h"

#include <cbsasl/password_database.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>

namespace cb::crypto {
struct DataEncryptionKey;
}
class MemcachedConnection;
enum class BucketType : uint8_t;

namespace cb::mcbp {
enum class ClientOpcode : uint8_t;
}

namespace cb::sasl::pwdb {
class MutablePasswordDatabase;
}

/**
 * The test bucket which tests are being run against.
 */
class TestBucketImpl {
public:
    TestBucketImpl(const std::filesystem::path& test_directory,
                   std::string extraConfig);

    virtual ~TestBucketImpl() = default;

    /**
     * Create a bucket with the provided name and the provided
     * configuration (in addition to the "mandatory" configuration
     * attributes).
     *
     * @param name the name to use for the bucket
     * @param config the configuration to add
     * @param conn the connection to use to create the bucket
     */
    void createBucket(const std::string& name,
                      const std::string& config,
                      MemcachedConnection& conn);

    /**
     * Create the named bucket to be controlled under
     * ewouldblock engine. See createBucket for a description
     * of the parameters.
     */
    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn);

    enum class BucketCreateMode { Clean, AllowRecreate };

    void setBucketCreateMode(BucketCreateMode mode) {
        bucketCreateMode = mode;
    }

    [[nodiscard]] constexpr static size_t getMaximumDocSize() {
        return 20 * 1024 * 1024;
    }

    [[nodiscard]] bool isFullEviction() const;

    [[nodiscard]] std::filesystem::path getDbPath() const {
        return dbPath;
    }

    /**
     * Make sure that xattr is enabled / disabled in the named bucket
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value set to true to enable, false to disable
     */
    static void setXattrEnabled(MemcachedConnection& conn,
                                const std::string& bucketName,
                                bool value);

    /**
     * Set the compression mode for the named bucket.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value The mode the bucket should use
     */
    static void setCompressionMode(MemcachedConnection& conn,
                                   const std::string& bucketName,
                                   const std::string& value);

    /**
     * Set the minimum compression ratio for the named bucket.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value The minimum compression ratio to use
     */
    static void setMinCompressionRatio(MemcachedConnection& conn,
                                       const std::string& bucketName,
                                       const std::string& value);

    /**
     * Set the mutation_mem_ratio in EP config.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param value The new param value
     */
    static void setMutationMemRatio(MemcachedConnection& conn,
                                    const std::string& value);

    /**
     * Set a configuration param for the named bucket.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param paramName
     * @param paramValue
     * @param paramType See cb::mcbp::request::SetParamPayload::Type for
     * details.
     */
    static void setParam(MemcachedConnection& conn,
                         const std::string& bucketName,
                         const std::string& paramName,
                         const std::string& paramValue,
                         cb::mcbp::request::SetParamPayload::Type paramType);

    /// Get the extra config provided to the test bcuket
    std::string_view getExtraConfig() const {
        return extraConfig;
    }

    /// The keystore used to keep track of the encrypt keys for the bucket
    cb::crypto::KeyStore keystore;

protected:
    static void createEwbBucket(const std::string& name,
                                BucketType type,
                                std::string_view config,
                                MemcachedConnection& conn);

    [[nodiscard]] std::string getEncryptionConfig() const;

    /// Directory for any database files.
    const std::filesystem::path dbPath;

    std::string mergeConfigString(const std::string& next);

    std::string extraConfig;

    BucketCreateMode bucketCreateMode = BucketCreateMode::Clean;
};

/**
 * The test environment
 *
 * The environment is set up once before the first test is run, and
 * shut down after the last test is run.
 */
class McdEnvironment {
public:
    explicit McdEnvironment(std::string engineConfig);

    /**
     * Shut down the test environment, clean up the test directory and
     * terminate the process by calling std::exit(exitcode)
     *
     * @param exitcode The exit code from running all tests
     */
    void terminate(int exitcode);

    /// Get the data encryption key manager used in the test
    [[nodiscard]] cb::dek::Manager& getDekManager() {
        return *dek_manager;
    }

    /// Get the name of the configuration file used by the audit daemon.
    [[nodiscard]] std::filesystem::path getAuditFilename() const {
        return audit_file_name;
    }

    /// Get the name of the directory containing the audit logs
    [[nodiscard]] std::filesystem::path getAuditLogDir() const {
        return audit_log_dir;
    }

    /**
     * Get a handle to the current audit configuration so that you may
     * modify the configuration (you may write it to the audit configuration
     * file by calling <code>rewriteAuditConfig()</code>
     *
     * @return the root object of the audit configuration.
     */
    [[nodiscard]] nlohmann::json& getAuditConfig() {
        return audit_config;
    }

    /**
     * Dump the internal representation of the audit configuration
     * (returned by <code>getAuditConfig()</code>) to the configuration file
     * (returned by <getAuditFilename()</config>)
     */
    void rewriteAuditConfig();

    /// Get the name of the RBAC file used.
    [[nodiscard]] std::filesystem::path getRbacFilename() const {
        return rbac_file_name;
    }

    /**
     * @return The bucket type being tested.
     */
    [[nodiscard]] TestBucketImpl& getTestBucket() {
        return *testBucket;
    }

    /// Get the dbPath of the bucket currently being tested
    [[nodiscard]] std::filesystem::path getDbPath() const {
        return testBucket->getDbPath();
    }

    /// Get the name of the configuration file to use
    [[nodiscard]] std::filesystem::path getConfigurationFile() const {
        return configuration_file;
    }

    [[nodiscard]] std::filesystem::path getPortnumberFile() const {
        return portnumber_file;
    }

    [[nodiscard]] std::filesystem::path getMinidumpDir() const {
        return minidump_dir;
    }

    [[nodiscard]] std::filesystem::path getLogFilePattern() const {
        return log_dir / "memcached";
    }

    /// Iterate over all files in the log directory and for each file iterate
    /// over all lines in the file and call the provided callback
    void iterateLogLines(
            const std::function<bool(std::string_view line)>& callback) const;

    /// Iterate over the audit events and call the provided callback
    /// with each entry. The callback may return true to terminate
    /// iteration. The function returns if the callback terminated the
    /// iteration or not
    bool iterateAuditEvents(
            const std::function<bool(const nlohmann::json&)>& callback) const;

    /// Do we have support for IPv4 addresses on the machine
    [[nodiscard]] bool haveIPv4() const {
        return !ipaddresses.first.empty();
    }

    /// Do we have support for IPv6 addresses on the machine
    [[nodiscard]] bool haveIPv6() const {
        return !ipaddresses.second.empty();
    }

    /// Get the password for the requested user
    [[nodiscard]] std::string getPassword(std::string_view user) const;

protected:
    void setupPasswordDatabase();

    /**
     * Read a file which may be concurrently written do by someone else
     * causing a "partial" read to occur (and in the case of an encrypted
     * file this would be a problem as they're chunked).
     *
     * @param entity the entity used to locate the key
     * @param path file to read
     * @return the content up until we hit a partial read
     */
    [[nodiscard]] std::string readConcurrentUpdatedFile(
            cb::dek::Entity entity, const std::filesystem::path& path) const;

    const std::unordered_map<std::string, std::string> users{
            {"almighty", "bruce"},
            {"@admin", "password"},
            {"@fts", "<#w`?D4QwY/x%j8M"},
            {"bucket-1", "1S|=,%#x1"},
            {"bucket-2", "secret"},
            {"bucket-3", "1S|=,%#x1"},
            {"smith", "smithpassword"},
            {"larry", "larrypassword"},
            {"legacy", "new"},
            {"jones", "jonespassword"},
            {"Luke", "Skywalker"},
            {"Jane", "Pandoras Box"},
            {"UserWithoutProfile", "password"}};

    const std::filesystem::path test_directory;
    const std::filesystem::path isasl_file_name;
    const std::filesystem::path configuration_file;
    const std::filesystem::path portnumber_file;
    const std::filesystem::path rbac_file_name;
    const std::filesystem::path audit_file_name;
    const std::filesystem::path audit_log_dir;
    const std::filesystem::path minidump_dir;
    const std::filesystem::path log_dir;

    std::unique_ptr<cb::dek::Manager> dek_manager = cb::dek::Manager::create();

    /// first entry is IPv4 addresses, second is IPv6
    /// (see cb::net::getIPAdresses)
    const std::pair<std::vector<std::string>, std::vector<std::string>>
            ipaddresses;

    nlohmann::json audit_config;
    nlohmann::json rbac_data;
    std::unique_ptr<TestBucketImpl> testBucket;
    cb::sasl::pwdb::MutablePasswordDatabase passwordDatabase;
};

extern std::unique_ptr<McdEnvironment> mcd_env;
