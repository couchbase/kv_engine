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

#include <memcached/protocol_binary.h>
#include <nlohmann/json_fwd.hpp>
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

namespace cb::dek {
class Manager;
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
    constexpr size_t getMaximumDocSize() const {
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

    /// The key to use for encryption@rest
    std::vector<std::shared_ptr<cb::crypto::DataEncryptionKey>> encryption_keys;

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
    virtual ~McdEnvironment() = default;

    static std::unique_ptr<McdEnvironment> create(std::string engineConfig);

    /**
     * Shut down the test environment, clean up the test directory and
     * terminate the process by calling std::exit(exitcode)
     *
     * @param exitcode The exit code from running all tests
     */
    virtual void terminate(int exitcode) = 0;

    /**
     *
     * Get the data encryption key manager used in the test
     */
    virtual cb::dek::Manager& getDekManager() = 0;

    /**
     * Get the name of the configuration file used by the audit daemon.
     *
     * @return the absolute path of the file containing the audit config
     */
    virtual std::string getAuditFilename() const = 0;

    /**
     * Get the name of the directory containing the audit logs
     *
     * @return the absolute path of the directory containing the audit config
     */
    virtual std::string getAuditLogDir() const = 0;

    /**
     * Get a handle to the current audit configuration so that you may
     * modify the configuration (you may write it to the audit configuration
     * file by calling <code>rewriteAuditConfig()</code>
     *
     * @return the root object of the audit configuration.
     */
    virtual nlohmann::json& getAuditConfig() = 0;

    /**
     * Dump the internal representation of the audit configuration
     * (returned by <code>getAuditConfig()</code>) to the configuration file
     * (returned by <getAuditFilename()</config>)
     */
    virtual void rewriteAuditConfig() = 0;

    /**
     * Get the name of the RBAC file used.
     *
     * @return the absolute path of the file containing the RBAC data
     */
    virtual std::string getRbacFilename() const = 0;

    /**
     * Get a handle to the current RBAC configuration so that you may
     * modify the configuration (you may write it to the rbac config
     * file by calling <code>rewriteRbacFile()</code>
     *
     * @return the object containing the RBAC configuration
     */
    virtual nlohmann::json& getRbacConfig() = 0;

    /**
     * Dump the internal representation of the rbac configuration
     * (returned by <code>getRbacConfig()</code>) to the configuration file
     * (returned by <getRbacFilename()</config>)
     */
    virtual void rewriteRbacFile() = 0;

    /**
     * @return The bucket type being tested.
     */
    virtual TestBucketImpl& getTestBucket() = 0;

    /// @returns the base directory this test uses.
    virtual std::string getTestDir() const = 0;

    /**
     * @return The dbPath of a persistent bucket (throws if not persistent)
     */
    virtual std::string getDbPath() const = 0;

    /// Get the name of the configuration file to use
    virtual std::string getConfigurationFile() const = 0;

    virtual std::string getPortnumberFile() const = 0;

    virtual std::string getMinidumpDir() const = 0;

    virtual std::string getLogDir() const = 0;

    virtual void iterateLogLines(
            const std::function<bool(std::string_view line)>& callback)
            const = 0;

    /// Iterate over the audit events and call the provided callback
    /// with each entry. The callback may return true to terminate
    /// iteration. The function returns if the callback terminated the
    /// iteration or not
    virtual bool iterateAuditEvents(
            const std::function<bool(const nlohmann::json&)>& callback)
            const = 0;

    virtual std::string getLogFilePattern() const = 0;

    /// Do we have support for IPv4 addresses on the machine
    virtual bool haveIPv4() const = 0;

    /// Do we have support for IPv6 addresses on the machine
    virtual bool haveIPv6() const = 0;

    /// Get the password for the requested user
    virtual std::string getPassword(std::string_view user) const = 0;
};

extern std::unique_ptr<McdEnvironment> mcd_env;
