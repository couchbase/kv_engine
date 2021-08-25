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
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <string>

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
    explicit TestBucketImpl(std::string extraConfig)
        : extraConfig(std::move(extraConfig)) {
    }

    virtual void createBucket(const std::string& name,
                              const std::string& config,
                              MemcachedConnection& conn) = 0;

    virtual void setUpBucket(const std::string& name,
                             const std::string& config,
                             MemcachedConnection& conn) = 0;

    enum class BucketCreateMode { Clean, AllowRecreate };
    virtual void setBucketCreateMode(BucketCreateMode) {
    }

    virtual ~TestBucketImpl() = default;

    virtual std::string getName() const = 0;

    virtual BucketType getType() const = 0;

    // Whether the given bucket type supports an opcode
    virtual bool supportsOp(cb::mcbp::ClientOpcode cmd) const = 0;
    virtual bool supportsPrivilegedBytes() const = 0;
    virtual size_t getMaximumDocSize() const = 0;
    virtual bool supportsLastModifiedVattr() const = 0;
    virtual bool supportsPersistence() const = 0;
    virtual bool supportsSyncWrites() const = 0;
    virtual bool supportsCollections() const = 0;

    /**
     * Make sure that xattr is enabled / disabled in the named bucket
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value set to true to enable, false to disable
     */
    void setXattrEnabled(MemcachedConnection& conn,
                         const std::string& bucketName,
                         bool value);

    /**
     * Set the compression mode for the named bucket.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value The mode the bucket should use
     */
    void setCompressionMode(MemcachedConnection& conn,
                            const std::string& bucketName,
                            const std::string& value);

    /**
     * Set the minimum compression ratio for the named bucket.
     *
     * @param conn The connection to use (must have admin privileges)
     * @param bucketName The name of the bucket to modify
     * @param value The the minimum compression ratio to use
     */
    void setMinCompressionRatio(MemcachedConnection& conn,
                                const std::string& bucketName,
                                const std::string& value);

protected:
    static void createEwbBucket(const std::string& name,
                                const std::string& plugin,
                                const std::string& config,
                                MemcachedConnection& conn);

    std::string mergeConfigString(const std::string& next);

    std::string extraConfig;
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

    static McdEnvironment* create(bool manageSSL_,
                                  const std::string& engineName,
                                  const std::string& engineConfig);

    /**
     * Shut down the test environment, clean up the test directory and
     * terminate the process by calling std::exit(exitcode)
     *
     * @param exitcode The exit code from running all tests
     */
    virtual void terminate(int exitcode) = 0;

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

    /**
     * @return The dbPath of a persistent bucket (throws if not persistent)
     */
    virtual std::string getDbPath() const = 0;

    /// Get the name of the configuration file to use
    virtual std::string getConfigurationFile() const = 0;

    virtual std::string getPortnumberFile() const = 0;

    virtual std::string getMinidumpDir() const = 0;

    virtual std::string getLogDir() const = 0;

    virtual std::string getLogFilePattern() const = 0;

    virtual cb::sasl::pwdb::MutablePasswordDatabase& getPasswordDatabase() = 0;

    /// Do we have support for IPv4 addresses on the machine
    virtual bool haveIPv4() const = 0;

    /// Do we have support for IPv6 addresses on the machine
    virtual bool haveIPv6() const = 0;

    /// Write the current password database to disk and tell memcached to
    /// reload the file
    ///
    /// @param connection a connection to the server (must have admin
    ///                   privileges)
    /// @throws std::exception for errors (file io, server failure etc)
    virtual void refreshPassordDatabase(MemcachedConnection& connection) = 0;
};

extern std::unique_ptr<McdEnvironment> mcd_env;
