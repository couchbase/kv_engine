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

#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <string>

class MemcachedConnection;

/**
 * The test bucket which tests are being run against.
 */
class TestBucketImpl {
public:
    explicit TestBucketImpl(std::string extraConfig = {})
        : extraConfig(std::move(extraConfig)) {
    }

    virtual void setUpBucket(const std::string& name,
                             const std::string& config,
                             MemcachedConnection& conn) = 0;

    enum class BucketCreateMode { Clean, AllowRecreate };
    virtual void setBucketCreateMode(BucketCreateMode) {
    }

    virtual ~TestBucketImpl() = default;

    virtual std::string getName() const = 0;

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
 * The test environment added to the Google Test Framework.
 *
 * The environment is set up once before the first test is run, and
 * shut down after the last test is run.
 */
class McdEnvironment : public ::testing::Environment {
public:
    /* In stand-alone mode we have to init/shutdown OpenSSL (i.e. manage it),
     * as the SetUp/TearDown methods only get called if at least
     * one Test is run; and we *need* to call shutdown_openssl() to
     * correctly free all memory allocated by OpenSSL's shared_library
     * constructor.  Therefore in this case we pass true into the McdEnvironment
     * constructor.
     *
     * @param manageSSL_ In embedded mode the memcached server is responsible
     *                   for init/shutdown of OpenSSL and therefore in this
     *                   case we pass false into the McdEnvironment constructor.
     * @param engineName The name of the engine which memcached will be started
     *                   with.
     * @param engineConfig Any additional engine config to pass when creating
     *                     buckets.
     */
    McdEnvironment(bool manageSSL_,
                   std::string engineName,
                   std::string engineConfig);

    ~McdEnvironment() override;

    /**
     * Create the test environment. This method is called automatically
     * from the Google Test Framework. You should not try to access any
     * of the members before this method is called. As long as you only
     * try to access this class from a test case you're on the safe side
     */
    void SetUp() override;

    /**
     * Tear down the test environment. This call invalidates the object
     * and calling the members may return rubbish.
     */
    void TearDown() override;

    /**
     * Get the name of the configuration file used by the audit daemon.
     *
     * @return the absolute path of the file containing the audit config
     */
    const std::string& getAuditFilename() const {
        return audit_file_name;
    }

    /**
     * Get the name of the directory containing the audit logs
     *
     * @return the absolute path of the directory containing the audit config
     */
    const std::string& getAuditLogDir() const {
        return audit_log_dir;
    }

    /**
     * Get a handle to the current audit configuration so that you may
     * modify the configuration (you may write it to the audit configuration
     * file by calling <code>rewriteAuditConfig()</code>
     *
     * @return the root object of the audit configuration.
     */
    nlohmann::json& getAuditConfig() {
        return audit_config;
    }

    /**
     * Dump the internal representation of the audit configuration
     * (returned by <code>getAuditConfig()</code>) to the configuration file
     * (returned by <getAuditFilename()</config>)
     */
    void rewriteAuditConfig();

    /**
     * Get the name of the RBAC file used.
     *
     * @return the absolute path of the file containing the RBAC data
     */
    const std::string& getRbacFilename() const {
        return rbac_file_name;
    }

    /**
     * Get a handle to the current RBAC configuration so that you may
     * modify the configuration (you may write it to the rbac config
     * file by calling <code>rewriteRbacFile()</code>
     *
     * @return the object containing the RBAC configuration
     */
    nlohmann::json& getRbacConfig() {
        return rbac_data;
    }

    /**
     * Dump the internal representation of the rbac configuration
     * (returned by <code>getRbacConfig()</code>) to the configuration file
     * (returned by <getRbacFilename()</config>)
     */
    void rewriteRbacFile();

    /**
     * @return The bucket type being tested.
     */
    TestBucketImpl& getTestBucket() {
        return *testBucket;
    }

    /**
     * @return The dbPath of a persistent bucket (throws if not persistent)
     */
    const std::string& getDbPath() const;

private:
    void SetupAuditFile();

    void SetupRbacFile();

    void SetupIsaslPw();

    std::string isasl_file_name;
    std::string rbac_file_name;
    std::string audit_file_name;
    std::string audit_log_dir;
    std::string cwd;
    nlohmann::json audit_config;
    nlohmann::json rbac_data;
    static char isasl_env_var[256];
    bool manageSSL;
    std::unique_ptr<TestBucketImpl> testBucket;
};

extern McdEnvironment* mcd_env;
