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
#include "config.h"

#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include "testapp_environment.h"
#include "utilities.h"

#include <cJSON_utils.h>
#include <platform/dirutils.h>
#include <platform/make_unique.h>
#include <platform/memorymap.h>
#include <platform/strerror.h>
#include <fstream>

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
    conn.authenticate("@admin", "password", "PLAIN");
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    BinprotResponse resp;
    cmd.setOp(PROTOCOL_BINARY_CMD_SET_PARAM);
    cmd.setKey("xattr_enabled");
    cmd.setExtrasValue<uint32_t>(htonl(protocol_binary_engine_param_flush));
    if (value) {
        cmd.setValue("true");
    } else {
        cmd.setValue("false");
    }

    conn.executeCommand(cmd, resp);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}

void TestBucketImpl::setCompressionMode(MemcachedConnection& conn,
                                        const std::string& bucketName,
                                        const std::string value) {
    conn.authenticate("@admin", "password", "PLAIN");
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    BinprotResponse resp;
    cmd.setOp(PROTOCOL_BINARY_CMD_SET_PARAM);
    cmd.setKey("compression_mode");
    cmd.setExtrasValue<uint32_t>(htonl(protocol_binary_engine_param_flush));
    cmd.setValue(value);

    conn.executeCommand(cmd, resp);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}

void TestBucketImpl::setMinCompressionRatio(MemcachedConnection& conn,
                                            const std::string& bucketName,
                                            const std::string value) {
    conn.authenticate("@admin", "password", "PLAIN");
    conn.selectBucket(bucketName);

    // Encode a set_flush_param (like cbepctl)
    BinprotGenericCommand cmd;
    BinprotResponse resp;
    cmd.setOp(PROTOCOL_BINARY_CMD_SET_PARAM);
    cmd.setKey("min_compression_ratio");
    cmd.setExtrasValue<uint32_t>(htonl(protocol_binary_engine_param_flush));
    cmd.setValue(value);

    conn.executeCommand(cmd, resp);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}

class DefaultBucketImpl : public TestBucketImpl {
public:
    DefaultBucketImpl(std::string extraConfig = {})
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

    bool supportsOp(protocol_binary_command cmd) const override {
        switch (cmd) {
            case PROTOCOL_BINARY_CMD_DCP_OPEN:
            case PROTOCOL_BINARY_CMD_DCP_ADD_STREAM:
            case PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM:
            case PROTOCOL_BINARY_CMD_DCP_STREAM_REQ:
            case PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG:
            case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
            case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
            case PROTOCOL_BINARY_CMD_DCP_MUTATION:
            case PROTOCOL_BINARY_CMD_DCP_DELETION:
            case PROTOCOL_BINARY_CMD_DCP_EXPIRATION:
            case PROTOCOL_BINARY_CMD_DCP_FLUSH:
            case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
            case PROTOCOL_BINARY_CMD_DCP_NOOP:
            case PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT:
            case PROTOCOL_BINARY_CMD_DCP_CONTROL:
            case PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT:
            case PROTOCOL_BINARY_CMD_SET_WITH_META:
            case PROTOCOL_BINARY_CMD_SETQ_WITH_META:
            case PROTOCOL_BINARY_CMD_ADD_WITH_META:
            case PROTOCOL_BINARY_CMD_ADDQ_WITH_META:
            case PROTOCOL_BINARY_CMD_DEL_WITH_META:
            case PROTOCOL_BINARY_CMD_DELQ_WITH_META:
            case PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC:
            case PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC:
            case PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG:
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
};

class EpBucketImpl : public TestBucketImpl {
public:
    EpBucketImpl(std::string extraConfig = {})
        : TestBucketImpl(extraConfig),
          dbPath("mc_testapp." + std::to_string(cb_getpid())) {
        // Cleanup any files from a previous run still on disk.
        try {
            cb::io::rmrf(dbPath);
        } catch (...) { /* nothing exists */
        }
    }

    ~EpBucketImpl() {
        // Cleanup any files created.
        try {
            cb::io::rmrf(dbPath);
        } catch (...) { /* nothing exists */
        }
    }

    void setUpBucket(const std::string& name,
                     const std::string& config,
                     MemcachedConnection& conn) override {
        std::string settings = "dbname=" + dbPath + "/" + name;
        if (!config.empty()) {
            settings += ";" + config;
        }
        if (!extraConfig.empty()) {
            settings += ";" + extraConfig;
        }
        createEwbBucket(name, "ep.so", settings, conn);

        BinprotGenericCommand cmd;
        BinprotResponse resp;

        cmd.setOp(PROTOCOL_BINARY_CMD_SELECT_BUCKET);
        cmd.setKey(name);
        conn.executeCommand(cmd, resp);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

        cmd.clear();
        resp.clear();

        cmd.setOp(PROTOCOL_BINARY_CMD_SET_VBUCKET);
        cmd.setExtrasValue<uint32_t>(htonl(1));

        conn.executeCommand(cmd, resp);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

        do {
            cmd.clear();
            resp.clear();
            cmd.setOp(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
            // Enable traffic
            conn.executeCommand(cmd, resp);
        } while (resp.getStatus() == PROTOCOL_BINARY_RESPONSE_ETMPFAIL);

        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    }

    std::string getName() const override {
        return "ep_engine";
    }

    bool supportsOp(protocol_binary_command cmd ) const override {
        switch (cmd) {
        case PROTOCOL_BINARY_CMD_FLUSH:
        case PROTOCOL_BINARY_CMD_FLUSHQ:
            // TODO: Flush *is* supported by ep-engine, but it needs traffic
            // disabling before it's permitted.
        case PROTOCOL_BINARY_CMD_SCRUB:
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
        audit_config.reset(cJSON_CreateObject());
        cJSON_AddNumberToObject(audit_config.get(), "version", 1);
        cJSON_AddFalseToObject(audit_config.get(), "auditd_enabled");
        cJSON_AddNumberToObject(audit_config.get(), "rotate_interval", 1440);
        cJSON_AddNumberToObject(audit_config.get(), "rotate_size", 20971520);
        cJSON_AddFalseToObject(audit_config.get(), "buffered");
        cJSON_AddStringToObject(audit_config.get(), "log_path",
                                audit_log_dir.c_str());
        cJSON_AddStringToObject(audit_config.get(), "descriptors_path",
                                descriptor.c_str());
        cJSON_AddItemToObject(audit_config.get(), "sync", cJSON_CreateArray());
        cJSON_AddItemToObject(audit_config.get(), "disabled",
                              cJSON_CreateArray());
    } catch (std::exception& e) {
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
        std::string audit_text = to_string(audit_config);
        std::ofstream out(audit_file_name);
        out.write(audit_text.c_str(), audit_text.size());
        out.close();
    } catch (std::exception& e) {
        FAIL() << "Failed to store audit configuration: " << e.what();
    }
}

void McdEnvironment::SetupRbacFile() {
    std::string input_file{SOURCE_ROOT};
    input_file.append("/tests/testapp/rbac.json");
#ifdef WIN32
    std::replace(input_file.begin(), input_file.end(), '\\', '/');
#endif
    cb::MemoryMappedFile map(input_file.c_str(),
                             cb::MemoryMappedFile::Mode::RDONLY);
    map.open();
    std::string input(reinterpret_cast<char*>(map.getRoot()),
                      map.getSize());
    map.close();

    rbac_data.reset(cJSON_Parse(input.c_str()));

    rbac_file_name = cwd + "/" + cb::io::mktemp("rbac.json.XXXXXX");
    rewriteRbacFile();
}

void McdEnvironment::rewriteRbacFile() {
    try {
        std::ofstream out(rbac_file_name);
        out << to_string(rbac_data, true) << std::endl;
        out.close();
    } catch (std::exception& e) {
        FAIL() << "Failed to store rbac configuration: " << e.what();
    }
}

char McdEnvironment::isasl_env_var[256];
