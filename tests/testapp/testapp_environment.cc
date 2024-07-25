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
#include <auditd/couchbase_audit_events.h>
#include <cbcrypto/common.h>
#include <cbcrypto/file_reader.h>
#include <cbsasl/password_database.h>
#include <cbsasl/user.h>
#include <dek/manager.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/split_string.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/string_utilities.h>
#include <filesystem>
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
    auto idx = key.find('=');
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
                                     BucketType type,
                                     const std::string_view config,
                                     MemcachedConnection& conn) {
    auto cfg = fmt::format("ewb_real_engine={}", bucket_type_to_module(type));
    if (!config.empty()) {
        cfg = fmt::format("{};{}", cfg, config);
    }
    conn.createBucket(name, cfg, BucketType::EWouldBlock);
}

// Both memcached and ep-engine buckets support set_param for xattr on/off
void TestBucketImpl::setXattrEnabled(MemcachedConnection& conn,
                                     const std::string& bucketName,
                                     bool value) {
    setParam(conn,
             bucketName,
             "xattr_enabled",
             value ? "true" : "false",
             cb::mcbp::request::SetParamPayload::Type::Flush);
}

void TestBucketImpl::setCompressionMode(MemcachedConnection& conn,
                                        const std::string& bucketName,
                                        const std::string& value) {
    setParam(conn,
             bucketName,
             "compression_mode",
             value,
             cb::mcbp::request::SetParamPayload::Type::Flush);
}

void TestBucketImpl::setMinCompressionRatio(MemcachedConnection& conn,
                                            const std::string& bucketName,
                                            const std::string& value) {
    setParam(conn,
             bucketName,
             "min_compression_ratio",
             value,
             cb::mcbp::request::SetParamPayload::Type::Flush);
}

void TestBucketImpl::setMutationMemRatio(MemcachedConnection& conn,
                                         const std::string& value) {
    BinprotGenericCommand cmd{
            cb::mcbp::ClientOpcode::SetParam, "mutation_mem_ratio", value};
    cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
            cb::mcbp::request::SetParamPayload::Type::Flush)));
    ASSERT_EQ(cb::mcbp::Status::Success, conn.execute(cmd).getStatus());
}

void TestBucketImpl::setParam(
        MemcachedConnection& conn,
        const std::string& bucketName,
        const std::string& paramName,
        const std::string& paramValue,
        cb::mcbp::request::SetParamPayload::Type paramType) {
    conn.executeInBucket(bucketName, [&](auto& connection) {
        BinprotGenericCommand cmd{
                cb::mcbp::ClientOpcode::SetParam, paramName, paramValue};
        cmd.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(paramType)));
        ASSERT_EQ(cb::mcbp::Status::Success,
                  connection.execute(cmd).getStatus());
    });
}

TestBucketImpl::TestBucketImpl(const std::filesystem::path& test_directory,
                               std::string extraConfig)
    : dbPath(test_directory / "dbase"), extraConfig(std::move(extraConfig)) {
    encryption_keys.emplace_back(cb::crypto::DataEncryptionKey::generate());
}

void TestBucketImpl::createBucket(const std::string& name,
                                  const std::string& config,
                                  MemcachedConnection& conn) {
    const auto dbdir = dbPath / name;
    if (exists(dbdir)) {
        remove_all(dbdir);
    }

    std::string settings = "dbname=" + dbdir.generic_string();
    if (!config.empty()) {
        settings += ";" + config;
    }

    settings = fmt::format("{};encryption={}", settings, getEncryptionConfig());

    conn.createBucket(name, settings, BucketType::Couchbase);
    conn.executeInBucket(name, [](auto& connection) {
        // Set the vBucket state. Set a single replica so that any
        // SyncWrites can be completed.
        nlohmann::json meta;
        meta["topology"] = nlohmann::json::array({{"active"}});
        connection.setVbucket(Vbid(0), vbucket_state_active, meta);

        auto auto_retry_tmpfail = connection.getAutoRetryTmpfail();
        connection.setAutoRetryTmpfail(true);
        auto resp = connection.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::EnableTraffic});
        connection.setAutoRetryTmpfail(auto_retry_tmpfail);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

void TestBucketImpl::setUpBucket(const std::string& name,
                                 const std::string& config,
                                 MemcachedConnection& conn) {
    const auto dbdir = dbPath / name;
    if (bucketCreateMode == BucketCreateMode::Clean) {
        if (exists(dbdir)) {
            remove_all(dbdir);
        }
    }

    std::string settings = "dbname=" + dbdir.generic_string();
    // Disable bloom_filters - for all memcahed testapp tests we want
    // to see things like gets of tombstones going to disk and not
    // getting skipped (to ensure correct EWOULDBLOCK handling etc).
    settings += ";bfilter_enabled=false";
    if (folly::kIsSanitizeThread) {
        // Reduce vBucket count to 16 - TSan cannot handle more than 64
        // mutexes being locked at once in a single thread, and Bucket
        // pause() / resume() functionality relies on locking all vBucket
        // mutexes - i.e. one per vBucket.
        // (While running with 32 vBuckets would appear to be ok, TSan
        //  still reports errors as it hits the 64 mutex limit somehow,
        //  so reduce to 16 vBuckets.)
        settings += ";max_vbuckets=16";
    }
    if (!config.empty()) {
        settings += ";" + config;
    }
    settings = fmt::format("{};encryption={}", settings, getEncryptionConfig());

    createEwbBucket(
            name, BucketType::Couchbase, mergeConfigString(settings), conn);
    conn.executeInBucket(name, [](auto& connection) {
        // Set the vBucket state. Set a single replica so that any
        // SyncWrites can be completed.
        nlohmann::json meta;
        meta["topology"] = nlohmann::json::array({{"active"}});
        connection.setVbucket(Vbid(0), vbucket_state_active, meta);

        auto auto_retry_tmpfail = connection.getAutoRetryTmpfail();
        connection.setAutoRetryTmpfail(true);
        const auto resp = connection.execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::EnableTraffic});
        connection.setAutoRetryTmpfail(auto_retry_tmpfail);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

bool TestBucketImpl::isFullEviction() const {
    return extraConfig.find("item_eviction_policy=full_eviction") !=
           std::string::npos;
}

std::string TestBucketImpl::getEncryptionConfig() const {
    nlohmann::json encryption = {{"active", encryption_keys.front()->id}};
    auto keys = nlohmann::json::array();
    for (const auto& key : encryption_keys) {
        keys.push_back(*key);
    }
    encryption["keys"] = std::move(keys);
    return encryption.dump();
}

McdEnvironment::McdEnvironment(std::string engineConfig)
    : test_directory(absolute(
              std::filesystem::path(cb::io::mkdtemp("memcached_testapp.")))),
      isasl_file_name(test_directory / "cbsaslpw.json"),
      configuration_file(test_directory / "memcached.json"),
      portnumber_file(test_directory / "ports.json"),
      rbac_file_name(test_directory / "rbac.json"),
      audit_file_name(test_directory / "audit.cfg"),
      audit_log_dir(test_directory / "audittrail"),
      minidump_dir(test_directory / "crash"),
      log_dir(test_directory / "log"),
      ipaddresses(cb::net::getIpAddresses(false)) {
    create_directories(minidump_dir);
    create_directories(log_dir);

    dek_manager->setActive(cb::dek::Entity::Logs,
                           cb::crypto::DataEncryptionKey::generate());
    dek_manager->setActive(cb::dek::Entity::Audit,
                           cb::crypto::DataEncryptionKey::generate());
    dek_manager->setActive(cb::dek::Entity::Config,
                           cb::crypto::DataEncryptionKey::generate());

    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    setenv("MEMCACHED_UNIT_TESTS", "true", 1);

    testBucket = std::make_unique<TestBucketImpl>(test_directory,
                                                  std::move(engineConfig));
    // I shouldn't need to use string() here, but it fails to compile on
    // windows without it:
    //
    // cannot convert argument 2 from
    // 'const std::filesystem::path::value_type *' to 'const char *'
    setenv("CBSASL_PWFILE", isasl_file_name.generic_string().c_str(), 1);
    setupPasswordDatabase();

    // Some of the tests wants to modify the RBAC file so we need
    // to make a local copy they can operate on
    const auto input_file =
            cb::io::sanitizePath(SOURCE_ROOT "/tests/testapp/rbac.json");
    rbac_data = nlohmann::json::parse(cb::io::loadFile(input_file));
    dek_manager->save(
            cb::dek::Entity::Config, rbac_file_name, rbac_data.dump());

    // And we need an audit daemon configuration
    audit_config = {{"version", 2},
                    {"uuid", "this_is_the_uuid"},
                    {"auditd_enabled", false},
                    {"rotate_interval", 1440},
                    {"rotate_size", 20971520},
                    {"buffered", false},
                    {"log_path", audit_log_dir.generic_string()},
                    {"sync", nlohmann::json::array()},
                    {"disabled", nlohmann::json::array()},
                    {"event_states",
                     {{std::to_string(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED),
                       "enabled"}}},
                    {"filtering_enabled", false},
                    {"disabled_userids", nlohmann::json::array()}};
    rewriteAuditConfig();
}

void McdEnvironment::setupPasswordDatabase() {
    // Write the initial password database:
    using cb::sasl::pwdb::User;
    using cb::sasl::pwdb::UserFactory;

    // Reduce the iteration count to speed up the unit tests
    UserFactory::setDefaultScramShaIterationCount(10);

    for (const auto& [u, p] : users) {
        passwordDatabase.upsert(UserFactory::create(u, p));
    }

    std::ofstream cbsasldb(isasl_file_name.generic_string());
    cbsasldb << nlohmann::json(passwordDatabase) << std::endl;
}

void McdEnvironment::terminate(int exitcode) {
    unsetenv("MEMCACHED_UNIT_TESTS");
    unsetenv("CBSASL_PWFILE");

    // If exit-code != EXIT_SUCCESS it means that we had at least one
    // failure. When running on the CV machines we might have information
    // in the log files so lets dump the last 8k of the log file to give
    // the user more information
    if (exitcode != EXIT_SUCCESS) {
        constexpr std::size_t max_log_size = 64 * 1024;

        // We've set the cycle size to be 200M, so we should expect
        // only a single log file (but for simplicity just iterate
        // over them all and print the last xk of each file
        std::cerr << "Last " << max_log_size / 1024 << "k of the log files"
                  << std::endl
                  << "========================" << std::endl;
        for (const auto& p : std::filesystem::directory_iterator(log_dir)) {
            if (is_regular_file(p)) {
                auto content =
                        readConcurrentUpdatedFile(cb::dek::Entity::Logs, p);
                if (content.size() > max_log_size) {
                    content = content.substr(
                            content.find('\n', content.size() - max_log_size));
                }
                std::cerr << p.path().generic_string() << std::endl
                          << content << std::endl
                          << "-----------------------------" << std::endl;
            }
        }
    }

    bool cleanup = true;
    for (const auto& p : std::filesystem::directory_iterator(minidump_dir)) {
        if (is_regular_file(p)) {
            cleanup = false;
            break;
        }
    }

    if (cleanup) {
        for (int ii = 0; ii < 100; ++ii) {
            try {
                std::filesystem::remove_all(test_directory);
                break;
            } catch (const std::exception& e) {
                std::cerr << "Failed to remove: "
                          << test_directory.generic_string() << ": " << e.what()
                          << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds{20});
            }
        }
    } else {
        std::cerr << "Test directory " << test_directory.generic_string()
                  << " not removed as minidump files exists" << std::endl;
    }

    std::exit(exitcode);
}

void McdEnvironment::rewriteAuditConfig() {
    dek_manager->save(
            cb::dek::Entity::Config, audit_file_name, audit_config.dump());
}

std::string McdEnvironment::getPassword(std::string_view user) const {
    auto iter = users.find(std::string{user});
    if (iter == users.end()) {
        throw std::runtime_error("getPassword(): Unknown user: " +
                                 std::string{user});
    }
    return iter->second;
}

void McdEnvironment::iterateLogLines(
        const std::function<bool(std::string_view line)>& callback) const {
    for (const auto& p : std::filesystem::directory_iterator(log_dir)) {
        if (is_regular_file(p)) {
            const auto content =
                    readConcurrentUpdatedFile(cb::dek::Entity::Logs, p.path());
            auto lines = cb::string::split(content, '\n');
            for (auto line : lines) {
                while (line.back() == '\r') {
                    line.remove_suffix(1);
                }
                if (!callback(line)) {
                    return;
                }
            }
        }
    }
}

bool McdEnvironment::iterateAuditEvents(
        const std::function<bool(const nlohmann::json&)>& callback) const {
    const auto files =
            cb::io::findFilesContaining(mcd_env->getAuditLogDir(), "audit.log");
    for (const auto& file : files) {
        std::filesystem::path path(file);
        if (is_symlink(path)) {
            continue;
        }
        auto content = readConcurrentUpdatedFile(cb::dek::Entity::Audit, file);
        auto lines = cb::string::split(content, '\n');
        for (const auto& line : lines) {
            try {
                if (callback(nlohmann::json::parse(line))) {
                    // We're done
                    return true;
                }
            } catch (const nlohmann::json::exception&) {
                break;
            }
        }
    }
    return false;
}

std::string McdEnvironment::readConcurrentUpdatedFile(
        const cb::dek::Entity entity, const std::filesystem::path& path) const {
    std::string content;
    try {
        auto lookup = [this, entity](auto key) {
            return dek_manager->lookup(entity, key);
        };
        auto file_reader = cb::crypto::FileReader::create(path, lookup, {});
        if (!file_reader->is_encrypted()) {
            throw std::runtime_error("Expected the file to be encrypted");
        }

        std::string chunk;
        while (!(chunk = file_reader->nextChunk()).empty()) {
            content.append(chunk);
        }
    } catch (const std::underflow_error&) {
        // We might have hit a partial log update...
    }
    return content;
}
