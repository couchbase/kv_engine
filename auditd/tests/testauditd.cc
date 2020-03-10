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
#include <folly/portability/GTest.h>
#include <getopt.h>
#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <memcached/server_cookie_iface.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>

#include "auditd/src/audit.h"
#include "auditd/src/auditconfig.h"
#include "auditd/tests/mock_auditconfig.h"

// The event descriptor file is normally stored in the directory named
// auditd relative to the binary.. let's just use that as the default
// and allow the user to override it with -e
static std::string event_descriptor("auditd");

static std::mutex mutex;
static std::condition_variable cond;
static bool ready = false;

class AuditMockServerCookieApi : public ServerCookieIface {
public:
    void store_engine_specific(gsl::not_null<const void*> cookie,
                               void* engine_data) override {
        throw std::runtime_error("Not implemented");
    }
    void* get_engine_specific(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    bool is_datatype_supported(gsl::not_null<const void*> cookie,
                               protocol_binary_datatype_t datatype) override {
        throw std::runtime_error("Not implemented");
    }
    bool is_mutation_extras_supported(
            gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    bool is_collections_supported(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    bool validate_session_cas(uint64_t cas) override {
        throw std::runtime_error("Not implemented");
    }
    void decrement_session_ctr() override {
        throw std::runtime_error("Not implemented");
    }
    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        std::lock_guard<std::mutex> lock(mutex);
        ready = true;
        cond.notify_one();
    }
    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    ENGINE_ERROR_CODE release(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    void set_priority(gsl::not_null<const void*> cookie,
                      CONN_PRIORITY priority) override {
        throw std::runtime_error("Not implemented");
    }
    CONN_PRIORITY get_priority(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    uint64_t get_connection_id(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    cb::rbac::PrivilegeAccess check_privilege(gsl::not_null<const void*> cookie,
                                              cb::rbac::Privilege privilege,
                                              ScopeID sid,
                                              CollectionID cid) override {
        throw std::runtime_error("Not implemented");
    }
    uint32_t get_privilege_context_revision(
            gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    cb::mcbp::Status engine_error2mcbp(gsl::not_null<const void*> cookie,
                                       ENGINE_ERROR_CODE code) override {
        throw std::runtime_error("Not implemented");
    }
    std::pair<uint32_t, std::string> get_log_info(
            gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    std::string get_authenticated_user(
            gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    in_port_t get_connected_port(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error("Not implemented");
    }
    void set_error_context(gsl::not_null<void*> cookie,
                           std::string_view message) override {
        throw std::runtime_error("Not implemented");
    }
    void set_error_json_extras(gsl::not_null<void*> cookie,
                               const nlohmann::json& json) override {
        throw std::runtime_error("set_error_json_extras not implemented");
    }
    std::string_view get_inflated_payload(
            gsl::not_null<const void*> cookie,
            const cb::mcbp::Request& request) override {
        throw std::runtime_error("get_inflated_payload not implemented");
    }
};

class AuditDaemonTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
public:
    static void SetUpTestCase() {
        // create the test directory
        testdir = cb::io::mktemp("auditd-test-");
        try {
            cb::io::rmrf(testdir);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        cb::io::mkdirp(testdir);

        // create the name of the configuration file to use
        cfgfile = cb::io::mktemp("test_audit-");
        try {
            cb::io::rmrf(cfgfile);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }

        // Start the audit daemon
        auditHandle = cb::audit::create_audit_daemon({}, &sapi);

        if (!auditHandle) {
            throw std::runtime_error(
                    "AuditDaemonTest::SetUpTestCase() Failed to start audit "
                    "daemon");
        }
    }

    static void TearDownTestCase() {
        auditHandle.reset();
        cb::io::rmrf(testdir);
        cb::io::rmrf(cfgfile);
    }

protected:
    void config_auditd(const std::string& fname) {
        // We don't have a real cookie, but configure_auditdaemon won't call
        // notify_io_complete unless it's set to a non-null value..
        // just pass on the ready variable
        const void* cookie = (const void*)&ready;
        if (auditHandle->configure_auditdaemon(fname, cookie)) {
            {
                // we have to wait
                std::unique_lock<std::mutex> lk(mutex);
                cond.wait(lk, [] { return ready; });
            }
            ready = false;
        } else {
            std::cerr << "initialize audit daemon: FAILED" << std::endl;
            exit(EXIT_FAILURE);
        };
    }

    void SetUp() override {
        config.set_descriptors_path(event_descriptor);
        config.set_rotate_size(2000);
        config.set_rotate_interval(900);
        config.set_log_directory(testdir);
        config.set_auditd_enabled(false);
        config.set_uuid("12345");
        config.set_version(2);
        cb::io::rmrf(testdir);
        cb::io::mkdirp(testdir);
    }

    void enable() {
        config.set_auditd_enabled(true);
        cb::io::rmrf(testdir);
        cb::io::mkdirp(testdir);
        configure();
    }

    void TearDown() override {
        config.set_auditd_enabled(false);
        configure();
    }

    void configure() {
        FILE* fp = fopen(cfgfile.c_str(), "w");
        ASSERT_NE(nullptr, fp);
        ASSERT_GE(fprintf(fp, "%s\n", config.to_json().dump().c_str()), 0);
        ASSERT_NE(-1, fclose(fp));
        config_auditd(cfgfile);
    }

    void assertNumberOfFiles(size_t num) {
        auto vec = cb::io::findFilesContaining(testdir, "");
        ASSERT_EQ(num, vec.size());
    }

    // Test to see if a given string exists in the audit log
    // @param searchString  string to atempt to find in the audit log
    // @returns bool  return true if searchString is found in the audit log,
    //                otherwise return false
    bool existsInAuditLog(const std::string& searchString) {
        // confirm that the audit file exists in the directory.
        assertNumberOfFiles(1);
        auto vec = cb::io::findFilesContaining(testdir, "");
        std::ifstream auditFile;
        auditFile.open(vec.front().c_str());
        std::string line;
        while (std::getline(auditFile, line)) {
            if (line.find(searchString) != std::string::npos) {
                auditFile.close();
                return true;
            }
        }
        auditFile.close();
        return false;
    }

    static AuditMockServerCookieApi sapi;
    MockAuditConfig config;
    static cb::audit::UniqueAuditPtr auditHandle;
    static std::string testdir;
    static std::string cfgfile;
};

AuditMockServerCookieApi AuditDaemonTest::sapi;

std::string AuditDaemonTest::testdir;
std::string AuditDaemonTest::cfgfile;
cb::audit::UniqueAuditPtr AuditDaemonTest::auditHandle;

TEST_P(AuditDaemonTest, StartupDisabledDontCreateFiles) {
    configure();
    assertNumberOfFiles(0);
}

TEST_P(AuditDaemonTest, StartupEnableCreateFile) {
    enable();
    assertNumberOfFiles(1);
}

class AuditDaemonFilteringTest : public AuditDaemonTest {
protected:
    void SetUp() override {
        AuditDaemonTest::SetUp();
        // Add the userid : {"source" : "internal", "user" : "johndoe"}
        // to the disabled users list
        nlohmann::json disabled_userids = nlohmann::json::array();
        nlohmann::json userIdRoot;
        userIdRoot["source"] = "internal";
        userIdRoot["user"] = "johndoe";
        disabled_userids.push_back(userIdRoot);

        config.public_set_disabled_userids(disabled_userids);
    }

    // Adds a new event that has the filtering_permitted attribute set according
    // to the input parameter.
    // @param filteringPermitted  indicates whether the event being added can
    //                            be filtered or not
    void addEvent(bool filteringPermitted) {
        nlohmann::json json;
        // We normally expect this to be an unsigned integer so we need to
        // explicitly pass a unsigned int to json[].
        size_t id = 1234;
        json["id"] = id;
        json["name"] = "newEvent";
        json["description"] = "description";
        json["sync"] = false;
        json["enabled"] = true;
        json["filtering_permitted"] = filteringPermitted;
        dynamic_cast<AuditImpl*>(auditHandle.get())->add_event_descriptor(json);
    }
};

/**
 * Tests the filtering of audit events by user.
 * An attempt is made to add the new event to the audit log (using
 * put_audit_event) with a real_userid:user = "johndoe".  Depending on the
 * global filter setting and the event's filtering permitted attribute, the
 * "johndoe" event may or may not appear in the audit log.
 */
TEST_P(AuditDaemonFilteringTest, AuditFilteringTest) {
    bool globalFilterSetting = std::get<0>(GetParam());
    bool eventFilteringPermitted = std::get<1>(GetParam());
    bool foundJohndoe{false};

    const std::string payloadjohndoe =
            R"({"id": 1234, "timestamp": "test", "real_userid":
                     {"source": "internal", "user": "johndoe"}})";

    const std::string payloadanother =
            R"({"id": 1234, "timestamp": "test", "real_userid":
                     {"source": "internal", "user": "another"}})";

    config.set_filtering_enabled(globalFilterSetting);
    enable();
    addEvent(eventFilteringPermitted);

    // generate the 1234 event with real_userid:user = johndoe
    auditHandle->put_event(1234, payloadjohndoe);
    // generate the 1234 event with real_userid:user = another
    auditHandle->put_event(1234, payloadanother);

    // Check the audit log exists
    assertNumberOfFiles(1);

    // wait up to 10 seconds for "another" to appear in the audit log
    uint16_t waitIteration = 0;
    while (!existsInAuditLog("another") && (waitIteration < 200)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waitIteration++;
    }

    // Check to see if "johndoe" exists or not in the audit log
    foundJohndoe = existsInAuditLog("johndoe");

    // If filtering is enabled and the event is permitted to be filtered
    // then the event should not be found in the audit log.
    if (globalFilterSetting && eventFilteringPermitted) {
        EXPECT_FALSE(foundJohndoe);
    } else {
        // exists in audit log
        EXPECT_TRUE(foundJohndoe);
    }
}

// Check to see if "uuid":"12345" is reported
TEST_F(AuditDaemonTest, UuidTest) {
    enable();
    // Check the audit log exists
    auto vec = cb::io::findFilesContaining(testdir, "");
    assertNumberOfFiles(1);

    // wait up to 10 seconds for "uuid" to appear in the audit log
    uint16_t waitIteration = 0;
    while (!existsInAuditLog("uuid") && (waitIteration < 200)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waitIteration++;
    }
    EXPECT_TRUE(existsInAuditLog(R"("uuid")"))
            << "uuid attribute is missing from audit log";
    EXPECT_TRUE(existsInAuditLog(R"("uuid":"12345")"))
            << "Wrong uuid in the audit log";
}

// Check to see if "version":2 is reported
TEST_F(AuditDaemonTest, VersionTest) {
    enable();
    // Check the audit log exists
    auto vec = cb::io::findFilesContaining(testdir, "");
    assertNumberOfFiles(1);

    // wait up to 10 seconds for "version" to appear in the audit log
    uint16_t waitIteration = 0;
    while (!existsInAuditLog("version") && (waitIteration < 200)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waitIteration++;
    }
    EXPECT_TRUE(existsInAuditLog(R"("version")"))
            << "version attribute is missing from audit log";
    EXPECT_TRUE(existsInAuditLog(R"("version":2)"))
            << "Wrong version in the audit log";
}

int main(int argc, char** argv) {
    cb::logger::createConsoleLogger();
    ::testing::InitGoogleTest(&argc, argv);
    // required for gethostname(); normally called by memcached's main()
    cb_initialize_sockets();

    int cmd;
    while ((cmd = getopt(argc, argv, "e:")) != EOF) {
        switch (cmd) {
        case 'e':
            event_descriptor = optarg;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-e]" << std::endl
                      << "\t-e\tPath to audit_events.json" << std::endl;
            return 1;
        }
    }

    std::string filename = event_descriptor + "/audit_events.json";
    FILE* fp = fopen(filename.c_str(), "r");
    if (fp == nullptr) {
        std::cerr << "Failed to open: " << filename << std::endl;
        return EXIT_FAILURE;
    }
    fclose(fp);

    return RUN_ALL_TESTS();
}

static std::vector<bool> allFilteringOptions = {{true, false}};

INSTANTIATE_TEST_SUITE_P(bool,
                         AuditDaemonTest,
                         ::testing::Values(std::make_tuple(true, true)));
INSTANTIATE_TEST_SUITE_P(
        bool,
        AuditDaemonFilteringTest,
        ::testing::Combine(::testing::ValuesIn(allFilteringOptions),
                           ::testing::ValuesIn(allFilteringOptions)));
