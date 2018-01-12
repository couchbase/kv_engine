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

#include <cJSON.h>
#include <getopt.h>
#include <gtest/gtest.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
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
#include "memcached/extension.h"
#include "memcached/extension_loggers.h"

// The event descriptor file is normally stored in the directory named
// auditd relative to the binary.. let's just use that as the default
// and allow the user to override it with -e
static std::string event_descriptor("auditd");

static std::mutex mutex;
static std::condition_variable cond;
static bool ready = false;

extern "C" {
static void notify_io_complete(gsl::not_null<const void*>, ENGINE_ERROR_CODE) {
    std::lock_guard<std::mutex> lock(mutex);
    ready = true;
    cond.notify_one();
}
}

class AuditDaemonTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
public:
    static void SetUpTestCase() {
        // create the test directory
        testdir = std::string("auditd-test-") + std::to_string(cb_getpid());
        try {
            cb::io::rmrf(testdir);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        cb::io::mkdirp(testdir);

        // create the name of the configuration file to use
        cfgfile = "test_audit-" + std::to_string(cb_getpid()) + ".json";
        try {
            cb::io::rmrf(cfgfile);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }

        // Start the audit daemon
        AUDIT_EXTENSION_DATA audit_extension_data;
        memset(&audit_extension_data, 0, sizeof(audit_extension_data));
        audit_extension_data.log_extension = get_stderr_logger();
        audit_extension_data.notify_io_complete = notify_io_complete;

        ASSERT_EQ(AUDIT_SUCCESS,
                  start_auditdaemon(&audit_extension_data, &auditHandle))
                << "start audit daemon: FAILED" << std::endl;
    }

    static void TearDownTestCase() {
        EXPECT_EQ(AUDIT_SUCCESS, shutdown_auditdaemon(auditHandle));
        cb::io::rmrf(testdir);
        cb::io::rmrf(cfgfile);
    }

protected:
    void config_auditd(const std::string& fname) {
        // We don't have a real cookie, but configure_auditdaemon won't call
        // notify_io_complete unless it's set to a non-null value..
        // just pass on the ready variable
        const void* cookie = (const void*)&ready;
        switch (configure_auditdaemon(auditHandle, fname.c_str(), cookie)) {
        case AUDIT_SUCCESS:
            break;
        case AUDIT_EWOULDBLOCK: {
            // we have to wait
            std::unique_lock<std::mutex> lk(mutex);
            cond.wait(lk, [] { return ready; });
        }
            ready = false;
            break;
        default:
            std::cerr << "initialize audit daemon: FAILED" << std::endl;
            exit(EXIT_FAILURE);
        };
    }

    void SetUp() {
        config.set_descriptors_path(event_descriptor);
        config.set_rotate_size(2000);
        config.set_rotate_interval(900);
        config.set_log_directory(testdir);
        config.set_auditd_enabled(false);
        config.set_uuid("12345");
        cb::io::rmrf(testdir);
        cb::io::mkdirp(testdir);
    }

    void enable() {
        config.set_auditd_enabled(true);
        cb::io::rmrf(testdir);
        cb::io::mkdirp(testdir);
        configure();
    }

    void TearDown() {
        config.set_auditd_enabled(false);
        configure();
    }

    void configure() {
        FILE* fp = fopen(cfgfile.c_str(), "w");
        ASSERT_NE(nullptr, fp);
        fprintf(fp, "%s\n", to_string(config.to_json()).c_str());
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

    MockAuditConfig config;
    static Audit* auditHandle;
    static std::string testdir;
    static std::string cfgfile;
};

std::string AuditDaemonTest::testdir;
std::string AuditDaemonTest::cfgfile;
Audit* AuditDaemonTest::auditHandle;

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
    void SetUp() {
        AuditDaemonTest::SetUp();
        // Add the user "johndoe" to the disabled users list
        unique_cJSON_ptr disabled_users(cJSON_CreateObject());
        cJSON_AddItemToArray(disabled_users.get(),
                             cJSON_CreateString("johndoe"));
        config.public_set_disabled_users(disabled_users.get());
    }

    // Adds a new event that has the filtering_permitted attribute set according
    // to the input parameter.
    // @param filteringPermitted  indicates whether the event being added can
    //                            be filtered or not
    void addEvent(bool filteringPermitted) {
        unique_cJSON_ptr root(cJSON_CreateObject());
        cJSON_AddNumberToObject(root.get(), "id", 1234);
        cJSON_AddStringToObject(root.get(), "name", "newEvent");
        cJSON_AddStringToObject(root.get(), "description", "description");
        cJSON_AddFalseToObject(root.get(), "sync");
        cJSON_AddTrueToObject(root.get(), "enabled");
        if (filteringPermitted) {
            cJSON_AddTrueToObject(root.get(), "filtering_permitted");
        } else {
            cJSON_AddFalseToObject(root.get(), "filtering_permitted");
        }
        auditHandle->initialize_event_data_structures(root.get());
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
    put_audit_event(
            auditHandle, 1234, payloadjohndoe.c_str(), payloadjohndoe.length());
    // generate the 1234 event with real_userid:user = another
    put_audit_event(
            auditHandle, 1234, payloadanother.c_str(), payloadanother.length());

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

int main(int argc, char** argv) {
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

INSTANTIATE_TEST_CASE_P(bool,
                        AuditDaemonTest,
                        ::testing::Values(std::make_tuple(true, true)), );
INSTANTIATE_TEST_CASE_P(
        bool,
        AuditDaemonFilteringTest,
        ::testing::Combine(::testing::ValuesIn(allFilteringOptions),
                           ::testing::ValuesIn(allFilteringOptions)), );
