/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include <atomic>
#include <cJSON.h>
#include <condition_variable>
#include <getopt.h>
#include <gtest/gtest.h>
#include <iostream>
#include <limits.h>
#include <mutex>
#include <platform/dirutils.h>
#include <stddef.h>

#include "memcached/extension.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "auditd_audit_events.h"
#include "auditd/src/auditconfig.h"

// The event descriptor file is normally stored in the directory named
// auditd relative to the binary.. let's just use that as the default
// and allow the user to override it with -e
static std::string event_descriptor("auditd");

static std::mutex mutex;
static std::condition_variable cond;
static bool ready = false;

extern "C" {
static void notify_io_complete(const void* cookie, ENGINE_ERROR_CODE status) {
    std::lock_guard<std::mutex> lock(mutex);
    ready = true;
    cond.notify_one();
}
}

std::atomic<size_t> expectedEventProcessed;
std::atomic<size_t> auditEventProcessed;

void audit_processed_listener(void) {
    std::lock_guard<std::mutex> lock(mutex);
    cond.notify_one();
    auditEventProcessed++;
}

static void waitForProcessingEvents(void) {
    // we have to wait
    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [] {
        return expectedEventProcessed <= auditEventProcessed;
    });
}

static void config_auditd(const std::string& fname) {
    // We don't have a real cookie, but configure_auditdaemon
    // won't call notify_io_complete unless it's set to a
    // non-null value.. just pass in anything
    const void* cookie = (const void*)&ready;
    switch (configure_auditdaemon(fname.c_str(), cookie)) {
    case AUDIT_SUCCESS:
        break;
    case AUDIT_EWOULDBLOCK: {
            // we have to wait
            std::unique_lock<std::mutex> lk(mutex);
            cond.wait(lk, [] {
                return ready;
            });
        }
        ready = false;
        break;
    default:
        std::cerr << "initialize audit daemon: FAILED" << std::endl;
        exit(EXIT_FAILURE);
    };
}

class AuditDaemonTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        // create the test directory
        testdir = std::string("auditd-test-") + std::to_string(cb_getpid());
        CouchbaseDirectoryUtilities::rmrf(testdir);
        ASSERT_TRUE(CouchbaseDirectoryUtilities::mkdirp(testdir));

        // create the name of the configuration file to use
        cfgfile = "test_audit-" + std::to_string(cb_getpid()) + ".json";
        CouchbaseDirectoryUtilities::rmrf(cfgfile);

        // Start the audit daemon
        AUDIT_EXTENSION_DATA audit_extension_data;
        audit_extension_data.version = 1;
        audit_extension_data.min_file_rotation_time = 1;
        audit_extension_data.max_file_rotation_time = 604800;  // 1 week = 60*60*24*7
        audit_extension_data.log_extension = get_stderr_logger();
        audit_extension_data.notify_io_complete = notify_io_complete;

        ASSERT_EQ(AUDIT_SUCCESS, start_auditdaemon(&audit_extension_data))
                        << "start audit daemon: FAILED" << std::endl;

        audit_set_audit_processed_listener(audit_processed_listener);
    }

    static void TearDownTestCase() {
        EXPECT_EQ(AUDIT_SUCCESS, shutdown_auditdaemon());
        CouchbaseDirectoryUtilities::rmrf(testdir);
        CouchbaseDirectoryUtilities::rmrf(cfgfile);
    }

    AuditConfig config;

    void SetUp() {
        config.set_descriptors_path(event_descriptor);
        config.set_rotate_size(2000);
        config.set_rotate_interval(900);
        config.set_log_directory(testdir);
        config.set_auditd_enabled(false);
        CouchbaseDirectoryUtilities::rmrf(testdir);
        ASSERT_TRUE(CouchbaseDirectoryUtilities::mkdirp(testdir));
    }

    void enable() {
        config.set_auditd_enabled(true);
        CouchbaseDirectoryUtilities::rmrf(testdir);
        ASSERT_TRUE(CouchbaseDirectoryUtilities::mkdirp(testdir));
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

        // I need to wait for processing the configure event to happen,
        expectedEventProcessed = auditEventProcessed + 1;
        config_auditd(cfgfile);
    }

    void assertNumberOfFiles(size_t num) {
        auto vec = CouchbaseDirectoryUtilities::findFilesContaining(testdir,
                                                                    "");
        ASSERT_EQ(num, vec.size());
    }

    void assertMinNumberOfFiles(size_t num) {
        auto vec = CouchbaseDirectoryUtilities::findFilesContaining(testdir,
                                                                    "");
        ASSERT_LE(num, vec.size());
    }

    std::string createEvent(void) {
        unique_cJSON_ptr json(cJSON_CreateObject());
        cJSON* payload = json.get();
        cJSON_AddStringToObject(payload, "timestamp",
                                audit_generate_timestamp().c_str());
        cJSON* real_userid = cJSON_CreateObject();
        cJSON_AddStringToObject(real_userid, "source", "internal");
        cJSON_AddStringToObject(real_userid, "user", "couchbase");
        cJSON_AddItemToObject(payload, "real_userid", real_userid);
        return to_string(json, false);
    }

    static std::string testdir;
    static std::string cfgfile;
};

std::string AuditDaemonTest::testdir;
std::string AuditDaemonTest::cfgfile;

TEST_F(AuditDaemonTest, StartupDisabledDontCreateFiles) {
    configure();
    assertNumberOfFiles(0);
}

TEST_F(AuditDaemonTest, StartupEnableCreateFile) {
    enable();
    assertNumberOfFiles(1);
}

TEST_F(AuditDaemonTest, TimeRotationTest) {
    config.set_rotate_interval(900);
    enable();

    for (int ii = 0; ii < 10; ++ii) {
        expectedEventProcessed = auditEventProcessed + 1;
        audit_test_timetravel(1000);
        std::string event = createEvent();
        put_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                        event.data(), event.size());
        waitForProcessingEvents();
    }
    assertMinNumberOfFiles(10);
}

TEST_F(AuditDaemonTest, SizeRotationTest) {
    config.set_rotate_size(10);
    enable();

    for (int ii = 0; ii < 10; ++ii) {
        expectedEventProcessed = auditEventProcessed + 1;
        std::string event = createEvent();
        put_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                        event.data(), event.size());
        waitForProcessingEvents();
    }
    assertMinNumberOfFiles(10);
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
