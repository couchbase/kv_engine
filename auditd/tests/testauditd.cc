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
#include <condition_variable>
#include <cstddef>
#include <getopt.h>
#include <gtest/gtest.h>
#include <iostream>
#include <limits>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <mutex>
#include <platform/dirutils.h>

#include "memcached/extension.h"
#include "memcached/extension_loggers.h"
#include "auditd/src/auditconfig.h"

// The event descriptor file is normally stored in the directory named
// auditd relative to the binary.. let's just use that as the default
// and allow the user to override it with -e
static std::string event_descriptor("auditd");

static std::mutex mutex;
static std::condition_variable cond;
static bool ready = false;

extern "C" {
static void notify_io_complete(const void*, ENGINE_ERROR_CODE) {
    std::lock_guard<std::mutex> lock(mutex);
    ready = true;
    cond.notify_one();
}
}

class AuditDaemonTest : public ::testing::Test {
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

    static void SetUpTestCase() {
        // create the test directory
        testdir = std::string("auditd-test-") + std::to_string(cb_getpid());
        CouchbaseDirectoryUtilities::rmrf(testdir);
        CouchbaseDirectoryUtilities::mkdirp(testdir);

        // create the name of the configuration file to use
        cfgfile = "test_audit-" + std::to_string(cb_getpid()) + ".json";
        CouchbaseDirectoryUtilities::rmrf(cfgfile);

        // Start the audit daemon
        AUDIT_EXTENSION_DATA audit_extension_data;
        memset(&audit_extension_data, 0, sizeof(audit_extension_data));
        audit_extension_data.log_extension = get_stderr_logger();
        audit_extension_data.notify_io_complete = notify_io_complete;

        ASSERT_EQ(AUDIT_SUCCESS, start_auditdaemon(&audit_extension_data,
                                                   &auditHandle))
                        << "start audit daemon: FAILED" << std::endl;
    }

    static void TearDownTestCase() {
        EXPECT_EQ(AUDIT_SUCCESS, shutdown_auditdaemon(auditHandle));
        CouchbaseDirectoryUtilities::rmrf(testdir);
        CouchbaseDirectoryUtilities::rmrf(cfgfile);
    }

    AuditConfig config;
    static Audit* auditHandle;

    void SetUp() {
        config.set_descriptors_path(event_descriptor);
        config.set_rotate_size(2000);
        config.set_rotate_interval(900);
        config.set_log_directory(testdir);
        config.set_auditd_enabled(false);
        CouchbaseDirectoryUtilities::rmrf(testdir);
        CouchbaseDirectoryUtilities::mkdirp(testdir);
    }

    void enable() {
        config.set_auditd_enabled(true);
        CouchbaseDirectoryUtilities::rmrf(testdir);
        CouchbaseDirectoryUtilities::mkdirp(testdir);
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
        auto vec = CouchbaseDirectoryUtilities::findFilesContaining(testdir,
                                                                    "");
        ASSERT_EQ(num, vec.size());
    }

    static std::string testdir;
    static std::string cfgfile;
};

std::string AuditDaemonTest::testdir;
std::string AuditDaemonTest::cfgfile;
Audit* AuditDaemonTest::auditHandle;


TEST_F(AuditDaemonTest, StartupDisabledDontCreateFiles) {
    configure();
    assertNumberOfFiles(0);
}

TEST_F(AuditDaemonTest, StartupEnableCreateFile) {
    enable();
    assertNumberOfFiles(1);
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
