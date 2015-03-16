/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <platform/dirutils.h>

#include "auditfile.h"
#include <iostream>
#include <map>
#include <atomic>
#include <cstring>
#include <time.h>

static std::atomic<time_t> auditd_test_timetravel_offset;

time_t auditd_time(time_t *tloc) {
    time_t now;
    time(&now);
    now += auditd_test_timetravel_offset.load(std::memory_order_acquire);

    if (tloc != NULL) {
        *tloc = now;
    }

    return now;
}

void audit_test_timetravel(time_t offset) {
    auditd_test_timetravel_offset += offset;
}


cJSON *create_audit_event() {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "timestamp", "2015-03-13T02:36:00.000-07:00");
    cJSON_AddStringToObject(root, "peername", "127.0.0.1:666");
    cJSON_AddStringToObject(root, "sockname", "127.0.0.1:555");
    cJSON *source = cJSON_CreateObject();
    cJSON_AddStringToObject(source, "source", "memcached");
    cJSON_AddStringToObject(source, "user", "myuser");
    cJSON_AddItemToObject(root, "real_userid", source);
    return root;
}

static bool time_rotate_test(void) {
    AuditConfig config;
    config.set_rotate_interval(60);
    config.set_rotate_size(1024*1024);
    config.set_log_directory("time-rotate-test");

    CouchbaseDirectoryUtilities::rmrf("time-rotate-test");

    AuditFile auditfile;
    auditfile.reconfigure(config);

    cJSON *obj = create_audit_event();
    cJSON_AddStringToObject(obj, "log_path", "fooo");

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(obj);
    }
    auditfile.close();

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("time-rotate-test/testing");
        if (files.size() != 1) {
            std::cerr << "Expected a single file.. found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        audit_test_timetravel(61);
        auditfile.write_event_to_disk(obj);
    }

    cJSON_Delete(obj);
    auditfile.close();

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("time-rotate-test/testing");
        if (files.size() != 11) {
            std::cerr << "Expected 10 files files... found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    CouchbaseDirectoryUtilities::rmrf("time-rotate-test");
    return true;
}

static bool size_rotate_test(void) {
    AuditConfig config;
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);
    config.set_log_directory("size-rotate-test");

    CouchbaseDirectoryUtilities::rmrf("size-rotate-test");

    AuditFile auditfile;
    auditfile.reconfigure(config);

    cJSON *obj = create_audit_event();
    cJSON_AddStringToObject(obj, "log_path", "fooo");

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(obj);
    }

    cJSON_Delete(obj);
    auditfile.close();

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("size-rotate-test/testing");
        if (files.size() != 10) {
            std::cerr << "Expected 10 files files... found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    CouchbaseDirectoryUtilities::rmrf("size-rotate-test");
    return true;
}

static bool successful_crash_recover_test(void) {
    CouchbaseDirectoryUtilities::mkdirp("crash-recovery-test");
    FILE *fp = fopen("crash-recovery-test/audit.log", "w");
    if (fp == NULL) {
        std::cerr << "Failed to create audit.log" << std::endl;
        return false;
    }
    cJSON *root = create_audit_event();
    char *content = cJSON_PrintUnformatted(root);
    fprintf(fp, "%s", content);
    fclose(fp);
    cJSON_Delete(root);
    cJSON_Free(content);

    AuditConfig config;
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);
    config.set_log_directory("crash-recovery-test");

    AuditFile auditfile;
    auditfile.reconfigure(config);

    try {
        auditfile.cleanup_old_logfile("crash-recovery-test");
    } catch (std::string &msg) {
        std::cerr << "Exception thrown: " << msg << std::endl;;
        return false;
    }

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("crash-recovery-test/testing-2015-03-13T02-36-00");
        if (files.size() != 1) {
            std::cerr << "Expected 1 files files... found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    CouchbaseDirectoryUtilities::rmrf("crash-recovery-test");
    return true;
}

static bool failed_crash_recover_test(void) {
    CouchbaseDirectoryUtilities::mkdirp("failed-crash-recovery");
    AuditConfig config;
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);
    config.set_log_directory("failed-crash-recovery");

    AuditFile auditfile;
    auditfile.reconfigure(config);

    FILE *fp = fopen("failed-crash-recovery/audit.log", "w");
    if (fp == NULL) {
        std::cerr << "Failed to create audit.log" << std::endl;
        return false;
    }
    fclose(fp);

    try {
        auditfile.cleanup_old_logfile("failed-crash-recovery");
        {
            using namespace CouchbaseDirectoryUtilities;
            auto files = findFilesWithPrefix("failed-crash-recovery/testing");
            if (files.size() != 0) {
                std::cerr << "Expected 0 files... found: " << files.size()
                          << std::endl;
                return false;
            }
        }
        {
            using namespace CouchbaseDirectoryUtilities;
            auto files = findFilesWithPrefix("failed-crash-recovery/audit");
            if (files.size() != 0) {
                std::cerr << "Expected 0 files... found: " << files.size()
                          << std::endl;
                return false;
            }
        }
    } catch (std::string &msg) {
        std::cerr << "ERROR: should not get an exception: " << msg << std::endl;
        return false;

    }


    fp = fopen("failed-crash-recovery/audit.log", "w");
    if (fp == NULL) {
        std::cerr << "Failed to create audit.log" << std::endl;
        return false;
    }
    fprintf(fp, "{}");
    fclose(fp);

    try {
        auditfile.cleanup_old_logfile("failed-crash-recovery");
        std::cerr << "Exception exception to be thrown for clean file without"
                  << " timestamp" << std::endl;
        return false;
    } catch (std::string &) {
    }

    if ((fp = fopen("failed-crash-recovery/audit.log", "w")) == NULL) {
        std::cerr << "Failed to create audit.log" << std::endl;
        return false;
    }

    cJSON *root = create_audit_event();
    char *content = cJSON_PrintUnformatted(root);
    char *ptr = strstr(content, "2015");
    *ptr = '\0';
    fprintf(fp, "%s", content);
    fclose(fp);
    cJSON_Delete(root);
    cJSON_Free(content);

    try {
        auditfile.cleanup_old_logfile("failed-crash-recovery");
        std::cerr << "Exception exception to be thrown for clean file with"
                  << " garbled timestamp" << std::endl;
        return false;
    } catch (std::string &) {
    }

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("failed-crash-recovery/testing");
        if (files.size() != 0) {
            std::cerr << "Expected 0 files... found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    {
        using namespace CouchbaseDirectoryUtilities;
        auto files = findFilesWithPrefix("failed-crash-recovery/audit.log");
        if (files.size() != 1) {
            std::cerr << "Expected 1 file... found: " << files.size()
                      << std::endl;
            return false;
        }
    }

    CouchbaseDirectoryUtilities::rmrf("failed-crash-recovery");
    return true;
}

static bool get_rollover_time_test(void) {
    CouchbaseDirectoryUtilities::mkdirp("rollover-time-test");
    AuditConfig config;
    config.set_rotate_interval(60);
    config.set_rotate_size(100);
    config.set_log_directory("rollover-time-test");
    AuditFile auditfile;
    auditfile.reconfigure(config);

    uint32_t secs = auditfile.get_seconds_to_rotation();
    audit_test_timetravel(10);
    if (secs != auditfile.get_seconds_to_rotation() || secs != 60) {
        std::cerr << "secs to rotation should be rotation interval "
                  << "when the file is closed" << std::endl;
        return false;
    }

    auditfile.ensure_open();

    secs = auditfile.get_seconds_to_rotation();
    if (!(secs == 60 || secs == 59)) {
        std::cerr << "Expected number of secs to be 60/59, is "
                  << secs << std::endl;
        return false;
    }

    audit_test_timetravel(10);
    secs = auditfile.get_seconds_to_rotation();

    if (!(secs == 50 || secs == 49)) {
        std::cerr << "Expected number of secs to be 50/49, is "
                  << secs << std::endl;
        return false;
    }


    CouchbaseDirectoryUtilities::rmrf("rollover-time-test");
    return true;
}


int main(void) {
   typedef bool (*testfunc)(void);
    std::map<std::string, testfunc> tests;
    int failed(0);

    AuditConfig::min_file_rotation_time = 10;

    tests["time rotate"] = time_rotate_test;
    tests["size rotate"] = size_rotate_test;
    tests["successful crash recover"] = successful_crash_recover_test;
    tests["failed crash recover"] = failed_crash_recover_test;
    tests["get rollover time "] = get_rollover_time_test;

    for (auto iter = tests.begin(); iter != tests.end(); ++iter) {
        std::cout << iter->first << "... ";
        std::cout.flush();
        if (iter->second()) {
            std::cout << "ok" << std::endl;
        } else {
            ++failed;
            // error should already be printed
        }
    }

    if (failed) {
        return EXIT_FAILURE;
    } else {
        return EXIT_SUCCESS;
    }
}
