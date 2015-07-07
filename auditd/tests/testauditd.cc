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

#include <mutex>
#include <condition_variable>
#include <iostream>
#include <fstream>
#include <sstream>
#include <limits.h>
#include <cJSON.h>
#include <platform/dirutils.h>
#include <stddef.h>

#include "memcached/extension.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "auditd_audit_events.h"

#ifdef WIN32
#include <direct.h>
#define sleep(a) Sleep(a * 1000)
#define mkdir(a, b) _mkdir(a)
#endif

class Configuration {
public:
    Configuration(const std::string &fname, const std::string &descr)
            : filename(fname),
              descriptor_path(descr),
              rotationSize(200),
              rotationInterval(10),
              logPath("test"),
              enabled(true)
    {
        writeFile();
    }

    void setRotationSize(size_t rotationSize) {
        Configuration::rotationSize = rotationSize;
        writeFile();
    }

    void setRotationInterval(size_t rotationInterval) {
        Configuration::rotationInterval = rotationInterval;
        writeFile();
    }

    void setLogPath(std::string &logPath) {
        Configuration::logPath = logPath;

        if (CouchbaseDirectoryUtilities::isDirectory(logPath)) {
            if (!CouchbaseDirectoryUtilities::rmrf(logPath)) {
                std::cerr << "Failed to remove test directory" << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (!CouchbaseDirectoryUtilities::mkdirp(logPath)) {
            std::cerr << "error, unable to create directory" << std::endl;
            exit(EXIT_FAILURE);
        }
        writeFile();
    }

    void setEnabled(bool enabled) {
        Configuration::enabled = enabled;
        writeFile();
    }


    const std::string &getFilename() const {
        return filename;
    }

private:
    void writeFile(void) {
        cJSON *config_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(config_json, "version", 1);
        if (enabled) {
            cJSON_AddTrueToObject(config_json, "auditd_enabled");
        } else {
            cJSON_AddFalseToObject(config_json, "auditd_enabled");
        }

        cJSON_AddNumberToObject(config_json, "rotate_size", (double)rotationSize);
        cJSON_AddNumberToObject(config_json, "rotate_interval", (double)rotationInterval);
        cJSON_AddStringToObject(config_json, "log_path", logPath.c_str());
        cJSON_AddStringToObject(config_json, "descriptors_path", descriptor_path.c_str());
        cJSON *disabled_arr = cJSON_CreateArray();
        cJSON_AddItemToObject(config_json, "disabled", disabled_arr);
        cJSON *sync_arr = cJSON_CreateArray();
        cJSON_AddItemToObject(config_json, "sync", sync_arr);
        char *data = cJSON_Print(config_json);
        FILE *fp = fopen(filename.c_str(), "wb");
        fprintf(fp, "%s\n", data);
        fclose(fp);
        cJSON_Free(data);
        cJSON_Delete(config_json);
    }

    std::string filename;
    std::string descriptor_path;
    size_t rotationSize;
    size_t rotationInterval;
    std::string logPath;
    bool enabled;
};

static std::mutex mutex;
static std::condition_variable cond;
static bool ready = false;

extern "C" {
static void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status) {
    std::lock_guard<std::mutex> lock(mutex);
    ready = true;
    cond.notify_one();
}
}

size_t expectedEventProcessed;
size_t auditEventProcessed;

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

static void config_auditd(const std::string &fname) {
    // We don't have a real cookie, but configure_auditdaemon
    // won't call notify_io_complete unless it's set to a
    // non-null value.. just pass in anything
    const void *cookie = (const void*)&ready;
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

static std::string createEvent(void) {
    cJSON *payload = cJSON_CreateObject();
    cJSON_AddStringToObject(payload, "timestamp", audit_generate_timestamp().c_str());
    cJSON *real_userid = cJSON_CreateObject();
    cJSON_AddStringToObject(real_userid, "source", "internal");
    cJSON_AddStringToObject(real_userid, "user", "couchbase");
    cJSON_AddItemReferenceToObject(payload, "real_userid", real_userid);
    char *content = cJSON_Print(payload);
    std::string ret(content);
    cJSON_Free(content);
    cJSON_Delete(payload);

    return ret;
}

void assertNumberOfFiles(const std::string &dir, size_t num) {
    auto vec = CouchbaseDirectoryUtilities::findFilesContaining(dir, "");
    if (num != vec.size()) {
        std::cerr << "FATAL: Expected " << num << " found " << vec.size() << std::endl;
        _exit(EXIT_FAILURE);
    }
}

void assertMinNumberOfFiles(const std::string &dir, size_t num) {
    auto vec = CouchbaseDirectoryUtilities::findFilesContaining(dir, "");
    if (vec.size() < num) {
        std::cerr << "FATAL: Expected at least " << num << " found " << vec.size() << std::endl;
        _exit(EXIT_FAILURE);
    }
}


int main(int argc, char **argv) {
    if (argc == 1) {
        std::cerr << "Usage: " << argv[0]
                  << " <directory containing audit_event.json>"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    /* create the test directory */
    std::vector<std::string> toremove;

    auto testdir = std::string("auditd-test-") + std::to_string(cb_getpid());
    toremove.push_back(testdir);
    std::string cfgfile = "test_audit-" + std::to_string(cb_getpid()) + ".json";
    toremove.push_back(cfgfile);
    Configuration configuration(cfgfile, argv[1]);

    configuration.setLogPath(testdir);

    // Start the audit daemon
    AUDIT_EXTENSION_DATA audit_extension_data;
    audit_extension_data.version = 1;
    audit_extension_data.min_file_rotation_time = 1;
    audit_extension_data.max_file_rotation_time = 604800;  // 1 week = 60*60*24*7
    audit_extension_data.log_extension = get_stderr_logger();
    audit_extension_data.notify_io_complete = notify_io_complete;

    if (start_auditdaemon(&audit_extension_data) != AUDIT_SUCCESS) {
        std::cerr << "start audit daemon: FAILED" << std::endl;
        return -1;
    }

    audit_set_audit_processed_listener(audit_processed_listener);

    // There shouldn't be any files there
    assertNumberOfFiles(testdir, 0);

    // Configure the audit daemon to be disabled
    configuration.setEnabled(false);
    config_auditd(configuration.getFilename());

    // There shouldn't be any files there
    assertNumberOfFiles(testdir, 0);

    configuration.setEnabled(true);

    // I need to wait for processing the configure event to happen,
    expectedEventProcessed = auditEventProcessed + 1;

    // Run configure (enable)
    config_auditd(configuration.getFilename());
    waitForProcessingEvents();

    // That should cause audit.log to appear
    assertNumberOfFiles(testdir, 1);
    testdir.assign("auditd-time-rotation-test-" + std::to_string(cb_getpid()));
    toremove.push_back(testdir);
    // rotate every 10 sec
    configuration.setRotationInterval(10);
    configuration.setLogPath(testdir);

    // I need to wait for processing the configure event to happen,
    expectedEventProcessed = auditEventProcessed + 1;
    config_auditd(configuration.getFilename());
    waitForProcessingEvents();

    for (int ii = 0; ii < 10; ++ii) {
        expectedEventProcessed = auditEventProcessed + 1;
        audit_test_timetravel(100);
        std::string event = createEvent();
        put_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                        event.data(), event.size());
        waitForProcessingEvents();
    }
    assertMinNumberOfFiles(testdir, 10);

    configuration.setRotationInterval(3600);
    configuration.setRotationSize(10);
    testdir.assign("auditd-size-rotation-test" + std::to_string(cb_getpid()));
    toremove.push_back(testdir);
    configuration.setLogPath(testdir);

    // I need to wait for processing the configure event to happen,
    expectedEventProcessed = auditEventProcessed + 1;
    config_auditd(configuration.getFilename());
    waitForProcessingEvents();

    for (int ii = 0; ii < 10; ++ii) {
        expectedEventProcessed = auditEventProcessed + 1;
        audit_test_timetravel(100);
        std::string event = createEvent();
        put_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                event.data(), event.size());
        waitForProcessingEvents();
    }
    assertMinNumberOfFiles(testdir, 10);

    if (shutdown_auditdaemon(configuration.getFilename().c_str()) != AUDIT_SUCCESS) {
        std::cerr << "shutdown audit daemon: FAILED" << std::endl;
        return -1;
    }

    for (auto name : toremove) {
        CouchbaseDirectoryUtilities::rmrf(name);
    }

    std::cout << "All tests pass" << std::endl;
    return 0;
}
