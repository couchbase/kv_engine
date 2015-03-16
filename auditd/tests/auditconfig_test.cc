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
#include <map>
#include <cstdlib>
#include <iostream>
#include <platform/dirutils.h>

#include "auditd.h"
#include "auditconfig.h"

static cJSON *createDefaultConfig(void) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "version", 1);
    cJSON_AddNumberToObject(root, "rotate_size", 20*1024*1024);
    cJSON_AddNumberToObject(root, "rotate_interval", 900);
    cJSON_AddTrueToObject(root, "auditd_enabled");
    cJSON_AddTrueToObject(root, "buffered");
    cJSON_AddStringToObject(root, "log_path", "auditconfig-test");
    cJSON_AddStringToObject(root, "descriptors_path", "auditconfig-test");
    cJSON *sync = cJSON_CreateArray();
    cJSON_AddItemToObject(root, "sync", sync);
    cJSON *disabled = cJSON_CreateArray();
    cJSON_AddItemToObject(root, "disabled", disabled);


    return root;
}

static bool test_version(void) {
    AuditConfig config;
    // version is a mandatory field...
    cJSON *obj = cJSON_CreateObject();
    try {
        config.initialize_config(obj);
        std::cerr << "Failed.. did not detect missing version attribute"
                  << std::endl;
        cJSON_Delete(obj);
        return false;
    } catch (std::string &) {
        cJSON_Delete(obj);
    } catch (...) {
        cJSON_Delete(obj);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    cJSON *root = createDefaultConfig();
    for (int version = -100; version < 100; ++version) {
        // std::cout << "Yo " << version << std::endl;

        cJSON_ReplaceItemInObject(root, "version",
                                  cJSON_CreateNumber(version));
        try {
            config.initialize_config(root);
            if (version != 1) {
                std::cerr << "Failed.. did not throw exception for illegal version"
                          << std::endl;
                cJSON_Delete(root);
                return false;
            }
        } catch (std::string &) {
            if (version == 1) {
                std::cerr << "Failed.. version 1 should not throw exception"
                          << std::endl;
                cJSON_Delete(root);
                return false;
            }
        } catch (...) {
            cJSON_Delete(obj);
            std::cerr << "Failed... Unknown exception type thrown"
                      << std::endl;
            return false;
        }
    }

    cJSON_Delete(root);

    return true;
}

static bool test_rotation_size(void) {
    AuditConfig config;
    config.set_rotate_size(100);
    if (config.get_rotate_size() != 100) {
        std::cerr << "Failed. Expected getter to return value just set"
                  << std::endl;
        return false;
    }

    cJSON *root = createDefaultConfig();
    cJSON_AddNumberToObject(root, "rotate_size", 1000);
    try {
        config.initialize_config(root);
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Expected to be able to set rotation size"
                  << " to 1000" << std::endl;
        return false;
    }

    cJSON_ReplaceItemInObject(root, "rotate_size", cJSON_CreateNumber(-1));
    try {
        config.initialize_config(root);
        std::cerr << "Failed: It should not be possible to set rotate "
                  << "size to a negative value" << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (std::string &) {
        // empty
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    cJSON_ReplaceItemInObject(root, "rotate_size",
                              cJSON_CreateNumber((double)AuditConfig::max_rotate_file_size + 1));
    try {
        config.initialize_config(root);
        std::cerr << "Failed: It should not be possible to set rotate "
                  << "size to greater than "
                  << AuditConfig::max_rotate_file_size << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (std::string &) {
        // empty
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    return true;
}

static bool test_rotation_interval(void) {
    AuditConfig config;
    config.set_rotate_interval(1000);
    if (config.get_rotate_interval() != 1000) {
        std::cerr << "Failed. Expected getter to return value just set"
                  << std::endl;
        return false;
    }

    cJSON *root = createDefaultConfig();
    cJSON_AddNumberToObject(root, "rotate_interval", 10000);
    try {
        config.initialize_config(root);
    } catch (std::string &msg) {
        cJSON_Delete(root);
        std::cerr << "Failed... Expected to be able to set rotation interval"
                  << " to 1000" << msg << std::endl;
        return false;
    }

    cJSON_ReplaceItemInObject(root, "rotate_interval",
                              cJSON_CreateNumber(AuditConfig::min_file_rotation_time - 1));
    try {
        config.initialize_config(root);
        std::cerr << "Failed: It should not be possible to set rotate "
                  << "interval to a value less than "
                  << AuditConfig::min_file_rotation_time << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (std::string &) {
        // empty
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    cJSON_ReplaceItemInObject(root, "rotate_size",
                              cJSON_CreateNumber(AuditConfig::max_file_rotation_time + 1));
    try {
        config.initialize_config(root);
        std::cerr << "Failed: It should not be possible to set rotate "
                  << "interval to a value greater than "
                  << AuditConfig::max_file_rotation_time << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (std::string &) {
        // empty
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    return true;
}

static bool test_auditd_enabled(void) {
    cJSON *root = createDefaultConfig();
    cJSON_ReplaceItemInObject(root, "auditd_enabled", cJSON_CreateTrue());
    AuditConfig config;
    try {
        config.initialize_config(root);
        if (!config.is_auditd_enabled()) {
            std::cerr << "Failed. should be enabled" << std::endl;
            cJSON_Delete(root);
            return false;
        }

        cJSON_ReplaceItemInObject(root, "auditd_enabled", cJSON_CreateFalse());
        config.initialize_config(root);
        if (config.is_auditd_enabled()) {
            std::cerr << "Failed. should be disabled" << std::endl;
            cJSON_Delete(root);
            return false;
        }
    } catch (std::string &msg) {
        std::cerr << "failed: " << msg << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    return true;
}

static bool test_buffered(void) {
    cJSON *root = createDefaultConfig();
    cJSON_ReplaceItemInObject(root, "buffered", cJSON_CreateTrue());
    AuditConfig config;
    try {
        config.initialize_config(root);
        if (!config.is_buffered()) {
            std::cerr << "Failed. should be enabled" << std::endl;
            cJSON_Delete(root);
            return false;
        }

        cJSON_ReplaceItemInObject(root, "buffered", cJSON_CreateFalse());
        config.initialize_config(root);
        if (config.is_buffered()) {
            std::cerr << "Failed. should be disabled" << std::endl;
            cJSON_Delete(root);
            return false;
        }
    } catch (std::string &msg) {
        std::cerr << "failed: " << msg << std::endl;
        cJSON_Delete(root);
        return false;
    } catch (...) {
        cJSON_Delete(root);
        std::cerr << "Failed... Unknown exception type thrown"
                  << std::endl;
        return false;
    }

    return true;
}

static bool test_log_path(void) {
#ifndef WIN32
    cJSON *root = createDefaultConfig();
    AuditConfig config;

    cJSON_ReplaceItemInObject(root, "log_path",
                              cJSON_CreateString("auditconfig-test/foo/"));

    try {
        config.initialize_config(root);
        if (config.get_log_directory() != "auditconfig-test/foo") {
            std::cerr << "Failed: expected to trim off trailing /: \""
                      << config.get_log_directory() << "\"" << std::endl;
            return false;
        }
    } catch (...) {
        std::cerr << "Failed to parse log_path of \"auditconfig-test/foo/\""
                  << std::endl;
        return false;
    }

    cJSON_ReplaceItemInObject(root, "log_path", cJSON_CreateString("/itwouldsuckifthisexists"));

    try {
        config.initialize_config(root);
        std::cerr << "Expected to throw exception with path we can't create"
                  << std::endl;
        return false;
    } catch (...) {
    }

    cJSON_Delete(root);
#endif
    return true;
}

static bool test_descriptors_path(void) {
    cJSON *root = createDefaultConfig();
    AuditConfig config;

    cJSON_ReplaceItemInObject(root, "descriptors_path",
                              cJSON_CreateString("auditconfig-test/foo/"));

    try {
        config.initialize_config(root);
        std::cerr << "Failed: expected to fail to locate audit_events.json"
                  << std::endl;
        return false;
    } catch (...) {
    }

    cJSON_Delete(root);
    return true;
}

static bool test_sync(void) {
    cJSON *root = createDefaultConfig();
    AuditConfig config;
    cJSON *array = cJSON_GetObjectItem(root, "sync");
    for (int ii = 0; ii < 10; ++ii) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(ii));
    }

    config.initialize_config(root);
    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10) {
            if (!config.is_event_sync(ii)) {
                std::cerr << "error: expected " << ii << " to be sync"
                          << std::endl;
                return false;
            }
        } else {
            if (config.is_event_sync(ii)) {
                std::cerr << "error: expected " << ii << " to be async"
                          << std::endl;
                return false;
            }
        }
    }

    cJSON_Delete(root);
    return true;
}

static bool test_disabled(void) {
    cJSON *root = createDefaultConfig();
    AuditConfig config;
    cJSON *array = cJSON_GetObjectItem(root, "disabled");
    for (int ii = 0; ii < 10; ++ii) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(ii));
    }

    config.initialize_config(root);
    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10) {
            if (!config.is_event_disabled(ii)) {
                std::cerr << "error: expected " << ii << " to be disabled"
                          << std::endl;
                return false;
            }
        } else {
            if (config.is_event_disabled(ii)) {
                std::cerr << "error: expected " << ii << " to be enabled"
                          << std::endl;
                return false;
            }
        }
    }

    cJSON_Delete(root);
    return true;
}

static bool test_unknown_tag(void) {
    cJSON *root = createDefaultConfig();
    cJSON_AddNumberToObject(root, "foo", 5);
    AuditConfig config;

    try {
        config.initialize_config(root);
        std::cerr << "Failed. Expected to detect unknown tag \"foo\"";
        return false;
    } catch (std::string &) {

    }

    return true;
}


int main(void)
{
    typedef bool (*testfunc)(void);
    std::map<std::string, testfunc> tests;
    int failed(0);

    CouchbaseDirectoryUtilities::mkdirp("auditconfig-test");

    // Create the audit_events.json file needed by the configuration
    fclose(fopen("auditconfig-test/audit_events.json", "w"));

    tests["version"] = test_version;
    tests["rotation size"] = test_rotation_size;
    tests["rotation interval"] = test_rotation_interval;
    tests["auditd enabled"] = test_auditd_enabled;
    tests["buffered"] = test_buffered;
    tests["log path"] = test_log_path;
    tests["descriptors path"] = test_descriptors_path;
    tests["sync"] = test_sync;
    tests["disabled"] = test_disabled;
    tests["unknown tag"] = test_unknown_tag;

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

    CouchbaseDirectoryUtilities::rmrf("auditconfig-test");

    if (failed) {
        return EXIT_FAILURE;
    } else {
        return EXIT_SUCCESS;
    }
}
