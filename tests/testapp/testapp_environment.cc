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

#include "programs/utilities.h"
#include "testapp_environment.h"
#include "testapp_rbac.h"

#include <cJSON_utils.h>
#include <fstream>
#include <platform/dirutils.h>
#include <platform/strerror.h>

static std::string get_working_current_directory() {
    bool ok = false;
    std::string result(4096, 0);
#ifdef WIN32
    ok = GetCurrentDirectory(result.size(), &result[0]) != 0;
#else
    ok = getcwd(&result[0], result.size()) != nullptr;
#endif
    /* memcached may throw a warning, but let's push through */
    if (!ok) {
        std::cerr << "Failed to determine current working directory: "
                  << cb_strerror() << std::endl;
        result = ".";
    }
    // Trim off any trailing \0 characters.
    result.resize(strlen(result.c_str()));
    return result;
}

void McdEnvironment::SetUp() {
    cwd = get_working_current_directory();
    SetupRbacFile();
    SetupAuditFile();
    SetupIsaslPw();
}

void McdEnvironment::SetupIsaslPw() {
    // Create an isasl file for all tests.
    isasl_file_name = "isasl." + std::to_string(cb_getpid()) +
                      "." + std::to_string(time(NULL)) + ".pw";

    // write out user/passwords
    std::ofstream isasl(isasl_file_name,
                        std::ofstream::out | std::ofstream::trunc);
    ASSERT_TRUE(isasl.is_open());
    isasl << "_admin password " << std::endl;
    for (int ii = 0; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::stringstream line;
        line << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        line << " mybucket_" << std::setfill('0') << std::setw(3) << ii
             << std::endl;
        isasl << line.rdbuf();
    }
    isasl << "bucket-1 1S|=,%#x1" << std::endl;
    isasl << "bucket-2 secret" << std::endl;

    // Add the file to the exec environment
    snprintf(isasl_env_var, sizeof(isasl_env_var),
             "ISASL_PWFILE=%s", isasl_file_name.c_str());
    putenv(isasl_env_var);
}

void McdEnvironment::SetupAuditFile() {
    try {
        audit_file_name = cwd + "/" + generateTempFile("audit.cfg.XXXXXX");
        audit_log_dir = cwd + "/" + generateTempFile("audit.log.XXXXXX");
        const std::string descriptor = cwd + "/auditd";
        EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(audit_log_dir));
        EXPECT_TRUE(CouchbaseDirectoryUtilities::mkdirp(audit_log_dir));

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

void McdEnvironment::SetupRbacFile() {
    try {
        rbac_file_name = cwd + "/" + generateTempFile("testapp_rbac.json.XXXXXX");

        unique_cJSON_ptr rbac(generate_rbac_config());
        std::string rbac_text = to_string(rbac);

        std::ofstream out(rbac_file_name);
        out.write(rbac_text.c_str(), rbac_text.size());
        out.flush();
        out.close();
    } catch (std::exception& e) {
        FAIL() << "Failed to store RBAC data: " << e.what();
    }
}

void McdEnvironment::TearDown() {
    // Cleanup Audit config file
    if (!audit_file_name.empty()) {
        EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(audit_file_name));
    }

    // Cleanup Audit log directory
    if (!audit_log_dir.empty()) {
        EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(audit_log_dir));
    }

    // Cleanup RBAC config file.
    EXPECT_NE(-1, remove(rbac_file_name.c_str()));

    // Cleanup isasl file
    EXPECT_NE(-1, remove(isasl_file_name.c_str()));

    shutdown_openssl();
}

std::string McdEnvironment::generateTempFile(const char* pattern) {
    char* file_pattern = strdup(pattern);
    if (file_pattern == nullptr) {
        throw std::bad_alloc();
    }

    if (cb_mktemp(file_pattern) == nullptr) {
        throw std::runtime_error(
            std::string("Failed to create temporary file with pattern: ") +
            std::string(pattern));
    }

    std::string ret(file_pattern);
    free(file_pattern);

    return ret;
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

char McdEnvironment::isasl_env_var[256];
