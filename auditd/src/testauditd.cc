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

#include <iostream>
#include <fstream>
#include <sstream>
#include <limits.h>
#include <cJSON.h>
#include <platform/dirutils.h>

#include "memcached/extension.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "auditd.h"

int main (int argc, char *argv[])
{
#ifdef WIN32
#define sleep(a) Sleep(a * 1000)
#endif
    /* create the test directory */
    if (!CouchbaseDirectoryUtilities::isDirectory(std::string("test"))) {
#ifdef WIN32
        if (!CreateDirectory("test", NULL)) {
#else
            if (mkdir("test", S_IREAD | S_IWRITE | S_IEXEC) != 0) {
#endif
                std::cerr << "error, unable to create directory" << std::endl;
                return -1;
            }
        }

    /* create the test_audit.json file */
    cJSON *config_json = cJSON_CreateObject();
    if (config_json == NULL) {
        std::cerr << "error, unable to create object" << std::endl;
        return -1;
    }
    cJSON_AddNumberToObject(config_json, "version", 1);
    cJSON_AddFalseToObject(config_json,"auditd_enabled");
    cJSON_AddNumberToObject(config_json, "rotate_interval", 1);
    cJSON_AddStringToObject(config_json, "log_path", "test");
    cJSON_AddStringToObject(config_json, "archive_path", "test");
    int integers[5] = {4096, 4097, 4098, 4099, 4100};
    cJSON *enabled_arr = cJSON_CreateIntArray(integers, 5);
    if (enabled_arr == NULL) {
        std::cerr << "error, unable to create int array" << std::endl;
        return -1;
    }
    cJSON_AddItemToObject(config_json, "enabled", enabled_arr);
    cJSON *sync_arr = cJSON_CreateArray();
    if (sync_arr == NULL) {
        std::cerr << "error, unable to create array" << std::endl;
        return -1;
    }
    cJSON_AddItemToObject(config_json, "sync", sync_arr);

    std::stringstream audit_fname;
    audit_fname << "test_audit.json";
    std::ofstream configfile;
    configfile.open(audit_fname.str().c_str(), std::ios::out | std::ios::binary);
    if (!configfile.is_open()) {
        std::cerr << "error, unable to create test_audit.json" << std::endl;
        return -1;
    }
    char *data = cJSON_Print(config_json);
    assert(data != NULL);
    configfile << data;
    configfile.close();

    /*create the test1_audit.json file */
    //cJSON_ReplaceItemInObject(config_json, "rotate_interval", cJSON_CreateNumber(3.0));
    cJSON_DeleteItemFromObject(config_json, "auditd_enabled");
    cJSON_AddTrueToObject(config_json,"auditd_enabled");
    std::stringstream audit_fname1;
    audit_fname1 << "test1_audit.json";
    configfile.open(audit_fname1.str().c_str(), std::ios::out | std::ios::binary);
        if (!configfile.is_open()) {
            std::cerr << "error, unable to create test1_audit.json" << std::endl;
            return -1;
        }
    data = cJSON_Print(config_json);
    assert(data != NULL);
    configfile << data;
    configfile.close();


    AUDIT_EXTENSION_DATA audit_extension_data;
    audit_extension_data.version = 1;
    audit_extension_data.min_file_rotation_time = 1;
    audit_extension_data.max_file_rotation_time = 604800;  // 1 week = 60*60*24*7
    audit_extension_data.log_extension = get_stderr_logger();

    /* start the tests */
    if (initialize_auditdaemon(audit_fname.str().c_str(), &audit_extension_data) != AUDIT_SUCCESS) {
        std::cerr << "initialize audit daemon: FAILED" << std::endl;
        return -1;
    } else {
        std::cerr << "initialize audit daemon: SUCCESS\n" << std::endl;
    }
    /* sleep is used to ensure get to cb_cond_wait(&events_arrived, &producer_consumer_lock); */
    /* will also give time to rotate */
    sleep(2);

    if (reload_auditdaemon_config(audit_fname1.str().c_str()) != AUDIT_SUCCESS) {
        std::cerr << "reload: FAILED" << std::endl;
        return -1;
    } else {
        std::cerr << "reload: SUCCESS\n" << std::endl;
    }

    sleep(4);
    if (shutdown_auditdaemon() != AUDIT_SUCCESS) {
        std::cerr << "shutdown audit daemon: FAILED" << std::endl;
        return -1;
    } else {
        std::cerr << "shutdown audit daemon: SUCCESS" << std::endl;
    }
    return 0;
}
