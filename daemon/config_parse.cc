/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
#include "memcached.h"

#include "config_parse.h"

#include <sstream>
#include "cmdline.h"
#include "config_util.h"
#include "config_parse.h"

/**
 * Load the configuration file from disk and parse it to JSON
 *
 * @param file the name of the file to load
 * @return A handle to a JSON representing the config file
 * @throws std::runtime_error if an error occurs
 */
static unique_cJSON_ptr load_config_file(const char* file) {
    cJSON *sys;
    config_error_t err = config_load_file(file, &sys);

    if (err != CONFIG_SUCCESS) {
        char *msg = config_strerror(file, err);
        std::string errormsg(msg);
        free(msg);
        throw std::runtime_error(errormsg);
    }

    return unique_cJSON_ptr(sys);
}


/******************************************************************************
 * Public functions
 *****************************************************************************/

void load_config_file(const char *file, Settings& settings)
{
    settings.reconfigure(load_config_file(file));
}

bool validate_proposed_config_changes(const char* new_cfg, cJSON* errors) {

    unique_cJSON_ptr config(cJSON_Parse(new_cfg));
    if (config.get() == nullptr) {
        cJSON_AddItemToArray(errors, cJSON_CreateString("JSON parse error"));
        return false;
    }

    // Earlier we returned all of the errors, now I'm terminating on
    // the first... Ideally all of the errors would be best, but
    // the code is easier if we can use exceptions to abort the parsing
    // when we hit an error. Given that this isn't something that the
    // user would be calling every time I don't think it is a big problem..
    try {
        Settings new_settings(config);
        settings.updateSettings(new_settings, false);
        return true;
    } catch (const std::exception& exception) {
        cJSON_AddItemToArray(errors, cJSON_CreateString(exception.what()));
    }

    return false;
}

void reload_config_file(void) {
    LOG_NOTICE(NULL, "Reloading config file %s", get_config_file());

    try {
        Settings new_settings(load_config_file(get_config_file()));
        settings.updateSettings(new_settings, true);
    } catch (const std::exception& exception) {
        LOG_WARNING(NULL,
                    "Validation failed while reloading config file '%s'. Error: %s",
                    get_config_file(), exception.what());
    }
}
