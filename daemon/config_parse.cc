/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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
#include "config_parse.h"
#include "cmdline.h"
#include "settings.h"

#include <JSON_checker.h>
#include <logger/logger.h>
#include <platform/dirutils.h>

/**
 * Load the configuration file from disk and parse it to JSON
 *
 * @param file the name of the file to load
 * @return A handle to a JSON representing the config file
 * @throws std::bad_alloc for memory allocation failures
 *         std::system_error for failures reading the file
 *         std::invalid_argument if the file content isn't JSON
 */
static unique_cJSON_ptr load_config_file(const char* file) {
    auto content = cb::io::loadFile(file);
    if (!checkUTF8JSON((const uint8_t*)content.data(), content.size())) {
        throw std::invalid_argument("load_config_file(): " + std::string(file) +
                                    " does not contain valid JSON");
    }

    return unique_cJSON_ptr{cJSON_Parse(content.c_str())};
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

void reload_config_file() {
    LOG_INFO("Reloading config file {}", get_config_file());

    try {
        Settings new_settings(load_config_file(get_config_file()));
        settings.updateSettings(new_settings, true);
    } catch (const std::exception& exception) {
        LOG_WARNING(
                "Validation failed while reloading config file '{}'. Error: {}",
                get_config_file(),
                exception.what());
    }
}
