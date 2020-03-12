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
#include "config_parse.h"
#include "cmdline.h"
#include "settings.h"

#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

/******************************************************************************
 * Public functions
 *****************************************************************************/

void load_config_file(const char *file, Settings& settings)
{
    auto content = cb::io::loadFile(file, std::chrono::seconds{5});
    settings.reconfigure(nlohmann::json::parse(content));
}

boost::optional<nlohmann::json> validate_proposed_config_changes(
        const char* new_cfg) {
    nlohmann::json errors = nlohmann::json::array();
    // Earlier we returned all of the errors, now I'm terminating on
    // the first... Ideally all of the errors would be best, but
    // the code is easier if we can use exceptions to abort the parsing
    // when we hit an error. Given that this isn't something that the
    // user would be calling every time I don't think it is a big problem..
    try {
        auto json = nlohmann::json::parse(new_cfg);
        Settings new_settings(json);
        Settings::instance().updateSettings(new_settings, false);
        return {};
    } catch (const std::exception& exception) {
        errors.push_back(exception.what());
    }

    return errors;
}

void reload_config_file() {
    LOG_INFO("Reloading config file {}", get_config_file());
    auto content = cb::io::loadFile(get_config_file(), std::chrono::seconds{5});
    Settings new_settings(nlohmann::json::parse(content));
    Settings::instance().updateSettings(new_settings, true);
}
