/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#pragma once

#include <cJSON.h>
#include <gsl/gsl>
#include <string>

/**
 * What information should breakpad minidumps contain?
 */
enum class BreakpadContent {
    /**
     * Default content (threads+stack+env+arguments)
     */
    Default
};

std::string to_string(BreakpadContent content);

/**
 * Settings for Breakpad crash catcher.
 */
class BreakpadSettings {
public:
    /**
     * Default constructor initialize the object to be in a disabled state
     */
    BreakpadSettings() = default;

    /**
     * Initialize the Breakpad object from the specified JSON structure
     * which looks like:
     *
     *     {
     *         "enabled" : true,
     *         "minidump_dir" : "/var/crash",
     *         "content" : "default"
     *     }
     *
     * @param json The json to parse
     * @throws std::invalid_argument if the json dosn't look as expected
     */
    explicit BreakpadSettings(gsl::not_null<const cJSON*> json);

    bool isEnabled() const {
        return enabled;
    }

    void setEnabled(bool enabled) {
        BreakpadSettings::enabled = enabled;
    }

    const std::string& getMinidumpDir() const {
        return minidump_dir;
    }

    void setMinidumpDir(const std::string& minidump_dir) {
        BreakpadSettings::minidump_dir = minidump_dir;
    }

    BreakpadContent getContent() const {
        return content;
    }

    void setContent(BreakpadContent content) {
        BreakpadSettings::content = content;
    }

protected:
    bool enabled{false};
    std::string minidump_dir;
    BreakpadContent content{BreakpadContent::Default};
};
