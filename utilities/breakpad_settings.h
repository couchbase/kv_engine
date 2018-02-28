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
#include <memcached/mcd_util-visibility.h>

#include <gsl/gsl>
#include <string>

namespace cb {
namespace breakpad {
/**
 * What information should breakpad minidumps contain?
 */
enum class MCD_UTIL_PUBLIC_API Content {
    /**
     * Default content (threads+stack+env+arguments)
     */
    Default
};

/**
 * Settings for Breakpad crash catcher.
 */
struct MCD_UTIL_PUBLIC_API Settings {
    /**
     * Default constructor initialize the object to be in a disabled state
     */
    Settings() = default;

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
    explicit Settings(gsl::not_null<const cJSON*> json);

    bool enabled{false};
    std::string minidump_dir;
    Content content{Content::Default};
};

} // namespace breakpad
} // namespace cb

MCD_UTIL_PUBLIC_API
std::string to_string(cb::breakpad::Content content);
