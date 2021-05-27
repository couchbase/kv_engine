/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json_fwd.hpp>

#include <string>

namespace cb::breakpad {
/**
 * What information should breakpad minidumps contain?
 */
enum class Content {
    /**
     * Default content (threads+stack+env+arguments)
     */
    Default
};

/**
 * Settings for Breakpad crash catcher.
 */
struct Settings {
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
    explicit Settings(const nlohmann::json& json);

    bool enabled{false};
    std::string minidump_dir;
    Content content{Content::Default};
};

} // namespace cb::breakpad

std::string to_string(cb::breakpad::Content content);
