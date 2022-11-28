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
#include <iosfwd>
#include <string>

namespace cb::breakpad {
/**
 * What information should breakpad minidumps contain? Currently we
 * only allow "default" which would be (threads+stack+env+arguments),
 * but allow for other values once breakpad supports it
 */
enum class Content : bool {
    /// Default content (threads+stack+env+arguments)
    Default
};
std::ostream& operator<<(std::ostream& os, const Content& content);

/// Settings for Breakpad crash catcher.
struct Settings {
    /// Perform validation on the settings (if it is enabled the
    /// directory must be specified and exists)
    /// @throws std::system_error if enabled and minidump_dir is nonexistent
    /// @throws std::invalid_argument if enabled and minidump_dir is empty
    void validate() const;

    std::string minidump_dir;
    Content content{Content::Default};
    bool enabled{false};
};

void to_json(nlohmann::json& json, const Settings& settings);
void from_json(const nlohmann::json& json, Settings& settings);
} // namespace cb::breakpad
