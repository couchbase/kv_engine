/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Configuration file parsing and handling.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <optional>

class Settings;
void load_config_file(const std::string& filename, Settings& settings);

/* Given a new, proposed config, check if it can be applied to the running
 * config. Returns true if all differences between new_cfg and the live config
 * can be applied, else returns false and updates error_msg to list of messages
 * describing what could not be applied.
 */
std::optional<nlohmann::json> validate_proposed_config_changes(
        const char* new_cfg);

