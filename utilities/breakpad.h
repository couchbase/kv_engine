/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "breakpad_settings.h"

namespace cb::logger {
struct Config;
}

namespace cb::breakpad {
/**
 * Initialize breakpad based on the specified settings struct.
 *
 * The function may be called multiple times and allow for reconfiguration
 * of the breakpad settings.
 */
void initialize(const cb::breakpad::Settings& settings,
                const cb::logger::Config& logConfig);

/**
 * Cleaning up when breakpad no longer needed
 * (Assuming it is enabled and has been initialized)
 */
void destroy();

} // namespace cb::breakpad
