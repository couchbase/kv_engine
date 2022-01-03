/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstddef>
#include <utility>

/**
 * Get the current terminal width and height
 *
 * @return width and height
 * @thros std::system_error if an error occurs
 */
std::pair<size_t, size_t> getTerminalSize();
