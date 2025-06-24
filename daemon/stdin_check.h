/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <chrono>
#include <functional>

/// The exit code used by the abnormal exit handler to std::_Exit()
constexpr int abnormal_exit_handler_exit_code = 9;

void start_stdin_listener(std::function<void()> function);
void abrupt_shutdown_timeout_changed(std::chrono::milliseconds timeout);
