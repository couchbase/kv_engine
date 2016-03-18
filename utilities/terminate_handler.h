/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "config.h"

#include <memcached/extension.h>

/**
 * Interposes our own C++ terminate handler to print backtrace upon failures.
 * Chains to the default handler (if exists) after printing the backtrace,
 * then calls std::abort().
 * @param logger the logger descriptor to use to print the backtrace in the
 *               event of termination. If nullptr then will log directly to
 *               stderr.
 */
void install_backtrace_terminate_handler(EXTENSION_LOGGER_DESCRIPTOR* logger = nullptr);
