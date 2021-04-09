/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/**
 * Interposes our own C++ terminate handler to print backtrace upon failures.
 * Chains to the default handler (if exists) after printing the backtrace,
 * then calls std::abort().
 */
void install_backtrace_terminate_handler();

/**
 * Control if our C++ terminate handler should include a backtrace or not.
 */
void set_terminate_handler_print_backtrace(bool print);
