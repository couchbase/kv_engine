/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/types.h>
#include <ctime>

struct ServerCoreIface;

/* Initializes the below time functions using the function pointers
 * provided by the specified SERVER_CORE_API. This function should be
 * called before attempting to use them, typically by the first engine
 * loaded.
 * Note: Only the first call to this function will have any effect,
 * i.e.  once initialized the functions should not be modified to
 * prevent data races between the different threads which use them.
 */
void initialize_time_functions(ServerCoreIface* core_api);

extern rel_time_t ep_current_time();
extern time_t ep_abs_time(rel_time_t);
extern rel_time_t ep_reltime(rel_time_t);
extern time_t ep_real_time();
extern time_t ep_limit_abstime(time_t t, std::chrono::seconds limit);
