/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc.
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

#include "config.h"
#include "ep_time.h"
#include <stdlib.h>

static rel_time_t uninitialized_current_time(void) {
    abort();
    return 0;
}

static time_t default_abs_time(rel_time_t notused) {
    (void)notused;
    abort();
    return 0;
}

static rel_time_t default_reltime(time_t notused) {
    (void)notused;
    abort();
    return 0;
}


void initialize_time_functions(const SERVER_CORE_API* core_api) {
    if (ep_current_time == uninitialized_current_time) {
        ep_current_time = core_api->get_current_time;
    }
    if (ep_abs_time == default_abs_time) {
        ep_abs_time = core_api->abstime;
    }
    if (ep_reltime == default_reltime) {
        ep_reltime = core_api->realtime;
    }
}

rel_time_t (*ep_current_time)(void) = uninitialized_current_time;
time_t (*ep_abs_time)(rel_time_t) = default_abs_time;
rel_time_t (*ep_reltime)(time_t) = default_reltime;

time_t ep_real_time(void) {
    return ep_abs_time(ep_current_time());
}
