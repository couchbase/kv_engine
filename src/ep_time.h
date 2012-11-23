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
#ifndef SRC_EP_TIME_H_
#define SRC_EP_TIME_H_ 1

#include <time.h>
#include <memcached/types.h>

#ifdef __cplusplus
extern "C" {
#endif

extern rel_time_t (*ep_current_time)(void);
extern time_t (*ep_abs_time)(rel_time_t);
extern rel_time_t (*ep_reltime)(time_t);
extern time_t ep_real_time(void);

#ifdef __cplusplus
}
#endif

#endif  // SRC_EP_TIME_H_
