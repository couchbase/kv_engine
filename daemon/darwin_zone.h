/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

/*
 * Support for replacing the default malloc zone on OS X, when using jemalloc
 * as the memory allocator. This is needed to allow us to insert wrappers
 * around (je)malloc & friends to perform allocation tracking for mem_used.
 */

#ifndef DARWIN_ZONE_H_
#define DARWIN_ZONE_H_

#include "alloc_hooks_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Register our wrapper zone as the system default allocator. The wrapper zone
 * simply forwards all requests to the "real" default allocator, adding in
 * memory tracking calls to the specified new_hook & delete_hook where necessary.
 */
void register_wrapper_zone(malloc_new_hook_t* new_hook,
                           malloc_delete_hook_t* delete_hook);

#ifdef __cplusplus
}
#endif

#endif /* DARWIN_ZONE_H_ */
