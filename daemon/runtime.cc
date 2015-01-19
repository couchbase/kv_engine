/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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
#include "runtime.h"

#ifdef HAVE_ATOMIC
#include <atomic>
#else
#include <cstdatomic>
#endif

static std::atomic<bool> server_initialized;

bool is_server_initialized(void) {
    return server_initialized.load(std::memory_order_acquire);
}

void set_server_initialized(bool enable) {
    server_initialized.store(enable, std::memory_order_release);
}
