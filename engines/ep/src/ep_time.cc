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

#include <memcached/server_core_iface.h>

#include <atomic>
#include <stdexcept>

static std::atomic<ServerCoreIface*> core{nullptr};

rel_time_t ep_current_time() {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->get_current_time();
    }
    throw std::logic_error("ep_current_time called, but core API not set");
}

time_t ep_abs_time(rel_time_t rel_time) {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->abstime(rel_time);
    }
    throw std::logic_error("ep_abs_time called, but core API not set");
}

rel_time_t ep_reltime(rel_time_t exptime) {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->realtime(exptime);
    }
    throw std::logic_error("ep_reltime called, but core API not set");
}

time_t ep_limit_abstime(time_t t, std::chrono::seconds limit) {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->limit_abstime(t, limit);
    }
    throw std::logic_error("ep_limit_abstime called, but core API not set");
}

void initialize_time_functions(ServerCoreIface* core_api) {
    core.store(core_api, std::memory_order_release);
}

time_t ep_real_time() {
    return ep_abs_time(ep_current_time());
}
