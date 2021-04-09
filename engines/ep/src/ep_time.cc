/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

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
