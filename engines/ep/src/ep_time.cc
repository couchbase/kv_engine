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

#include <gsl/gsl-lite.hpp>
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

std::chrono::steady_clock::time_point ep_uptime_now() {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->get_uptime_now();
    }
    throw std::logic_error("ep_uptime_now called, but core API not set");
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

uint32_t ep_limit_expiry_time(uint32_t t, std::chrono::seconds limit) {
    auto* iface = core.load(std::memory_order_acquire);
    if (iface) {
        return iface->limit_expiry_time(t, limit);
    }
    throw std::logic_error("ep_limit_expiry_time called, but core API not set");
}

void initialize_time_functions(ServerCoreIface* core_api) {
    core.store(core_api, std::memory_order_release);
}

time_t ep_real_time() {
    return ep_abs_time(ep_current_time());
}

uint32_t ep_convert_to_expiry_time(uint32_t mcbpExpTime) {
    // @todo: MB-67576. The value being casted could genuienly be > 2^32 as it
    // could be the current time + mcbpExpTime. On a well managed system this
    // isn't a problem for a long time (2^32 seconds since unix epoch). But
    // never trust the system clock as such, however if we cannot generate
    // an expiry time that fits u32, we will have to just fail.
    return (mcbpExpTime == 0) ? 0
                              : gsl::narrow_cast<uint32_t>(
                                        ep_abs_time(ep_reltime(mcbpExpTime)));
}

uint32_t ep_generate_delete_time() {
    // Narrowing note: For now narrow_cast accepting that this has always been a
    // data loss (overflow) risk in the case when ep_real_time returns more than
    // 2^32 (unlikely). For now we don't want to add a new failure point with
    // narrow<>
    return gsl::narrow_cast<uint32_t>(ep_real_time());
}