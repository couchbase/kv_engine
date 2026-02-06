/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ioctl_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/ioctl.h>
#include <executor/globaltask.h>

IoctlCommandContext::IoctlCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_IoCtlTask,
              fmt::format("IoctlCommandContext:{}",
                          cookie.getRequest().getClientOpcode()),
              ConcurrencySemaphores::instance().ioctl) {
}

cb::engine_errc IoctlCommandContext::execute() {
    auto& req = cookie.getRequest();
    const auto key = std::string{cookie.getRequest().getKeyString()};
    if (req.getClientOpcode() == cb::mcbp::ClientOpcode::IoctlSet) {
        const auto value = std::string{cookie.getRequest().getValueString()};
        return ioctl_set_property(cookie, key, value, response, datatype);
    }
    return ioctl_get_property(cookie, key, response, datatype);
}
