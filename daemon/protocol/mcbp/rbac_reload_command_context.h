/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "file_reload_command_context.h"

/**
 * RbacReloadCommandContext is responsible for handling the
 * rbac reload command. Due to the fact that this involves disk IO
 * it'll offload the task to another thread to do the
 * actual work which notifies the command cookie when it's done.
 */
class RbacReloadCommandContext : public FileReloadCommandContext {
public:
    explicit RbacReloadCommandContext(Cookie& cookie)
        : FileReloadCommandContext(cookie) {
    }

protected:
    cb::engine_errc reload() override;

private:
    cb::engine_errc doRbacReload();
};
