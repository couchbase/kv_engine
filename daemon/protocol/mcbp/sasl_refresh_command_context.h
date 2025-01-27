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

#include "background_thread_command_context.h"

#include <daemon/memcached.h>

/**
 * SaslRefreshCommandContext is responsible for handling the
 * isasl refresh command. Due to the fact that the task involves file IO
 * it'll offload the task to another thread to do the actual work which
 * notifies the command cookie when it's done.
 */
class SaslRefreshCommandContext : public BackgroundThreadCommandContext {
public:
    explicit SaslRefreshCommandContext(Cookie& cookie);

protected:
    cb::engine_errc execute() override;

private:
    cb::engine_errc doSaslRefresh();
};
