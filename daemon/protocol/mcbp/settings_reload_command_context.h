/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "file_reload_command_context.h"

/// Class to deal with the reload of memcached.json and reconfigure
/// the system
class SettingsReloadCommandContext : public FileReloadCommandContext {
public:
    explicit SettingsReloadCommandContext(Cookie& cookie);

protected:
    cb::engine_errc reload() override;

private:
    /// Do the actual settings reload
    cb::engine_errc doSettingsReload();
};
