/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cblogger/file_sink_encryption_config.h>

namespace cb::logger {

/**
 * Build the encryption config passed to initialize() so cblogger's file sink
 * can fetch the active log DEK from cb::dek::Manager
 */
FileSinkEncryptionConfig makeFileSinkEncryptionConfig();

} // namespace cb::logger
