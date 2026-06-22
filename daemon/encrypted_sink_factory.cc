/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "encrypted_sink_factory.h"

#include <dek/manager.h>

namespace cb::logger {

FileSinkEncryptionConfig makeFileSinkEncryptionConfig() {
    auto& manager = cb::dek::Manager::instance();
    auto lookup_function = [&manager]() {
        return manager.lookup(cb::dek::Entity::Logs);
    };
    auto current_version =
            manager.getEntityGenerationCounter(cb::dek::Entity::Logs);

    return FileSinkEncryptionConfig{lookup_function, current_version};
}

} // namespace cb::logger
