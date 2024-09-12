/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "memcached/vbucket.h"
#include <string_view>

namespace Backup {

std::string encodeBackupMetadata(
        uint64_t maxCas, const std::vector<vbucket_failover_t>& failoverTable);

nlohmann::json decodeBackupMetadata(std::string_view data);

} // namespace Backup
