/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "rollback_callback.h"
#include "kvstore.h"

void RollbackCB::setKVFileHandle(std::unique_ptr<KVFileHandle> handle) {
    kvFileHandle = std::move(handle);
}
