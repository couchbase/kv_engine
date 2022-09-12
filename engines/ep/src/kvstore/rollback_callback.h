/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "callbacks.h"

class KVFileHandle;

/**
 * Callback class used by DcpConsumer, for rollback operation
 */
class RollbackCB : public StatusCallback<GetValue> {
public:
    void callback(GetValue& val) override = 0;

    virtual void setKVFileHandle(std::unique_ptr<KVFileHandle> handle);

    virtual const KVFileHandle* getKVFileHandle() const {
        return kvFileHandle.get();
    }

protected:
    /// The database handle to use when lookup up items in the new, rolled back
    /// database.
    std::unique_ptr<KVFileHandle> kvFileHandle;
};
