/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstddef>

/**
 * Abstract file handle class to allow a DB file to be opened and held open
 * for multiple KVStore methods.
 */
class KVFileHandle {
public:
    virtual ~KVFileHandle() = default;

    // @returns how many bytes could be freed by closing this hndle. This
    // occurs when the open snapshot is not current, i.e. we're holding a delete
    // snapshot which is "inflating" disk usage.
    virtual size_t getHowManyBytesCouldBeFreed() const {
        return 0; // default implementation is to say nothing.
    }
};
