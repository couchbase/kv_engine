/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_CONFLICT_RESOLUTION_H_
#define SRC_CONFLICT_RESOLUTION_H_ 1

#include "config.h"
#include "item.h"
#include <vbucket.h>

class ItemMetaData;
class StoredValue;

/**
 * An abstract class for doing conflict resolution for documents sent from
 * different datacenters.
 */
class ConflictResolution {
public:
    ConflictResolution() {}

    ~ConflictResolution() {}

    /**
     * Resolves a conflict between two documents.
     *
     * @param v the local document meta data
     * @param meta the remote document's meta data
     * @param isDelete the flag indicating if conflict resolution is
     * for delete operations
     * @param itmConfResMode conflict resolution mode of the
     * remote document
     * @return true is the remote document is the winner, false otherwise
     */
    bool resolve(RCPtr<VBucket> &vb, StoredValue *v, const ItemMetaData &meta,
                 bool isDelete = false,
                 enum conflict_resolution_mode itmConfResMode = revision_seqno);

private:
    bool resolve_rev_seqno(StoredValue *v, const ItemMetaData &meta,
                           bool isDelete = false);

    bool resolve_lww(StoredValue *v, const ItemMetaData &meta,
                     bool isDelete = false);
};

#endif  // SRC_CONFLICT_RESOLUTION_H_
