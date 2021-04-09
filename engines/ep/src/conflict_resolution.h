/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <mcbp/protocol/datatype.h>

class ItemMetaData;
class StoredValue;

/**
 * An abstract class for doing conflict resolution for documents sent from
 * different datacenters.
 */
class ConflictResolution {
public:
    ConflictResolution() {}

    virtual ~ConflictResolution() {}

    /**
     * Resolves a conflict between two documents.
     *
     * @param v the local document meta data
     * @param meta the remote document's meta data
     * @param meta_dataype datatype of remote document
     * @param isDelete the flag indicating if conflict resolution is
     *                 for delete operations
     * @return true is the remote document is the winner, false otherwise
     */
    virtual bool resolve(const StoredValue& v,
                         const ItemMetaData& meta,
                         const protocol_binary_datatype_t meta_datatype,
                         bool isDelete = false) const = 0;

};

class RevisionSeqnoResolution : public ConflictResolution {
public:
    bool resolve(const StoredValue& v,
                 const ItemMetaData& meta,
                 const protocol_binary_datatype_t meta_datatype,
                 bool isDelete = false) const override;
};

class LastWriteWinsResolution : public ConflictResolution {
public:
    bool resolve(const StoredValue& v,
                 const ItemMetaData& meta,
                 const protocol_binary_datatype_t meta_datatype,
                 bool isDelete = false) const override;
};
