/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "item.h"

namespace magmakv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to Magma and when reading a Document
// from Magma.

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
class MetaData {
public:
    // The Operation this represents - maps to queue_op types:
    enum class Operation {
        // A standard mutation (or deletion). Present in the 'normal'
        // (committed) namespace.
        Mutation,

        // A prepared SyncWrite. `durability_level` field indicates the level
        // Present in the DurabilityPrepare namespace.
        PreparedSyncWrite,

        // A committed SyncWrite.
        // This exists so we can correctly backfill from disk a Committed
        // mutation and sent out as a DCP_COMMIT to sync_replication
        // enabled DCP clients.
        // Present in the 'normal' (committed) namespace.
        CommittedSyncWrite,

        // An aborted SyncWrite.
        // This exists so we can correctly backfill from disk an Aborted
        // mutation and sent out as a DCP_ABORT to sync_replication
        // enabled DCP clients.
        // Present in the DurabilityPrepare namespace.
        Abort,
    };

    MetaData()
        : deleted(0), deleteSource(0), operation(0), durabilityLevel(0){};

    explicit MetaData(const Item& it);

    // Magma requires meta data for local documents. Rather than support 2
    // different meta data versions, we simplify by using just 1.
    MetaData(bool isDeleted, uint32_t valueSize, int64_t seqno, Vbid vbid);

    Operation getOperation() const {
        return static_cast<Operation>(operation);
    }

    cb::durability::Level getDurabilityLevel() const;
    std::string to_string(Operation op) const;

    std::string to_string() const;

    uint8_t metaDataVersion = 0;
    int64_t bySeqno = 0;
    uint64_t cas = 0;
    cb::uint48_t revSeqno = 0;
    uint32_t exptime = 0;
    uint32_t flags = 0;
    uint32_t valueSize = 0;
    uint16_t vbid = 0;
    uint8_t datatype = 0;
    uint8_t deleted : 1;
    uint8_t deleteSource : 1;
    uint8_t operation : 2;
    uint8_t durabilityLevel : 2;
    cb::uint48_t prepareSeqno = 0;

private:
    static Operation toOperation(queue_op op);
};
#pragma pack()

static_assert(sizeof(MetaData) == 45,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv
