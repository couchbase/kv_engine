/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

    MetaData() = default;
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

    uint8_t metaDataVersion;
    int64_t bySeqno;
    uint64_t cas;
    cb::uint48_t revSeqno;
    uint32_t exptime;
    uint32_t flags;
    uint32_t valueSize;
    uint16_t vbid;
    uint8_t datatype;
    uint8_t deleted : 1;
    uint8_t deleteSource : 1;
    uint8_t operation : 2;
    uint8_t durabilityLevel : 2;
    cb::uint48_t prepareSeqno;

private:
    static Operation toOperation(queue_op op);
};
#pragma pack()

static_assert(sizeof(MetaData) == 45,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv
