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
    MetaData() : deleted(0), deleteSource(0){};

    explicit MetaData(const Item& it);

    // Magma requires meta data for local documents. Rather than support 2
    // different meta data versions, we simplify by using just 1.
    MetaData(bool isDeleted, uint32_t valueSize, int64_t seqno, Vbid vbid);

    cb::durability::Level getDurabilityLevel() const;

    cb::uint48_t getPrepareSeqno() const {
        return durabilityDetails.completed.prepareSeqno;
    }

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

    union durabilityDetails {
        // Need to supply a default constructor or the compiler will
        // complain about cb::uint48_t
        durabilityDetails() : raw(0){};

        struct {
            // 0:pendingSyncWrite, 1:pendingSyncDelete
            uint8_t isDelete : 1;
            // cb::durability::level
            uint8_t level : 2;
        } pending;

        struct completedDetails {
            // prepareSeqno of the completed Syncwrite
            cb::uint48_t prepareSeqno;
        } completed;

        cb::uint48_t raw;
    } durabilityDetails;
};
#pragma pack()

static_assert(sizeof(MetaData) == 45,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv
