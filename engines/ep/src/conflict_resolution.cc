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

#include "conflict_resolution.h"
#include "item.h"
#include "stored-value.h"

/**
 * A conflict resolution strategy that compares the meta data for a document
 * from a remote node and this node. The conflict strategy works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other documents field. The fields are
 * compared in the following order: rev seqno, cas, expiration, flags, datatype.
 * If all fields are equal than the local document is chosen as the winner.
 */
ConflictResolution::Result RevisionSeqnoResolution::resolve(
        const StoredValue& v,
        const ItemMetaData& meta,
        const protocol_binary_datatype_t meta_datatype,
        bool isDelete) const {
    if (v.isTempNonExistentItem()) {
        return Result::Accept;
    }

    if (isDelete) {
        // *RevSeqno* checked first for seqno resolution
        auto existing = std::make_tuple(v.getRevSeqno(), v.getCas());

        auto incoming = std::make_tuple(meta.revSeqno, meta.cas);

        if (incoming > existing) {
            // accept the incoming version, it is ahead
            return Result::Accept;
        }

        if (incoming < existing) {
            // reject the incoming version, it is behind
            return Result::RejectBehind;
        }
    } else {
        auto existing = std::make_tuple(
                v.getRevSeqno(), v.getCas(), v.getExptime(), v.getFlags());

        auto incoming = std::make_tuple(
                meta.revSeqno, meta.cas, meta.exptime, meta.flags);

        if (incoming > existing) {
            // accept the incoming version, it is ahead
            return Result::Accept;
        }

        if (incoming < existing) {
            // reject the incoming version, it is behind
            return Result::RejectBehind;
        }

        if (cb::mcbp::datatype::is_xattr(meta_datatype) &&
            !cb::mcbp::datatype::is_xattr(v.getDatatype())) {
            return Result::Accept;
        }
    }
    return Result::RejectIdentical;
}

/**
 * A conflict resolution strategy that compares the meta data for a document
 * from a remote node and this node. This conflict resolution works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other document's fields. The fields are
 * compared in the following order: cas, rev seqno, expiration, flags, datatype.
 * Regardless of conflict resolution mode, all CAS values are generated from
 * a Hybrid Logical Clock (HLC), so a larger CAS is the last write.
 * If all fields are equal than the local document is chosen as the winner.
 */
ConflictResolution::Result LastWriteWinsResolution::resolve(
        const StoredValue& v,
        const ItemMetaData& meta,
        const protocol_binary_datatype_t meta_datatype,
        bool isDelete) const {
    if (v.isTempNonExistentItem()) {
        return Result::Accept;
    }

    if (isDelete) {
        // *CAS* checked first for LWW resolution
        auto existing = std::make_tuple(v.getCas(), v.getRevSeqno());

        auto incoming = std::make_tuple(meta.cas, meta.revSeqno);

        if (incoming > existing) {
            // accept the incoming version, it is ahead
            return Result::Accept;
        }

        if (incoming < existing) {
            // reject the incoming version, it is behind
            return Result::RejectBehind;
        }
    } else {
        auto existing = std::make_tuple(
                v.getCas(), v.getRevSeqno(), v.getExptime(), v.getFlags());

        auto incoming = std::make_tuple(
                meta.cas, meta.revSeqno, meta.exptime, meta.flags);

        if (incoming > existing) {
            // accept the incoming version, it is ahead
            return Result::Accept;
        }

        if (incoming < existing) {
            // reject the incoming version, it is behind
            return Result::RejectBehind;
        }
        if (cb::mcbp::datatype::is_xattr(meta_datatype) &&
            !cb::mcbp::datatype::is_xattr(v.getDatatype())) {
            return Result::Accept;
        }
    }
    return Result::RejectIdentical;
}
