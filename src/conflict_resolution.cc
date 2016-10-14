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

#include "config.h"
#include "conflict_resolution.h"
#include "stored-value.h"

/**
 * A conflict resolution strategy that compares the meta data for a document
 * from a remote node and this node. The conflict strategy works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other documents field. The fields are
 * compared in the following order: rev seqno, cas, expiration, flags. If all
 * fields are equal than the local document is chosen as the winner.
 */
bool RevisionSeqnoResolution::resolve(const StoredValue& v,
                                      const ItemMetaData& meta,
                                      bool isDelete) const {
    if (!v.isTempNonExistentItem()) {
        if (v.getRevSeqno() > meta.revSeqno) {
            return false;
        } else if (v.getRevSeqno() == meta.revSeqno) {
            if (v.getCas() > meta.cas) {
                return false;
            } else if (v.getCas() == meta.cas) {
                if (isDelete || v.getExptime() > meta.exptime) {
                    return false;
                } else if (v.getExptime() == meta.exptime) {
                    if (v.getFlags() >= meta.flags) {
                        return false;
                    }
                }
            }
        }
    }
    return true;

}

/**
 * A conflict resolution strategy that compares the meta data for a document
 * from a remote node and this node. This conflict resolution works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other document's fields. The fields are
 * compared in the following order: cas, rev seqno, expiration, flags.
 * Regardless of conflict resolution mode, all CAS values are generated from
 * a Hybrid Logical Clock (HLC), so a larger CAS is the last write.
 * If all fields are equal than the local document is chosen as the winner.
 */
bool LastWriteWinsResolution::resolve(const StoredValue& v,
                                      const ItemMetaData& meta,
                                      bool isDelete) const {
    if (!v.isTempNonExistentItem()) {
        if (v.getCas() > meta.cas) {
            return false;
        } else if (v.getCas() == meta.cas) {
            if (v.getRevSeqno() > meta.revSeqno) {
                return false;
            } else if (v.getRevSeqno() == meta.revSeqno) {
                if (isDelete || v.getExptime() > meta.exptime) {
                    return false;
                } else if (v.getExptime() == meta.exptime) {
                    if (v.getFlags() >= meta.flags) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}