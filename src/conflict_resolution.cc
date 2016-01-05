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

static const char * getConflictResModeStr(enum conflict_resolution_mode confResMode) {
    switch (confResMode) {
        case revision_seqno:
            return "revision_seqno";
        case last_write_wins:
            return "last_write_wins";
        default:
            return "unknown";
    }
}

/**
 * A conflict resolution strategy that compares the meta data for a document
 * from a remote node and this node. This conflict resolution works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other document's fields. The fields are
 * compared in the following order: cas, rev seqno, expiration, flags. The cas
 * is chosen as the first field of comparison if the document's conflict
 * resolution is set last_write_wins. The last_write_wins indicates that the cas
 * generated for the document was using a Hybrid Logical Clock (HLC). If all
 * fields are equal than the local document is chosen as the winner.
 */
bool ConflictResolution::resolve_lww(StoredValue *v,
                                     const ItemMetaData &meta,
                                     bool deletion) {
    if (!v->isTempNonExistentItem()) {
        if (v->getCas() > meta.cas) {
            return false;
        } else if (v->getCas() == meta.cas) {
            if (v->getRevSeqno() > meta.revSeqno) {
                return false;
            } else if (v->getRevSeqno() == meta.revSeqno) {
                if (deletion || v->getExptime() > meta.exptime) {
                    return false;
                } else if (v->getExptime() == meta.exptime) {
                    if (v->getFlags() >= meta.flags) {
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
 * from a remote node and this node. The conflict strategy works by picking
 * a winning document based on comparing meta data fields and finding a field
 * that has a larger value than the other documents field. The fields are
 * compared in the following order: rev seqno, cas, expiration, flags. If all
 * fields are equal than the local document is chosen as the winner.
 */
bool ConflictResolution::resolve_rev_seqno(StoredValue *v,
                                           const ItemMetaData &meta,
                                           bool deletion) {
    if (!v->isTempNonExistentItem()) {
        if (v->getRevSeqno() > meta.revSeqno) {
            return false;
        } else if (v->getRevSeqno() == meta.revSeqno) {
            if (v->getCas() > meta.cas) {
                return false;
            } else if (v->getCas() == meta.cas) {
                if (deletion || v->getExptime() > meta.exptime) {
                    return false;
                } else if (v->getExptime() == meta.exptime) {
                    if (v->getFlags() >= meta.flags) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

bool ConflictResolution::resolve(RCPtr<VBucket> &vb , StoredValue *v,
                                 const ItemMetaData &meta,
                                 bool deletion, enum conflict_resolution_mode
                                 itmConfResMode) {
    if (vb->isTimeSyncEnabled()) {
        if (v->getConflictResMode() == last_write_wins &&
                itmConfResMode == last_write_wins) {
            return resolve_lww(v, meta, deletion);
        } else if ((v->getConflictResMode() != last_write_wins &&
                       itmConfResMode == last_write_wins) ||
                   (v->getConflictResMode() == last_write_wins &&
                       itmConfResMode != last_write_wins)) {
                   // Log the event when the time sync is enabled and
                   // the mutation is not eligible for last_write_wins
                   LOG(EXTENSION_LOG_DEBUG,
                       "Resolving conflict by comparing rev seqno: key: %s,"
                       "source conflict resolution mode: %s, target conflict resolution"
                       "mode: %s", v->getKey().c_str(),
                       getConflictResModeStr(itmConfResMode),
                       getConflictResModeStr(v->getConflictResMode()));
        }
    }

    return resolve_rev_seqno(v, meta, deletion);
}
