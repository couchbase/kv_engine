/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_types.h"

#include <memcached/durability_spec.h>
#include <optional>
#include <variant>

class CookieIface;
class PreLinkDocumentContext;

/**
 * Structure which holds the information needed when enqueueing an item which
 * has Durability requirements.
 */
struct DurabilityItemCtx {
    /**
     * Stores:
     * - the Durability Requirements, if we queue a Prepare
     * - the prepared-seqno, if we queue a Commit
     */
    std::variant<cb::durability::Requirements, int64_t>
            requirementsOrPreparedSeqno;
    /**
     * The client cookie associated with the Durability operation. If non-null
     * then notifyIOComplete will be called on it when operation is committed.
     */
    const CookieIface* cookie = nullptr;
};

/**
 * Structure that holds info needed to queue an item in chkpt or vb backfill
 * queue
 *
 * GenerateDeleteTime - Only the queueing of items where isDeleted() == true
 * does this parameter have any affect. E.g. an add of an Item with this set to
 * Yes will have no effect, whilst an explicit delete this parameter will have
 * have an effect of generating a tombstone time.
 *
 * Note that when queueing a delete with and expiry time of 0, the delete time
 * is always generated. It is invalid to queue an Item with a 0 delete time,
 * in this case the GenerateDeleteTime setting is ignored.
 *
 */
struct VBQueueItemCtx {
    VBQueueItemCtx() = default;

    VBQueueItemCtx(GenerateBySeqno genBySeqno,
                   GenerateCas genCas,
                   GenerateDeleteTime generateDeleteTime,
                   TrackCasDrift trackCasDrift,
                   std::optional<DurabilityItemCtx> durability,
                   PreLinkDocumentContext* preLinkDocumentContext_,
                   std::optional<int64_t> overwritingPrepareSeqno)
        : genBySeqno(genBySeqno),
          genCas(genCas),
          generateDeleteTime(generateDeleteTime),
          trackCasDrift(trackCasDrift),
          durability(durability),
          preLinkDocumentContext(preLinkDocumentContext_),
          overwritingPrepareSeqno(overwritingPrepareSeqno) {
    }

    GenerateBySeqno genBySeqno = GenerateBySeqno::Yes;
    GenerateCas genCas = GenerateCas::Yes;
    GenerateDeleteTime generateDeleteTime = GenerateDeleteTime::Yes;
    TrackCasDrift trackCasDrift = TrackCasDrift::No;
    /// Durability requirements. Only present for SyncWrites.
    std::optional<DurabilityItemCtx> durability = {};
    /// Context object that allows running the pre-link callback after the CAS
    /// is assigned but the document is not yet available for reading
    PreLinkDocumentContext* preLinkDocumentContext = nullptr;
    /// Passed into the durability monitor to instruct it to remove an old
    /// prepare with the given seqno
    std::optional<int64_t> overwritingPrepareSeqno = {};
    // Ephemeral only, high completed seqno to update the seqlist with
    std::optional<int64_t> hcs = {};
};
