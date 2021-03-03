/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "ep_types.h"

#include <memcached/durability_spec.h>
#include <optional>
#include <variant>

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
    const void* cookie = nullptr;
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
