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
#include "item.h"

#include <cstdint>

/**
 * Captures the result of a rollback request.
 * Contains if the rollback was successful, highSeqno of the vBucket after
 * rollback, and the last snaspshot range in the vb after rollback. Also
 * contains the high completed and high prepared seqnos.
 */
class RollbackResult {
public:
    /**
     * Constructor only to be used for unsuccessful rollback. Takes a single
     * bool to indicate success. Must be false or an assertion will fire.
     */
    explicit RollbackResult(bool success);

    RollbackResult(bool success,
                   uint64_t highSeqno,
                   uint64_t snapStartSeqno,
                   uint64_t snapEndSeqno);

    bool success;
    uint64_t highSeqno;
    uint64_t snapStartSeqno;
    uint64_t snapEndSeqno;
};
