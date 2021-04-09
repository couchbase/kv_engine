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

#include "rollback_result.h"
#include <gsl/gsl-lite.hpp>

RollbackResult::RollbackResult(bool success)
    : success(success), highSeqno(0), snapStartSeqno(0), snapEndSeqno(0) {
    Expects(!success);
}

RollbackResult::RollbackResult(bool success,
                               uint64_t highSeqno,
                               uint64_t snapStartSeqno,
                               uint64_t snapEndSeqno)
    : success(success),
      highSeqno(highSeqno),
      snapStartSeqno(snapStartSeqno),
      snapEndSeqno(snapEndSeqno) {
}