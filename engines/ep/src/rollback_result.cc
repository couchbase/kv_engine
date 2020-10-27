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