/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp/active_stream.h"
#include "dcp/backfill.h"

DCPBackfill::DCPBackfill(std::shared_ptr<ActiveStream> s)
    : streamPtr(s), vbid(s->getVBucket()) {
}

// Task should be cancelled if the stream cannot be obtained or is now dead
bool DCPBackfill::shouldCancel() const {
    auto stream = streamPtr.lock();
    return !stream || !stream->isActive();
}
