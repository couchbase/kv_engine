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

#include "dcp/backfill.h"

#include <memory>

class ActiveStream;

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress - this provides for execution of a ById (key to key)
 * backfill
 */
class DCPBackfillToStream : public DCPBackfill {
public:
    DCPBackfillToStream() = delete;

    DCPBackfillToStream(std::shared_ptr<ActiveStream> s);

    /**
     * Indicates if the DCP stream associated with the backfill is dead
     *
     * @return true if stream is in dead state; else false
     */
    bool shouldCancel() const override;

protected:
    /**
     * Ptr to the associated Active DCP stream. Backfill can be run for only
     * an active DCP stream.
     * We use a weak_ptr instead of a shared_ptr to avoid cyclic reference.
     * DCPBackfill objs do not primarily own the stream objs, they only need
     * reference to a valid stream obj when backfills are run. Hence, they
     * should only hold a weak_ptr to the stream objs.
     */
    std::weak_ptr<ActiveStream> streamPtr;
};
