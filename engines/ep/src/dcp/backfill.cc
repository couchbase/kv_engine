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

#include "dcp/backfill.h"
#include "bucket_logger.h"
#include "dcp/active_stream.h"

#include <folly/ScopeGuard.h>
#include <phosphor/phosphor.h>

DCPBackfill::DCPBackfill(std::shared_ptr<ActiveStream> s)
    : streamPtr(s), vbid(s->getVBucket()) {
}

backfill_status_t DCPBackfill::run() {
    std::lock_guard<std::mutex> lh(lock);
    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });

    TRACE_EVENT2("dcp/backfill",
                 "DCPBackfill::run",
                 "vbid",
                 getVBucketId().get(),
                 "state",
                 uint8_t(state));

    backfill_status_t status = backfill_finished;
    switch (state) {
    case State::Create:
        status = create();
        break;
    case State::Scan:
        status = scan();
        break;
    case State::Done:
        // As soon as we return finished, we change to State::Done, finished
        // signals the caller should not call us again so throw if that occurs
        throw std::logic_error(fmt::format("{}: {} called in State::Done",
                                           __PRETTY_FUNCTION__,
                                           getVBucketId()));
    }

    if (status == backfill_finished) {
        transitionState(State::Done);
    }

    return status;
}

void DCPBackfill::cancel() {
    std::lock_guard<std::mutex> lh(lock);
    if (state != State::Done) {
        EP_LOG_WARN(
                "DCPBackfill::cancel ({}) cancelled before reaching "
                "State::Done",
                getVBucketId());
    }
}

std::ostream& operator<<(std::ostream& os, DCPBackfill::State state) {
    switch (state) {
    case DCPBackfill::State::Create:
        return os << "State::Create";
    case DCPBackfill::State::Scan:
        return os << "State::Scan";
    case DCPBackfill::State::Done:
        return os << "State::Done";
    }
    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
    return os;
}

void DCPBackfill::transitionState(State newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
    case State::Create:
        // No valid transition to 'create'
        break;
    case State::Scan:
        if (state == State::Create) {
            validTransition = true;
        }
        break;
    case State::Done:
        if (state == State::Create || state == State::Scan) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                fmt::format("{}: newState:{} is not valid "
                            "for current state:{}",
                            __PRETTY_FUNCTION__,
                            newState,
                            state));
    }

    state = newState;
}

// Task should be cancelled if the stream cannot be obtained or is now dead
bool DCPBackfill::shouldCancel() const {
    auto stream = streamPtr.lock();
    return !stream || !stream->isActive();
}
