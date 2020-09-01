/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "replicationthrottle.h"
#include <folly/portability/GMock.h>

/**
 * A mock ReplicationThrottle, using GoogleMock.
 */
struct MockReplicationThrottle : public ReplicationThrottle {
    /**
     * Construct a MockReplicationThrottle wrapping the specified real
     * ReplicationThrottle. By default all member function calls will be
     * forwarded to the real throttle.
     * @param real Pointer to real throttle. Note this is an *owning* pointer -
     *        the Mock takes ownership of the real throttle and will delete
     *        it when the mock is deleted.
     *        Ideally would use std::unique_ptr here but cannot due to
     *        limitations in GoogleMock's NiceMock class which wraps
     *        MockReplicationThrottle - all arguments to that must be
     *        passed via const&.
     */
    MockReplicationThrottle(ReplicationThrottle* real);

    MOCK_CONST_METHOD0(getStatus, Status());
    MOCK_CONST_METHOD0(doDisconnectOnNoMem, bool());
    MOCK_METHOD1(setCapPercent, void(size_t));
    MOCK_METHOD1(setQueueCap, void(ssize_t));
    MOCK_METHOD1(adjustWriteQueueCap, void(size_t));

    std::unique_ptr<ReplicationThrottle> realThrottle;
};
