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
