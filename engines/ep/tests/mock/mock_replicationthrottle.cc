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

#include "mock_replicationthrottle.h"

MockReplicationThrottle::MockReplicationThrottle(ReplicationThrottle* real)
    : realThrottle(real) {
    // By default, all calls to mocked methods are delegated to the real
    // object.
    using namespace ::testing;
    ON_CALL(*this, getStatus())
            .WillByDefault(Invoke(realThrottle.get(),
                                  &ReplicationThrottle::getStatus));

    ON_CALL(*this, doDisconnectOnNoMem())
            .WillByDefault(Invoke(realThrottle.get(),
                                  &ReplicationThrottle::doDisconnectOnNoMem));

    ON_CALL(*this, setCapPercent(_))
            .WillByDefault(Invoke(realThrottle.get(),
                                  &ReplicationThrottle::setCapPercent));
    ON_CALL(*this, setQueueCap(_))
            .WillByDefault(Invoke(realThrottle.get(),
                                  &ReplicationThrottle::setQueueCap));
    ON_CALL(*this, adjustWriteQueueCap(_))
            .WillByDefault(Invoke(realThrottle.get(),
                                  &ReplicationThrottle::adjustWriteQueueCap));
}
