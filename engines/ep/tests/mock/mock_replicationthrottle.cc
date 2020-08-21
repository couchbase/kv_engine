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
