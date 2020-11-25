/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "item.h"
#include "systemevent_factory.h"

#include <folly/portability/GTest.h>

TEST(SystemEventTest, make) {
    auto value = SystemEventFactory::makeCollectionEvent(
            CollectionID::Default, {}, OptionalSeqno(/*none*/));
    EXPECT_EQ(queue_op::system_event, value->getOperation());
    EXPECT_EQ(0, value->getNBytes());
    EXPECT_EQ(uint32_t(SystemEvent::Collection), value->getFlags());
    EXPECT_EQ(-1, value->getBySeqno());

    std::array<uint8_t, 100> data;
    value = SystemEventFactory::makeCollectionEvent(CollectionID::Default,
                                                    {data.data(), data.size()},
                                                    OptionalSeqno(/*none*/));
    EXPECT_EQ(queue_op::system_event, value->getOperation());
    EXPECT_EQ(100, value->getNBytes());
    EXPECT_EQ(uint32_t(SystemEvent::Collection), value->getFlags());
}
