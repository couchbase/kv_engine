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
