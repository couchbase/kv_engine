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

#include "systemevent.h"

#include <gtest/gtest.h>

TEST(SystemEventTest, make) {
    auto value = SystemEventFactory::make(
            SystemEvent::CreateCollection, "SUFFIX", 0);
    EXPECT_EQ(queue_op::system_event, value->getOperation());
    EXPECT_EQ(0, value->getNBytes());
    EXPECT_NE(nullptr, strstr(value->getKey().c_str(), "SUFFIX"));
    EXPECT_EQ(uint32_t(SystemEvent::CreateCollection), value->getFlags());

    value = SystemEventFactory::make(
            SystemEvent::CreateCollection, "SUFFIX", 100);
    EXPECT_EQ(queue_op::system_event, value->getOperation());
    EXPECT_EQ(100, value->getNBytes());
    EXPECT_NE(nullptr, strstr(value->getKey().c_str(), "SUFFIX"));
    EXPECT_EQ(uint32_t(SystemEvent::CreateCollection), value->getFlags());

    value = SystemEventFactory::make(
            SystemEvent::BeginDeleteCollection, "SUFFIX", 100);
    EXPECT_EQ(queue_op::system_event, value->getOperation());
    EXPECT_EQ(100, value->getNBytes());
    EXPECT_NE(nullptr, strstr(value->getKey().c_str(), "SUFFIX"));
    EXPECT_EQ(uint32_t(SystemEvent::BeginDeleteCollection), value->getFlags());
}