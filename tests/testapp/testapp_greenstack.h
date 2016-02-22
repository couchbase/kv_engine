/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "testapp.h"
#include <protocol/connection/client_greenstack_connection.h>

#include <algorithm>


class GreenstackTest
    : public TestappTest, public ::testing::WithParamInterface<Transport> {

public:
    GreenstackTest() {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        name.assign(info->test_case_name());
        name.append("_");
        name.append(info->name());
        std::replace(name.begin(), name.end(), '/', '_');
    }

protected:
    /**
     * Helper function to get an instance of the MemcachedGreenstackConnection
     * using the underlying transport attributes for the test (IPv4/6
     * Plain/SSL)
     */
    MemcachedGreenstackConnection& getConnection();

    std::string name;
};
