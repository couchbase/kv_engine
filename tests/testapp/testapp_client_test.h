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

#include  <algorithm>

#include "testapp.h"

enum class TransportProtocols {
    McbpPlain,
    GreenstackPlain,
    McbpSsl,
    GreenstackSsl,
    McbpIpv6Plain,
    GreenstackIpv6Plain,
    McbpIpv6Ssl,
    GreenstackIpv6Ssl
};

std::ostream& operator << (std::ostream& os, const TransportProtocols& t);
std::string to_string(const TransportProtocols& transport);

class TestappClientTest
    : public TestappTest,
      public ::testing::WithParamInterface<TransportProtocols> {

public:
    TestappClientTest() {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        name.assign(info->test_case_name());
        name.append("_");
        name.append(info->name());
        std::replace(name.begin(), name.end(), '/', '_');
    }

protected:
    std::string name;

    MemcachedConnection& getConnection();

    MemcachedConnection& prepare(MemcachedConnection& connection);
};
