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

enum class TransportProtocols {
    PlainMcbp,
    PlainGreenstack,
    SslMcbp,
    SslGreenstack,
    PlainIpv6Mcbp,
    PlainIpv6Greenstack,
    SslIpv6Mcbp,
    SslIpv6Greenstack
};

std::ostream& operator << (std::ostream& os, const TransportProtocols& t);
const char* to_string(const TransportProtocols& transport);

class BucketTest
    : public TestappTest, public ::testing::WithParamInterface<TransportProtocols> {

protected:
    MemcachedConnection& getConnection();
    MemcachedConnection& prepare(MemcachedConnection& connection);
};
