/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "testapp_client_test.h"

#include <algorithm>

class SaslTest : public TestappClientTest {

public:
    SaslTest() {
        mechanisms.push_back("PLAIN");
        mechanisms.push_back("CRAM-MD5");
#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
        mechanisms.push_back("SCRAM-SHA1");
#endif
#ifdef HAVE_PKCS5_PBKDF2_HMAC
        mechanisms.push_back("SCRAM-SHA256");
        mechanisms.push_back("SCRAM-SHA512");
#endif
    }

    virtual void SetUp();

    virtual void TearDown();

protected:
    void testMixStartingFrom(const std::string& mech);

    std::vector<std::string> mechanisms;
};
