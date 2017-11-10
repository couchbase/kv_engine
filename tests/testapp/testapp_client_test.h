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

class TestappClientTest
    : public TestappTest,
      public ::testing::WithParamInterface<TransportProtocols> {
protected:
    MemcachedConnection& getConnection() override;
    void setClusterSessionToken(uint64_t new_value);
};

enum class XattrSupport { Yes, No };
std::ostream& operator<<(std::ostream& os, const XattrSupport& xattrSupport);
std::string to_string(const XattrSupport& xattrSupport);

class TestappXattrClientTest
        : public TestappTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<TransportProtocols, XattrSupport>> {
public:
    TestappXattrClientTest()
        : xattrOperationStatus(PROTOCOL_BINARY_RESPONSE_SUCCESS) {
    }
    void SetUp() override;

    MemcachedConnection& getConnection() override;

    BinprotSubdocResponse getXattr(const std::string& path,
                                   bool deleted = false);
    void createXattr(const std::string& path,
                     const std::string& value,
                     bool macro = false);

protected:
    Document document;
    protocol_binary_response_status xattrOperationStatus;
};

struct PrintToStringCombinedName {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<TransportProtocols, XattrSupport>>& info) const;
};
