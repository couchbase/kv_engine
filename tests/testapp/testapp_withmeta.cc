/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <protocol/connection/client_mcbp_commands.h>
#include "testapp.h"
#include "testapp_client_test.h"

#include <utilities/string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

class WithMetaTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        document.info.cas = testCas; // Must have a cas for meta ops
    }

    /**
     * Check the CAS of the set document against our value
     * using vattr for the lookup
     */
    void checkCas() {
        auto& conn = getConnection();
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("$document");
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        cmd.addDocFlags(mcbp::subdoc::doc_flag::None);

        conn.sendCommand(cmd);

        BinprotSubdocResponse resp;
        conn.recvResponse(resp);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
        unique_cJSON_ptr vattr(cJSON_Parse(resp.getValue().c_str()));

        EXPECT_STREQ(testCasStr,
                     cJSON_GetObjectItem(vattr.get(), "CAS")->valuestring);
    }

    /**
     * Make ::document an xattr value
     */
    void makeDocumentXattrValue() {
        cb::xattr::Blob blob;
        blob.set("user", "{\"author\":\"bubba\"}");
        blob.set("meta", "{\"content-type\":\"text\"}");

        auto xattrValue = blob.finalize();

        // append body to the xattrs and store in data
        std::string body = "document_body";
        document.value.clear();
        std::copy_n(xattrValue.buf,
                    xattrValue.len,
                    std::back_inserter(document.value));
        std::copy_n(
                body.c_str(), body.size(), std::back_inserter(document.value));
        cb::const_char_buffer xattr{(char*)document.value.data(),
                                    document.value.size()};

        document.info.datatype = cb::mcbp::Datatype::Xattr;
    }

protected:
    const uint64_t testCas = 0xb33ff00dcafef00dull;
    const char* testCasStr = "0xb33ff00dcafef00d";
};

INSTANTIATE_TEST_CASE_P(
        TransportProtocols,
        WithMetaTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpIpv6Plain,
                                             TransportProtocols::McbpSsl,
                                             TransportProtocols::McbpIpv6Ssl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        PrintToStringCombinedName());

TEST_P(WithMetaTest, basicSet) {
    TESTAPP_SKIP_IF_UNSUPPORTED(PROTOCOL_BINARY_CMD_SET_WITH_META);

    MutationInfo resp;
    try {
        resp = getConnection().mutateWithMeta(document,
                                              0,
                                              mcbp::cas::Wildcard,
                                              /*seqno*/ 1,
                                              /*options*/ 0,
                                              {});
    } catch (std::exception&) {
        FAIL() << "mutateWithMeta threw an exception";
    }

    if (::testing::get<1>(GetParam()) == XattrSupport::Yes) {
        checkCas();
    }
}

TEST_P(WithMetaTest, basicSetXattr) {
    TESTAPP_SKIP_IF_UNSUPPORTED(PROTOCOL_BINARY_CMD_SET_WITH_META);
    makeDocumentXattrValue();

    MutationInfo resp;
    try {
        resp = getConnection().mutateWithMeta(document,
                                              0,
                                              mcbp::cas::Wildcard,
                                              /*seqno*/ 1,
                                              /*options*/ 0,
                                              {});
        EXPECT_EQ(XattrSupport::Yes, ::testing::get<1>(GetParam()));
        EXPECT_EQ(testCas, ntohll(resp.cas));
    } catch (std::exception&) {
        EXPECT_EQ(XattrSupport::No, ::testing::get<1>(GetParam()));
    }

    if (::testing::get<1>(GetParam()) == XattrSupport::Yes) {
        checkCas();
    }
}
