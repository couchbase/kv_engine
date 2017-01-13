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
#include "testapp_xattr.h"

// @todo add the other transport protocols
INSTANTIATE_TEST_CASE_P(TransportProtocols,
    XattrTest,
    ::testing::Values(TransportProtocols::McbpPlain),
    ::testing::PrintToStringParamName());

TEST_P(XattrTest, AddSingleXattr) {
    store_object(name.c_str(), "{}");

    // Lets store the macro and verify that it isn't expanded without the
    // expand macro flag
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setValue("\"${Mutation.CAS}\"");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);

        BinprotResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        EXPECT_EQ("\"${Mutation.CAS}\"", resp.getValue());
    }

    {
        // Let's update the body version..
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setValue("\"If you don't know me by now\"");
        cmd.setFlags(SUBDOC_FLAG_MKDIR_P);

        BinprotResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    // The xattr version should have been unchanged...
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        EXPECT_EQ("\"${Mutation.CAS}\"", resp.getValue());
    }

    // And the body version should be what we set it to
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_NONE);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        EXPECT_EQ("\"If you don't know me by now\"", resp.getValue());
    }

    // The change it to macro expansion
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS);
        cmd.setValue("\"${Mutation.CAS}\"");

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    std::string cas_string;
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        ss << "\"0x" << std::setw(16) << resp.getCas() << "\"";
        cas_string = ss.str();

        EXPECT_EQ(cas_string, resp.getValue());
    }

    {
        // Let's update the body version..
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setValue("\"If you don't know me by now\"");
        cmd.setFlags(SUBDOC_FLAG_MKDIR_P);

        BinprotResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    // The macro should not have been expanded again...
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("_sync.cas");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        EXPECT_EQ(cas_string, resp.getValue());

        // But the cas for the last version should be different
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        ss << "\"0x" << std::setw(16) << resp.getCas() << "\"";
        const auto this_cas_string = ss.str();
        EXPECT_NE(cas_string, this_cas_string);
    }

}

TEST_P(XattrTest, OperateOnDeletedItem) {
    store_object(name.c_str(), "{}");
    getConnection().remove(name, 0);

    // The change it to macro expansion
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
        cmd.setKey(name);
        cmd.setPath("_sync.deleted");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_ACCESS_DELETED| SUBDOC_FLAG_MKDIR_P);
        cmd.setValue("true");

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED);
    }
}

TEST_P(XattrTest, MB_22319) {
    store_object(name.c_str(), "{}");

    // This is listed as working in the bug report
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc.readcount", "0"));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc.author", "\"jack\""));

    // The failing bits is:
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.add_mutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                     SUBDOC_FLAG_XATTR_PATH,
                    "doc.readcount", "1");
    cmd.add_mutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                     SUBDOC_FLAG_XATTR_PATH,
                     "doc.author", "\"jones\"");

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}
