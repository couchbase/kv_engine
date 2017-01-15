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

#include "testapp_client_test.h"

class XattrTest : public TestappClientTest {

protected:
    protocol_binary_response_status xattr_upsert(const std::string& path,
                                                 const std::string& value) {
        auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());

        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
        cmd.setKey(name);
        cmd.setPath(path);
        cmd.setValue(value);
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);

        conn.sendCommand(cmd);

        BinprotResponse resp;
        conn.recvResponse(resp);
        return resp.getStatus();
    }
};
