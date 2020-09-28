/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "mock_dcp_conn_map.h"

#include "conn_store.h"
#include "dcp/response.h"
#include "mock_dcp_consumer.h"

std::shared_ptr<DcpConsumer> MockDcpConnMap::makeConsumer(
        EventuallyPersistentEngine& engine,
        const void* cookie,
        const std::string& connName,
        const std::string& consumerName) const {
    return std::make_shared<MockDcpConsumer>(
            engine, cookie, connName, consumerName);
}

void MockDcpConnMap::addConn(const void* cookie,
                             std::shared_ptr<ConnHandler> conn) {
    connStore->getCookieToConnectionMapHandle()->addConnByCookie(cookie, conn);
}

bool MockDcpConnMap::removeConn(const void* cookie) {
    connStore->getCookieToConnectionMapHandle()->removeConnByCookie(cookie);
    return true;
}

bool MockDcpConnMap::doesVbConnExist(Vbid vbid, const std::string& name) {
    return connStore->doesVbConnExist(vbid, name);
}
