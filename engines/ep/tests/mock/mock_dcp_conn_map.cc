/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_dcp_conn_map.h"

#include "conn_store.h"
#include "dcp/response.h"
#include "mock_dcp_consumer.h"

std::shared_ptr<DcpConsumer> MockDcpConnMap::makeConsumer(
        EventuallyPersistentEngine& engine,
        const CookieIface* cookie,
        const std::string& connName,
        const std::string& consumerName) const {
    return std::make_shared<MockDcpConsumer>(
            engine, cookie, connName, consumerName);
}

void MockDcpConnMap::addConn(const CookieIface* cookie,
                             std::shared_ptr<ConnHandler> conn) {
    connStore->getCookieToConnectionMapHandle()->addConnByCookie(cookie, conn);
}

bool MockDcpConnMap::removeConn(const CookieIface* cookie) {
    connStore->getCookieToConnectionMapHandle()->removeConnByCookie(cookie);
    return true;
}

bool MockDcpConnMap::doesVbConnExist(Vbid vbid, const std::string& name) {
    return connStore->doesVbConnExist(vbid, name);
}

bool MockDcpConnMap::doesConnHandlerExist(const std::string& name) {
    return connStore->getCookieToConnectionMapHandle()
            ->findConnHandlerByName(name)
            .get();
}
