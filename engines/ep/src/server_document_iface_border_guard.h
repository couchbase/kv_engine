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

#pragma once

#include <memcached/server_document_iface.h>

class CookieIface;

/**
 * A class which wraps a given instance of the server document interface,
 * and "guards" calls to all methods of that instance by switching away from
 * the current thread's engine, calling the underlying guarded method, and then
 * switching back.
 *
 * This ensures that any memory allocations performed within any of the
 * interfaces' methods are correctly accounted (to the "Non Bucket").
 */
class ServerDocumentIfaceBorderGuard : public ServerDocumentIface {
public:
    explicit ServerDocumentIfaceBorderGuard(ServerDocumentIface& guarded);
    cb::engine_errc pre_link(CookieIface& cookie, item_info& info) override;
    std::string pre_expiry(const item_info& itm_info) override;
    void audit_document_access(
            CookieIface& cookie,
            cb::audit::document::Operation operation) override;

protected:
    ServerDocumentIface& guarded;
};
