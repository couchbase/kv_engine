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

#include "server_document_iface_border_guard.h"
#include "objectregistry.h"

ServerDocumentIfaceBorderGuard::ServerDocumentIfaceBorderGuard(
        ServerDocumentIface& guarded)
    : guarded(guarded) {
}

cb::engine_errc ServerDocumentIfaceBorderGuard::pre_link(CookieIface& cookie,
                                                         item_info& info) {
    NonBucketAllocationGuard guard;
    return guarded.pre_link(cookie, info);
}

std::string ServerDocumentIfaceBorderGuard::pre_expiry(
        const item_info& itm_info) {
    NonBucketAllocationGuard guard;
    return guarded.pre_expiry(itm_info);
}

void ServerDocumentIfaceBorderGuard::audit_document_access(
        CookieIface& cookie, cb::audit::document::Operation operation) {
    NonBucketAllocationGuard guard;
    guarded.audit_document_access(cookie, operation);
}

void ServerDocumentIfaceBorderGuard::document_expired(const EngineIface& engine,
                                                      size_t nbytes) {
    NonBucketAllocationGuard guard;
    guarded.document_expired(engine, nbytes);
}
