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

#pragma once

#include <memcached/server_document_iface.h>

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
    cb::engine_errc pre_link(gsl::not_null<const void*> cookie,
                             item_info& info) override;
    std::string pre_expiry(const item_info& itm_info) override;
    void audit_document_access(
            gsl::not_null<const void*> cookie,
            cb::audit::document::Operation operation) override;

protected:
    ServerDocumentIface& guarded;
};
