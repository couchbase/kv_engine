/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "engine_error.h"
#include "types.h"

#include <gsl/gsl>

namespace cb::audit::document {
enum class Operation;
} // namespace cb::audit::document

struct ServerDocumentIface {
    virtual ~ServerDocumentIface() = default;

    /**
     * This callback is called from the underlying engine right before
     * it is linked into the list of available documents (it is currently
     * not visible to anyone). The engine should have validated all
     * properties set in the document by the client and the core, and
     * assigned a new CAS number for the document (and sequence number if
     * the underlying engine use those).
     *
     * The callback may at this time do post processing of the document
     * content (it is allowed to modify the content data, but not
     * reallocate or change the size of the data in any way).
     *
     * Given that the engine MAY HOLD LOCKS when calling this function
     * the core is *NOT* allowed to acquire *ANY* locks (except for doing
     * some sort of memory allocation for a temporary buffer).
     *
     * @param cookie The cookie provided to the engine for the storage
     *               command which may (which may hold more context)
     * @param info the items underlying data
     * @return cb::engine_errc::success means that the underlying engine should
     *                        proceed to link the item. All other
     *                        error codes means that the engine should
     *                        *NOT* link the item
     */
    virtual cb::engine_errc pre_link(gsl::not_null<const void*> cookie,
                                     item_info& info) = 0;

    /**
     * This callback is called from the underlying engine right before
     * a particular document expires. The callback is responsible examining
     * the value and possibly returning a new and modified value.
     *
     * @param itm_info info pertaining to the item that is to be expired.
     * @return std::string empty if the value required no modification, not
     *         empty then the string contains the modified value. When not empty
     *         the datatype of the new value is datatype xattr only.
     *
     * @throws std::bad_alloc in case of memory allocation failure
     */
    virtual std::string pre_expiry(const item_info& itm_info) = 0;

    /**
     * Add an entry to the audit trail for access to the document specified
     * in the key for this cookie.
     *
     * @param cookie The cookie representing the operation
     * @param operation The type of access for the operation
     */
    virtual void audit_document_access(
            gsl::not_null<const void*> cookie,
            cb::audit::document::Operation operation) = 0;
};
