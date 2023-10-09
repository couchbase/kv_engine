/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/engine.h>
#include <memory>
#include <string_view>

namespace folly {
class IOBuf;
}

class SendBuffer;
class Bucket;
class CookieIface;

/**
 * The ItemDissector holds an item through a unique_ptr into the
 * underlying engine, but also an inflated version of the value
 * if that is needed (for instance if one needs to strip off the
 * xattrs).
 */
class ItemDissector {
public:
    /**
     * Create a new instance of the ItemDissector.
     *
     * If the item contains Extended Attributes and is compressed
     * the item will be inflated
     *
     * @param cookie The cookie performing the operation
     * @param item The item to operate on
     * @param forceInflate If set to true the item will be inflated
     *                     even if it don't contain any xattrs
     */
    ItemDissector(CookieIface& cookie,
                  cb::unique_item_ptr item,
                  bool forceInflate);

    ~ItemDissector();

    /**
     * Get a send buffer with the provided view (Which MUST be from within
     * a view returned from getXattrs or getValue).
     *
     * Calling this method invalidates the ItemDissector as the backend for
     * the view is moved into the returned SendBuffer.
     *
     * @param view The view to send
     * @param bucket The bucket where the item resides
     * @return A send buffer wrapping the internal data (unique_ptr for the
     *         item or the unique_ptr for the inflated data)
     */
    std::unique_ptr<SendBuffer> takeSendBuffer(std::string_view view,
                                               Bucket& bucket);

    /// Get the XAttr section from the item
    std::string_view getExtendedAttributes() const {
        return xattr_view;
    }

    /// Get the Value from the item
    std::string_view getValue() const {
        return value_view;
    }

    /// Get the datatype for the in-memory representation of whats returned
    /// through getValue, which means that it will NEVER include XATTR
    uint8_t getDatatype() const {
        return datatype;
    }

    /// Get the item to allow fetching key/flags etc
    const ItemIface& getItem() const {
        return *item;
    }

protected:
    cb::unique_item_ptr item;
    std::unique_ptr<folly::IOBuf> inflated_value;
    std::string_view xattr_view;
    std::string_view value_view;
    uint8_t datatype;
};
