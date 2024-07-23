/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "item_dissector.h"
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/sendbuffer.h>
#include <folly/io/IOBuf.h>
#include <logger/logger.h>
#include <utilities/logtags.h>
#include <xattr/utils.h>

using cb::mcbp::datatype::is_snappy;
using cb::mcbp::datatype::is_xattr;

ItemDissector::ItemDissector(CookieIface& cookie,
                             cb::unique_item_ptr doc,
                             bool forceInflate)
    : item(std::move(doc)), datatype(item->getDataType()) {
    std::string_view item_view = item->getValueView();

    if (is_snappy(datatype)) {
        if (forceInflate || is_xattr(datatype)) {
            try {
                inflated_value = cookie.inflateSnappy(item->getValueView());
            } catch (const std::exception& error) {
                LOG_ERROR(
                        "{}: Failed to inflate body, Key: {} may have an "
                        "incorrect datatype. Datatype indicates that document "
                        "is {}: {}",
                        cookie.getConnectionId(),
                        cb::UserDataView(item->getDocKey().toPrintableString()),
                        cb::mcbp::datatype::to_string(datatype),
                        error.what());
                throw;
            }
            datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
            item_view = folly::StringPiece(inflated_value->coalesce());
        }
    }

    if (is_xattr(datatype)) {
        value_view = cb::xattr::get_body(item_view);
        xattr_view = {item_view.data(), cb::xattr::get_body_offset(item_view)};
        datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    } else {
        value_view = item_view;
        xattr_view = {};
    }
}

ItemDissector::~ItemDissector() = default;

std::unique_ptr<SendBuffer> ItemDissector::takeSendBuffer(std::string_view view,
                                                          Bucket& bucket) {
    if (inflated_value) {
        return std::make_unique<IOBufSendBuffer>(std::move(inflated_value),
                                                 view);
    }
    return std::make_unique<ItemSendBuffer>(std::move(item), view, bucket);
}
