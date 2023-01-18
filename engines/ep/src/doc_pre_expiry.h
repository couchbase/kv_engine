/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

/**
 * This method is called right before a particular document expires in order
 * to examine the value, and possibly return a new and modified value to
 * be used for the expired document.
 *
 * The current implementation removes the entire value unless it contains
 * any system xattrs. If it does contain any system attrs it'll remove
 * <b>everything</b> else and return a segment with just the system
 * xattrs.
 *
 * @param itm_info info pertaining to the item that is to be expired.
 * @return std::string empty if the value required no modification, not
 *         empty then the string contains the modified value. When not empty
 *         the datatype of the new value is datatype xattr only.
 *
 * @throws std::bad_alloc in case of memory allocation failure
 */
std::string document_pre_expiry(std::string_view data, uint8_t datatype);
