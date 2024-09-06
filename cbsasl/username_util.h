/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <string>
#include <string_view>

namespace cb::sasl::username {
/**
 * According to https://www.ietf.org/rfc/rfc5802.txt all occurrences
 * of ',' and '=' needs to be transferred as =2C and =3D.
 *
 * @param username the username to encode
 * @return the escaped string
 */
std::string encode(std::string_view username);

/**
 * According to https://www.ietf.org/rfc/rfc5802.txt all occurrences
 * of ',' and '=' needs to be transferred as =2C and =3D. This method
 * decodes that encoding
 *
 * @param username the username to decode
 * @return the decoded username
 * @throws std::runtime_error if the username contains an illegal
 *         sequence of characters.
 */
std::string decode(std::string_view username);
} // namespace cb::sasl::username
