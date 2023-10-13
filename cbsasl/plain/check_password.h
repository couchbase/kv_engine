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

#include <cbsasl/context.h>
#include <cbsasl/error.h>
#include <cbsasl/user.h>

namespace cb::sasl::plain {

/**
 * Check if the supplied password match what's stored in the
 * provided user object
 *
 * @param user the user object to check
 * @param password the password to compare
 * @return Error::OK if the provided password match the supplied
 *                   password.
 */
Error check_password(const cb::sasl::pwdb::User& user,
                     std::string_view password);
} // namespace cb::sasl::plain
