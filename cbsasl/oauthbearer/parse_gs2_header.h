/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <string>

namespace cb::sasl::mechanism::oauthbearer {

/**
 * Parse the GS2 header and return the authzid (if any)
 *
 * @param input The GS2 header to parse
 * @return authzid if present, empty string if not
 * @throws std::invalid_argument if the input is invalid
 */
std::string parse_gs2_header(std::string_view input);

} // namespace cb::sasl::mechanism::oauthbearer
