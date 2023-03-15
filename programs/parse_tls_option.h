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

#include <filesystem>
#include <optional>
#include <tuple>

using TlsSpec = std::tuple<std::optional<std::filesystem::path>,
                           std::optional<std::filesystem::path>,
                           std::optional<std::filesystem::path>>;

/**
 * Parse the --tls command line option which is in the format:
 *
 *    --tls[=cert,key[,castore]]
 *
 * Verify that all files exists (if not an error message is printed
 * to standard error and the program terminates!
 *
 * @param argument the argument to --tls
 * @returns hostname, port number and family of the resolved address
 */
TlsSpec parse_tls_option_or_exit(std::string_view argument);
