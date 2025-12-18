/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "parse_tls_option.h"
#include <platform/split_string.h>
#include <platform/terminal_color.h>
#include <cstdlib>
#include <iostream>

using cb::terminal::TerminalColor;

TlsSpec parse_tls_option_or_exit(std::string_view argument) {
    if (argument.empty()) {
        return {{}, {}, {}};
    }

    std::filesystem::path ssl_cert;
    std::filesystem::path ssl_key;
    std::filesystem::path ca_store;

    auto parts = cb::string::split(argument, ',');
    if (parts.size() < 2 || parts.size() > 3) {
        std::cerr << TerminalColor::Red
                  << "Incorrect format for --tls=certificate,key[,castore]"
                  << TerminalColor::Reset << std::endl;
        exit(EXIT_FAILURE);
    }
    ssl_cert = std::move(parts[0]);
    ssl_key = std::move(parts[1]);

    if (parts.size() == 3) {
        ca_store = std::move(parts[2]);
    }

    auto validate = [](auto file, auto descr) {
        if (!file.empty() && !exists(file)) {
            std::cerr << TerminalColor::Red << descr << " file " << file
                      << " does not exists" << TerminalColor::Reset
                      << std::endl;
            exit(EXIT_FAILURE);
        }
    };
    validate(ssl_cert, "Certificate");
    validate(ssl_key, "Private key");
    validate(ca_store, "CA store");

    return {ssl_cert, ssl_key, ca_store};
}
