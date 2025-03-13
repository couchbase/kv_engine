/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "platform/command_line_options_parser.h"

#include <fmt/format.h>
#include <json_web_token/builder.h>
#include <platform/dirutils.h>
#include <platform/timeutils.h>
#include <cctype>
#include <iostream>

std::filesystem::path passphrase_file =
        fmt::format("{}/.couchbase/shared_secret", getenv("HOME"));
std::filesystem::path skeleton_file =
        fmt::format("{}/.couchbase/token-skeleton.json", getenv("HOME"));

static void usage(cb::getopt::CommandLineOptionsParser& instance) {
    std::cerr << R"(Usage: mktoken [options]

Options:

)" << instance << std::endl
              << std::endl
              << R"(
You should create ~/.couchbase/token-skeleton.json with the content you'd like
the token to contain:

    {
      "aud": "couchbase",
      "iss": "couchbase",
      "sub": "trond",
      "name": "Trond Talsnes Norbye"
    }
)" << std::endl
              << std::endl;
    std::exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
    cb::getopt::CommandLineOptionsParser getopt;
    std::string passphrase;
    nlohmann::json payload;

    std::chrono::seconds lifetime = std::chrono::minutes(1);

    using cb::getopt::Argument;
    getopt.addOption({[&passphrase](auto value) { passphrase = value; },
                      "passphrase",
                      Argument::Required,
                      "value",
                      fmt::format("Use the provided passphrase instead of {}",
                                  passphrase_file.string())});

    getopt.addOption({[&lifetime](auto value) {
                          std::string val(value);
                          if (std::ranges::all_of(val, isdigit)) {
                              int seconds = std::stoi(val);
                              lifetime = std::chrono::seconds(seconds);
                          } else {
                              try {
                                  lifetime = std::chrono::duration_cast<
                                          std::chrono::seconds>(
                                          cb::text2time(value));
                              } catch (const std::exception& exception) {
                                  fmt::println(stderr,
                                               "Failed to parse lifetime: {}",
                                               exception.what());
                                  std::exit(EXIT_FAILURE);
                              }
                          }
                      },
                      "lifetime",
                      Argument::Required,
                      "1 h",
                      "Use the provided lifetime for the token (default: 1m)"});

    auto arguments = getopt.parse(argc, argv, [&getopt]() { usage(getopt); });

    if (passphrase.empty()) {
        if (!exists(passphrase_file)) {
            fmt::print(stderr,
                       "Passphrase file \"{}\" does not exists\n",
                       passphrase_file.string());
            return EXIT_FAILURE;
        }
        passphrase = cb::io::loadFile(passphrase_file);
        while (!passphrase.empty() &&
               (passphrase.back() == '\n' || passphrase.back() == '\r')) {
            passphrase.pop_back();
        }
    }

    if (exists(skeleton_file)) {
        payload = nlohmann::json::parse(cb::io::loadFile(skeleton_file));
    }

    auto builder =
            cb::jwt::Builder::create("HS256", passphrase, std::move(payload));
    const auto now = std::chrono::system_clock::now();
    const auto expiration = now + lifetime;

    builder->setIssuedAt(now);
    builder->setExpiration(expiration);

    fmt::print("{}", builder->build());
    return EXIT_SUCCESS;
}
