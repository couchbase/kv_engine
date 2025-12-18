/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mc_program_getopt.h"
#include <cbsasl/mechanism.h>
#include <json_web_token/builder.h>
#include <memcached/unit_test_mode.h>
#include <platform/dirutils.h>
#include <platform/getpass.h>
#include <platform/terminal_color.h>
#include <platform/timeutils.h>
#include <programs/hostname_utils.h>
#include <programs/parse_tls_option.h>
#include <protocol/connection/client_connection.h>
#include <utilities/terminate_handler.h>
#include <iostream>

using cb::getopt::Argument;
using cb::getopt::Option;

static std::filesystem::path passphrase_file = ".";
static std::filesystem::path skeleton_file = ".";

McProgramGetopt::~McProgramGetopt() = default;

McProgramGetopt::McProgramGetopt() {
    // Disable unit test mode as that would change the default value for
    // ssl-peer-verify
    setUnitTestMode(false);

    const auto* home = getenv("HOME");
    if (home) {
        passphrase_file = fmt::format("{}/.couchbase/shared_secret", home);
        skeleton_file = fmt::format("{}/.couchbase/token-skeleton.json", home);
    }

    addOption({[this](auto value) { host = std::string{value}; },
               'h',
               "host",
               Argument::Required,
               "hostname[:port]",
               "The host (with an optional port) to connect to (for IPv6 use: "
               "[address]:port if you'd like to specify port)"});
    addOption({[this](auto value) { port = std::string{value}; },
               'p',
               "port",
               Argument::Required,
               "number",
               "The port number to connect to"});
    addOption({[this](auto value) { user = std::string{value}; },
               'u',
               "user",
               Argument::Required,
               "username",
               "The name of the user to authenticate as"});
    addOption({[this](auto value) { password = std::string{value}; },
               'P',
               "password",
               Argument::Required,
               "password",
               "The password to use for authentication (use '-' to read from "
               "standard input, or set the environment variable CB_PASSWORD)"});
    addOption({[this](auto value) {
                   secure = true;
                   std::tie(ssl_cert, ssl_key, ca_store) =
                           parse_tls_option_or_exit(value);
               },
               {},
               "tls",
               Argument::Optional,
               "cert,key[,castore]",
               "Use TLS. If 'cert' and 'key' is provided (they are optional) "
               "they contains the certificate and private key to use to "
               "connect to the server (and if the server is configured to do "
               "so it may authenticate to the server by using the information "
               "in the certificate). A non-default CA store may optionally be "
               "provided."});
    addOption({[this](auto value) { ssl_peer_verify = false; },
               "no-peer-verify",
               "Disable verification of peer's certificate"});
    addOption({[this](auto value) { family = AF_INET; },
               '4',
               "ipv4",
               "Connect over IPv4"});
    addOption({[this](auto value) { family = AF_INET6; },
               '6',
               "ipv6",
               "Connect over IPv6"});
    addOption({[this](auto value) { sasl_mechanism = std::string{value}; },
               {},
               "sasl_mechanism",
               Argument::Required,
               "mechanism",
               "Use the provided mechanism for SASL authentication"});

#ifdef CB_DEVELOPMENT_ASSERTS
    addOption({[this](auto) { token_auth = true; },
               "token-auth",
               "Use JWT token authentication"});
    addOption({[this](auto value) {
                   std::string val(value);
                   if (std::ranges::all_of(val, isdigit)) {
                       int seconds = std::stoi(val);
                       token_lifetime = std::chrono::seconds(seconds);
                   } else {
                       try {
                           token_lifetime = std::chrono::duration_cast<
                                   std::chrono::seconds>(cb::text2time(value));
                       } catch (const std::exception& exception) {
                           fmt::println(stderr,
                                        "Failed to parse lifetime: {}",
                                        exception.what());
                           std::exit(EXIT_FAILURE);
                       }
                   }
               },
               "token-lifetime",
               Argument::Required,
               "value",
               "Use the provided lifetime for the token (default: 1m)"});
    addOption({[](auto value) { skeleton_file = value; },
               "token-skeleton-file",
               Argument::Required,
               "value",
               "Use the provided file as skeleton for tokens"});
    addOption({[](auto value) { passphrase_file = value; },
               "token-passphrase-file",
               Argument::Required,
               "value",
               "Use the provided file containing token passphrase"});
#endif

#ifndef WIN32
    addOption({[](auto) { cb::terminal::setTerminalColorSupport(false); },
               'n',
               "no-color",
               "Disable colors"});
#endif
    addOption({[this](auto) {
                   std::cout << "Couchbase Server " << PRODUCT_VERSION
                             << std::endl;
                   std::exit(EXIT_SUCCESS);
               },
               "version",
               "Print program version and exit"});

    cb::net::initialize();
    install_backtrace_terminate_handler();
#ifndef WIN32
    cb::terminal::setTerminalColorSupport(isatty(STDERR_FILENO) &&
                                          isatty(STDOUT_FILENO));
#endif
}

void McProgramGetopt::addOption(Option option) {
    parser.addOption(std::move(option));
}

std::vector<std::string_view> McProgramGetopt::parse(
        int argc, char* const* argv, std::function<void()> error) const {
    return parser.parse(argc, argv, std::move(error));
}

void McProgramGetopt::assemble(std::shared_ptr<folly::EventBase> base) {
    if (password == "-") {
        password.assign(cb::getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    if (token_auth) {
        if (!exists(skeleton_file)) {
            throw std::runtime_error(
                    fmt::format("skeleton file \"{}\" does not exists. Specify "
                                "with --token-skeleton-file=filename",
                                skeleton_file.string()));
        }
        if (!exists(passphrase_file)) {
            throw std::runtime_error(
                    fmt::format("passphrase file \"{}\" does not exists. "
                                "Specify with --token-passphrase-file=filename",
                                passphrase_file.string()));
        }
        auto payload = nlohmann::json::parse(cb::io::loadFile(skeleton_file));
        if (user.empty()) {
            // it must be in the token
            if (!payload.contains("sub")) {
                throw std::runtime_error(
                        "The token skeleton file does not contain a 'sub' "
                        "field");
            }
        } else {
            // override the one in the token with the one provided on the
            // command line
            payload["sub"] = user;
        }

        if (!password.empty()) {
            token_builder = cb::jwt::Builder::create(
                    "HS256", password, std::move(payload), token_lifetime);
        } else if (exists(passphrase_file)) {
            auto passphrase = cb::io::loadFile(passphrase_file);
            while (!passphrase.empty() &&
                   (passphrase.back() == '\n' || passphrase.back() == '\r')) {
                passphrase.pop_back();
            }
            token_builder = cb::jwt::Builder::create(
                    "HS256", passphrase, std::move(payload), token_lifetime);
        } else {
            token_builder = cb::jwt::Builder::create(
                    "none", {}, std::move(payload), token_lifetime);
        }
    } else if (!sasl_mechanism.empty()) {
        try {
            cb::sasl::selectMechanism(sasl_mechanism);
        } catch (const std::invalid_argument& ex) {
            throw std::runtime_error(fmt::format(
                    "Unknown (or unsupported) mechanism: {}", ex.what()));
        }
    }

    if (port.empty()) {
        port = secure ? "11207" : "11210";
    }

    in_port_t in_port;
    sa_family_t fam;
    std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

    if (family == AF_UNSPEC) { // The user may have used -4 or -6
        family = fam;
    }

    connection = std::make_unique<MemcachedConnection>(
            host, in_port, family, secure, std::move(base));
    connection->setTlsConfigFiles(ssl_cert, ssl_key, ca_store);
    connection->setSslPeerVerify(ssl_peer_verify);
    if (token_auth) {
        connection->setTokenBuilder(token_builder->clone());
    }
}

std::unique_ptr<MemcachedConnection>
McProgramGetopt::createAuthenticatedConnection(
        std::string h,
        in_port_t p,
        sa_family_t f,
        bool s,
        std::shared_ptr<folly::EventBase> base) const {
    auto ret = std::make_unique<MemcachedConnection>(
            std::move(h), p, f, s, std::move(base));
    ret->setTlsConfigFiles(ssl_cert, ssl_key, ca_store);
    ret->setSslPeerVerify(ssl_peer_verify);
    if (token_builder) {
        ret->setTokenBuilder(token_builder->clone());
    }

    try {
        ret->connect();
    } catch (std::runtime_error& e) {
        std::string message = e.what();
        if (message.contains("certificate verify failed")) {
            throw std::runtime_error(fmt::format(
                    "Failed to connect to \"{}\" as certificate "
                    "verification failed.\nIf you want to disable "
                    "certificate verification, use the --no-peer-verify "
                    "option.\nFull error message:{}",
                    h,
                    message));
        }
        throw;
    }
    if (token_auth) {
        ret->authenticateWithToken();
    } else if (!user.empty() && !password.empty()) {
        ret->authenticate(user,
                          password,
                          sasl_mechanism.empty() ? ret->getSaslMechanisms()
                                                 : sasl_mechanism);
    }

    return ret;
}

std::unique_ptr<MemcachedConnection> McProgramGetopt::getConnection() {
    if (connection) {
        auto ret = connection->clone(false);
        try {
            ret->connect();
        } catch (std::runtime_error& e) {
            std::string message = e.what();
            if (message.contains("certificate verify failed")) {
                throw std::runtime_error(fmt::format(
                        "Failed to connect to \"{}\" as certificate "
                        "verification failed.\nIf you want to disable "
                        "certificate verification, use the --no-peer-verify "
                        "option.\nFull error message:{}",
                        host,
                        message));
            }
            throw;
        }
        if (token_auth) {
            ret->authenticateWithToken();
        } else if (!user.empty() && !password.empty()) {
            ret->authenticate(user,
                              password,
                              sasl_mechanism.empty() ? ret->getSaslMechanisms()
                                                     : sasl_mechanism);
        }
        return ret;
    }

    return {};
}

void McProgramGetopt::usage(std::ostream& out) const {
    parser.usage(out);
}

std::ostream& operator<<(std::ostream& os,
                         const McProgramGetopt& mcProgramGetopt) {
    mcProgramGetopt.usage(os);
    return os;
}
