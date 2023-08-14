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
#include <platform/terminal_color.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <programs/parse_tls_option.h>
#include <protocol/connection/client_connection.h>
#include <utilities/terminate_handler.h>
#include <unordered_map>

using cb::getopt::Argument;
using cb::getopt::Option;

McProgramGetopt::McProgramGetopt() {
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
#ifndef WIN32
    addOption({[](auto) { cb::terminal::setTerminalColorSupport(false); },
               'n',
               "no-color",
               "Disable colors"});
#endif

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

void McProgramGetopt::assemble() {
    // try to build up the client
    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
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
            host, in_port, family, secure);
    if (ssl_cert && ssl_key) {
        connection->setTlsConfigFiles(*ssl_cert, *ssl_key, ca_store);
    }
}

std::unique_ptr<MemcachedConnection> McProgramGetopt::getConnection() {
    if (connection) {
        auto ret = connection->clone(false);
        ret->connect();
        if (!user.empty()) {
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
