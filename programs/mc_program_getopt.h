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

#include <folly/io/async/EventBase.h>
#include <platform/command_line_options_parser.h>
#include <protocol/connection/client_connection.h>
#include <filesystem>
#include <optional>

/***
 * McProgramGetopt is a class providing the common options used by all
 * "mc programs" which needs to configure a connection to a node
 * (--user --host --port etc). So that all of the programs just need
 * to add the options needed for that application to work (and we can
 * add common flags in this class and all applications will pick it up).
 *
 * Usage:
 *
 *     McProgramGetopt parser;
 *     parser.addOption( ... add a program specific option ... )
 *     auto arguments = parser.parse(argc, argv, [](){ error });
 *
 *     // assemble a client configuration (read password from stdin if
 *     // requested etc).
 *     parser.assemble();
 *
 *     // Get an authenticated connection!
 *     auto connection = parser.getConnection();
 *
 * The class utilize CommandLineOptionsParser to parse the command line
 * options (which in turn is built on top of getopt) so that the caller
 * may use optind, opterr, optreset etc.
 */
class McProgramGetopt {
public:
    McProgramGetopt();
    ~McProgramGetopt();

    /**
     * Add a command line option to the list of command line options
     * to accept
     */
    void addOption(cb::getopt::Option option);

    /**
     * Parse the command line options and call the callbacks for all
     * options found.
     *
     * @param argc argument count
     * @param argv argument vector
     * @param error an error callback for unknown options
     */
    std::vector<std::string_view> parse(int argc,
                                        char* const* argv,
                                        std::function<void()> error) const;

    /**
     * Try to assemble a working instance of the CommandLineConfiguredClient
     * by looking at the provided options (and possibly fetch password
     * from the user etc)
     *
     * @param base the event base to use for the connection (if empty
     *              a new one will be created to be used within the instance)
     * @throws std::exception (subclass) if anything goes wrong
     */
    void assemble(std::shared_ptr<folly::EventBase> base = {});

    /**
     * Get a new authenticated client connection
     *
     * @throws std::exception (subclass) if anything goes wrong
     */
    std::unique_ptr<MemcachedConnection> getConnection();

    /// Print the common command line options to the output stream
    void usage(std::ostream&) const;

    /**
     * Sometimes one may want to fetch the cluster config from the node
     * provided on the command line options and then connect to the other
     * nodes in the cluster. In those cases this method would come in handy
     * to connect to the provided host and authenticate with the options
     * provided on the command line (SASL or via certificates.
     */
    [[nodiscard]] std::unique_ptr<MemcachedConnection>
    createAuthenticatedConnection(
            std::string host,
            in_port_t port,
            sa_family_t family,
            bool secure,
            std::shared_ptr<folly::EventBase> base = {}) const;

protected:
    cb::getopt::CommandLineOptionsParser parser;
    std::unique_ptr<MemcachedConnection> connection;
    std::string port;
    std::string host{"localhost"};
    std::string user;
    std::string password;
    std::string sasl_mechanism;
    std::filesystem::path ssl_cert;
    std::filesystem::path ssl_key;
    std::filesystem::path ca_store;
    sa_family_t family = AF_UNSPEC;
    bool secure = false;
    bool token_auth = false;
    bool ssl_peer_verify = true;
    std::unique_ptr<cb::jwt::Builder> token_builder;
    std::chrono::seconds token_lifetime = std::chrono::minutes{1};
};

std::ostream& operator<<(std::ostream& os,
                         const McProgramGetopt& mcProgramGetopt);
