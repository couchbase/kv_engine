/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * mctrace - Utility program to easily perform trace dumps on a running
 * memcached process
 */
#include <getopt.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <platform/interrupt.h>
#include <platform/strerror.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>

#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

static bool caughtSigInt = false;

static void sigint_handler() {
    // We only want to soft-exit once, if we sigint twice just bail out
    if (caughtSigInt) {
        exit(1);
    }
    caughtSigInt = true;
}

static void usage() {
    static const char* text = R"(Usage: mctrace [options]

Options:
    --ipv4 / -4       Use IPv4
    --ipv6 / -6       Use IPv6
    --host= / -h      Connect to the specified host (with an optional port
                      number). By default this is set to "localhost".
    --port= / -p      Connect to the specified port (By default this is 11210)
    --user= / -u      The username to use for authentication.
    --password= / -P  The password to use for authentication. If not specified
                      the textual string set in the environment variable
                      CB_PASSWORD is used. If '-' is specified the password
                      is read from standard input.
    --ssl / -s        Connect over SSL
    --ssl=cert,key    Try to authenticate over SSL by using the provided SSL
                      certificate and private key.
    --config= / -c    Specify the trace configuration to use on the server
                      (note that this will override the current configuration
                      and the previous configuration will NOT be restored
                      when the program terminates).
                      ex:
                      "buffer-mode:ring;buffer-size:2000000;enabled-categories:*"
    --output / -o     Store the trace information in the named file.
    --wait / -w       Wait until the user press ctrl-c before returning the
                      data. This option clears the data on the server before
                      waiting for the user to press ctrl-c and may be used
                      to get information for a known window of time.
    --help            This help text

)";

    std::cerr << text << std::endl;
    exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string ssl_cert;
    std::string ssl_key;
    sa_family_t family = AF_UNSPEC;
    bool secure = false;
    std::string trace_config;
    std::string output("-");
    bool interactive = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    struct option long_options[] = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"user", required_argument, nullptr, 'u'},
            {"password", required_argument, nullptr, 'P'},
            {"ssl=", optional_argument, nullptr, 's'},
            {"config", required_argument, nullptr, 'c'},
            {"output", required_argument, nullptr, 'o'},
            {"wait", no_argument, nullptr, 'w'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(
                    argc, argv, "46h:p:u:P:sc:o:w", long_options, nullptr)) !=
           EOF) {
        switch (cmd) {
        case '6':
            family = AF_INET6;
            break;
        case '4':
            family = AF_INET;
            break;
        case 'h':
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'u':
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 's':
            secure = true;
            if (optarg) {
                ssl_cert.assign(optarg);
                auto idx = ssl_cert.find(',');
                if (idx == std::string::npos) {
                    std::cerr << "Invalid format for SSL certificate and key\n";
                    exit(EXIT_FAILURE);
                }
                ssl_key = ssl_cert.substr(idx + 1);
                ssl_cert.resize(idx);

                if (!cb::io::isFile(ssl_cert)) {
                    std::cerr << "SSL certificate file " << ssl_cert
                              << " does not exists";
                    exit(EXIT_FAILURE);
                }

                if (!cb::io::isFile(ssl_key)) {
                    std::cerr << "SSL private file " << ssl_key
                              << " does not exists";
                    exit(EXIT_FAILURE);
                }
            }
            break;
        case 'c':
            trace_config.assign(optarg);
            break;
        case 'o':
            output.assign(optarg);
            break;
        case 'w':
            interactive = true;
            break;
        default:
            usage();
        }
    }

    if (ssl_cert.empty() && ssl_key.empty()) {
        // Use normal authentication
        if (password == "-") {
            password.assign(getpass());
        } else if (password.empty()) {
            const char* env_password = std::getenv("CB_PASSWORD");
            if (env_password) {
                password = env_password;
            }
        }
    }

    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }

        MemcachedConnection connection(host, in_port, family, secure);
        connection.setSslCertFile(ssl_cert);
        connection.setSslKeyFile(ssl_key);

        connection.connect();

        // MEMCACHED_VERSION contains the git sha
        connection.setAgentName("mctrace " MEMCACHED_VERSION);
        connection.setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

        if (!user.empty()) {
            connection.authenticate(
                    user, password, connection.getSaslMechanisms());
        }

        if (!trace_config.empty()) {
            // Start the trace
            connection.ioctl_set("trace.config", trace_config);
            connection.ioctl_set("trace.start", {});
        } else {
            if (connection.ioctl_get("trace.status") != "enabled") {
                std::cerr << "Trace is not running. Specify a configuration."
                          << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (interactive) {
            // Clear the trace by stopping and starting it
            connection.ioctl_set("trace.stop", {});
            connection.ioctl_set("trace.start", {});

            // Register our SIGINT handler
            cb::console::set_sigint_handler(sigint_handler);

            std::cerr << "Press CTRL-C to stop trace" << std::endl;
            // Wait for the trace to automatically stop or ctrl+c
            do {
                // In the ideal world we'd use a condition variable to do this
                // so we can bail out quickly. Unfortunately it's illegal to do
                // that from a signal handler.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } while (!caughtSigInt);
        }

        FILE* destination = stdout;
        if (!output.empty() && output != "-") {
            destination = fopen(output.c_str(), "w");
            if (destination == nullptr) {
                fprintf(stderr,
                        R"(Failed to open "%s": %s)",
                        output.c_str(),
                        cb_strerror().c_str());
                exit(EXIT_FAILURE);
            }
        }

        // Start a dump
        auto uuid = connection.ioctl_get("trace.dump.begin");
        const std::string chunk_key = "trace.dump.chunk?id=" + uuid;

        // Print the dump to stdout
        std::string chunk;
        do {
            chunk = connection.ioctl_get(chunk_key);
            fwrite(chunk.data(), chunk.size(), 1, destination);
        } while (!chunk.empty());
        fprintf(destination, "\n");

        if (destination != stdout) {
            fclose(destination);
        }

        // Remove the dump
        connection.ioctl_set("trace.dump.clear", uuid);
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
