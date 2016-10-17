/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "config.h"

#include <memcached/protocol_binary.h>
#include <platform/platform.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <getopt.h>
#include <iostream>
#include <map>
#include <string>
#include <utilities/protocol2text.h>
#include <cbsasl/pwconv.h>
#include <cbsasl/user.h>

#include "programs/utilities.h"

// Unfortunately cbsasl don't use namespace internally..
namespace cbsasladm {
/**
 * Small class used to wrap the ServerConnection details
 */
class ServerConnection {
public:
    ServerConnection(const char* host_, const char* port_, const char* user_,
                     const char* pass_, int secure_)
        : ctx(nullptr),
          bio(nullptr),
          host(host_),
          port(port_),
          user(user_),
          pass(pass_),
          secure(secure_) {

    }

    bool connect() {
        return create_ssl_connection(&ctx, &bio, host.c_str(),
                                     port.c_str(), user.c_str(),
                                     pass.c_str(), secure) == 0;
    }

    BIO* getBIO() {
        return bio;
    }

    ~ServerConnection() {
        if (bio != nullptr) {
            BIO_free_all(bio);
        }

        if (ctx != nullptr) {
            SSL_CTX_free(ctx);
        }
    }

private:
    SSL_CTX* ctx = nullptr;
    BIO* bio;
    const std::string host;
    const std::string port;
    const std::string user;
    const std::string pass;
    const int secure;
};
}

/**
 * Handle cbsasl refresh
 */
static int handle_refresh(int argc, char**,
                          cbsasladm::ServerConnection& connection) {
    if (optind + 1 != argc) {
        std::cerr << "Error: cbsasl refresh don't take any arguments"
                  << std::endl;
        return EXIT_FAILURE;
    }

    if (!connection.connect()) {
        return EXIT_FAILURE;
    }

    protocol_binary_response_no_extras response;
    protocol_binary_request_no_extras request;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_ISASL_REFRESH;

    ensure_send(connection.getBIO(), &request, sizeof(request));
    ensure_recv(connection.getBIO(), &response, sizeof(response.bytes));
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        auto status =  (protocol_binary_response_status)err;

        std::cerr << "Failed to refresh cbsasl password database: "
                  << memcached_status_2_text(status) << std::endl;

        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}


int handle_pwconv(int argc, char** argv,
                  cbsasladm::ServerConnection& connection) {
    if (optind + 3 != argc) {
        std::cerr << "Usage: cbsasl pwconv inputfile outputfile" << std::endl;
        return EXIT_FAILURE;
    }

    try {
        cbsasl_pwconv(argv[optind + 1], argv[optind + 2]);
    } catch (std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::bad_alloc&) {
        std::cerr << "Error: out of memory" << std::endl;
        return EXIT_SUCCESS;
    }

    return EXIT_SUCCESS;
}

/**
 * Program entry point.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv) {
    int cmd;
    const char* port = "11210";
    const char* host = "localhost";
    const char* user = "";
    const char* pass = "";
    int secure = 0;
    char* ptr;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:su:P:i:")) != EOF) {
        switch (cmd) {
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        case 's':
            secure = 1;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        case 'i':
            try {
                using namespace Couchbase;
                UserFactory::setDefaultHmacIterationCount(std::stoi(optarg));
            } catch (...) {
                std::cerr << "Error: iteration count must be an integer"
                          << std::endl;
                return EXIT_FAILURE;
            }
            break;
        default:
            fprintf(stderr,
                    "Usage: cbsasladm [-h host[:port]] [-p port] [-s] "
                        "[-u user] [-P password] cmd\n"
                        "   The following command(s) exists:\n"
                        "\trefresh - tell memcached to reload its internal "
                        "cache\n"
                        "\tpwconv input ouput - convert isasl.pw to cbsasl.pw "
                        "format\n");
            return 1;
        }
    }

    if (optind == argc) {
        fprintf(stderr, "You need to supply a command\n");
        return EXIT_FAILURE;
    }

    typedef int (* command_handler)(int argc, char** argv,
                                    cbsasladm::ServerConnection& connection);
    const std::map<std::string, command_handler> commands{
        {"refresh", handle_refresh},
        {"pwconv",  handle_pwconv}
    };

    const auto& iter = commands.find(argv[optind]);
    if (iter != commands.end()) {
        cbsasladm::ServerConnection connection(host, port, user, pass, secure);
        return iter->second(argc, argv, connection);
    } else {
        std::cerr << "Error: Unknown command" << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
