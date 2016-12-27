/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/**
 * mcbucket is a small standalone program you may use to:
 *     * Create new buckets
 *     * Delete buckets
 *     * list buckets
 *
 * Note that you cannot use this program to create/delete buckets on
 * the individual nodes on a running Couchbase cluster, because it
 * talks directly to the memcached nodes and bypasses ns_server which
 * is responsible for defining the buckets in the cluster. You may
 * however use this program if you want to test out resource usage,
 * how many buckets you may create etc.
 */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <memcached/util.h>
#include <utilities/protocol2text.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_connection.h>
#include <protocol/connection/client_greenstack_connection.h>
#include "programs/utilities.h"

static int usage() {
    using namespace std;
    cerr << "Usage: " << endl
         << "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s]"
         << " create name type config" << endl
         << "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s]"
         << " delete name" << endl
         << "\tmcbucket [-h host[:port]] [-p port] [-u user] [-P pass] [-s]"
         << " list'" << endl;
    return EXIT_FAILURE;
}

typedef int (* handler_t)(MemcachedConnection& client,
                          const std::vector<std::string>& arguments);


/**
 * Create a bucket
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 on success, errorcode otherwise
 */
static int create_bucket(MemcachedConnection& client,
                         const std::vector<std::string>& arguments) {
    if (arguments.size() != 3) {
        return usage();
    }

    const std::string& name = arguments[0];
    const std::string& type = arguments[1];
    const std::string& config = arguments[2];

    Greenstack::BucketType bucketType(Greenstack::BucketType::Invalid);
    if (type == "memcached") {
        bucketType = Greenstack::BucketType::Memcached;
    } else if (type == "couchbase") {
        bucketType = Greenstack::BucketType::Couchbase;
    } else {
        std::cerr << "Invalid bucket type specified. Must be \"memcached\""
                  << " or \"couchbase\"." << std::endl;
        return EXIT_FAILURE;
    }

    try {
        client.createBucket(name, config, bucketType);
        std::cout << "Bucket " << name << " successfully created" << std::endl;
    } catch (const ConnectionError& error) {
        std::cerr << "Failed to create bucket: " << error.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

static int delete_bucket(MemcachedConnection& client,
                         const std::vector<std::string>& arguments) {
    if (arguments.size() != 1) {
        return usage();
    }

    const std::string& name = arguments[0];
    try {
        client.deleteBucket(name);
        std::cout << "Bucket " << name << " successfully deleted" << std::endl;
    } catch (const ConnectionError& error) {
        std::cerr << "Failed to delete bucket: " << error.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

static int list_bucket(MemcachedConnection& client,
                       const std::vector<std::string>& arguments) {
    if (!arguments.empty()) {
        return usage();
    }

    try {
        auto buckets = client.listBuckets();
        if (buckets.empty()) {
            std::cout << "No buckets" << std::endl;
            return EXIT_SUCCESS;
        }

        for (const auto& bucket : buckets) {
            std::cout << bucket << std::endl;
        }
    } catch (const ConnectionError& error) {
        std::cerr << "Failed to list buckets: " << error.what() << std::endl;
    }
    return EXIT_SUCCESS;
}

in_port_t parsePort(const std::string& value) {
    try {
        return static_cast<in_port_t>(std::stoi(value));
    } catch (...) {
        std::cerr << "Error: failed to parse " << value << " as a number"
                  << std::endl;
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    int cmd;
    in_port_t port = 11210;
    std::string host("localhost");
    std::string user;
    std::string pass;
    std::string::size_type idx;
    bool secure = false;
    handler_t handler = nullptr;
    bool greenstack = false;

    struct {
        const char* name;
        handler_t handler;
    } handlers[] = {
        {"create", create_bucket},
        {"delete", delete_bucket},
        {"list",   list_bucket},
        {nullptr,  nullptr}
    };

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:sG")) != EOF) {
        switch (cmd) {
        case 'h' :
            host.assign(optarg);
            idx = host.find(":");
            if (idx != std::string::npos) {
                // An IPv6 address may contain colon... but then it's
                // going to be more than one ...
                auto last = host.rfind(":");
                if (idx == last) {
                    port = parsePort(host.substr(idx + 1));
                    host.resize(idx);
                } else {
                    // We have multiple ::, and it has to be enclosed with []
                    // if one of them specifies a port..
                    if (host[last - 1] == ']') {
                        if (host[0] != '[') {
                            std::cerr << "Invalid IPv6 address specified. "
                                      << "Should be: \"[address]:port\""
                                      << std::endl;
                            return EXIT_FAILURE;
                        }

                        port = parsePort(host.substr(last + 1));
                        host.resize(last - 1);
                        host = host.substr(1);
                    }
                }
            }
            break;
        case 'p':
            port = parsePort(optarg);
            break;
        case 'u' :
            user.assign(optarg);
            break;
        case 'P':
            pass.assign(optarg);
            break;
        case 's':
            secure = true;
            break;
        case 'G':
            greenstack = true;
            break;
        default:
            return usage();
        }
    }

    if (optind == argc) {
        return usage();
    }

    int ii = 0;
    while (handlers[ii].name != nullptr) {
        if (strcasecmp(handlers[ii].name, argv[optind]) == 0) {
            handler = handlers[ii].handler;
            break;
        }
        ++ii;
    }

    if (handler == nullptr) {
        return usage();
    }

    std::unique_ptr<MemcachedConnection> connection;
    if (greenstack) {
#ifdef ENABLE_GEENSTACK
        connection.reset(new MemcachedGreenstackConnection(host, port,
                                                           AF_UNSPEC, secure));
#else
        std::cerr << "Build without support for greenstack" << std::endl;
        exit(EXIT_FAILURE);
#endif
    } else {
        connection.reset(new MemcachedBinprotConnection(host, port,
                                                        AF_UNSPEC, secure));
    }

    connection->hello("mcbucket", "1.0.0", "");
    if (!user.empty() && !pass.empty()) {
        try {
            connection->authenticate(user, pass,
                                     connection->getSaslMechanisms());
        } catch (const ConnectionError& error) {
            std::cerr << "Failed to authenticate: " << error.what()
                      << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    ++optind;
    std::vector<std::string> arguments;
    while (optind < argc) {
        arguments.push_back(argv[optind++]);
    }

    return handler(*connection.get(), arguments);
}
