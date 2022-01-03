/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/Unistd.h>
#include <getopt.h>
#include <memcached/stat_group.h>
#include <platform/dirutils.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/frameinfo.h>
#include <utilities/string_utilities.h>
#include <utilities/terminal_color.h>
#include <utilities/terminal_size.h>
#include <utilities/terminate_handler.h>
#include <iostream>
#include <limits>

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (empty == ALL)
 * @param json if true print as json otherwise print old-style
 */
static void request_stat(MemcachedConnection& connection,
                         const std::string& key,
                         bool json,
                         bool format,
                         const std::string& impersonate) {
    try {
        auto getFrameInfos = [&impersonate]() -> FrameInfoVector {
            if (impersonate.empty()) {
                return {};
            }
            FrameInfoVector ret;
            ret.emplace_back(
                    std::make_unique<ImpersonateUserFrameInfo>(impersonate));
            return ret;
        };
        if (json) {
            auto stats = connection.stats(key, getFrameInfos);
            std::cout << stats.dump(format ? 1 : -1, '\t') << std::endl;
        } else {
            connection.stats(
                    [](const std::string& key,
                       const std::string& value) -> void {
                        std::cout << key << " " << value << std::endl;
                    },
                    key,
                    getFrameInfos);
        }
    } catch (const ConnectionError& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
    }
}

static void usage() {
    std::cerr
            << R"(Usage: mcstat [options] statkey ...

Options:

  -h or --host hostname[:port]   The host (with an optional port) to connect to
                                 (for IPv6 use: [address]:port if you'd like to
                                 specify port)
  -p or --port port              The port number to connect to
  -b or --bucket bucketname      The name of the bucket to operate on
  -u or --user username          The name of the user to authenticate as
  -P or --password password      The password to use for authentication
                                 (use '-' to read from standard input, or
                                 set the environment variable CB_PASSWORD)
  --tls[=cert,key]               Use TLS and optionally try to authenticate
                                 by using the provided certificate and
                                 private key.
  -s or --ssl                    Deprecated. Use --tls
  -C or --ssl-cert filename      Deprecated. Use --tls=[cert,key]
  -C or --ssl-cert filename      Deprecated. Use --tls=[cert,key]
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  -j or --json                   Print result as JSON (unformatted)
  -J or --json=pretty            Print result in JSON (formatted)
  -I or --impersonate username   Try to impersonate the specified user
  -a or --all-buckets            Iterate buckets that list bucket returns
)"
#ifndef WIN32
            << "  --no-color                     Disable colors\n"
#endif
            << R"(  --help[=statkey]               This help text (or description of statkeys)
  statkey ...  Statistic(s) to request

)";
    exit(EXIT_FAILURE);
}

void printStatkeyHelp(std::string_view key) {
    size_t dw;
    try {
        auto [width, height] = getTerminalSize();
        (void)height;
        if (width < 60) {
            // if you've got a small terminal we'll just print it in the normal
            // way.
            width = std::numeric_limits<size_t>::max();
        };

        dw = width;
    } catch (std::exception&) {
        dw = std::numeric_limits<size_t>::max();
    }

    bool found = false;

    StatsGroupManager::getInstance().iterate([dw, &found, key](const auto& e) {
        if (key != e.key && !(key == "default" && e.key.empty())) {
            return;
        }

        found = true;
        std::cout << TerminalColor::Green << key << std::endl;
        for (const auto& c : key) {
            (void)c;
            std::cout << "=";
        }
        std::cout << std::endl << std::endl;
        auto descr = e.description;
        while (true) {
            if (descr.size() < dw) {
                std::cout << descr.data() << std::endl;
                break;
            }

            auto idx = descr.rfind(' ', std::min(dw, descr.size()));
            if (idx == std::string::npos) {
                std::cout << descr.data() << std::endl;
                break;
            }
            std::cout.write(descr.data(), idx);
            std::cout << std::endl;
            descr.remove_prefix(idx + 1);
        }
        std::cout << std::endl;
        if (e.bucket) {
            std::cout << TerminalColor::Yellow << "Bucket specific stat group"
                      << std::endl;
        }
        if (e.privileged) {
            std::cout << TerminalColor::Yellow << "Privileged stat group"
                      << std::endl;
        }
        std::cout << TerminalColor::Reset;
    });

    if (!found) {
        std::cerr << TerminalColor::Red << key << " is not a valid stat group"
                  << TerminalColor::Reset << std::endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}

void printStatkeyHelp() {
    size_t dw;
    try {
        auto [width, height] = getTerminalSize();
        (void)height;
        if (width < 60) {
            // if you've got a small terminal we'll just print it in the normal
            // way.
            width = std::numeric_limits<size_t>::max();
        };

        dw = width - 34;
    } catch (std::exception&) {
        dw = std::numeric_limits<size_t>::max();
    }

    std::cerr << "statkey may be one of: " << std::endl;

    StatsGroupManager::getInstance().iterate([dw](const auto& e) {
        std::array<char, 34> kbuf;
        snprintf(kbuf.data(), kbuf.size(), "    %-24s  ", e.key.data());
        std::cerr << kbuf.data();
        if (e.bucket) {
            std::cerr << TerminalColor::Yellow << 'B' << TerminalColor::Reset;
        } else {
            std::cerr << ' ';
        }

        if (e.privileged) {
            std::cerr << TerminalColor::Yellow << 'P' << TerminalColor::Reset;
        } else {
            std::cerr << ' ';
        }
        std::cerr << " ";
        std::fill(kbuf.begin(), kbuf.end(), ' ');

        auto descr = e.description;
        while (true) {
            if (descr.size() < dw) {
                std::cerr << descr.data() << std::endl;
                return;
            }

            auto idx = descr.rfind(' ', std::min(dw, descr.size()));
            if (idx == std::string::npos) {
                std::cerr << descr.data() << std::endl;
                return;
            }
            std::cerr.write(descr.data(), idx);
            std::cerr << std::endl;
            descr.remove_prefix(idx + 1);
            std::cerr.write(kbuf.data(), kbuf.size() - 1);
        }
    });

    std::cerr << "B - bucket specific stat group" << std::endl
              << "P - privileged stat" << std::endl;

    exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();
#ifndef WIN32
    setTerminalColorSupport(isatty(STDERR_FILENO) && isatty(STDOUT_FILENO));
#endif

    int cmd;
    std::string port;
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string ssl_cert;
    std::string ssl_key;
    std::string impersonate;
    sa_family_t family = AF_UNSPEC;
    bool secure = false;
    bool json = false;
    bool format = false;
    bool allBuckets = false;
    std::vector<std::string> buckets;

    cb::net::initialize();

    const std::vector<option> options = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"password", required_argument, nullptr, 'P'},
            {"user", required_argument, nullptr, 'u'},
            {"tls=", optional_argument, nullptr, 't'},
            {"ssl", no_argument, nullptr, 's'},
            {"ssl-cert", required_argument, nullptr, 'C'},
            {"ssl-key", required_argument, nullptr, 'K'},
            {"impersonate", required_argument, nullptr, 'I'},
            {"json", optional_argument, nullptr, 'j'},
            {"all-buckets", no_argument, nullptr, 'a'},
#ifndef WIN32
            {"no-color", no_argument, nullptr, 'n'},
#endif
            {"help", optional_argument, nullptr, 1},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc,
                              argv,
                              "46h:p:u:b:P:SsjJC:K:I:at",
                              options.data(),
                              nullptr)) != EOF) {
        switch (cmd) {
        case '6' :
            family = AF_INET6;
            break;
        case '4' :
            family = AF_INET;
            break;
        case 'h' :
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'b' :
            buckets.emplace_back(optarg);
            break;
        case 'u' :
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 'S':
            // Deprecated and not shown in the help text
            std::cerr << TerminalColor::Yellow
                      << R"(mcstat: -S is deprecated. Use "-P -" instead)"
                      << TerminalColor::Reset << std::endl;
            password.assign(getpass());
            break;
        case 's':
            secure = true;
            break;
        case 'J':
            format = true;
            // FALLTHROUGH
        case 'j':
            json = true;
            if (optarg && std::string{optarg} == "pretty") {
                format = true;
            }
            break;
        case 'C':
            ssl_cert.assign(optarg);
            break;
        case 'K':
            ssl_key.assign(optarg);
            break;
        case 'I':
            impersonate.assign(optarg);
            break;
        case 'n':
            setTerminalColorSupport(false);
            break;
        case 'a':
            allBuckets = true;
            break;
        case 't':
            secure = true;
            if (optarg) {
                auto parts = split_string(optarg, ",");
                if (parts.size() != 2) {
                    std::cerr << TerminalColor::Red
                              << "Incorrect format for --tls=certificate,key"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }
                ssl_cert = std::move(parts.front());
                ssl_key = std::move(parts.back());

                if (!cb::io::isFile(ssl_cert)) {
                    std::cerr << TerminalColor::Red << "Certificate file "
                              << ssl_cert << " does not exists"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }

                if (!cb::io::isFile(ssl_key)) {
                    std::cerr << TerminalColor::Red << "Private key file "
                              << ssl_key << " does not exists"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
            break;
        case 1:
            if (optarg) {
                if (std::string_view{optarg} == "statkey") {
                    printStatkeyHelp();
                } else {
                    printStatkeyHelp(optarg);
                }
            }
            // fallthrough
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    if (allBuckets && !buckets.empty()) {
        std::cerr << TerminalColor::Red
                  << "Cannot use both bucket and all-buckets options"
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    }

    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    try {
        if (port.empty()) {
            port = secure ? "11207" : "11210";
        }
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

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        // MEMCACHED_VERSION contains the git sha
        connection.setAgentName("mcstat " MEMCACHED_VERSION);
        connection.setFeatures({cb::mcbp::Feature::XERROR});

        if (allBuckets) {
            buckets = connection.listBuckets();
        }

        // buckets can be empty, so do..while at least one stat call
        auto bucketItr = buckets.begin();
        do {
            if (bucketItr != buckets.end()) {
                // When all buckets is enabled, clone what cbstats does
                if (allBuckets) {
                    static std::string bucketSeparator(78, '*');
                    std::cout << TerminalColor::Green << bucketSeparator
                              << std::endl
                              << *bucketItr << TerminalColor::Reset << std::endl
                              << std::endl;
                }
                connection.selectBucket(*bucketItr);
                bucketItr++;
            }

            if (optind == argc) {
                request_stat(connection, "", json, format, impersonate);
            } else {
                for (int ii = optind; ii < argc; ++ii) {
                    request_stat(
                            connection, argv[ii], json, format, impersonate);
                }
            }
        } while (bucketItr != buckets.end());

    } catch (const ConnectionError& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
