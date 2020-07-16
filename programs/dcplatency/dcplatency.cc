/*
 *     Copyright 2020 Couchbase, Inc.
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

// dcplatency is a program used to test the "performance" of DCP on a server.
// It starts by creating two connections to the server. The first connection
// is opened as a DCP producer streaming vbucket 0, the second connection then
// store a single document. Once the document is received over DCP it is sent
// the same document is stored once again on the server. We'll loop doing this
// until we performed the requested number of iterations.

#include <event2/buffer.h>
#include <event2/util.h>
#include <getopt.h>
#include <libevent/utilities.h>
#include <platform/socket.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/terminate_handler.h>
#include <cstdlib>
#include <iostream>

std::vector<uint8_t> mutation;
size_t count = 0;
bool verbose = false;
size_t iterations = 200000;

bool isPacketAvailable(evbuffer* input) {
    auto size = evbuffer_get_length(input);
    if (size < sizeof(cb::mcbp::Header)) {
        return false;
    }

    const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
    if (header == nullptr) {
        throw std::runtime_error(
                "isPacketAvailable(): Failed to reallocate event "
                "input buffer: " +
                std::to_string(sizeof(cb::mcbp::Header)));
    }

    if (!header->isValid()) {
        std::cerr << header->toJSON(false) << std::endl;
        throw std::runtime_error(
                "isPacketAvailable(): Invalid packet header detected");
    }

    const auto framesize = sizeof(*header) + header->getBodylen();
    if (size >= framesize) {
        // We've got the entire buffer available.. make sure it is continuous
        if (evbuffer_pullup(input, framesize) == nullptr) {
            throw std::runtime_error(
                    "isPacketAvailable(): Failed to reallocate "
                    "event input buffer: " +
                    std::to_string(framesize));
        }
        return true;
    }

    return false;
}

void read_callback(bufferevent* bev, void* ctx) {
    auto* input = bufferevent_get_input(bev);
    while (isPacketAvailable(input)) {
        const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
                evbuffer_pullup(input, sizeof(cb::mcbp::Header)));

        if (header->isRequest() &&
            header->getRequest().getClientOpcode() ==
                    cb::mcbp::ClientOpcode::DcpMutation) {
            auto* client = reinterpret_cast<bufferevent*>(ctx);
            if (++count == iterations) {
                event_base_loopbreak(bufferevent_get_base(bev));
                return;
            }

            if (evbuffer_add_reference(bufferevent_get_output(client),
                                       mutation.data(),
                                       mutation.size(),
                                       {},
                                       {}) == -1) {
                std::cerr << "Failed to send next mutation" << std::endl;
            }
        }

        evbuffer_drain(input, header->getBodylen() + sizeof(cb::mcbp::Header));
    }
}

void event_callback(bufferevent* bev, short event, void*) {
    if (((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) ||
        ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR)) {
        std::cerr << "Other side closed connection: " << event << std::endl;
        bufferevent_free(bev);
    }
}

std::unique_ptr<MemcachedConnection> createConnection(
        const std::string& host,
        in_port_t in_port,
        sa_family_t family,
        const std::string& user,
        const std::string& password,
        const std::string& bucket) {
    auto ret =
            std::make_unique<MemcachedConnection>(host, in_port, family, false);
    ret->connect();
    ret->authenticate(user, password, "PLAIN");
    ret->selectBucket(bucket);
    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON}};
    ret->setFeatures(features);
    return ret;
}

void setupDcpConnection(MemcachedConnection& connection) {
    auto rsp = connection.execute(BinprotDcpOpenCommand{
            "dcpdrain", cb::mcbp::request::DcpOpenPayload::Producer});
    if (!rsp.isSuccess()) {
        std::cerr << "Failed to open DCP stream: " << to_string(rsp.getStatus())
                  << std::endl
                  << "\t" << rsp.getDataString() << std::endl;
        exit(EXIT_FAILURE);
    }

    // we're only going to use a single  vbucket
    BinprotDcpStreamRequestCommand streamRequestCommand;
    streamRequestCommand.setDcpFlags(0);
    streamRequestCommand.setDcpReserved(0);
    streamRequestCommand.setDcpStartSeqno(0);
    streamRequestCommand.setDcpEndSeqno(0xffffffff);
    streamRequestCommand.setDcpVbucketUuid(0);
    streamRequestCommand.setDcpSnapStartSeqno(0);
    streamRequestCommand.setDcpSnapEndSeqno(0xfffffff);
    streamRequestCommand.setVBucket(Vbid(0));
    rsp = connection.execute(streamRequestCommand);
    if (!rsp.isSuccess()) {
        std::cerr << "DCP stream request failed: " << to_string(rsp.getStatus())
                  << std::endl
                  << "\t" << rsp.getDataString() << std::endl;
        exit(EXIT_FAILURE);
    }
}

static void setupTest(size_t size) {
    count = 0;
    mutation.clear();
    mutation.reserve(size + sizeof(cb::mcbp::Header) +
                     sizeof(cb::mcbp::request::MutationPayload) + 3);
    std::vector<uint8_t> body(size);

    BinprotMutationCommand mutationCommand;
    mutationCommand.setKey("key");
    mutationCommand.setMutationType(MutationType::Set);
    mutationCommand.setVBucket(Vbid{0});
    mutationCommand.addValueBuffer({body.data(), body.size()});
    mutationCommand.encode(mutation);
    mutation[1] = static_cast<uint8_t>(cb::mcbp::ClientOpcode::Setq);
}

static unsigned long strtoul(const char* arg) {
    try {
        char* end = nullptr;
        auto ret = std::strtoul(arg, &end, 10);
        if (end != nullptr) {
            const std::string rest{end};
            if (rest == "k" || rest == "K") {
                ret *= 1024;
            } else if (rest == "m" || rest == "M") {
                ret *= 1024 * 1024;
            } else if (!rest.empty()) {
                std::cerr << "Failed to parse string (extra characters at the "
                             "end): "
                          << rest << std::endl;
                std::exit(EXIT_FAILURE);
            }
        }
        return ret;
    } catch (const std::exception& exception) {
        std::cerr << "Failed to parse string: " << exception.what()
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();
    // Initialize the socket subsystem
    cb_initialize_sockets();

    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{};
    sa_family_t family = AF_UNSPEC;
    std::string name = "dcplatency";
    std::size_t size = 8192;

    std::vector<option> long_options = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"password", required_argument, nullptr, 'P'},
            {"user", required_argument, nullptr, 'u'},
            {"help", no_argument, nullptr, 0},
            {"name", required_argument, nullptr, 'N'},
            {"verbose", no_argument, nullptr, 'v'},
            {"size", required_argument, nullptr, 's'},
            {"iterations", required_argument, nullptr, 'i'},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc, argv, "", long_options.data(), nullptr)) !=
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
        case 'b':
            bucket.assign(optarg);
            break;
        case 'u':
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 'v':
            verbose = true;
            break;
        case 'N':
            name = optarg;
            break;
        case 's':
            size = strtoul(optarg);
            break;
        case 'i':
            iterations = strtoul(optarg);
            break;
        default:
            std::cerr << R"(Usage: dcplatency [options]

Options:

  --host hostname[:port]   The host (with an optional port) to connect to
                           (for IPv6 use: [address]:port if you'd like to
                           specify port)
  --port port              The port number to connect to
  --bucket bucketname      The name of the bucket to operate on
  --user username          The name of the user to authenticate as
  --password password      The passord to use for authentication
                           (use '-' to read from standard input)
  --name                   The dcp name to use
  --size size              The document size (in bytes)
  --verbose                Add more output
  --ipv4                   Connect over IPv4
  --ipv6                   Connect over IPv6
  --help                   This help text

)";
            return EXIT_FAILURE;
        }
    }

    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    if (bucket.empty()) {
        std::cerr << "Please specify bucket with -b" << std::endl;
        return EXIT_FAILURE;
    }

    cb::libevent::unique_event_base_ptr base(event_base_new());
    std::vector<cb::libevent::unique_bufferevent_ptr> events;
    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }

        auto loadClient =
                createConnection(host, in_port, family, user, password, bucket);
        loadClient->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, "key"});

        auto dcpConnection =
                createConnection(host, in_port, family, user, password, bucket);
        setupDcpConnection(*dcpConnection);

        // Setup the Client
        auto socket = loadClient->releaseSocket();
        evutil_make_socket_nonblocking(socket);
        auto* clientBev = bufferevent_socket_new(
                base.get(),
                socket,
                BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS);

        events.emplace_back(clientBev);
        bufferevent_setcb(clientBev, {}, {}, event_callback, nullptr);
        bufferevent_disable(clientBev, EV_READ);

        // Setup the DCP connection
        socket = dcpConnection->releaseSocket();
        evutil_make_socket_nonblocking(socket);
        auto* bev = bufferevent_socket_new(
                base.get(),
                socket,
                BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS);
        events.emplace_back(bev);
        bufferevent_setcb(bev, read_callback, {}, event_callback, clientBev);
        bufferevent_enable(bev, EV_READ);
        setupTest(size);

        if (evbuffer_add_reference(bufferevent_get_output(clientBev),
                                   mutation.data(),
                                   mutation.size(),
                                   {},
                                   {}) == -1) {
            std::cerr << "Failed to send next mutation" << std::endl;
            return EXIT_FAILURE;
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    const auto start = std::chrono::steady_clock::now();
    event_base_loop(base.get(), 0);
    const auto stop = std::chrono::steady_clock::now();

    const auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << size << ";" << duration.count() / iterations << std::endl;

    return EXIT_SUCCESS;
}
