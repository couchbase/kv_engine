/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <getopt.h>
#include <mcbp/protocol/framebuilder.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/json_utilities.h>
#include <utilities/string_utilities.h>
#include <utilities/terminal_color.h>
#include <utilities/terminate_handler.h>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <vector>

/// Callback class to send to folly to error out if an error occurs while
/// trying to send data on the wire.
class TerminateOnErrorWriteCallback : public folly::AsyncWriter::WriteCallback {
public:
    void writeSuccess() noexcept override {
    }
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override {
        std::cerr << "Failed to send data the server: " << ex.what()
                  << std::endl
                  << "Terminate process." << std::endl;
        std::cerr.flush();
        std::_Exit(EXIT_FAILURE);
    }
} terminateOnErrorWriteCallback;

/// Set to true if TLS mode is requested. We'll use the same TLS configuration
/// on all connections we're trying to create
bool tls = false;
/// The TLS certificate file if provided
std::string tls_certificate_file;
/// The TLS private key file if provided
std::string tls_private_key_file;

/// We're going to use the same buffersize on all connections
size_t buffersize = 13421772;

/// When set to true we'll print out each message we see
bool verbose = false;

static void usage() {
    std::cerr << R"(Usage: dcpdrain [options]

Options:

  -h or --host hostname[:port]   The host (with an optional port) to connect to
                                 (for IPv6 use: [address]:port if you'd like to
                                 specify port)
  -p or --port port              The port number to connect to
  -b or --bucket bucketname      The name of the bucket to operate on
  -u or --user username          The name of the user to authenticate as
  -P or --password password      The password to use for authentication
                                 (use '-' to read from standard input)
  --tls[=cert,key]               Use TLS and optionally try to authenticate
                                 by using the provided certificate and
                                 private key.
  --num-connections=num          The number of connections to use to each host
  -B or --buffer-size size       Specify the DCP buffer size to use
                                 [Default = 13421772]. Set to 0 to disable
                                 DCP flow control (may use k or m to specify
                                 kilo or megabytes).
  -c or --control key=value      Add a control message
  -C or --csv                    Print out the result as csv (ms;bytes;#items)
  -N or --name                   The dcp name to use
  -v or --verbose                Add more output
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  --enable-oso                   Enable 'Out-of-Sequence Order' backfills
  --disable-collections          Disable Hello::Collections negotiation (for use
                                 with pre-7.0 versions).
  --stream-request-value         Path to a file containing stream-request value.
                                 This must be a file storing a JSON object
                                 matching the stream-request value
                                 specification.
  --stream-id                    Path to a file containing stream-ID config.
                                 This must be a file storing a JSON object that
                                 stores a single array of JSON objects, the
                                 JSON objects are stream-request values (with
                                 stream-ID configured). Use of this parameter
                                 enables DCP stream-id. Example:
                                 {
                                    "streams":[
                                      {"collections" : ["0"], "sid":2},
                                      {"collections" : ["8"], "sid":5}]
                                 }
  --stream-request-flags         Value to use for the 4-byte stream-request
                                 flags field.
                                 Default value is DCP_ADD_STREAM_FLAG_TO_LATEST
  --help                         This help text
)";

    exit(EXIT_FAILURE);
}

std::string calculateThroughput(size_t bytes, size_t sec) {
    if (sec > 1) {
        bytes /= sec;
    }

    std::vector<const char*> suffix = {"B/s", "kB/s", "MB/s", "GB/s"};
    int ii = 0;

    while (bytes > 10240) {
        bytes /= 1024;
        ++ii;
        if (ii == 3) {
            break;
        }
    }

    return std::to_string(bytes) + suffix[ii];
}

/// The DcpConnection class is responsible for a single DCP connection to
/// the server and may consist of a number of DCP streams.
/// We may have multiple DCP connections to the same server.
class DcpConnection {
public:
    DcpConnection(const std::string& hostname,
                  std::vector<uint16_t> v,
                  std::shared_ptr<folly::EventBase> eb)
        : vbuckets(std::move(v)) {
        auto [host, port, family] = cb::inet::parse_hostname(hostname, {});
        connection = std::make_unique<MemcachedConnection>(
                host, port, family, tls, eb);
        connection->connect();
    }

    MemcachedConnection& getConnection() {
        return *connection;
    }

    void enterMessagePump(std::string streamRequestValue,
                          std::optional<nlohmann::json>& streamIdConfig,
                          uint32_t streamRequestFlags) {
        if (vbuckets.empty()) {
            return;
        }
        int streamsPerVb = streamIdConfig ? streamIdConfig.value().size() : 1;

        std::unique_ptr<folly::IOBuf> head;
        folly::IOBuf* tailp = nullptr;

        for (int ii = 0; ii < streamsPerVb; ii++) {
            for (auto vb : vbuckets) {
                totalStreams++;
                BinprotDcpStreamRequestCommand streamRequestCommand;
                streamRequestCommand.setDcpFlags(streamRequestFlags);
                streamRequestCommand.setDcpReserved(0);
                streamRequestCommand.setDcpStartSeqno(0);
                streamRequestCommand.setDcpEndSeqno(~0);
                streamRequestCommand.setDcpVbucketUuid(0);
                streamRequestCommand.setDcpSnapStartSeqno(0);
                streamRequestCommand.setDcpSnapEndSeqno(0);
                streamRequestCommand.setVBucket(Vbid(vb));

                if (!streamRequestValue.empty()) {
                    streamRequestCommand.setValue(
                            std::string_view{streamRequestValue});
                }

                if (streamIdConfig) {
                    streamRequestCommand.setValue(streamIdConfig.value()[ii]);
                }

                std::vector<uint8_t> vec;
                streamRequestCommand.encode(vec);
                auto iob = folly::IOBuf::createCombined(vec.size());
                std::memcpy(iob->writableData(), vec.data(), vec.size());
                iob->append(vec.size());
                if (tailp) {
                    tailp->appendChain(std::move(iob));
                } else {
                    head = std::move(iob);
                    tailp = head.get();
                }
            }
        }
        connection->getUnderlyingAsyncSocket().writeChain(
                &terminateOnErrorWriteCallback, std::move(head));
        connection->enterMessagePumpMode(
                [this](const cb::mcbp::Header& header) {
                    if (verbose) {
                        std::cout << header.toJSON(true).dump() << std::endl;
                    }
                    if (header.isRequest()) {
                        handleRequest(header.getRequest());
                    } else {
                        handleResponse(header.getResponse());
                    }
                });
        start = std::chrono::steady_clock::now();
    }

    size_t getMutations() const {
        return mutations;
    }

    size_t getMutationBytes() const {
        return mutation_bytes;
    }

    size_t getTotalBytesReceived() const {
        return connection->getUnderlyingAsyncSocket().getAppBytesReceived();
    }

    void reportConnectionStats() {
        const auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                      start);

        size_t total_bytes = getTotalBytesReceived();
        std::cout << "Connection took " << duration.count() << " ms - "
                  << mutations << " mutations with a total of " << total_bytes
                  << " bytes received (overhead "
                  << total_bytes - mutation_bytes << ") ("
                  << calculateThroughput(total_bytes, duration.count() / 1000)
                  << ")" << std::endl;
    }

protected:
    std::unique_ptr<MemcachedConnection> connection;
    std::vector<uint16_t> vbuckets;
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point stop;

    void handleRequest(const cb::mcbp::Request& req) {
        bool dcpmsg = false;

        switch (req.getClientOpcode()) {
        case cb::mcbp::ClientOpcode::DcpStreamEnd:
            ++stream_end;
            if (stream_end == vbuckets.size()) {
                // we got all we wanted
                connection->getUnderlyingAsyncSocket().setReadCB(nullptr);
                connection->getUnderlyingAsyncSocket().close();
                stop = std::chrono::steady_clock::now();
            }
            dcpmsg = true;
            break;
        case cb::mcbp::ClientOpcode::DcpNoop:
            handleDcpNoop(req);
            break;
        case cb::mcbp::ClientOpcode::DcpMutation:
            ++mutations;
            mutation_bytes += req.getBodylen();
            dcpmsg = true;
            break;

        case cb::mcbp::ClientOpcode::DcpAddStream:
        case cb::mcbp::ClientOpcode::DcpCloseStream:
        case cb::mcbp::ClientOpcode::DcpStreamReq:
        case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
        case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
        case cb::mcbp::ClientOpcode::DcpDeletion:
        case cb::mcbp::ClientOpcode::DcpExpiration:
        case cb::mcbp::ClientOpcode::DcpFlush_Unsupported:
        case cb::mcbp::ClientOpcode::DcpSetVbucketState:
        case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
        case cb::mcbp::ClientOpcode::DcpControl:
        case cb::mcbp::ClientOpcode::DcpSystemEvent:
        case cb::mcbp::ClientOpcode::DcpPrepare:
        case cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged:
        case cb::mcbp::ClientOpcode::DcpCommit:
        case cb::mcbp::ClientOpcode::DcpAbort:
        case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
        case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
            dcpmsg = true;
            break;

        default:
            std::cerr << "Received unexpected message: " << req.toJSON(false)
                      << std::endl;
        }

        if (dcpmsg && buffersize > 0) {
            current_buffer_window +=
                    req.getBodylen() + sizeof(cb::mcbp::Header);
            if (current_buffer_window > (buffersize / 2)) {
                sendBufferAck();
            }
        }
    }

    void handleResponse(const cb::mcbp::Response& response) {
        // Do nothing
    }

    void handleDcpNoop(const cb::mcbp::Request& header) {
        cb::mcbp::Response resp = {};
        resp.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.setOpaque(header.getOpaque());
        resp.setOpcode(header.getClientOpcode());

        auto iob = folly::IOBuf::createCombined(sizeof(resp));
        std::memcpy(iob->writableData(), &resp, sizeof(resp));
        iob->append(sizeof(resp));
        connection->getUnderlyingAsyncSocket().writeChain(
                &terminateOnErrorWriteCallback, std::move(iob));
    }

    void sendBufferAck() {
        // send buffer ack
        std::array<uint8_t, sizeof(cb::mcbp::Header) + 4> backing;
        cb::mcbp::RequestBuilder builder({backing.data(), backing.size()});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement);
        cb::mcbp::request::DcpBufferAckPayload payload;
        payload.setBufferBytes(current_buffer_window);
        builder.setExtras(payload.getBuffer());

        auto packet = builder.getFrame()->getFrame();
        auto iob = folly::IOBuf::createCombined(packet.size());
        std::memcpy(iob->writableData(), packet.data(), packet.size());
        iob->append(packet.size());
        connection->getUnderlyingAsyncSocket().writeChain(
                &terminateOnErrorWriteCallback, std::move(iob));
        current_buffer_window = 0;
    }

    size_t mutation_bytes = 0;
    size_t stream_end = 0;
    size_t mutations = 0;
    size_t current_buffer_window = 0;
    size_t max_vbuckets = 0;
    size_t totalStreams = 0;
};

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

static void setControlMessages(
        MemcachedConnection& connection,
        const std::vector<std::pair<std::string, std::string>>& controls) {
    for (const auto& p : controls) {
        if (verbose) {
            std::cout << "Set DCP control message: " << p.first << "="
                      << p.second << std::endl;
        }
        if (!connection
                     .execute(BinprotGenericCommand{
                             cb::mcbp::ClientOpcode::DcpControl,
                             p.first,
                             p.second})
                     .isSuccess()) {
            std::cerr << "Failed to set " << p.first << " to " << p.second
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }
}

std::pair<std::string, std::string> parseControlMessage(std::string value) {
    auto idx = value.find('=');
    if (idx == std::string::npos) {
        std::cerr << "Error: control message should be key=value" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    return std::make_pair<std::string, std::string>(value.substr(0, idx),
                                                    value.substr(idx + 1));
}

/// The vucketmap is a vector of pairs where the first entry is the
/// hostname (and port) and the second entry is a vector containing
/// all of the vbuckets there
std::vector<std::pair<std::string, std::vector<uint16_t>>> vbucketmap;

void setupVBMap(const std::string& host,
                in_port_t in_port,
                sa_family_t family,
                const std::string& user,
                const std::string& password,
                const std::string& bucket,
                bool enableCollections,
                std::shared_ptr<folly::EventBase> base) {
    MemcachedConnection connection(host, in_port, family, tls, base);
    connection.setSslCertFile(tls_certificate_file);
    connection.setSslKeyFile(tls_private_key_file);
    connection.connect();

    if (!user.empty()) {
        connection.authenticate(user, password, "PLAIN");
    }

    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON}};
    if (enableCollections) {
        features.push_back(cb::mcbp::Feature::Collections);
    }
    connection.setFeatures(features);
    connection.selectBucket(bucket);

    // get the CCCP
    auto rsp = connection.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    if (!rsp.isSuccess()) {
        std::cout << "Failed to fetch cluster map: "
                  << to_string(rsp.getStatus()) << std::endl;
        std::exit(EXIT_FAILURE);
    }
    auto json = rsp.getDataJson();
    auto vbservermap = json["vBucketServerMap"];

    auto nodes = vbservermap["serverList"];
    for (const auto& n : nodes) {
        auto h = n.get<std::string>();
        auto idx = h.find(':');
        auto p = strtoul(h.substr(idx + 1).c_str());
        h.resize(idx);
        if (h.find("$HOST") != std::string::npos) {
            h = host;
        }

        if (p == 11210 && tls) {
            // @todo we should look this up from the cccp payload
            p = 11207;
        }

        h += ":" + std::to_string(p);
        vbucketmap.emplace_back(
                std::make_pair<std::string, std::vector<uint16_t>>(std::move(h),
                                                                   {}));
    }

    auto map = vbservermap["vBucketMap"];
    size_t max_vbuckets = 0;
    for (const auto& e : map) {
        int nodeidx = e[0].get<int>();
        vbucketmap[nodeidx].second.emplace_back(max_vbuckets++);
    }
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
    std::string bucket{};
    sa_family_t family = AF_UNSPEC;
    bool csv = false;
    std::vector<std::pair<std::string, std::string>> controls;
    std::string name = "dcpdrain";
    bool enableOso{false};
    bool enableCollections{true};
    std::string streamRequestFileName;
    std::string streamIdFileName;
    uint32_t streamRequestFlags = DCP_ADD_STREAM_FLAG_TO_LATEST;
    size_t num_connections = 1;

    cb::net::initialize();

    const int valueOptionId = 1;
    const int streamIdOptionId = 2;
    const int enableOsoOptionId = 3;
    const int disableCollectionsOptionId = 4;
    const int streamRequestFlagsOptionId = 5;

    std::vector<option> long_options = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"password", required_argument, nullptr, 'P'},
            {"user", required_argument, nullptr, 'u'},
            {"tls=", optional_argument, nullptr, 't'},
            {"help", no_argument, nullptr, 0},
            {"buffer-size", required_argument, nullptr, 'B'},
            {"control", required_argument, nullptr, 'c'},
            {"csv", no_argument, nullptr, 'C'},
            {"name", required_argument, nullptr, 'N'},
            {"num-connections", required_argument, nullptr, 'n'},
            {"verbose", no_argument, nullptr, 'v'},
            {"enable-oso", no_argument, nullptr, enableOsoOptionId},
            {"disable-collections",
             no_argument,
             nullptr,
             disableCollectionsOptionId},
            {"stream-request-value", required_argument, nullptr, valueOptionId},
            {"stream-id", required_argument, nullptr, streamIdOptionId},
            {"stream-request-flags",
             required_argument,
             nullptr,
             streamRequestFlagsOptionId},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc,
                              argv,
                              "46h:p:u:b:P:B:c:vCMN:",
                              long_options.data(),
                              nullptr)) != EOF) {
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
        case 't':
            tls = true;
            if (optarg) {
                auto parts = split_string(optarg, ",");
                if (parts.size() != 2) {
                    std::cerr << TerminalColor::Red
                              << "Incorrect format for --tls=certificate,key"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }
                tls_certificate_file = std::move(parts.front());
                tls_private_key_file = std::move(parts.back());

                if (!cb::io::isFile(tls_certificate_file)) {
                    std::cerr << TerminalColor::Red << "Certificate file "
                              << tls_certificate_file << " does not exists"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }

                if (!cb::io::isFile(tls_private_key_file)) {
                    std::cerr << TerminalColor::Red << "Private key file "
                              << tls_private_key_file << " does not exists"
                              << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
            break;
        case 'n':
            num_connections = strtoul(optarg);
            break;
        case 'B':
            buffersize = strtoul(optarg);
            break;
        case 'c':
            controls.emplace_back(parseControlMessage(optarg));
            break;
        case 'v':
            verbose = true;
            break;
        case 'C':
            csv = true;
            break;
        case 'N':
            name = optarg;
            break;
        case enableOsoOptionId:
            enableOso = true;
            break;
        case disableCollectionsOptionId:
            enableCollections = false;
            break;
        case valueOptionId:
            streamRequestFileName = optarg;
            break;
        case streamIdOptionId:
            streamIdFileName = optarg;
            break;
        case streamRequestFlagsOptionId:
            streamRequestFlags = strtoul(optarg);
            break;
        default:
            usage();
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

    if (!streamRequestFileName.empty() && !streamIdFileName.empty()) {
        std::cerr << "Please only specify --stream-request-value or --stream-id"
                  << std::endl;
        return EXIT_FAILURE;
    }

    std::string streamRequestValue;
    if (!streamRequestFileName.empty()) {
        streamRequestValue = cb::io::loadFile(streamRequestFileName);
    }

    std::optional<nlohmann::json> streamIdConfig;
    if (!streamIdFileName.empty()) {
        auto json = nlohmann::json::parse(cb::io::loadFile(streamIdFileName));
        // Expected that the document is an array of stream-request values
        auto itr = json.find("streams");
        if (itr == json.end()) {
            std::cerr << "stream-id content invalid:" << json.dump()
                      << std::endl;
            return EXIT_FAILURE;
        }
        cb::throwIfWrongType("streams", *itr, nlohmann::json::value_t::array);
        streamIdConfig = *itr;
    }

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE\n";
        return EXIT_FAILURE;
    }
#endif

    auto event_base = std::make_shared<folly::EventBase>();
    std::vector<std::unique_ptr<DcpConnection>> connections;
    try {
        if (port.empty()) {
            port = tls ? "11207" : "11210";
        }
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }

        setupVBMap(host,
                   in_port,
                   family,
                   user,
                   password,
                   bucket,
                   enableCollections,
                   event_base);

        std::vector<cb::mcbp::Feature> features = {
                {cb::mcbp::Feature::MUTATION_SEQNO,
                 cb::mcbp::Feature::XATTR,
                 cb::mcbp::Feature::XERROR,
                 cb::mcbp::Feature::SNAPPY,
                 cb::mcbp::Feature::JSON}};
        if (enableCollections) {
            features.push_back(cb::mcbp::Feature::Collections);
        }

        for (const auto& [h, vb] : vbucketmap) {
            // We'll use a number fixed number of connections to each host
            // so we need to redistribute the vbuckets across the connections
            // to this host.
            std::vector<std::vector<uint16_t>> perConnVbuckets{num_connections};

            if (num_connections == 1) {
                perConnVbuckets[0] = vb;
            } else {
                // spread the vbuckets across the connections
                int idx = 0;
                for (auto vbucket : vb) {
                    perConnVbuckets[idx++ % perConnVbuckets.size()]
                            .emplace_back(vbucket);
                }
            }

            int idx = 0;
            for (auto vbuckets : perConnVbuckets) {
                connections.emplace_back(std::make_unique<DcpConnection>(
                        h, vbuckets, event_base));
                auto& c = connections.back()->getConnection();
                if (!user.empty()) {
                    c.authenticate(user, password, "PLAIN");
                }

                c.setFeatures(features);
                c.selectBucket(bucket);

                std::string nm =
                        (idx++ == 0) ? name : name + ":" + std::to_string(idx);
                auto rsp = c.execute(BinprotDcpOpenCommand{
                        std::move(nm),
                        cb::mcbp::request::DcpOpenPayload::Producer});
                if (!rsp.isSuccess()) {
                    std::cerr << "Failed to open DCP stream: "
                              << to_string(rsp.getStatus()) << std::endl
                              << "\t" << rsp.getDataString() << std::endl;
                    return EXIT_FAILURE;
                }

                if (buffersize == 0) {
                    if (verbose) {
                        std::cout << "Not using DCP flow control" << std::endl;
                    }
                } else {
                    if (verbose) {
                        std::cout << "Using DCP flow control with buffer size: "
                                  << buffersize << std::endl;
                    }
                    if (!c.execute(BinprotGenericCommand{
                                           cb::mcbp::ClientOpcode::DcpControl,
                                           "connection_buffer_size",
                                           std::to_string(buffersize)})
                                 .isSuccess()) {
                        std::cerr << "Failed to set connection buffer size to "
                                  << buffersize << std::endl;
                        std::exit(EXIT_FAILURE);
                    }
                }

                auto ctrls = controls;
                if (ctrls.empty()) {
                    ctrls = {{"set_priority", "high"},
                             {"supports_cursor_dropping_vulcan", "true"},
                             {"supports_hifi_MFU", "true"},
                             {"send_stream_end_on_client_close_stream", "true"},
                             {"enable_expiry_opcode", "true"},
                             {"set_noop_interval", "1"},
                             {"enable_noop", "true"}};
                }

                if (streamIdConfig) {
                    ctrls.emplace_back(
                            std::make_pair("enable_stream_id", "true"));
                }

                if (enableOso) {
                    ctrls.emplace_back(std::make_pair(
                            "enable_out_of_order_snapshots", "true"));
                }

                setControlMessages(c, ctrls);
            }
        }

        // Now that they're all set up; go ahead and tell them to enter
        // the message pump!
        for (auto& c : connections) {
            c->enterMessagePump(
                    streamRequestValue, streamIdConfig, streamRequestFlags);
        }

    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    const auto start = std::chrono::steady_clock::now();
    event_base->loop();
    const auto stop = std::chrono::steady_clock::now();

    const auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);

    size_t total_bytes = 0;
    size_t mutations = 0;
    size_t mutation_bytes = 0;
    for (const auto& c : connections) {
        total_bytes += c->getTotalBytesReceived();
        mutations += c->getMutations();
        mutation_bytes += c->getMutationBytes();
        if (!csv && connections.size() > 1) {
            c->reportConnectionStats();
        }
    }

    if (csv) {
        std::cout << duration.count() << ';' << total_bytes << ';' << mutations
                  << std::endl;
    } else {
        std::cout << "Took " << duration.count() << " ms - " << mutations
                  << " mutations with a total of " << total_bytes
                  << " bytes received (overhead "
                  << total_bytes - mutation_bytes << ") ("
                  << calculateThroughput(total_bytes, duration.count() / 1000)
                  << ")" << std::endl;
    }

    connections.clear();
    return EXIT_SUCCESS;
}
