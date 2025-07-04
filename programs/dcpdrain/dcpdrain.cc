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
#include <platform/getpass.h>
#include <platform/socket.h>
#include <platform/terminal_color.h>
#include <platform/timeutils.h>
#include <programs/hostname_utils.h>
#include <programs/parse_tls_option.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/json_utilities.h>
#include <utilities/string_utilities.h>
#include <utilities/terminate_handler.h>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <vector>

using namespace cb::terminal;

enum class EnableOSO { False, True, TrueWithSeqnoAdvanced };

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
std::optional<std::filesystem::path> tls_certificate_file;
/// The TLS private key file if provided
std::optional<std::filesystem::path> tls_private_key_file;
std::optional<std::filesystem::path> tls_ca_store_file;

/// We're going to use the same buffersize on all connections
size_t buffersize = 13421772;
float acknowledge_ratio = 0.5;

/// When set to true we'll print out each message we see
bool verbose = false;

/// When true, we will not ack anything
bool hang = false;

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
  --tls[=cert,key[,castore]]     Use TLS
                                 If 'cert' and 'key' is provided (they are
                                 optional) they contains the certificate and
                                 private key to use to connect to the server
                                 (and if the server is configured to do so
                                 it may authenticate to the server by using
                                 the information in the certificate).
                                 A non-default CA store may optionally be
                                 provided.
  --num-connections=num          The number of connections to use to each host
  -B or --buffer-size size       Specify the DCP buffer size to use
                                 [Default = 13421772]. Set to 0 to disable
                                 DCP flow control (may use k or m to specify
                                 kilo or megabytes).
  -a or --acknowledge-ratio      Percentage of buffer-size to receive before
                                 acknowledge is sent back to memcached [Default
                                 = 0.5]. Represented as a float, i.e. 0.5 is
                                 50%.
  -c or --control key=value      Add a control message
  -C or --csv                    Print out the result as csv (ms;bytes;#items)
  -N or --name                   The dcp name to use
  -v or --verbose                Add more output
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  --enable-oso[=with-seqno-advanced]
                                 Enable 'Out-of-Sequence Order' backfills.
                                 If the optional value 'with-seqno-advanced' is
                                 specified, also enable support for sending a
                                 SeqnoAdvanced message when an out of order
                                 snapshot is used and the transmitted item with
                                 the greatest seqno is not the greatest seqno
                                 of the disk snapshot.
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
  --vbuckets=1,2,...             Only stream from a subset of vBuckets. List
                                 vbucket as a comma-separated list of integers.
  --enable-flatbuffer-sysevents  Turn on system events with flatbuffer values
  --enable-change-streams        Turn on change-stream support
  --hang                         Create streams, but do not drain them.
  --start-inside-snapshot        Start the stream inside of a snapshot. The
                                 client will query vbucket-details and begin at
                                 1/2 of each vbucket's high-seqno.
  --help                         This help text
)";

    exit(EXIT_FAILURE);
}

/// The DcpConnection class is responsible for a single DCP connection to
/// the server and may consist of a number of DCP streams.
/// We may have multiple DCP connections to the same server.
class DcpConnection {
public:
    DcpConnection(const std::string& hostname,
                  std::vector<uint16_t> v,
                  std::shared_ptr<folly::EventBase> eb) {
        for (auto vb : v) {
            vbuckets.emplace_back(vb);
        }
        auto [host, port, family] = cb::inet::parse_hostname(hostname, {});
        connection = std::make_unique<MemcachedConnection>(
                host, port, family, tls, std::move(eb));
        connection->connect();
    }

    MemcachedConnection& getConnection() const {
        return *connection;
    }

    void enterMessagePump(std::string_view streamRequestValue,
                          const std::optional<nlohmann::json>& streamIdConfig,
                          cb::mcbp::DcpAddStreamFlag streamRequestFlags) {
        if (vbuckets.empty()) {
            return;
        }
        const auto streamsPerVb =
                streamIdConfig ? streamIdConfig.value().size() : 1;

        std::unique_ptr<folly::IOBuf> head;
        folly::IOBuf* tailp = nullptr;

        for (std::size_t ii = 0; ii < streamsPerVb; ++ii) {
            for (const auto& vb : vbuckets) {
                totalStreams++;
                BinprotDcpStreamRequestCommand streamRequestCommand;
                streamRequestCommand.setDcpFlags(streamRequestFlags);
                streamRequestCommand.setDcpReserved(0);
                streamRequestCommand.setDcpStartSeqno(vb.startSeqno);
                streamRequestCommand.setDcpEndSeqno(~0);
                streamRequestCommand.setDcpVbucketUuid(vb.vbucketUuid);
                streamRequestCommand.setDcpSnapStartSeqno(vb.snapStartSeqno);
                streamRequestCommand.setDcpSnapEndSeqno(vb.snapEndSeqno);
                streamRequestCommand.setVBucket(Vbid(vb.vbucket));

                if (!streamRequestValue.empty()) {
                    streamRequestCommand.setValue(streamRequestValue);
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
                        std::cout << header.to_json(true).dump() << std::endl;
                    }
                    if (header.isRequest()) {
                        handleRequest(header.getRequest());
                    } else {
                        handleResponse(header.getResponse());
                    }
                });
        start = std::chrono::steady_clock::now();
    }

    size_t getSnapshots() const {
        return snapshots;
    }

    size_t getOsoSnapshots() const {
        return oso_snapshots;
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
        fmt::print(
                "Connection took {} ms - {} mutations with a total of {} bytes "
                "received in {} snapshots and {} oso snapshots (overhead {} "
                "bytes, {})\n",
                duration.count(),
                mutations,
                total_bytes,
                snapshots,
                oso_snapshots,
                total_bytes - mutation_bytes,
                cb::calculateThroughput(total_bytes, stop - start));
    }

    /**
     * For all of the vbuckets on this connection, override our default
     * stream-request values. Instead use the vbuckets high-seqno as the end of
     * the snapshot and choose 1/2 of that as the start.
     *
     * e.g. start=high-seqno/2, snapshot{0, high-seqno}
     */
    void queryServerAndSetupStreamRequestInsideSnapshot();

protected:
    std::unique_ptr<MemcachedConnection> connection;
    struct StreamRequestParams {
        StreamRequestParams(uint16_t vbucket) : vbucket(vbucket) {
        }
        uint16_t vbucket{0};
        uint64_t startSeqno{0};
        uint64_t snapStartSeqno{0};
        uint64_t snapEndSeqno{0};
        uint64_t vbucketUuid{0};
    };
    std::vector<StreamRequestParams> vbuckets;
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

        case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
            ++snapshots;
            dcpmsg = true;
            break;

        case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
            ++oso_snapshots;
            dcpmsg = true;
            break;

        case cb::mcbp::ClientOpcode::DcpAddStream:
        case cb::mcbp::ClientOpcode::DcpCloseStream:
        case cb::mcbp::ClientOpcode::DcpStreamReq:
        case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
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
            dcpmsg = true;
            break;

        default:
            std::cerr << "Received unexpected message: " << req.to_json(false)
                      << std::endl;
        }

        if (dcpmsg && buffersize > 0) {
            current_buffer_window +=
                    req.getBodylen() + sizeof(cb::mcbp::Header);
            if (current_buffer_window > (buffersize * acknowledge_ratio) &&
                !hang) {
                sendBufferAck();
            }
        }
    }

    void handleResponse(const cb::mcbp::Response& response) {
        if (cb::mcbp::isStatusSuccess(response.getStatus())) {
            return;
        }

        if (response.getClientOpcode() ==
            cb::mcbp::ClientOpcode::DcpStreamReq) {
            std::cerr << TerminalColor::Red << response.to_json(true).dump()
                      << TerminalColor::Reset << std::endl;
            stream_end++;
            if (stream_end == vbuckets.size()) {
                // we got all we wanted
                connection->getUnderlyingAsyncSocket().setReadCB(nullptr);
                connection->getUnderlyingAsyncSocket().close();
                stop = std::chrono::steady_clock::now();
            }
        }
    }

    void handleDcpNoop(const cb::mcbp::Request& header) const {
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
    size_t snapshots = 0;
    size_t oso_snapshots = 0;
    size_t current_buffer_window = 0;
    size_t max_vbuckets = 0;
    size_t totalStreams = 0;
};

void DcpConnection::queryServerAndSetupStreamRequestInsideSnapshot() {
    // One stat call and then we'll find each of the vbuckets.
    auto vbucketDetailsMap = getConnection().stats("vbucket-details");

    for (auto& vb : vbuckets) {
        std::string prefix = "vb_" + std::to_string(vb.vbucket);
        std::string highSeqKey = prefix + ":high_seqno";
        std::string uuidSeqKey = prefix + ":uuid";
        Expects(vbucketDetailsMap.count(highSeqKey));
        Expects(vbucketDetailsMap.count(uuidSeqKey));

        vb.snapEndSeqno = vbucketDetailsMap[highSeqKey].get<uint64_t>();
        vb.snapStartSeqno = 0;
        vb.startSeqno = vb.snapEndSeqno / 2;
        vb.vbucketUuid = vbucketDetailsMap[uuidSeqKey].get<uint64_t>();
    }
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
                ret *= 1_MiB;
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

static std::pair<std::string, std::string> parseControlMessage(
        const std::string& value) {
    auto idx = value.find('=');
    if (idx == std::string::npos) {
        std::cerr << "Error: control message should be key=value" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    return {value.substr(0, idx), value.substr(idx + 1)};
}

/**
 * Given a string in the form of comma-separated integers, parse and return
 * a set of Vbids of those numbers.
 */
static std::unordered_set<Vbid> parseVBuckets(const std::string& value) {
    std::unordered_set<Vbid> vbuckets;
    for (auto& element : split_string(value, ",")) {
        Vbid::id_type vb;
        auto result = std::from_chars(
                element.data(), element.data() + element.size(), vb);
        if (result.ec == std::errc()) {
            vbuckets.emplace(vb);
        } else {
            std::cerr << "Error: Invalid vbucket specified: " << element
                      << " - " << std::make_error_code(result.ec).message()
                      << "\n";
            std::exit(EXIT_FAILURE);
        }
    }
    if (verbose) {
        std::cout << "Streaming a subset of vBuckets: ";
        bool first = true;
        for (auto& vb : vbuckets) {
            if (!first) {
                std::cout << ',';
            }
            first = false;
            std::cout << vb.get();
        }
        std::cout << "\n";
    }
    return vbuckets;
}

/// The vucketmap is a vector of pairs where the first entry is the
/// hostname (and port) and the second entry is a vector containing
/// all of the vbuckets there
static std::vector<std::pair<std::string, std::vector<uint16_t>>> vbucketmap;

static void setupVBMap(const std::string& host,
                       in_port_t in_port,
                       sa_family_t family,
                       const std::string& user,
                       const std::string& password,
                       const std::string& bucket,
                       bool enableCollections,
                       const std::unordered_set<Vbid>& vbuckets,
                       std::shared_ptr<folly::EventBase> base) {
    MemcachedConnection connection(host, in_port, family, tls, std::move(base));
    if (tls_certificate_file && tls_private_key_file) {
        connection.setTlsConfigFiles(*tls_certificate_file,
                                     *tls_private_key_file,
                                     tls_ca_store_file);
    }
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
        std::cout << "Failed to fetch cluster map: " << rsp.getStatus()
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
    auto json = rsp.getDataJson();
    auto vbservermap = json["vBucketServerMap"];

    auto nodes = vbservermap["serverList"];
    for (std::size_t i = 0; i < nodes.size(); i++) {
        auto h = nodes[i].get<std::string>();
        auto idx = h.find(':');
        h.resize(idx);
        if (h.find("$HOST") != std::string::npos) {
            h = host;
        }

        const auto& services =
                json["nodesExt"].at(static_cast<int>(i)).at("services");
        uint16_t port = tls ? services.at("kvSSL") : services.at("kv");

        h += ":" + std::to_string(port);
        vbucketmap.emplace_back(
                std::make_pair<std::string, std::vector<uint16_t>>(std::move(h),
                                                                   {}));
    }

    auto map = vbservermap["vBucketMap"];
    Vbid current_vbucket{0};
    for (const auto& e : map) {
        // If user specified a subset of vBuckets, we only add to map if
        // this vbucket was included.
        if (vbuckets.empty() || vbuckets.contains(current_vbucket)) {
            int nodeidx = e[0].get<int>();
            vbucketmap[nodeidx].second.emplace_back(current_vbucket.get());
        }
        ++current_vbucket;
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
    std::vector<std::pair<std::string, std::string>> controls = {
            {"set_priority", "high"},
            {"supports_cursor_dropping_vulcan", "true"},
            {"supports_hifi_MFU", "true"},
            {"send_stream_end_on_client_close_stream", "true"},
            {"enable_expiry_opcode", "true"},
            {"set_noop_interval", "60"},
            {"enable_noop", "true"}};
    std::string name = "dcpdrain:" + std::to_string(::getpid());
    EnableOSO enableOso{EnableOSO::False};
    bool enableCollections{true};
    std::string streamRequestFileName;
    std::string streamIdFileName;
    cb::mcbp::DcpAddStreamFlag streamRequestFlags =
            cb::mcbp::DcpAddStreamFlag::ToLatest;
    size_t num_connections = 1;
    bool enableFlatbufferSysEvents{false};
    bool enableChangeStreams{false};
    std::unordered_set<Vbid> vbuckets;
    bool startInsideSnapshot{false};

    cb::net::initialize();

    // values for getopt_long option.val fields. Must start from non-zero.
    enum Options {
        Value = 1,
        StreamId,
        EnableOso,
        EnableOsoWithSeqnoAdvanced,
        DisableCollections,
        StreamRequestFlags,
        EnableFlatbufferSysEvents,
        EnableChangeStreams,
        VBuckets,
        Hang,
        StartInsideSnapshot
    };

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
            {"acknowledge-ratio", required_argument, nullptr, 'a'},
            {"control", required_argument, nullptr, 'c'},
            {"csv", no_argument, nullptr, 'C'},
            {"name", required_argument, nullptr, 'N'},
            {"num-connections", required_argument, nullptr, 'n'},
            {"verbose", no_argument, nullptr, 'v'},
            {"enable-oso", optional_argument, nullptr, Options::EnableOso},
            {"disable-collections",
             no_argument,
             nullptr,
             Options::DisableCollections},
            {"stream-request-value",
             required_argument,
             nullptr,
             Options::Value},
            {"stream-id", required_argument, nullptr, Options::StreamId},
            {"stream-request-flags",
             required_argument,
             nullptr,
             Options::StreamRequestFlags},
            {"vbuckets", required_argument, nullptr, Options::VBuckets},
            {"enable-flatbuffer-sysevents",
             no_argument,
             nullptr,
             Options::EnableFlatbufferSysEvents},
            {"enable-change-streams",
             no_argument,
             nullptr,
             Options::EnableChangeStreams},
            {"hang", no_argument, nullptr, Options::Hang},
            {"start-inside-snapshot",
             no_argument,
             nullptr,
             Options::StartInsideSnapshot},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc,
                              argv,
                              "46h:p:u:b:P:B:a:c:vCMN:",
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
                std::tie(tls_certificate_file,
                         tls_private_key_file,
                         tls_ca_store_file) = parse_tls_option_or_exit(optarg);
            }
            break;
        case 'n':
            num_connections = strtoul(optarg);
            break;
        case 'B':
            buffersize = strtoul(optarg);
            break;
        case 'a':
            acknowledge_ratio = std::strtof(optarg, nullptr);
            if (acknowledge_ratio <= 0 || acknowledge_ratio > 1) {
                std::cerr << "Error: invalid value '" << optarg
                          << "' specified for -acknowledge-ratio. Supported "
                             "values are between 0 and 1\n";
                return EXIT_FAILURE;
            }
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
        case Options::EnableOso:
            if (optarg) {
                if (std::string_view{optarg} == "with-seqno-advanced") {
                    enableOso = EnableOSO::TrueWithSeqnoAdvanced;
                } else {
                    std::cerr
                            << "Error: invalid option '" << optarg
                            << "' specified for --enable-oso. Supported values "
                               "are 'with-seqno-advanced'\n";
                    return EXIT_FAILURE;
                }
            } else {
                enableOso = EnableOSO::True;
            }
            break;
        case Options::EnableOsoWithSeqnoAdvanced:
            break;
        case Options::DisableCollections:
            enableCollections = false;
            break;
        case Options::Value:
            streamRequestFileName = optarg;
            break;
        case Options::StreamId:
            streamIdFileName = optarg;
            break;
        case Options::StreamRequestFlags:
            streamRequestFlags =
                    static_cast<cb::mcbp::DcpAddStreamFlag>(strtoul(optarg));
            break;
        case Options::EnableFlatbufferSysEvents:
            enableFlatbufferSysEvents = true;
            break;
        case Options::EnableChangeStreams:
            enableChangeStreams = true;
            break;
        case Options::VBuckets:
            vbuckets = parseVBuckets(optarg);
            break;
        case Options::Hang:
            hang = true;
            break;
        case Options::StartInsideSnapshot:
            startInsideSnapshot = true;
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    if (password == "-") {
        password.assign(cb::getpass());
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
                   vbuckets,
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
            for (const auto& vbuckets : perConnVbuckets) {
                connections.emplace_back(std::make_unique<DcpConnection>(
                        h, vbuckets, event_base));
                auto& c = connections.back()->getConnection();
                if (!user.empty()) {
                    c.authenticate(user, password, "PLAIN");
                }

                c.setFeatures(features);
                c.selectBucket(bucket);

                if (startInsideSnapshot) {
                    connections.back()
                            ->queryServerAndSetupStreamRequestInsideSnapshot();
                }

                std::string nm = name + ":" + std::to_string(idx++);
                auto rsp = c.execute(BinprotDcpOpenCommand{
                        std::move(nm), cb::mcbp::DcpOpenFlag::Producer});
                if (!rsp.isSuccess()) {
                    std::cerr
                            << "Failed to open DCP stream: " << rsp.getStatus()
                            << std::endl
                            << "\t" << rsp.getDataView() << std::endl;
                    return EXIT_FAILURE;
                }

                if (hang) {
                    if (verbose) {
                        std::cout << "Hang specified, setting DCP flow control "
                                     "to 24, and not sending any BufferAcks"
                                  << std::endl;
                    }
                    buffersize = 24;
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

                if (streamIdConfig) {
                    controls.emplace_back("enable_stream_id", "true");
                }

                switch (enableOso) {
                case EnableOSO::False:
                    break;
                case EnableOSO::True:
                    controls.emplace_back("enable_out_of_order_snapshots",
                                          "true");
                    break;
                case EnableOSO::TrueWithSeqnoAdvanced:
                    controls.emplace_back("enable_out_of_order_snapshots",
                                          "true_with_seqno_advanced");
                    break;
                }

                if (enableFlatbufferSysEvents) {
                    controls.emplace_back("flatbuffers_system_events", "true");
                }

                if (enableChangeStreams) {
                    controls.emplace_back("change_streams", "true");
                }

                setControlMessages(c, controls);
            }
        }

        // Now that they're all set up; go ahead and tell them to enter
        // the message pump!
        for (auto& c : connections) {
            c->enterMessagePump(
                    streamRequestValue, streamIdConfig, streamRequestFlags);
        }

    } catch (const std::exception& ex) {
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
    size_t snapshots = 0;
    size_t oso_snapshots = 0;
    for (const auto& c : connections) {
        total_bytes += c->getTotalBytesReceived();
        mutations += c->getMutations();
        mutation_bytes += c->getMutationBytes();
        snapshots += c->getSnapshots();
        oso_snapshots += c->getOsoSnapshots();
        if (!csv && connections.size() > 1) {
            c->reportConnectionStats();
        }
    }

    if (csv) {
        std::cout << duration.count() << ';' << total_bytes << ';' << mutations
                  << std::endl;
    } else {
        fmt::print(
                "Execution took {} ms - {} mutations with a total of {} bytes "
                "received in {} snapshots and {} oso snapshots (overhead {} "
                "bytes, {})\n",
                duration.count(),
                mutations,
                total_bytes,
                snapshots,
                oso_snapshots,
                total_bytes - mutation_bytes,
                cb::calculateThroughput(total_bytes, stop - start));
    }

    connections.clear();
    return EXIT_SUCCESS;
}
