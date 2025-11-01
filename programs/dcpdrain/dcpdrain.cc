/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <engines/ep/src/dcp/dcp-types.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/json_utilities.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <platform/split_string.h>
#include <platform/terminal_color.h>
#include <platform/timeutils.h>
#include <programs/hostname_utils.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/json_utilities.h>
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

/// We're going to use the same buffersize on all connections
size_t buffersize = 13_MiB;
float acknowledge_ratio = 0.5;

/// When set to true we'll print out each message we see
bool verbose = false;

/// When true, we will not ack anything
bool hang = false;

static void usage(const McProgramGetopt& instance, const int exitcode) {
    std::cerr << R"(Usage: dcpdrain [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

/// The DcpConnection class is responsible for a single DCP connection to
/// the server and may consist of a number of DCP streams.
/// We may have multiple DCP connections to the same server.
class DcpConnection {
public:
    DcpConnection(std::unique_ptr<MemcachedConnection> c,
                  std::string streamname,
                  const std::vector<uint16_t>& v)
        : connection(std::move(c)), streamname(std::move(streamname)) {
        for (const auto& vb : v) {
            vbuckets.emplace_back(vb);
        }
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
                streamRequestCommand.setDcpEndSeqno(~0ULL);
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

    nlohmann::json getConnectionStats() const {
        const auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                      start);
        const auto total_bytes = getTotalBytesReceived();
        std::vector<uint16_t> vb;
        for (const auto& v : vbuckets) {
            vb.emplace_back(v.vbucket);
        }
        nlohmann::json entry = {
                {"name", streamname},
                {"duration_ms", duration.count()},
                {"mutations", mutations},
                {"total_bytes", total_bytes},
                {"snapshots", snapshots},
                {"oso_snapshots", oso_snapshots},
                {"overhead_bytes", total_bytes - mutation_bytes},
                {"throughput",
                 cb::calculateThroughput(total_bytes, stop - start)}};
        if (!vb.empty()) {
            entry["vbuckets"] = vb;
        }
        if (connection) {
            entry["host"] = connection->getHostname();
            entry["port"] = connection->getPort();
        }
        return entry;
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
    const std::string streamname;
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
        case cb::mcbp::ClientOpcode::DcpCachedValue:
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
        const std::string_view value) {
    auto pieces = cb::string::split(value, '=');
    if (pieces.size() != 2) {
        std::cerr << "Error: control message should be key=value" << std::endl;
        std::exit(EXIT_FAILURE);
    }
    return {std::string(pieces[0]), std::string(pieces[1])};
}

/**
 * Given a string in the form of comma-separated integers, parse and return
 * a set of Vbids of those numbers.
 */
static std::unordered_set<Vbid> parseVBuckets(const std::string_view value) {
    std::unordered_set<Vbid> vbuckets;
    for (auto& element : cb::string::split(value, ',')) {
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

static void setupVBMap(MemcachedConnection& connection,
                       const std::unordered_set<Vbid>& vbuckets) {
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
            h = connection.getHostname();
        }

        const auto& services =
                json["nodesExt"].at(static_cast<int>(i)).at("services");
        uint16_t port =
                connection.isSsl() ? services.at("kvSSL") : services.at("kv");

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

cb::mcbp::DcpOpenFlag parseExtraOpenFlags(std::string_view value) {
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const std::exception&) {
        json = value;
    }
    cb::mcbp::DcpOpenFlag flag = json;
    return flag;
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();
#ifndef WIN32
    setTerminalColorSupport(isatty(STDERR_FILENO) && isatty(STDOUT_FILENO));
#endif
    cb::net::initialize();

    std::string bucket{};
    bool json = false;
    std::vector<std::pair<std::string, std::string>> controls = {
            {std::string{DcpControlKeys::SetPriority}, "high"},
            {std::string{DcpControlKeys::SupportsCursorDroppingVulcan}, "true"},
            {std::string{DcpControlKeys::SupportsHifiMfu}, "true"},
            {std::string{DcpControlKeys::SendStreamEndOnClientCloseStream},
             "true"},
            {std::string{DcpControlKeys::EnableExpiryOpcode}, "true"},
            {std::string{DcpControlKeys::SetNoopInterval}, "60"},
            {std::string{DcpControlKeys::EnableNoop}, "true"}};
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
    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON}};
    auto dcpOpenFlag = cb::mcbp::DcpOpenFlag::Producer;

    McProgramGetopt options;
    using namespace cb::getopt;
    options.addOption({[&bucket](auto value) { bucket.assign(value); },
                       'b',
                       "bucket",
                       Argument::Required,
                       "name",
                       "The name of the bucket to operate on"});

    options.addOption({[&num_connections](auto value) {
                           num_connections = std::stoi(std::string(value));
                       },
                       "num-connections",
                       Argument::Required,
                       "num",
                       "The number of connections to use to each host"});

    options.addOption(
            {[](auto value) { buffersize = parse_size_string(value); },
             'B',
             "buffer-size",
             Argument::Required,
             "size",
             "Specify the DCP buffer size to use [Default = 13421772]. Set to "
             "0 to disable DCP flow control (may use k or m to specify kilo or "
             "megabytes)."});

    options.addOption(
            {[](auto value) {
                 acknowledge_ratio = std::stof(std::string(value));
                 if (acknowledge_ratio <= 0 || acknowledge_ratio > 1) {
                     std::cerr
                             << "Error: invalid value '" << value
                             << "' specified for -acknowledge-ratio. Supported "
                                "values are between 0 and 1\n";
                     std::exit(EXIT_FAILURE);
                 }
             },
             'a',
             "acknowledge-ratio",
             Argument::Required,
             "ratio",
             "Percentage of buffer-size to receive before acknowledge is sent "
             "back to memcached [Default = 0.5]. Represented as a float, i.e. "
             "0.5 is 50%."});

    options.addOption({[&controls](auto value) {
                           controls.emplace_back(parseControlMessage(value));
                       },
                       'c',
                       "control",
                       Argument::Required,
                       "key=value",
                       "Add a control message"});

    options.addOption({[&json](auto) { json = true; },
                       "json",
                       "Print the result as a JSON object"});

    options.addOption({[&name](auto value) { name.assign(value); },
                       'N',
                       "name",
                       Argument::Required,
                       "name",
                       "The DCP name to use"});
    options.addOption({[&dcpOpenFlag](auto value) {
                           dcpOpenFlag |= parseExtraOpenFlags(value);
                       },
                       "open-flags",
                       Argument::Required,
                       "flag",
                       "Extra flags to pass to DCP open command"});

    options.addOption(
            {[](auto) { verbose = true; }, 'v', "verbose", "Add more output"});

    options.addOption(
            {[&enableOso](auto value) {
                 if (value.empty()) {
                     enableOso = EnableOSO::True;
                 } else {
                     if (value == "with-seqno-advanced") {
                         enableOso = EnableOSO::TrueWithSeqnoAdvanced;
                     } else {
                         std::cerr << "Error: invalid option '" << value
                                   << "' specified for --enable-oso. Supported "
                                      "values "
                                      "are 'with-seqno-advanced'\n";
                         std::exit(EXIT_FAILURE);
                     }
                 }
             },
             "enable-oso",
             Argument::Optional,
             "with-seqno-advanced",
             "Enable 'Out-of-Sequence Order' backfills. If the optional value "
             "'with-seqno-advanced' is specified, also enable support for "
             "sending a SeqnoAdvanced message when an out of order snapshot is "
             "used and the transmitted item with the greatest seqno is not the "
             "greatest seqno of the disk snapshot."});

    options.addOption(
            {[&enableCollections](auto) { enableCollections = false; },
             "disable-collections",
             "Disable Hello::Collections negotiation (for use with pre-7.0 "
             "versions)"});

    options.addOption({[&streamRequestFileName](auto value) {
                           streamRequestFileName = value;
                       },
                       "stream-request-value",
                       Argument::Required,
                       "filename",
                       "Path to a file containing stream-request value. This "
                       "must be a file storing a JSON object matching the "
                       "stream-request value specification."});

    options.addOption(
            {[&streamIdFileName](auto value) { streamIdFileName = value; },
             "stream-id",
             Argument::Required,
             "filename",
             "Path to a file containing stream-ID config. This must be a file "
             "storing a JSON object that stores a single array of JSON "
             "objects, the JSON objects are stream-request values (with "
             "stream-ID configured). Use of this parameter enables DCP "
             "stream-id. Example: {\"streams\":[{\"collections\" : [\"0\"], "
             "\"sid\":2},{\"collections\" : [\"8\"], \"sid\":5}]}"});

    options.addOption(
            {[&streamRequestFlags](auto value) {
                 streamRequestFlags = static_cast<cb::mcbp::DcpAddStreamFlag>(
                         parse_size_string(value));
             },
             "stream-request-flags",
             Argument::Required,
             "number",
             "Value to use for the 4-byte stream-request flags field. Default "
             "value is DCP_ADD_STREAM_FLAG_TO_LATEST"});

    options.addOption(
            {[&vbuckets](auto value) { vbuckets = parseVBuckets(value); },
             "vbuckets",
             Argument::Required,
             "1,2,...",
             "Only stream from a subset of vBuckets. List vbucket as "
             "a comma-separated list of integers."});

    options.addOption({[&enableFlatbufferSysEvents](auto) {
                           enableFlatbufferSysEvents = true;
                       },
                       "enable-flatbuffer-sysevents",
                       "Turn on system events with flatbuffer values"});

    options.addOption(
            {[&enableChangeStreams](auto) { enableChangeStreams = true; },
             "enable-change-streams",
             "Turn on change-stream support"});

    options.addOption({[](auto) { hang = true; },
                       "hang",
                       "Create streams, but do not drain them."});

    options.addOption(
            {[&startInsideSnapshot](auto) { startInsideSnapshot = true; },
             "start-inside-snapshot",
             "Start the stream inside of a snapshot. The client will query "
             "vbucket-details and begin at 1/2 of each vbucket's high-seqno."});

    options.addOption({[&options](auto) { usage(options, EXIT_SUCCESS); },
                       "help",
                       "This help text"});

    try {
        options.parse(argc, argv, [&options] { usage(options, EXIT_FAILURE); });
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return EXIT_FAILURE;
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
        auto parsed = nlohmann::json::parse(cb::io::loadFile(streamIdFileName));
        // Expected that the document is an array of stream-request values
        auto itr = parsed.find("streams");
        if (itr == parsed.end()) {
            std::cerr << "stream-id content invalid:" << parsed.dump()
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
    try {
        options.assemble(event_base);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }

    if (enableCollections) {
        features.push_back(cb::mcbp::Feature::Collections);
    }

    std::vector<std::unique_ptr<DcpConnection>> connections;
    try {
        auto connection = options.getConnection();
        connection->setFeatures(features);
        connection->selectBucket(bucket);

        setupVBMap(*connection, vbuckets);

        for (const auto& [host, vb] : vbucketmap) {
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
            for (const auto& node_vbuckets : perConnVbuckets) {
                const auto streamname = fmt::format("{}:{}", name, idx++);
                auto [hostname, port, family] =
                        cb::inet::parse_hostname(host, {});
                auto conn = options.createAuthenticatedConnection(
                        hostname,
                        port,
                        family,
                        connection->isSsl(),
                        event_base);
                conn->setFeatures(features);
                conn->selectBucket(bucket);

                connections.emplace_back(std::make_unique<DcpConnection>(
                        std::move(conn), streamname, node_vbuckets));
                auto& c = connections.back()->getConnection();

                if (startInsideSnapshot) {
                    connections.back()
                            ->queryServerAndSetupStreamRequestInsideSnapshot();
                }

                auto rsp = c.execute(
                        BinprotDcpOpenCommand{streamname, dcpOpenFlag});
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
                    if (!c
                                 .execute(BinprotGenericCommand{
                                         cb::mcbp::ClientOpcode::DcpControl,
                                         std::string{
                                                 DcpControlKeys::
                                                         ConnectionBufferSize},
                                         std::to_string(buffersize)})
                                 .isSuccess()) {
                        std::cerr << "Failed to set connection buffer size to "
                                  << buffersize << std::endl;
                        std::exit(EXIT_FAILURE);
                    }
                }

                if (streamIdConfig) {
                    controls.emplace_back(
                            std::string{DcpControlKeys::EnableStreamId},
                            "true");
                }

                switch (enableOso) {
                case EnableOSO::False:
                    break;
                case EnableOSO::True:
                    controls.emplace_back(
                            std::string{
                                    DcpControlKeys::EnableOutOfOrderSnapshots},
                            "true");
                    break;
                case EnableOSO::TrueWithSeqnoAdvanced:
                    controls.emplace_back(
                            std::string{
                                    DcpControlKeys::EnableOutOfOrderSnapshots},
                            "true_with_seqno_advanced");
                    break;
                }

                if (enableFlatbufferSysEvents) {
                    controls.emplace_back(
                            std::string{
                                    DcpControlKeys::FlatBuffersSystemEvents},
                            "true");
                }

                if (enableChangeStreams) {
                    controls.emplace_back(
                            std::string{DcpControlKeys::ChangeStreams}, "true");
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
    nlohmann::json individual = nlohmann::json::array();
    for (const auto& c : connections) {
        total_bytes += c->getTotalBytesReceived();
        mutations += c->getMutations();
        mutation_bytes += c->getMutationBytes();
        snapshots += c->getSnapshots();
        oso_snapshots += c->getOsoSnapshots();
        if (connections.size() > 1) {
            individual.push_back(c->getConnectionStats());
        }
    }

    if (json) {
        nlohmann::json report = {
                {"duration_ms", duration.count()},
                {"mutations", mutations},
                {"total_bytes", total_bytes},
                {"snapshots", snapshots},
                {"oso_snapshots", oso_snapshots},
                {"overhead_bytes", total_bytes - mutation_bytes},
                {"throughput",
                 cb::calculateThroughput(total_bytes, stop - start)}};
        if (!individual.empty()) {
            report["per_connection"] = std::move(individual);
        }
        fmt::println("{}", report.dump());
    } else {
        for (const auto& entry : individual) {
            std::chrono::milliseconds dur{entry["duration_ms"].get<size_t>()};
            fmt::print(
                    "Connection took {} ms ({}) - {} mutations with a total of "
                    "{} bytes received in {} snapshots and {} oso snapshots "
                    "(overhead {} bytes, {})\n",
                    dur.count(),
                    cb::time2text(dur),
                    entry["mutations"].get<size_t>(),
                    entry["total_bytes"].get<size_t>(),
                    entry["snapshots"].get<size_t>(),
                    entry["oso_snapshots"].get<size_t>(),
                    entry["overhead_bytes"].get<size_t>(),
                    entry["throughput"].get<std::string>());
        }

        fmt::print(
                "Execution took {} ms ({}) - {} mutations with a total of {} "
                "bytes received in {} snapshots and {} oso snapshots (overhead "
                "{} bytes, {})\n",
                duration.count(),
                cb::time2text(duration),
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
