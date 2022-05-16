/*
 *     Copyright 2022-Present Couchbase, Inc.
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
#include <mcbp/codec/range_scan_continue_codec.h>

#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/unsigned_leb128.h>

#include <nlohmann/json.hpp>
#include <platform/base64.h>
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

/// When set to true we'll print out each message we see
bool verbose = false;

static void usage(std::string_view program) {
    std::cerr << "Usage: " << program << R"( [options]

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
  -n or --num-connections=num    The number of connections to use to each host
  -v or --verbose                Add more output
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  -V or --vbucket-uuid           Option only valid with --vbucket, the vbucket
                                 UUID must match this UUID.
  -S or --seqno                  Option only valid with --vbucket, the vbucket
                                 must of persisted upto this seqno.
  -X or --seqno-exists           Option only valid with --vbucket, the vbucket
                                 must still store an item for this seqno.as
  -T or --seqno-timeout-ms       Option only valid with --vbucket, the vbucket
                                 create request can wait this long for the
                                 vbucket to reach the --seqno value.
  --sample-seed                  Enable random sampling and use this as the seed
                                 (default to 0)
  --sample-size                  Enable random sampling and use this as the
                                 sample size (required if --sample-seed)
  --start                        Enable a range scan and use this as the start
                                 (required if --end)
  --end                          Enable a range scan and use this as the end
                                 (required if --start)
  -d or --document-scan          Return key+meta+value (default mode is a key
                                 scan)
  -c or --collection             Scan in this collection (default setting is to
                                 scan the default collection)
  --continue-item-limit          How many items each continue can return,
                                 default is no limit.
  --continue-time-limit          How long a continue can execute for (ms),
                                 default is no limit.
  --vbucket                      Scan only this vbucket (default is all)
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

/// The RangeScanConnection class is responsible for a single connection and
/// drives a RangeScan for each vbucket in the given vector of vbid.
class RangeScanConnection {
public:
    RangeScanConnection(const std::string& hostname,
                        std::vector<uint16_t> v,
                        std::shared_ptr<folly::EventBase> eb,
                        bool keyOnly,
                        size_t continueItemLimit,
                        std::chrono::milliseconds continueTimeLimit)
        : vbuckets(std::move(v)),
          keyOnly(keyOnly),
          continueItemLimit(continueItemLimit),
          continueTimeLimit(continueTimeLimit) {
        auto [host, port, family] = cb::inet::parse_hostname(hostname, {});
        connection = std::make_unique<MemcachedConnection>(
                host, port, family, tls, eb);
        connection->connect();
    }

    MemcachedConnection& getConnection() {
        return *connection;
    }

    void enterMessagePump(const nlohmann::json& scanConfig) {
        if (vbuckets.empty()) {
            return;
        }

        std::unique_ptr<folly::IOBuf> head;
        folly::IOBuf* tailp = nullptr;

        // Create all the RangeScans (1 per vbucket)
        for (auto vb : vbuckets) {
            totalScans++;
            BinprotRangeScanCreate rangeScanCreate(Vbid(vb), scanConfig);
            auto id = ++opaque;
            creates.emplace(id, vb);
            rangeScanCreate.setOpaque(id);
            std::vector<uint8_t> vec;
            rangeScanCreate.encode(vec);
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
        connection->getUnderlyingAsyncSocket().writeChain(
                &terminateOnErrorWriteCallback, std::move(head));
        connection->enterMessagePumpMode(
                [this](const cb::mcbp::Header& header) {
                    if (verbose) {
                        std::cout << header.toJSON(true).dump() << std::endl;
                    }
                    if (header.isRequest()) {
                        Expects(false); // no requests
                    } else {
                        handleResponse(header.getResponse());
                    }
                });
        start = std::chrono::steady_clock::now();
    }

    size_t getTotalBytesReceived() const {
        return connection->getUnderlyingAsyncSocket().getAppBytesReceived();
    }

    size_t getRecords() const {
        return records;
    }

    void reportConnectionStats() {
        const auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                      start);

        size_t total_bytes = getTotalBytesReceived();
        std::cout << "Connection took " << duration.count() << " ms - "
                  << records << " records with a total of " << total_bytes
                  << " bytes received (overhead "
                  << total_bytes - mutation_bytes << ") ("
                  << calculateThroughput(total_bytes, duration.count() / 1000)
                  << ")" << std::endl;
    }

protected:
    std::unique_ptr<MemcachedConnection> connection;

    // All of the vbuckets this connection will scan
    std::vector<uint16_t> vbuckets;

    // The range-scan-id for each vbucket
    std::unordered_map<uint16_t, cb::rangescan::Id> scans;

    // In flight opaques for each continue
    std::unordered_map<uint32_t, uint16_t> opaques;

    // In flight opaques for each create
    std::unordered_map<uint32_t, uint16_t> creates;

    // A queue of vbuckets which are ready to be continued
    std::queue<uint16_t> ready;

    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point stop;

    void handleResponse(const cb::mcbp::Response& response) {
        bool nextScan = true;
        if (creates.count(response.getOpaque()) == 1) {
            handleCreateResponse(response);
        } else {
            nextScan = handleContinueResponse(response);
        }

        if (!ready.empty() && nextScan) {
            continueNextScan();
        }
    }

    void handleCreateResponse(const cb::mcbp::Response& response) {
        if (response.getStatus() != cb::mcbp::Status::Success ||
            response.getClientOpcode() !=
                    cb::mcbp::ClientOpcode::RangeScanCreate) {
            std::cerr << "handleCreateResponse failure"
                      << response.toJSON(false).dump() << std::endl;
            std::exit(EXIT_FAILURE);
        }

        cb::rangescan::Id id;
        std::memcpy(id.data,
                    response.getValueString().data(),
                    response.getValueString().size());

        // Lookup the VB from opaque
        auto vb = creates.at(response.getOpaque());
        creates.erase(response.getOpaque());
        scans.emplace(vb, id);
        ready.push(vb);
    }

    void continueNextScan() {
        Expects(ready.size());
        auto vb = ready.front();
        ready.pop();
        auto id = ++opaque;
        opaques.emplace(id, vb);

        BinprotRangeScanContinue rangeScanContinue(
                Vbid(vb), scans[vb], continueItemLimit, continueTimeLimit);
        rangeScanContinue.setOpaque(id);
        std::vector<uint8_t> vec;
        rangeScanContinue.encode(vec);

        auto iob = folly::IOBuf::createCombined(vec.size());
        std::memcpy(iob->writableData(), vec.data(), vec.size());
        iob->append(vec.size());
        connection->getUnderlyingAsyncSocket().writeChain(
                &terminateOnErrorWriteCallback, std::move(iob));
    }

    bool handleContinueResponse(const cb::mcbp::Response& response) {
        if (response.getStatus() != cb::mcbp::Status::Success &&
            response.getStatus() != cb::mcbp::Status::RangeScanMore &&
            response.getStatus() != cb::mcbp::Status::RangeScanComplete) {
            std::cerr << "Failure of a scan " << response.toJSON(false).dump()
                      << std::endl;
            return false;
        }

        bool rv = false;

        if (response.getStatus() == cb::mcbp::Status::RangeScanMore) {
            // RangeScanMore ends a continue request, more data may be available
            auto vb = opaques.at(response.getOpaque());
            opaques.erase(response.getOpaque());
            ready.push(vb);
            rv = true; // can now try the next range-scan
        } else if (response.getStatus() ==
                   cb::mcbp::Status::RangeScanComplete) {
            // RangeScanComplete denotes the end of the scan, drop it out of
            // our tracking structures
            auto vb = opaques.at(response.getOpaque());
            opaques.erase(response.getOpaque());
            scans.erase(vb);

            // All scans done?
            if (scans.empty()) {
                connection->getUnderlyingAsyncSocket().setReadCB(nullptr);
                connection->getUnderlyingAsyncSocket().close();
                stop = std::chrono::steady_clock::now();
                rv = false;
            } else {
                rv = !ready.empty(); // try for another scan if any are left
            }
        }

        // If the response has a value, we can now split into the keys/items
        if (response.getValue().size()) {
            if (keyOnly) {
                cb::mcbp::response::RangeScanContinueKeyPayload payload(
                        response.getValueString());
                auto key = payload.next();
                while (key.data()) {
                    records++;
                    if (verbose) {
                        std::cout << "KEY:" << key << std::endl;
                    }
                    key = payload.next();
                }
            } else {
                cb::mcbp::response::RangeScanContinueValuePayload payload(
                        response.getValueString());
                auto record = payload.next();
                while (record.key.data()) {
                    records++;
                    if (verbose) {
                        std::cout << "KEY:" << record.key
                                  << " VALUE:" << record.value << std::endl;
                    }
                    record = payload.next();
                }
            }
        }

        return rv;
    }

    uint32_t opaque{1};
    size_t mutation_bytes = 0;
    size_t records = 0;
    size_t current_buffer_window = 0;
    size_t max_vbuckets = 0;
    size_t totalScans = 0;
    bool keyOnly{false};
    size_t continueItemLimit{0};
    std::chrono::milliseconds continueTimeLimit{0};
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
             cb::mcbp::Feature::JSON,
             cb::mcbp::Feature::Collections}};

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
    bool sample{false}; // default to range-scan
    std::string rStart, rEnd;
    std::string cid;
    bool value{false}; // default to key-scan

    // snapshot requirements
    bool snapshotRequirements{false};
    uint64_t vbucketUuid{0};
    uint64_t seqno{0};
    bool seqnoExists{false};
    std::chrono::milliseconds seqnoTimeout{0};

    // sampling config
    size_t seed{0};
    size_t samples{0};

    size_t num_connections = 1;

    size_t continueItemLimit = 0;
    std::chrono::milliseconds continueTimeLimit{0};
    std::optional<uint16_t> vbucketOption;

    cb::net::initialize();

    const int sampleOptionId = 1;
    const int seedOptionId = 2;
    const int startOptionId = 3;
    const int endOptionId = 4;
    const int itemLimitOptionId = 5;
    const int timeLimitOptionId = 6;
    const int vbucketOptionId = 7;

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
            {"vbucket-uuid", optional_argument, nullptr, 'V'},
            {"seqno", optional_argument, nullptr, 'S'},
            {"seqno-exists", no_argument, nullptr, 'X'},
            {"seqno-timeout-ms", optional_argument, nullptr, 'T'},
            {"sample-seed", required_argument, nullptr, seedOptionId},
            {"sample-size", required_argument, nullptr, sampleOptionId},
            {"start", required_argument, nullptr, startOptionId},
            {"end", required_argument, nullptr, endOptionId},
            {"num-connections", required_argument, nullptr, 'n'},
            {"document-scan", no_argument, nullptr, 'd'},
            {"collection", required_argument, nullptr, 'c'},
            {"continue-item-limit",
             required_argument,
             nullptr,
             itemLimitOptionId},
            {"continue-time-limit",
             required_argument,
             nullptr,
             timeLimitOptionId},
            {"vbucket", required_argument, nullptr, vbucketOptionId},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc,
                              argv,
                              "vX46dh:p:u:b:t:P:S:T:n:c:",
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
        case 'V':
            snapshotRequirements = true;
            vbucketUuid = strtoul(optarg);
            break;
        case 'S':
            snapshotRequirements = true;
            seqno = strtoul(optarg);
            break;
        case 'X':
            snapshotRequirements = true;
            seqnoExists = true;
            break;
        case 'T':
            snapshotRequirements = true;
            seqnoTimeout = std::chrono::milliseconds(strtoul(optarg));
            break;
        case sampleOptionId:
            samples = strtoul(optarg);
            sample = true;
            break;
        case seedOptionId:
            seed = strtoul(optarg);
            sample = true;
            break;
        case startOptionId:
            rStart.assign(optarg);
            rEnd = rStart + '\xFF';
            break;
        case endOptionId:
            rEnd.assign(optarg);
            break;
        case 'v':
            verbose = true;
            break;
        case 'd':
            value = true;
            break;
        case 'c':
            cid.assign(optarg);
            break;
        case 'n':
            num_connections = strtoul(optarg);
            break;
        case itemLimitOptionId:
            continueItemLimit = strtoul(optarg);
            break;
        case timeLimitOptionId:
            continueTimeLimit = std::chrono::milliseconds(strtoul(optarg));
            break;
        case vbucketOptionId:
            vbucketOption = strtoul(optarg);
            num_connections = 1;
            break;
        default:
            usage(argv[0]);
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

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE\n";
        return EXIT_FAILURE;
    }
#endif

    rStart = cb::base64::encode(rStart, false);
    rEnd = cb::base64::encode(rEnd, false);

    // Build the JSON create config
    nlohmann::json jsonConfig;
    if (sample) {
        if (!rStart.empty() || !rEnd.empty()) {
            std::cerr << "start/end cannot be set for random sample\n";
            return EXIT_FAILURE;
        }
        if (!samples) {
            std::cerr << "error: samples not set or was set to zero\n";
            return EXIT_FAILURE;
        }
        jsonConfig = {{"sampling", {{"samples", samples}, {"seed", seed}}}};
    } else {
        if (sample) {
            std::cerr << "random sample cannot be used with a range scan\n";
            return EXIT_FAILURE;
        }
        jsonConfig = {{"range", {{"start", rStart}, {"end", rEnd}}}};
    }

    if (!value) {
        jsonConfig["key_only"] = true;
    }

    if (!cid.empty()) {
        jsonConfig["collection"] = cid;
    }

    if (snapshotRequirements) {
        if (!vbucketOption) {
            std::cerr << "No vbucket specified for snapshot_requirements\n";
            return EXIT_FAILURE;
        }
        jsonConfig["snapshot_requirements"] = {};
        jsonConfig["snapshot_requirements"]["vb_uuid"] = vbucketUuid;
        jsonConfig["snapshot_requirements"]["seqno"] = seqno;
        jsonConfig["snapshot_requirements"]["seqno_exists"] = seqnoExists;
        jsonConfig["snapshot_requirements"]["timeout_ms"] =
                seqnoTimeout.count();
    }

    if (verbose) {
        std::cout << "Creating scans with the following configuration\n"
                  << jsonConfig.dump() << std::endl;
    }

    auto event_base = std::make_shared<folly::EventBase>();
    std::vector<std::unique_ptr<RangeScanConnection>> connections;
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

        setupVBMap(host, in_port, family, user, password, bucket, event_base);

        std::vector<cb::mcbp::Feature> features = {
                {cb::mcbp::Feature::MUTATION_SEQNO,
                 cb::mcbp::Feature::XATTR,
                 cb::mcbp::Feature::XERROR,
                 cb::mcbp::Feature::SNAPPY,
                 cb::mcbp::Feature::JSON,
                 cb::mcbp::Feature::Collections}};

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

            for (auto vbuckets : perConnVbuckets) {
                if (vbucketOption) {
                    // From the vbucket "map" find the requested vbucket and
                    // only connect to that one node.
                    bool found = false;
                    for (auto vbid : vbuckets) {
                        if (vbid == vbucketOption.value()) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // not found, try the next 'set'
                        continue;
                    }

                    // The requested vbucket was found, continue with
                    // just this vbucket in the vbuckets "set"
                    vbuckets.clear();
                    vbuckets.emplace_back(vbucketOption.value());
                }

                connections.emplace_back(std::make_unique<RangeScanConnection>(
                        h,
                        vbuckets,
                        event_base,
                        !value,
                        continueItemLimit,
                        continueTimeLimit));
                auto& c = connections.back()->getConnection();
                if (!user.empty()) {
                    c.authenticate(user, password, "PLAIN");
                }

                c.setFeatures(features);
                c.selectBucket(bucket);
            }
        }

        // Now that they're all set up; go ahead and tell them to enter
        // the message pump!
        for (auto& c : connections) {
            c->enterMessagePump(jsonConfig);
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
    size_t records = 0;
    for (const auto& c : connections) {
        total_bytes += c->getTotalBytesReceived();
        records += c->getRecords();
        if (connections.size() > 1) {
            c->reportConnectionStats();
        }
    }

    std::cout << "Took " << duration.count() << " ms - " << records
              << " records with a total of " << total_bytes << " ("
              << calculateThroughput(total_bytes, duration.count() / 1000)
              << ")" << std::endl;

    connections.clear();
    return EXIT_SUCCESS;
}
