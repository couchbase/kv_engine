/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/// mcbasher is a program used to generate load to a single node Couchbase
/// cluster. It was written in order to try to reproduce two different bugs:
///
///   - Connections stuck in the executors as part of authentication
///   - Crash in the Connection destructor caused by an invalid pointer
///     being stored in a std::unique_ptr inside a std::deque
///
/// As it stands right now it will fire off an operation and ignore the
/// result (except for the command used to authenticate the user and select
/// the bucket to operate on)
///
/// @todo Add support for TLS
/// @todo Add support for multinode clusters
/// @todo Add support for sending more command types
/// @todo Improve the workload (add logic to verify results, type of which ops)
/// @todo Add support for timings (clientside)
/// @todo Add support for progress monitoring (#reconnects, #ops etc)

#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/Unistd.h>
#include <getopt.h>
#include <mcbp/protocol/header.h>
#include <programs/getpass.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/terminal_color.h>
#include <iostream>
#include <random>
#include <thread>

folly::SocketAddress address;
std::string user;
std::string password;
std::string bucket;
bool doDisconnects = false;
bool pipeline = false;
bool ooo = false;
int keyspace = 100000;
int pipelineSize = 512;

static void usage() {
    std::cerr << R"(Usage: mcbasher [options]

Options:

  --host hostname[:port]   The host (with an optional port) to connect to
                           (for IPv6 use: [address]:port if you'd like to
                           specify port)
  --port port              The port number to connect to
  --bucket bucketname      The name of the bucket to operate on
  --user username          The name of the user to authenticate as
  --password password      The password to use for authentication
                           (use '-' to read from standard input, or
                           set the environment variable CB_PASSWORD)
  --no-color               Disable colors
  --threads #number        The number of threads to use (default 1)
  --connections #number    The number of connections to operate on (per thread)
  --disconnects            Add disconnects to the mix
  --pipeline               Send commands in a pipeline
  --pipeline-size          The maximum number of commands in the pipeline [512]
  --keyspace #number       The number of keys in the keyspace [100000]
  --ooo                    Enable out of order
  --help                   This help text
)";

    exit(EXIT_FAILURE);
}

class McBasherConnection : public folly::AsyncReader::ReadCallback,
                           public folly::AsyncWriter::WriteCallback,
                           public folly::AsyncSocket::ConnectCallback,
                           public folly::AsyncTimeout {
public:
    McBasherConnection(folly::EventBase& base)
        : AsyncTimeout(&base),
          backing(folly::IOBuf::CREATE, 1024),
          eventBase(base),
          generator(random_device()) {
        scheduleReconnect();
        vbmap.resize(1024);
        for (int ii = 0; ii < 1024; ++ii) {
            vbmap[ii] = Vbid(ii);
        }
    };

    ~McBasherConnection() override = default;

    /// Timeout callback API
    void timeoutExpired() noexcept override {
        std::cerr << TerminalColor::Red
                  << "INFO: Timeout expired. Authentication took too long!"
                  << TerminalColor::Reset << std::endl;
    }

    /// Connect callback API
    void connectSuccess() noexcept override {
        // Create a pipeline of SASL AUTH, Hello and Select bucket
        sendSaslAuth();
        sendHello();
        sendSelectBucket();
    }

    void connectErr(const folly::AsyncSocketException& ex) noexcept override {
        std::cerr << TerminalColor::Red << "Failed to connect! " << ex.what()
                  << TerminalColor::Reset << std::endl;
        std::cerr << "commands = " << commands << std::endl;
        exit(EXIT_FAILURE);
    }

    /// Write callback API
    void writeSuccess() noexcept override {
    }

    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override {
        if (!doDisconnects) {
            std::cerr << TerminalColor::Red << "Write error: " << ex.what()
                      << TerminalColor::Reset << std::endl;
        }
        scheduleReconnect();
    }

    /// Read callback API
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
        if (backing.tailroom() == 0) {
            // out of space in the buffer, double the buffer capacity and move
            // all current valid data to the start of the new buffer.
            backing.reserve(
                    0 /*headroom*/,
                    backing.capacity() * 2 - backing.length() /*tailroom*/);
        }

        *bufReturn = static_cast<void*>(backing.writableTail());
        *lenReturn = backing.tailroom();
    }

    void readDataAvailable(size_t len) noexcept override {
        try {
            backing.append(len);
            const auto* header = getNextFrame();
            while (header && !scheduledReconnect) {
                size_t consumed = sizeof(*header) + header->getBodylen();
                frameReceivedCallback(*header);
                // advance the head ptr, getNextFrame() will look at the next
                // unread bytes, if any.
                drain(consumed);
                header = getNextFrame();
            }
        } catch (const std::exception& exception) {
            std::cerr << TerminalColor::Red
                      << "Exception occurred while processing input data: "
                      << exception.what() << TerminalColor::Reset << std::endl;
            scheduleReconnect();
        }
    }

    void readEOF() noexcept override {
        scheduleReconnect();
    }

    void readErr(const folly::AsyncSocketException& ex) noexcept override {
        scheduleReconnect();
    }

protected:
    const cb::mcbp::Header* getNextFrame() {
        if (backing.length() < sizeof(cb::mcbp::Header)) {
            return nullptr;
        }

        auto* hdr = reinterpret_cast<const cb::mcbp::Header*>(backing.data());
        if (!hdr->isValid()) {
            throw std::runtime_error("Invalid header received!");
        }

        if (backing.length() < (sizeof(cb::mcbp::Header) + hdr->getBodylen())) {
            return nullptr;
        }
        return hdr;
    }

    /// Drain a number of bytes from the backing store
    void drain(size_t nb) {
        // data has been read, advance the start of the data ptr in the buffer
        // past the consumed data.
        backing.trimStart(nb);
        if (backing.empty()) {
            // There's now no valid data in the buffer, so the data ptr can be
            // reset to the start of the buffer without having to memmove any
            // data to the start of the buffer.
            backing.retreat(backing.headroom());
        }
    }

    void injectCommand(const BinprotCommand& cmd) {
        if (scheduledReconnect) {
            return;
        }
        ++commands;
        std::vector<uint8_t> encoded;
        cmd.encode(encoded);

        auto iob = folly::IOBuf::createCombined(encoded.size());
        std::copy(encoded.begin(), encoded.end(), iob->writableData());
        iob->append(encoded.size());
        asyncSocket->writeChain(this, std::move(iob));
    }

    void sendSaslAuth() {
        std::string challenge;
        challenge.push_back('\0');
        challenge.append(user);
        challenge.push_back('\0');
        challenge.append(password);
        BinprotSaslAuthCommand cmd;
        cmd.setChallenge(challenge);
        cmd.setMechanism("PLAIN");
        injectCommand(cmd);
        scheduleTimeout(authenticationTimeout.count());
    }

    void sendHello() {
        BinprotHelloCommand cmd(R"({"a":"mcbasher"})");
        cmd.enableFeature(cb::mcbp::Feature::XERROR, true);
        cmd.enableFeature(cb::mcbp::Feature::Tracing, true);
        cmd.enableFeature(cb::mcbp::Feature::JSON, true);
        cmd.enableFeature(cb::mcbp::Feature::MUTATION_SEQNO, true);
        if (ooo) {
            cmd.enableFeature(cb::mcbp::Feature::UnorderedExecution, true);
        }
        injectCommand(cmd);
    }

    void sendSelectBucket() {
        BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::SelectBucket, bucket};
        injectCommand(cmd);
    }

    void sendMutation(cb::mcbp::ClientOpcode op) {
        BinprotMutationCommand cmd;
        if (op == cb::mcbp::ClientOpcode::Set) {
            cmd.setMutationType(MutationType::Set);
        } else if (op == cb::mcbp::ClientOpcode::Add) {
            cmd.setMutationType(MutationType::Add);
        } else if (op == cb::mcbp::ClientOpcode::Replace) {
            cmd.setMutationType(MutationType::Replace);
        } else if (op == cb::mcbp::ClientOpcode::Append) {
            cmd.setMutationType(MutationType::Append);
        } else if (op == cb::mcbp::ClientOpcode::Prepend) {
            cmd.setMutationType(MutationType::Prepend);
        } else {
            std::cerr << TerminalColor::Red << "sendMutation: Invalid opcode "
                      << to_string(op) << ". Fix your program and try again"
                      << TerminalColor::Reset << std::endl;
            std::_Exit(EXIT_FAILURE);
        }
        cmd.setKey(getKey());
        std::vector<uint8_t> val(1);
        val[0] = '0';
        cmd.setValue(std::move(val));
        cmd.setVBucket(getVbucket());
        injectCommand(cmd);
    }

    void sendArithmetic(cb::mcbp::ClientOpcode next) {
        BinprotIncrDecrCommand cmd;
        cmd.setOp(next);
        cmd.setKey(getKey());
        cmd.setDelta(1);
        cmd.setVBucket(getVbucket());
        injectCommand(cmd);
    }

    void sendGat(cb::mcbp::ClientOpcode next) {
        BinprotGetAndTouchCommand cmd;
        cmd.setOp(next);
        cmd.setKey(getKey());
        cmd.setVBucket(getVbucket());
        injectCommand(cmd);
    }

    std::string getKey() {
        return std::to_string(distribution(generator) % keyspace);
    }

    Vbid getVbucket() {
        return vbmap[distribution(generator) % vbmap.size()];
    }

    void injectNextAction() {
        if (pipeline && !doDisconnects) {
            while (commands < pipelineSize) {
                doInjectNextAction();
            }
        } else {
            doInjectNextAction();
        }
    }

    void doInjectNextAction() {
        const auto next = cb::mcbp::ClientOpcode(distribution(generator));
        switch (next) {
            // We don't want to mess up the current connection
        case cb::mcbp::ClientOpcode::SelectBucket:
        case cb::mcbp::ClientOpcode::SaslAuth:
        case cb::mcbp::ClientOpcode::Hello:
            injectCommand(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
            break;

        case cb::mcbp::ClientOpcode::Quit:
        case cb::mcbp::ClientOpcode::Quitq:
            if (!doDisconnects) {
                injectCommand(
                        BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
                break;
            }
        case cb::mcbp::ClientOpcode::SaslListMechs:
        case cb::mcbp::ClientOpcode::Noop:
        case cb::mcbp::ClientOpcode::GetRandomKey:
        case cb::mcbp::ClientOpcode::ListBuckets:
        case cb::mcbp::ClientOpcode::GetClusterConfig:
        case cb::mcbp::ClientOpcode::CollectionsGetManifest:
            injectCommand(BinprotGenericCommand{next});
            break;

        case cb::mcbp::ClientOpcode::Get:
        case cb::mcbp::ClientOpcode::Delete:
        case cb::mcbp::ClientOpcode::GetReplica:
        case cb::mcbp::ClientOpcode::GetLocked:
            // I should probably make a better one ;)
            {
                BinprotGenericCommand cmd(next, getKey());
                cmd.setVBucket(getVbucket());
                injectCommand(cmd);
            }
            break;

        case cb::mcbp::ClientOpcode::Set:
        case cb::mcbp::ClientOpcode::Add:
        case cb::mcbp::ClientOpcode::Replace:
        case cb::mcbp::ClientOpcode::Append:
        case cb::mcbp::ClientOpcode::Prepend:
            sendMutation(next);
            break;

        case cb::mcbp::ClientOpcode::Increment:
        case cb::mcbp::ClientOpcode::Decrement:
            sendArithmetic(next);
            break;

        case cb::mcbp::ClientOpcode::Touch:
        case cb::mcbp::ClientOpcode::Gat:
            sendGat(next);
            break;

            // We'd like to add support for the following commands,
            // but we don't have that support yet so fall back to
            // call noop
        case cb::mcbp::ClientOpcode::ObserveSeqno:
        case cb::mcbp::ClientOpcode::Observe:
        case cb::mcbp::ClientOpcode::UnlockKey:
        case cb::mcbp::ClientOpcode::GetFailoverLog:
        case cb::mcbp::ClientOpcode::GetMeta:
        case cb::mcbp::ClientOpcode::SetWithMeta:
        case cb::mcbp::ClientOpcode::AddWithMeta:
        case cb::mcbp::ClientOpcode::DelWithMeta:
        case cb::mcbp::ClientOpcode::DelqWithMeta:
        case cb::mcbp::ClientOpcode::CollectionsGetID:
        case cb::mcbp::ClientOpcode::CollectionsGetScopeID:
        case cb::mcbp::ClientOpcode::SubdocGet:
        case cb::mcbp::ClientOpcode::SubdocExists:
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocDelete:
        case cb::mcbp::ClientOpcode::SubdocReplace:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
        case cb::mcbp::ClientOpcode::SubdocCounter:
        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
        case cb::mcbp::ClientOpcode::SubdocGetCount:
        case cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr:

            // We use this one to disconnect!
        case cb::mcbp::ClientOpcode::TapConnect_Unsupported:
        case cb::mcbp::ClientOpcode::TapMutation_Unsupported:
        case cb::mcbp::ClientOpcode::TapDelete_Unsupported:
        case cb::mcbp::ClientOpcode::TapFlush_Unsupported:
        case cb::mcbp::ClientOpcode::TapOpaque_Unsupported:
        case cb::mcbp::ClientOpcode::TapVbucketSet_Unsupported:
        case cb::mcbp::ClientOpcode::TapCheckpointStart_Unsupported:
        case cb::mcbp::ClientOpcode::TapCheckpointEnd_Unsupported:
            if (doDisconnects) {
                scheduleReconnect();
                break;
            }

        default: {
            BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Get, getKey());
            cmd.setVBucket(getVbucket());
            injectCommand(cmd);
        }
        }
    }

    void scheduleReconnect() {
        if (scheduledReconnect) {
            return;
        }
        scheduledReconnect = true;
        if (asyncSocket) {
            asyncSocket->setReadCB(nullptr);
        }

        eventBase.runInEventBaseThread([this]() {
            if (asyncSocket) {
                asyncSocket->closeWithReset();
            }
            asyncSocket = folly::AsyncSocket::newSocket(&eventBase);
            asyncSocket->connect(this, address);
            asyncSocket->setReadCB(this);
            backing.clear();
            scheduledReconnect = false;
            commands = 0;
        });
    }

    void frameReceivedCallback(const cb::mcbp::Header& header) {
        --commands;
        const auto& response = header.getResponse();
        const auto op = response.getClientOpcode();
        switch (op) {
        // The bootstrap logic send a pipeline with SASL Auth, Hello and
        // SelectBucket. If any of these fails it is a fatal error!
        case cb::mcbp::ClientOpcode::SelectBucket:
            injectNextAction();

        case cb::mcbp::ClientOpcode::SaslAuth:
            if (op == cb::mcbp::ClientOpcode::SaslAuth) {
                cancelTimeout();
            }
        case cb::mcbp::ClientOpcode::Hello:
            if (response.getStatus() != cb::mcbp::Status::Success) {
                std::cerr << to_string(response.getClientOpcode()) << " failed"
                          << std::endl;
                std::exit(EXIT_FAILURE);
            }
            break;
        default:
            injectNextAction();
            break;
        }
    }

    bool scheduledReconnect = false;
    int commands = 0;
    folly::IOBuf backing;
    folly::EventBase& eventBase;
    std::unique_ptr<folly::AsyncSocket, folly::DelayedDestruction::Destructor>
            asyncSocket;
    std::random_device random_device;
    std::mt19937 generator;
    std::uniform_int_distribution<uint16_t> distribution;
    std::chrono::milliseconds authenticationTimeout{std::chrono::minutes{1}};
    std::vector<Vbid> vbmap;
};

int main(int argc, char** argv) {
#ifdef WIN32
    cb::net::initialize();
#else
    setTerminalColorSupport(isatty(STDERR_FILENO) && isatty(STDOUT_FILENO));
#endif
    int cmd;
    int num_threads = 1;
    int num_connections = 1;
    std::string host{"127.0.0.1"};
    in_port_t port = 0;

    const std::vector<option> options = {
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"user", required_argument, nullptr, 'u'},
            {"password", required_argument, nullptr, 'P'},
            {"threads", required_argument, nullptr, 'T'},
            {"connections", required_argument, nullptr, 'C'},
            {"disconnects", no_argument, nullptr, 'D'},
            {"pipeline", no_argument, nullptr, 'Z'},
            {"pipeline-size", required_argument, nullptr, 's'},
            {"keyspace", required_argument, nullptr, 'S'},
            {"ooo", no_argument, nullptr, 'o'},
            {"no-color", no_argument, nullptr, 'n'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc, argv, "", options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'h':
            host.assign(optarg);
            break;
        case 'p':
            port = std::stoi(optarg);
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
        case 'n':
            setTerminalColorSupport(false);
            break;
        case 'S':
            if (optarg) { // clang-analyzer seems to think it could be null
                keyspace = std::atoi(optarg);
            }
            break;
        case 's':
            if (optarg) { // clang-analyzer seems to think it could be null
                pipelineSize = std::atoi(optarg);
            }
            break;
        case 'T':
            if (optarg) { // clang-analyzer seems to think it could be null
                num_threads = std::atoi(optarg);
            }
            break;
        case 'C':
            if (optarg) { // clang-analyzer seems to think it could be null
                num_connections = std::atoi(optarg);
            }
            break;
        case 'D':
            if (pipeline || ooo) {
                std::cerr << TerminalColor::Red
                          << "Disconnects not supported with pipeline and OoO"
                          << TerminalColor::Reset << std::endl;
                return EXIT_FAILURE;
            }
            doDisconnects = true;
            break;
        case 'Z':
            if (doDisconnects) {
                std::cerr << TerminalColor::Red
                          << "Pipeline not supported with disconnects"
                          << TerminalColor::Reset << std::endl;
                return EXIT_FAILURE;
            }
            pipeline = true;
            break;
        case 'o':
            if (doDisconnects) {
                std::cerr << TerminalColor::Red
                          << "Out of order not supported with disconnects"
                          << TerminalColor::Reset << std::endl;
                return EXIT_FAILURE;
            }
            ooo = true;
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

    if (!port) {
        port = 11210;
    }

    try {
        address = folly::SocketAddress(host, port, true);
    } catch (const std::system_error& error) {
        std::cerr << TerminalColor::Red << error.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    try {
        std::deque<std::thread> threads;

        for (int jj = 0; jj < num_threads; ++jj) {
            threads.emplace_back(std::thread{[num_connections] {
                folly::EventBase eventBase;
                std::deque<McBasherConnection> connections;
                for (int ii = 0; ii < num_connections; ++ii) {
                    connections.emplace_back(eventBase);
                }

                eventBase.loopForever();
            }});
        }

        for (auto& t : threads) {
            t.join();
        }
    } catch (const std::exception& exception) {
        std::cerr << TerminalColor::Red << "Got exception: " << exception.what()
                  << TerminalColor::Reset << std::endl;
        exit(EXIT_SUCCESS);
    }

    return EXIT_SUCCESS;
}
