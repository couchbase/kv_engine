/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "client_connection.h"
#include "client_mcbp_commands.h"

#include <cbsasl/client.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <json/syntax_validator.h>
#include <json_web_token/builder.h>
#include <mcbp/codec/dcp_snapshot_marker.h>
#include <mcbp/codec/frameinfo.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/magic.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/file_sink.h>
#include <platform/socket.h>
#include <platform/string_hex.h>
#include <xattr/utils.h>
#include <cerrno>
#include <functional>
#include <iostream>
#include <memory>
#ifndef WIN32
#include <netdb.h>
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif
#include <json_web_token/token.h>

#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

static const bool packet_dump = getenv("COUCHBASE_PACKET_DUMP") != nullptr;

folly::Synchronized<std::function<std::string(const std::string&)>, std::mutex>
        MemcachedConnection::lookupPasswordCallback;
void MemcachedConnection::setLookupUserPasswordFunction(
        std::function<std::string(const std::string&)> func) {
    *lookupPasswordCallback.lock() = std::move(func);
}

nlohmann::json MemcachedConnection::ifconfig(std::string_view command,
                                             const nlohmann::json& spec) {
    auto rsp = execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Ifconfig,
                                             std::string(command),
                                             spec.dump()});
    if (!rsp.isSuccess()) {
        throw ConnectionError(fmt::format("ifconfig({}) Failed", command), rsp);
    }
    return rsp.getDataJson();
}

/// Helper function to check if we're running in unit test mode or not
static bool is_unit_test_mode() {
    return getenv("MEMCACHED_UNIT_TESTS") != nullptr;
}

/**
 * We can't throw the exception from inside folly's event loop and we're
 * provided a "AsyncSocketException" from folly which may have different
 * origins (it would have been a ton easier to use if was based on system_error
 * with its own category).
 *
 * Map some of the network events over to std::system_error (network based)
 * so that we can deal with them in a sane way in the application code.
 * For "unknown" errors we just rethrow the folly exception.
 *
 * @param ex The exception set by folly
 */
static void handleFollyAsyncSocketException(
        const folly::AsyncSocketException& ex) {
    if (ex.getType() == folly::AsyncSocketException::END_OF_FILE) {
        throw std::system_error(
                std::make_error_code(std::errc::connection_reset), ex.what());
    }

    // I've seen cases where errno represents one of the "connection reset"
    // cases, but the type isn't set to NETWORK ERROR. To avoid getting
    // unit test failures caused by that lets handle the situation
    // explicitly
    const auto code = std::make_error_code(std::errc::connection_reset);
    if (ex.getErrno() == code.value()) {
        throw std::system_error(code, ex.what());
    }

#ifdef WIN32
    // To make it easier to write code on top of this treat
    // the windows specific WSAECONNABORTED as reset
    if (ex.getErrno() == WSAECONNABORTED || ex.getErrno() == WSAECONNRESET) {
        throw std::system_error(
                std::make_error_code(std::errc::connection_reset), ex.what());
    }
#endif

    if (ex.getType() == folly::AsyncSocketException::NETWORK_ERROR) {
        throw std::system_error(
                ex.getErrno(), std::system_category(), ex.what());
    }

    throw ex;
}

/**
 * When using folly we install a ReadCallback which gets called when
 * folly detects that it may read data from the network. Whenever the
 * callback is registered it monitors for incoming data.
 *
 * In theory we could have forced folly to read out frame by frame from
 * the underlying network by first returning a buffer which is big enough
 * to contain the header, then return a buffer which is big enough to contain
 * the rest of the packet and finally break out. We'll probably get better
 * performance by just giving it a bigger buffer break out of the read loop
 * once we have at least one full frame. It does however mean that we need
 * to check if we've got data _before_ we try to install the callback
 * and enter the loop.
 */
class AsyncReadCallback : public folly::AsyncReader::ReadCallback {
private:
    static constexpr size_t DefaultBufferSize = 8_KiB;
    static constexpr size_t DefaultChunkSize = 256;

public:
    AsyncReadCallback(folly::EventBase& base) : base(base) {
    }

    ~AsyncReadCallback() override = default;

    void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
        try {
            size_t chunk = DefaultChunkSize;
            if (read_chunk_size_callback) {
                chunk = read_chunk_size_callback(DefaultBufferSize);
            }
            auto [p, s] = input.preallocate(chunk,
                                            std::max(chunk, DefaultBufferSize));
            *bufReturn = p;
            *lenReturn = s;
        } catch (const std::bad_alloc&) {
            *lenReturn = 0;
            *bufReturn = nullptr;
            return;
        }
    }

    void readDataAvailable(size_t len) noexcept override {
        input.postallocate(len);
        input_bytes += len;
        onDataReceived();
    }

    bool isBufferMovable() noexcept override {
        return true;
    }

    size_t maxBufferSize() const override {
        if (read_chunk_size_callback) {
            return read_chunk_size_callback(DefaultChunkSize);
        }
        return DefaultBufferSize;
    }

    void readBufferAvailable(
            std::unique_ptr<folly::IOBuf> ptr) noexcept override {
        input_bytes += ptr->length();
        input.append(std::move(ptr));
        onDataReceived();
    }

    void readEOF() noexcept override {
        eof = true;
        if (!frameReceivedCallback) {
            base.terminateLoopSoon();
        }
    }

    void readErr(const folly::AsyncSocketException& ex) noexcept override {
        exception = ex;
        if (!frameReceivedCallback) {
            base.terminateLoopSoon();
        }
    }

    void onDataReceived() {
        if (frameReceivedCallback) {
            if (!scheduled_callback && isPacketAvailable(false)) {
                scheduled_callback = true;
                base.runInEventBaseThreadAlwaysEnqueue([this]() {
                    scheduled_callback = false;
                    const auto* header = getNextFrame();
                    while (header) {
                        size_t consumed =
                                sizeof(*header) + header->getBodylen();
                        frameReceivedCallback(*header);
                        drain(consumed);
                        header = getNextFrame();
                    }
                });
            }
        } else {
            try {
                if (getNextFrame()) {
                    // We have received at least one full packet
                    base.terminateLoopSoon();
                }
            } catch (const std::exception&) {
                // There is a something wrong with the packet on the stream
                // We'll throw the exception once we return from folly
                base.terminateLoopSoon();
            }
        }
    }

    const cb::mcbp::Header& getPacket() const {
        return *reinterpret_cast<const cb::mcbp::Header*>(
                input.front()->data());
    }

    bool isPacketAvailable(bool throwOnError) {
        if (input.empty() || input_bytes < sizeof(cb::mcbp::Header)) {
            return false;
        }

        try {
            // gather may throw an exception if:
            // it fails to allocate memory to a continuous segment
            // there isn't enough bytes of data available
            input.gather(sizeof(cb::mcbp::Header));
        } catch (const std::exception&) {
            return false;
        }

        if (input.front()->length() < sizeof(cb::mcbp::Header)) {
            return false;
        }

        auto& packet = getPacket();
        if (!packet.isValid()) {
            if (throwOnError) {
                throw std::runtime_error(
                        fmt::format("AsyncReadCallback::isPacketAvailable(): "
                                    "Invalid packet header detected: ({})",
                                    packet));
            }
            return true;
        }

        const auto framesize = packet.getFrame().size();
        if (input_bytes < framesize) {
            return false;
        }

        try {
            input.gather(framesize);
        } catch (const std::overflow_error&) {
            // not enough bytes available in the chain of buffers
            return false;
        } catch (const std::bad_alloc&) {
            // Failed to allocate continuous space..
            return false;
        }
        return (input.front()->length() >= framesize);
    }

    /**
     * Try to get the next memcached frame from the input buffer
     *
     * @return The next frame if available or nullptr if we don't have
     *         the full frame available
     * @throws std::runtime_error if there is a format error on the
     *                            header of the packet
     */
    const cb::mcbp::Header* getNextFrame() {
        if (!isPacketAvailable(true)) {
            return nullptr;
        }

        return &getPacket();
    }

    /// Drain a number of bytes from the backing store
    void drain(size_t nb) {
        Expects(input_bytes >= nb);
        input_bytes -= nb;
        input.trimStart(nb);
    }

    void handlePotentialNetworkException() {
        if (exception) {
            handleFollyAsyncSocketException(*exception);
        }
    }

    void setFrameReceivedCallback(
            std::function<void(const cb::mcbp::Header& header)> func) {
        frameReceivedCallback = std::move(func);
    }

    std::function<void(const cb::mcbp::Header& header)> frameReceivedCallback;

    /// A callback to determine the size of the chunk to allocate (may be used
    /// in unit tests to simulate slow network conditions)
    std::function<std::size_t(std::size_t)> read_chunk_size_callback;

    /// Set to true once we see EOF
    bool eof = false;
    /// Contains the exception if we encountered a read error
    std::optional<folly::AsyncSocketException> exception;
    /// The EventBase we're bound to so that we can jump out of the loop
    folly::EventBase& base;
    /// The memory buffer we're currently using
    folly::IOBufQueue input;
    /// The total number of bytes in our input queue
    std::size_t input_bytes = 0;

    bool scheduled_callback{false};
};

void MemcachedConnection::enterMessagePumpMode(
        std::function<void(const cb::mcbp::Header&)> messageCallback) {
    asyncReadCallback->setFrameReceivedCallback(std::move(messageCallback));
    asyncSocket->setReadCB(asyncReadCallback.get());
}

::std::ostream& operator<<(::std::ostream& os, const Frame& frame) {
    const auto magic = frame.getMagic();
    if (!is_legal(magic)) {
        return os << fmt::format("Frame(magic={0:x} payload={{{0:x}}})",
                                 frame.payload.front(),
                                 fmt::join(frame.payload, " "));
    }
    if (is_request(magic)) {
        return os << "Frame(" << frame.getRequest()->to_json(false).dump()
                  << ")";
    }
    return os << "Frame(" << frame.getResponse()->to_json(false).dump() << ")";
}

::std::ostream& operator<<(::std::ostream& os, const DocumentInfo& info) {
    return os << "id:" << info.id << " flags:" << info.flags
              << " exp:" << info.expiration
              << " datatype:" << int(info.datatype) << " cas:" << info.cas;
}

::std::ostream& operator<<(::std::ostream& os, const Document& doc) {
    os << "info:" << doc.info << " value: [" << std::hex;
    for (auto& v : doc.value) {
        os << int(v) << " ";
    }
    return os << std::dec << "]";
}

::std::ostream& operator<<(::std::ostream& os, const TlsVersion& version) {
    switch (version) {
    case TlsVersion::Any:
        return os << "Any";
    case TlsVersion::V1_2:
        return os << "TLS 1.2";
    case TlsVersion::V1_3:
        return os << "TLS 1.3";
    }
    throw std::invalid_argument("Unknown TLS version");
}

void Document::compress() {
    if (cb::mcbp::datatype::is_snappy(
                protocol_binary_datatype_t(info.datatype))) {
        throw std::invalid_argument(
                "Document::compress: Cannot compress already compressed "
                "document.");
    }

    cb::compression::Buffer buf;
    if (!deflateSnappy(value, buf)) {
        throw std::runtime_error("Failed to deflate value");
    }
    value = {buf.data(), buf.size()};
    info.datatype = cb::mcbp::Datatype(uint8_t(info.datatype) |
                                       uint8_t(cb::mcbp::Datatype::Snappy));
}

void Document::uncompress() {
    if (!cb::mcbp::datatype::is_snappy(
                protocol_binary_datatype_t(info.datatype))) {
        return;
    }

    cb::compression::Buffer buf;
    if (!inflateSnappy(value, buf)) {
        throw std::runtime_error("Failed to inflate document");
    }
    value = {buf.data(), buf.size()};

    auto in_datatype = static_cast<uint8_t>(info.datatype);
    in_datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    info.datatype = cb::mcbp::Datatype(in_datatype);
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the MemcachedConnection class
/////////////////////////////////////////////////////////////////////////
MemcachedConnection::MemcachedConnection(std::string host,
                                         in_port_t port,
                                         sa_family_t family,
                                         bool ssl,
                                         std::shared_ptr<folly::EventBase> eb)
    : host(std::move(host)),
      port(port),
      family(family),
      ssl(ssl),
      eventBase(eb ? std::move(eb) : std::make_shared<folly::EventBase>()),
      validateReceivedFrame(is_unit_test_mode()) {
    if (is_unit_test_mode()) {
        // When running in unit tests we want it to fail fast, but not
        // too fast as the CV builders are sometimes heavily loaded
        timeout = std::chrono::seconds{10};
    } else {
        // None of the command line commands we had used to have a timeout
        // specified so lets bump it to 30 minutes to make sure that
        // for instance 'mcstat connections' starts to fail on a busy
        // cluster
        timeout = std::chrono::minutes{30};
    }

    agentInfo["a"] = "cb-internal/" PRODUCT_VERSION;

    if (packet_dump) {
        packet_dump_callback =
                [](MemcachedConnection& instance, bool sending, auto packet) {
                    if (sending) {
                        std::cout << "Sending over socket "
                                  << instance.getUnderlyingAsyncSocket()
                                             .getNetworkSocket()
                                             .toFd()
                                  << std::endl;
                    } else {
                        std::cout << "Received on socket "
                                  << instance.getUnderlyingAsyncSocket()
                                             .getNetworkSocket()
                                             .toFd()
                                  << std::endl;
                    }
                    std::cerr << packet;
                };
    }
}

MemcachedConnection::~MemcachedConnection() {
    close();
}

void MemcachedConnection::close() {
    asyncSocket.reset();
    eventBase->loop();
    asyncReadCallback.reset();
}

std::atomic<size_t> MemcachedConnection::totalSocketsCreated;

SOCKET try_connect_socket(struct addrinfo* next,
                          const std::string& hostname,
                          in_port_t port) {
    SOCKET sfd = cb::net::socket(
            next->ai_family, next->ai_socktype, next->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        throw std::system_error(cb::net::get_socket_error(),
                                std::system_category(),
                                "socket() failed (" + hostname + " " +
                                        std::to_string(port) + ")");
    }

    // When running unit tests on our Windows CV system we somtimes
    // see connect fail with WSAEADDRINUSE. For a client socket
    // we don't bind the socket as that's implicit from calling
    // connect. Mark the socket reusable so that the kernel may
    // reuse the socket earlier
    constexpr int flag = 1;
    cb::net::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));

    // Try to set the nodelay mode on the socket (but ignore
    // if we fail to do so..
    cb::net::setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    if (cb::net::connect(sfd, next->ai_addr, next->ai_addrlen) ==
        SOCKET_ERROR) {
        auto error = cb::net::get_socket_error();
        cb::net::closesocket(sfd);
#ifdef WIN32
        WSASetLastError(error);
#endif
        throw std::system_error(error,
                                std::system_category(),
                                "connect() failed (" + hostname + " " +
                                        std::to_string(port) + ")");
    }

    MemcachedConnection::totalSocketsCreated++;
    // Socket is connected and ready to use
    return sfd;
}

static SOCKET new_socket(const std::string& host,
                         in_port_t port,
                         sa_family_t family) {
    struct addrinfo hints = {};
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = family;

    struct addrinfo* ai;
    std::string hostname{host};

    if (hostname.empty() || hostname == "localhost") {
        if (family == AF_INET) {
            hostname.assign("127.0.0.1");
        } else if (family == AF_INET6) {
            hostname.assign("::1");
        } else if (family == AF_UNSPEC) {
            hostname.assign("localhost");
        }
    }

    const auto error = getaddrinfo(
            hostname.c_str(), std::to_string(port).c_str(), &hints, &ai);

    if (error != 0) {
        throw std::system_error(
                error,
                std::system_category(),
                fmt::format("Failed to resolve address host: \"{}\" Port: {}",
                            hostname,
                            port));
    }

    // Iterate over all entries returned by getaddrinfo
    // and try to connect to them. Depending on the input data we
    // might get multiple returns (ex: localhost with AF_UNSPEC returns
    // both IPv4 and IPv6 address, and IPv4 could fail while IPv6
    // might succeed.
    for (auto* next = ai; next; next = next->ai_next) {
        int retry = is_unit_test_mode() ? 200 : 0;
        do {
            try {
                auto sfd = try_connect_socket(next, hostname, port);
                freeaddrinfo(ai);
                return sfd;
            } catch (const std::system_error& exception) {
                if (is_unit_test_mode()) {
                    std::cerr << "Failed building socket: " << exception.what()
                              << std::endl;
#ifndef WIN32
                    constexpr auto WSAEADDRINUSE = EADDRINUSE;
#endif
                    if (exception.code().value() == WSAEADDRINUSE) {
                        std::cerr << "EADDRINUSE.. backing off" << std::endl;
                        std::this_thread::sleep_for(
                                std::chrono::milliseconds(10));
                    } else {
                        // Not subject for backoff and retry
                        retry = 0;
                    }
                }
            }
        } while (retry-- > 0);
        // Try next entry returned from getaddinfo
    }

    freeaddrinfo(ai);
    return INVALID_SOCKET;
}

SOCKET MemcachedConnection::releaseSocket() {
    if (ssl && !is_unit_test_mode()) {
        throw std::runtime_error(
                "MemcachedConnection::releaseSocket: Can't release SSL socket");
    }

    if (!asyncSocket) {
        throw std::runtime_error(
                "MemcachedConnection::releaseSocket: Socket not connected");
    }

    auto ns = asyncSocket->detachNetworkSocket();

#ifdef WIN32
    // For some reason toFd doesn't return the real socket (or at least
    // the unit tests failed with "not a socket" when trying to use it for
    // ioctlsocket so we need to use ns.data instead (which is a SOCKET))
    cb::net::set_socket_blocking(ns.data);
    return ns.data;
#else
    auto ret = ns.toFd();
    cb::net::set_socket_blocking(ret);
    return ret;
#endif
}

intptr_t MemcachedConnection::getServerConnectionId() {
    auto st = stats("connections self");
    if (st.size() != 1) {
        throw std::runtime_error(
                "MemcachedConnection::getServerConnectionId: Unexpected stats "
                "size returned");
    }

    return st.front()["socket"].get<size_t>();
}

uint64_t tls_protocol_to_options(TlsVersion protocol) {
    uint64_t disallow = SSL_OP_NO_SSL_MASK | SSL_OP_NO_RENEGOTIATION;
    switch (protocol) {
    case TlsVersion::Any:
        disallow &= ~(SSL_OP_NO_TLSv1_2 | SSL_OP_NO_TLSv1_3);
        break;
    case TlsVersion::V1_2:
        disallow &= ~SSL_OP_NO_TLSv1_2;
        break;
    case TlsVersion::V1_3:
        disallow &= ~SSL_OP_NO_TLSv1_3;
        break;
    }

    return disallow;
}

static int my_pem_password_cb(char* buf, int size, int, void* userdata) {
    if (!userdata) {
        throw std::logic_error("my_pem_password_cb called without userdata");
    }

    auto instance = static_cast<MemcachedConnection*>(userdata);
    const auto passphrase = instance->getPemPassphrase();
    if (passphrase.size() > std::size_t(size)) {
        std::cerr << "my_pem_password_cb: passphrase too long" << std::endl;
        return 0;
    }

    std::ranges::copy(passphrase, buf);
    return passphrase.size();
}

void MemcachedConnection::connect() {
    if (asyncSocket) {
        // drop the previous one
        asyncSocket.reset();
    }

    auto sock = new_socket(host, port, family);
    if (sock == INVALID_SOCKET) {
        auto error = cb::net::get_socket_error();
        std::string msg("Failed to connect to: ");
        if (family == AF_INET || family == AF_UNSPEC) {
            if (host.empty()) {
                msg += "localhost:";
            } else {
                msg += host + ":";
            }
        } else {
            if (host.empty()) {
                msg += "[::1]:";
            } else {
                msg += "[" + host + "]:";
            }
        }
        msg.append(std::to_string(port));
        throw std::system_error(error, std::system_category(), msg);
    }

    if (cb::net::set_socket_noblocking(sock) == -1) {
        throw std::runtime_error("Failed to make socket nonblocking");
    }

    if (ssl) {
        auto* context = SSL_CTX_new(SSLv23_client_method());
        if (context == nullptr) {
            throw std::runtime_error("Failed to create openssl client context");
        }

        // Ensure read/write operations only return after the
        // handshake and successful completion.
        SSL_CTX_set_mode(context, SSL_MODE_AUTO_RETRY);
        SSL_CTX_set_options(context, tls_protocol_to_options(tls_protocol));

        if (SSL_CTX_set_ciphersuites(context, tls13_ciphers.c_str()) == 0 &&
            !tls13_ciphers.empty()) {
            throw std::runtime_error("Failed to select a cipher suite from: " +
                                     tls13_ciphers);
        }

        if (SSL_CTX_set_cipher_list(context, tls12_ciphers.c_str()) == 0 &&
            !tls12_ciphers.empty()) {
            throw std::runtime_error("Failed to select a cipher suite from: " +
                                     tls12_ciphers);
        }

        if (ssl_cert_file && ssl_key_file) {
            SSL_CTX_set_default_passwd_cb(context, my_pem_password_cb);
            SSL_CTX_set_default_passwd_cb_userdata(context, this);
            if (!SSL_CTX_use_certificate_chain_file(
                        context, ssl_cert_file->generic_string().c_str()) ||
                !SSL_CTX_use_PrivateKey_file(
                        context,
                        ssl_key_file->generic_string().c_str(),
                        SSL_FILETYPE_PEM) ||
                !SSL_CTX_check_private_key(context)) {
                std::vector<char> ssl_err(1024);
                ERR_error_string_n(
                        ERR_get_error(), ssl_err.data(), ssl_err.size());
                SSL_CTX_free(context);
                throw std::runtime_error(
                        std::string("Failed to use SSL cert and "
                                    "key: ") +
                        ssl_err.data());
            }
        }

        if (ca_file &&
            !SSL_CTX_load_verify_locations(
                    context, ca_file->generic_string().c_str(), nullptr)) {
            std::vector<char> ssl_err(1024);
            ERR_error_string_n(ERR_get_error(), ssl_err.data(), ssl_err.size());
            SSL_CTX_free(context);
            throw std::runtime_error(std::string("Failed to use CA file: ") +
                                     ssl_err.data());
        }

        auto ctx = std::make_shared<folly::SSLContext>(context);
        SSL_CTX_free(context);

        auto ss = folly::AsyncSSLSocket::newSocket(
                ctx, eventBase.get(), folly::NetworkSocket(sock), false);

        class HandshakeHandler : public folly::AsyncSSLSocket::HandshakeCB {
        public:
            ~HandshakeHandler() override = default;
            void handshakeSuc(folly::AsyncSSLSocket* sock) noexcept override {
            }
            void handshakeErr(
                    folly::AsyncSSLSocket* sock,
                    const folly::AsyncSocketException& ex) noexcept override {
                error = ex.what();
            }
            std::string error;
        } handler;

        ss->sslConn(&handler);
        asyncSocket = std::move(ss);
        eventBase->loop();
        if (!handler.error.empty()) {
            throw std::runtime_error("SSL handshake failed: " + handler.error);
        }
    } else {
        asyncSocket = folly::AsyncSocket::newSocket(eventBase.get(),
                                                    folly::NetworkSocket(sock));
    }

    if (!asyncSocket) {
        cb::net::closesocket(sock);
        throw std::runtime_error("Failed to create folly async socket");
    }

    if (!ssl && is_unit_test_mode()) {
        // Enable LINGER with zero timeout. This changes the
        // behaviour of close() - any unsent data will be
        // discarded, and the connection will be immediately
        // closed with a RST, and is immediately destroyed.  This
        // has the advantage that the socket doesn't enter
        // TIME_WAIT; and hence doesn't consume an emphemeral port
        // until it times out (default 60s).
        //
        // By using LINGER we (hopefully!) avoid issues in CV jobs
        // where ephemeral ports are exhausted and hence tests
        // intermittently fail. One minor downside the RST
        // triggers a warning in the server side logs: 'read
        // error: Connection reset by peer'.
        //
        // Note that this isn't enabled for SSL sockets, which don't
        // appear to be happy with having the underlying socket closed
        // immediately; I suspect due to the additional out-of-band
        // messages SSL may send/recv in addition to normal traffic.
        linger sl{};
        sl.l_onoff = 1;
        sl.l_linger = 0;
        asyncSocket->setSockOpt<linger>(SOL_SOCKET, SO_LINGER, &sl);
    }

    asyncReadCallback = std::make_unique<AsyncReadCallback>(*eventBase);
}

void MemcachedConnection::setReadChunkSizeCallback(
        std::function<std::size_t(std::size_t)> callback) {
    asyncReadCallback->read_chunk_size_callback = std::move(callback);
}

/// Terminate the loop once we've sent all of the data
class WriteCallback : public folly::AsyncWriter::WriteCallback {
public:
    WriteCallback(folly::EventBase& base) : base(base) {
    }
    ~WriteCallback() override = default;
    void writeSuccess() noexcept override {
        base.terminateLoopSoon();
    }
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override {
        exception = ex;
        base.terminateLoopSoon();
    }

    void handlePotentialNetworkException() {
        if (exception) {
            handleFollyAsyncSocketException(*exception);
        }
    }

    folly::EventBase& base;
    std::optional<folly::AsyncSocketException> exception;
};

void MemcachedConnection::sendBuffer(const std::vector<iovec>& list) {
    if (packet_dump_callback) {
        for (const auto& entry : list) {
            sendBuffer({reinterpret_cast<const uint8_t*>(entry.iov_base),
                        entry.iov_len});
        }
    } else {
        WriteCallback writeCallback(*eventBase);
        asyncSocket->writev(&writeCallback, list.data(), list.size());
        if (!eventBase->loop()) {
            throw std::runtime_error(
                    "MemcachedConnection::sendBuffer(): Failed running the "
                    "event "
                    "pump");
        }
        writeCallback.handlePotentialNetworkException();
    }
}

void MemcachedConnection::sendBuffer(cb::const_byte_buffer buf) {
    if (packet_dump_callback) {
        try {
            std::stringstream ss;
            cb::mcbp::dumpStream(buf, ss);
            packet_dump_callback(*this, true, ss.str());
        } catch (const std::exception&) {
            // ignore..
        }
    }

    WriteCallback writeCallback(*eventBase);
    asyncSocket->write(&writeCallback, buf.data(), buf.size());
    // Running loop here will terminate once we've sent the data..
    if (!eventBase->loop()) {
        throw std::runtime_error(
                "MemcachedConnection::sendBuffer(): Failed running the event "
                "pump");
    }
    writeCallback.handlePotentialNetworkException();
}

void MemcachedConnection::sendPartialFrame(Frame& frame,
                                           Frame::size_type length) {
    // Move the remainder to a new frame.
    auto rem_first = frame.payload.begin() + length;
    auto rem_last = frame.payload.end();
    std::vector<uint8_t> remainder;
    std::copy(rem_first, rem_last, std::back_inserter(remainder));
    frame.payload.erase(rem_first, rem_last);

    // Send the partial frame.
    sendFrame(frame);

    // Swap the old payload with the remainder.
    frame.payload.swap(remainder);
}

nlohmann::json MemcachedConnection::stats(
        const std::string& subcommand,
        const GetFrameInfoFunction& getFrameInfo) {
    nlohmann::json ret;
    stats(
            [&ret](const std::string& key, const std::string& value) -> void {
                if (value.empty()) {
                    ret[key] = "";
                    return;
                }
                try {
                    auto v = nlohmann::json::parse(value);
                    ret[key] = v;
                } catch (const nlohmann::json::exception&) {
                    ret[key] = value;
                }
            },
            subcommand,
            {},
            getFrameInfo);
    return ret;
}

void MemcachedConnection::setTlsConfigFiles(
        std::filesystem::path cert,
        std::filesystem::path key,
        std::optional<std::filesystem::path> castore) {
    auto validate = [](auto file, auto description) {
        if (!exists(file)) {
            throw std::system_error(
                    std::make_error_code(std::errc::no_such_file_or_directory),
                    fmt::format("Can't use [{}] as {} file",
                                file.generic_string(),
                                description));
        }
    };

    validate(cert, "certificate");
    validate(key, "key");
    if (castore) {
        validate(*castore, "CA store");
    }

    ssl_cert_file = std::move(cert);
    ssl_key_file = std::move(key);
    ca_file = std::move(castore);
}

void MemcachedConnection::setTlsProtocol(TlsVersion protocol) {
    tls_protocol = protocol;
}

void MemcachedConnection::setTls12Ciphers(std::string ciphers) {
    tls12_ciphers = std::move(ciphers);
}

void MemcachedConnection::setTls13Ciphers(std::string ciphers) {
    tls13_ciphers = std::move(ciphers);
}

static Frame to_frame(const BinprotCommand& command) {
    Frame frame;
    command.encode(frame.payload);
    return frame;
}

std::unique_ptr<MemcachedConnection> MemcachedConnection::clone(
        bool connect,
        const std::vector<cb::mcbp::Feature>& features,
        std::string agent_name) const {
    auto ret = std::make_unique<MemcachedConnection>(host, port, family, ssl);
    ret->auto_retry_tmpfail = auto_retry_tmpfail;
    ret->auto_retry_enomem = auto_retry_enomem;
    ret->ssl_cert_file = ssl_cert_file;
    ret->ssl_key_file = ssl_key_file;
    ret->ca_file = ca_file;
    ret->tls_protocol = tls_protocol;
    ret->tls12_ciphers = tls12_ciphers;
    ret->tls13_ciphers = tls13_ciphers;
    ret->packet_dump_callback = packet_dump_callback;
    ret->userValidateReceivedFrameCallback = userValidateReceivedFrameCallback;
    if (!agent_name.empty()) {
        ret->setAgentName(std::move(agent_name));
    }
    if (tokenBuilder) {
        ret->tokenBuilder = tokenBuilder->clone();
    }

    if (connect) {
        ret->connect();
        if (features.empty()) {
            ret->applyFeatures(effective_features);
        } else {
            Featureset featureset = effective_features;
            for (const auto& f : features) {
                featureset.insert(static_cast<uint16_t>(f));
            }
            ret->applyFeatures(featureset);
        }
    } else {
        if (!features.empty()) {
            throw std::logic_error(
                    "MemcachedConnection::clone: features can only be "
                    "specified if connect is set to true");
        }
        ret->effective_features.clear();
    }
    return ret;
}

void MemcachedConnection::doValidateReceivedFrame(
        const cb::mcbp::Header& packet) {
    using namespace cb::mcbp;
    using namespace cb::mcbp::datatype;

    if (!packet.isValid()) {
        throw std::runtime_error(fmt::format(
                "MemcachedConnection::doValidateReceivedFrame: Invalid frame "
                "received: {}",
                cb::to_hex({reinterpret_cast<const uint8_t*>(&packet),
                            sizeof(packet)})));
    }

    if (userValidateReceivedFrameCallback) {
        userValidateReceivedFrameCallback(packet);
    }

    const auto magic = Magic(packet.getMagic());
    if (is_server_magic(magic) || is_request(magic)) {
        return;
    }
    auto& response = packet.getResponse();

    std::string backing;
    std::string_view payload = packet.getValueString();

    if (is_snappy(packet.getDatatype())) {
        if (!hasFeature(Feature::SNAPPY) &&
            !hasFeature(Feature::SnappyEverywhere)) {
            throw std::runtime_error(fmt::format(
                    "Received package with Snappy datatype, but that's "
                    "not enabled. packet: {}",
                    packet.to_json(false).dump()));
        }
        using namespace cb::compression;
        Buffer output;
        if (!inflateSnappy(payload, output)) {
            throw std::runtime_error(fmt::format(
                    "Received snappy compressed frame we failed to inflate: {}",
                    packet.to_json(false).dump()));
        }
        backing = std::string{output.data(), output.size()};
        payload = backing;
    }

    const bool xattr = is_xattr(packet.getDatatype());
    if (xattr) {
        if (!hasFeature(Feature::XATTR)) {
            throw std::runtime_error(fmt::format(
                    "Received package with XATTR datatype, but that's "
                    "not enabled. packet: {}",
                    packet.to_json(false).dump()));
        }
        payload = cb::xattr::get_body(payload);
    }

    if (!jsonValidator) {
        jsonValidator = cb::json::SyntaxValidator::New();
    }

    const auto json = jsonValidator->validate(payload);
    if (hasFeature(Feature::JSON)) {
        const auto opcode = response.getClientOpcode();
        if (json) {
            if (!is_json(packet.getDatatype())) {
                if (opcode != ClientOpcode::Stat &&
                    opcode != ClientOpcode::DcpControl) {
                    // Stat and DcpControl send a bunch of kv-pair which might
                    // be parsable as JSON (true/false/numbers). The packet
                    // validator for DcpControl enforce that the datatype must
                    // be Raw, so we can't really start setting the correct
                    // value unless breaking communication with an old
                    // server. It's easier to ignore those two commands for
                    // now
                    throw std::runtime_error(
                            fmt::format("Received package with JSON content "
                                        "without JSON datatype set. Packet: {}",
                                        packet.to_json(false).dump()));
                }
            }
        } else if (is_json(packet.getDatatype()) &&
                   opcode != ClientOpcode::ReturnMeta) {
            // ReturnMeta returns the datatype as part of the response
            // without the value for some reason
            throw std::runtime_error(
                    fmt::format("Received package with JSON datatype set, "
                                "but the content isn't JSON. Packet: {}",
                                packet.to_json(false).dump()));
        }
    } else if (is_json(packet.getDatatype())) {
        throw std::runtime_error(
                fmt::format("Received package with JSON datatype, but that's "
                            "not enabled. Packet: {}",
                            packet.to_json(false).dump()));
    }

    if (response.getClientOpcode() == ClientOpcode::SubdocMultiLookup &&
        response.getStatus() == Status::SubdocMultiPathFailure &&
        response.getCas() == cas::Wildcard) {
        throw std::runtime_error(
                fmt::format("Rececived SubdocMultiPathFailure response error "
                            "CAS == 0. Packet: {}",
                            packet.to_json(false).dump()));
    }
}

void MemcachedConnection::recvFrame(Frame& frame,
                                    cb::mcbp::ClientOpcode opcode,
                                    std::chrono::milliseconds readTimeout) {
    frame.reset();

    if (!asyncReadCallback) {
        throw std::runtime_error(
                "MemcachedConnection::recvFrame(): Not connected");
    }

    if (!asyncReadCallback->getNextFrame()) {
        if (asyncReadCallback->eof) {
            throw std::system_error(
                    std::make_error_code(std::errc::connection_reset), "EOF");
        }
        asyncReadCallback->handlePotentialNetworkException();

        // Create a timeout
        class Timeout : public folly::AsyncTimeout {
        public:
            Timeout(folly::EventBase& base) : AsyncTimeout(&base), base(base) {
            }

            void timeoutExpired() noexcept override {
                base.terminateLoopSoon();
                fired = true;
            }

            folly::EventBase& base;
            bool fired{false};
        } tmo(*eventBase);

        auto timeoutvalue = readTimeout;
        if (readTimeout.count() == 0) {
            // none specified, use default
            tmo.scheduleTimeout(timeout.count(), {});
            timeoutvalue = timeout;
        } else {
            tmo.scheduleTimeout(readTimeout.count(), {});
        }
        asyncSocket->setReadCB(asyncReadCallback.get());
        if (!eventBase->loop()) {
            tmo.cancelTimeout();
            throw std::runtime_error(
                    "MemcachedConnection::recvFrame(): Failed running the "
                    "event loop");
        }
        asyncSocket->setReadCB(nullptr);
        tmo.cancelTimeout();
        if (tmo.fired) {
            if (opcode == cb::mcbp::ClientOpcode::Invalid) {
                throw TimeoutException(
                        "MemcachedConnection::recvFrame(): Timed out after "
                        "waiting " +
                                std::to_string(timeoutvalue.count()) +
                                "ms for a response",
                        opcode,
                        readTimeout);
            }
            throw TimeoutException(
                    "MemcachedConnection::recvFrame(): Timed out after "
                    "waiting " +
                            std::to_string(timeoutvalue.count()) +
                            "ms for a response for " + ::to_string(opcode),
                    opcode,
                    readTimeout);
        }
    }

    const auto* next = asyncReadCallback->getNextFrame();
    if (next == nullptr) {
        if (asyncReadCallback->eof) {
            throw std::system_error(
                    std::make_error_code(std::errc::connection_reset), "EOF");
        }
        asyncReadCallback->handlePotentialNetworkException();
        throw std::runtime_error(
                "MemcachedConnection::recvFrame: Failed to fetch next frame");
    }

    if (validateReceivedFrame) {
        doValidateReceivedFrame(*next);
    }

    auto blob = next->getFrame();
    std::ranges::copy(blob, std::back_inserter(frame.payload));
    asyncReadCallback->drain(blob.size());

    if (packet_dump_callback) {
        std::stringstream ss;
        cb::mcbp::dump(frame.payload.data(), ss);
        packet_dump_callback(*this, false, ss.str());
    }
}

size_t MemcachedConnection::sendCommand(const BinprotCommand& command) {
    traceData.reset();

    auto encoded = command.encode();

    // encoded contains the message header (as owning vector<uint8_t>),
    // plus a variable number of (non-owning) byte buffers. Create
    // a single vector of byte buffers for all; then send in a single
    // sendmsg() call (to avoid copying any data), with a single syscall.

    // Perf: this function previously used multiple calls to
    // sendBuffer() (one per header / buffer) to send the data without
    // copying / re-forming it. While this does reduce copying cost; it requires
    // one send() syscall per chunk. Benchmarks show that is actually
    // *more* expensive overall (particulary when measuring server
    // performance) as the server can read the first header chunk;
    // then attempts to read the body which hasn't been delievered yet
    // and hence has to go around the libevent loop again to read the
    // body.

    std::vector<iovec> message;
    iovec iov{};
    iov.iov_base = encoded.header.data();
    iov.iov_len = encoded.header.size();
    size_t sentBytes = iov.iov_len;
    message.push_back(iov);
    for (auto buf : encoded.bufs) {
        iov.iov_base = const_cast<uint8_t*>(buf.data());
        iov.iov_len = buf.size();
        sentBytes += iov.iov_len;
        message.push_back(iov);
    }

    sendBuffer(message);
    return sentBytes;
}

void MemcachedConnection::recvResponse(BinprotResponse& response,
                                       cb::mcbp::ClientOpcode opcode,
                                       std::chrono::milliseconds readTimeout) {
    Frame frame;
    traceData.reset();
    recvFrame(frame, opcode, readTimeout);
    if (!cb::mcbp::is_response(frame.getMagic())) {
        throw std::logic_error(fmt::format(
                "{}: Expected response frame, got {}", __func__, frame));
    }

    if (opcode != cb::mcbp::ClientOpcode::Invalid &&
        frame.getResponse()->getClientOpcode() != opcode) {
        throw std::logic_error(
                fmt::format("{}: Expected opcode {} from response, got {}",
                            __func__,
                            opcode,
                            frame.getResponse()->getClientOpcode()));
    }

    response.assign(std::move(frame.payload));
    traceData = response.getTracingData();
}

void MemcachedConnection::setTokenBuilder(
        std::unique_ptr<cb::jwt::Builder> builder) {
    if (!ssl) {
        throw std::logic_error(
                "MemcachedConnection::setTokenBuilder: "
                "SSL must be enabled to use JWT");
    }
    tokenBuilder = std::move(builder);
}

void MemcachedConnection::authenticateWithToken() {
    auto token = tokenBuilder->build();

    auto parsed = cb::jwt::Token::parse(tokenBuilder->build());
    if (!parsed->payload.contains("sub")) {
        throw std::logic_error(
                "MemcachedConnection::authenticateWithToken: "
                "The token must contain a sub field");
    }

    doSaslAuthenticate(parsed->payload["sub"], token, "OAUTHBEARER");
}

void MemcachedConnection::authenticate(
        const std::string& user,
        const std::optional<std::string>& password,
        const std::string& mech) {
    std::string pw;
    if (password) {
        pw = *password;
    } else {
        pw = lookupPasswordCallback.withLock([&user](auto& cb) -> std::string {
            if (cb) {
                return cb(user);
            }
            return {};
        });
    }

    doSaslAuthenticate(user, pw, mech);
}

void MemcachedConnection::doSaslAuthenticate(const std::string& username,
                                             const std::string& password,
                                             const std::string& mech) {
    cb::sasl::client::ClientContext client(
            [username]() -> std::string { return username; },
            [password]() -> std::string { return password; },
            mech,
            {},
            scram_property_listener);
    auto client_data = client.start();

    if (client_data.first != cb::sasl::Error::OK) {
        throw std::runtime_error(fmt::format("cbsasl_client_start ({}): {}",
                                             client.getName(),
                                             client_data.first));
    }

    BinprotSaslAuthCommand authCommand;
    authCommand.setChallenge(client_data.second);
    authCommand.setMechanism(client.getName());
    auto response = execute(authCommand);

    while (response.getStatus() == cb::mcbp::Status::AuthContinue) {
        client_data = client.step(response.getDataView());
        if (client_data.first != cb::sasl::Error::OK &&
            client_data.first != cb::sasl::Error::CONTINUE) {
            reconnect();
            throw std::runtime_error(fmt::format("cbsasl_client_step ({}): {}",
                                                 client.getName(),
                                                 client_data.first));
        }

        BinprotSaslStepCommand stepCommand;
        stepCommand.setMechanism(client.getName());
        stepCommand.setChallenge(client_data.second);
        response = execute(stepCommand);
    }

    if (response.isSuccess()) {
        const auto challenge = response.getDataView();
        if (!challenge.empty()) {
            auto [e, c] = client.step(challenge);
            if (e != cb::sasl::Error::OK) {
                throw std::runtime_error(
                        fmt::format("Authentication failed as part of final "
                                    "verification: Error:{} Reason:{}",
                                    e,
                                    c));
            }
        }
    }

    if (!response.isSuccess()) {
        throw ConnectionError("Authentication failed", response);
    }
}

static std::string bucketTypeToModule(BucketType type) {
    switch (type) {
    case BucketType::Unknown:
        throw std::runtime_error(
                "bucketTypeToModule: Can't create an unknown bucket type");
    case BucketType::NoBucket:
        throw std::runtime_error("NoBucket can't be created from clients");
    case BucketType::Couchbase:
        return "ep.so";
    case BucketType::ClusterConfigOnly:
        throw std::runtime_error(
                "ClusterConfigOnly bucket should be created with set "
                "clustermap");
    case BucketType::EWouldBlock:
        return "ewouldblock_engine.so";
    }

    throw std::runtime_error("bucketTypeToModule: Invalid BucketType: " +
                             std::to_string(int(type)));
}

void MemcachedConnection::createBucket(const std::string& bucketName,
                                       const std::string& config,
                                       BucketType type) {
    BinprotCreateBucketCommand command(
            bucketName, bucketTypeToModule(type), config);

    const auto response = execute(
            command, std::max(std::chrono::milliseconds{20000}, timeout));
    if (!response.isSuccess()) {
        throw ConnectionError("Create bucket failed", response);
    }
}

void MemcachedConnection::deleteBucket(const std::string& bucketName) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::DeleteBucket,
                                  bucketName);
    const auto response = execute(
            command, std::max(std::chrono::milliseconds{20000}, timeout));
    if (!response.isSuccess()) {
        throw ConnectionError("Delete bucket failed", response);
    }
}

void MemcachedConnection::selectBucket(std::string_view bucketName) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::SelectBucket,
                                  std::string{bucketName});
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError(
                fmt::format("Select bucket [{}] failed", bucketName), response);
    }
}

void MemcachedConnection::executeInBucket(
        const std::string& bucket,
        const std::function<void(MemcachedConnection&)>& func) {
    auto scopeGuard = folly::makeGuard([this] {
        if (asyncSocket->good()) {
            unselectBucket();
        }
    });
    selectBucket(bucket);
    func(*this);
}

std::string MemcachedConnection::to_string() const {
    std::string ret("Memcached connection ");
    if (family == AF_INET6) {
        ret.append("[::1]:");
    } else {
        ret.append("127.0.0.1:");
    }

    ret.append(std::to_string(port));

    if (ssl) {
        ret.append(" ssl");
    }

    return ret;
}

std::vector<std::string> MemcachedConnection::listBuckets(
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::ListBuckets);
    applyFrameInfos(command, getFrameInfo);
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("List bucket failed", response);
    }

    std::vector<std::string> ret;

    // the value contains a list of bucket names separated by space.
    std::istringstream iss(std::string{response.getDataView()});
    std::copy(std::istream_iterator<std::string>(iss),
              std::istream_iterator<std::string>(),
              std::back_inserter(ret));

    return ret;
}

Document MemcachedConnection::get(const std::string& id,
                                  Vbid vbucket,
                                  const GetFrameInfoFunction& getFrameInfo) {
    BinprotGetCommand command{id, vbucket};
    applyFrameInfos(command, getFrameInfo);

    const auto response = BinprotGetResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to get: " + id, response);
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = id;
    ret.info.datatype = response.getResponse().getDatatype();
    ret.value = response.getDataView();
    return ret;
}

void MemcachedConnection::mget(
        const std::vector<std::pair<const std::string, Vbid>>& id,
        std::function<void(std::unique_ptr<Document>&)> documentCallback,
        std::function<void(const std::string&, const cb::mcbp::Response&)>
                errorCallback,
        const GetFrameInfoFunction& getFrameInfo) {
    using cb::mcbp::ClientOpcode;

    // One of the motivations for this method is to be able to test a
    // pipeline of commands (to get them reordered on the server if OoO
    // is enabled). Sending each command as an individual packet may
    // cause the server to completely execute the command before it goes
    // back into the read state and sees the next command.
    std::vector<uint8_t> pipeline;

    int ii = 0;
    for (const auto& doc : id) {
        BinprotGetCommand command{doc.first, doc.second};
        command.setOpaque(ii++);
        applyFrameInfos(command, getFrameInfo);

        std::vector<uint8_t> cmd;
        command.encode(cmd);
        std::ranges::copy(cmd, std::back_inserter(pipeline));
    }

    // Add a noop command to terminate the sequence
    {
        BinprotGenericCommand command{ClientOpcode::Noop};
        std::vector<uint8_t> cmd;
        command.encode(cmd);
        std::ranges::copy(cmd, std::back_inserter(pipeline));
    }

    // Now send the pipeline to the other end!
    sendBuffer(cb::const_byte_buffer{pipeline.data(), pipeline.size()});

    // read until I see the noop response
    auto done = false;
    do {
        BinprotResponse rsp;
        recvResponse(rsp);
        auto opcode = rsp.getOp();
        if (opcode == ClientOpcode::Noop) {
            done = true;
        } else if (opcode == ClientOpcode::Get) {
            BinprotGetResponse getResponse(std::move(rsp));
            auto opaque = getResponse.getResponse().getOpaque();
            if (opaque >= id.size()) {
                throw std::runtime_error(
                        "MemcachedConnection::mget: Invalid opaque received");
            }
            const auto& key = id[opaque].first;

            if (getResponse.isSuccess()) {
                auto doc = std::make_unique<Document>();
                doc->info.flags = getResponse.getDocumentFlags();
                doc->info.cas = getResponse.getCas();
                doc->info.id = key;
                doc->info.datatype = getResponse.getResponse().getDatatype();
                doc->value = getResponse.getDataView();
                documentCallback(doc);
            } else if (errorCallback &&
                       getResponse.getStatus() != cb::mcbp::Status::KeyEnoent) {
                errorCallback(key, getResponse.getResponse());
            }
        } else {
            throw std::runtime_error(
                    "MemcachedConnection::mget: Received unexpected opcode: " +
                    ::to_string(opcode));
        }
    } while (!done);
}

Frame MemcachedConnection::encodeCmdGet(const std::string& id, Vbid vbucket) {
    BinprotGetCommand command{id, vbucket};
    return to_frame(command);
}

MutationInfo MemcachedConnection::mutate(
        const DocumentInfo& info,
        Vbid vbucket,
        cb::const_byte_buffer value,
        MutationType type,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer(value);
    command.setVBucket(vbucket);
    command.setMutationType(type);
    applyFrameInfos(command, getFrameInfo);

    const auto response = BinprotMutationResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to store " + info.id, response);
    }

    return response.getMutationInfo();
}

MutationInfo MemcachedConnection::store(
        const std::string& id,
        Vbid vbucket,
        std::string value,
        cb::mcbp::Datatype datatype,
        uint32_t expiry,
        const GetFrameInfoFunction& getFrameInfo) {
    Document doc{};
    doc.value = std::move(value);
    doc.info.id = id;
    doc.info.datatype = datatype;
    doc.info.expiration = expiry;
    return mutate(doc, vbucket, MutationType::Set, getFrameInfo);
}

void MemcachedConnection::stats(
        std::function<void(const std::string&, const std::string&)> callback,
        const std::string& group,
        const std::string& value,
        const GetFrameInfoFunction& getFrameInfo) {
    stats([&callback](const auto& k, const auto& v, auto) { callback(k, v); },
          group,
          value,
          getFrameInfo);
}

void MemcachedConnection::stats(
        const std::function<void(const std::string&,
                                 const std::string&,
                                 cb::mcbp::Datatype)>& callback,
        std::string group,
        std::string value,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Stat, std::move(group));
    if (!value.empty() && hasFeature(cb::mcbp::Feature::JSON)) {
        try {
            auto json = nlohmann::json::parse(value);
            cmd.setDatatype(cb::mcbp::Datatype::JSON);
        } catch (const std::exception&) {
        }
    }
    cmd.setValue(std::move(value));
    applyFrameInfos(cmd, getFrameInfo);
    BinprotResponse rsp;

    backoff_execute(
            [&cmd, &rsp, &callback, this]() -> bool {
                sendCommand(cmd);
                recvResponse(rsp);

                if (rsp.getStatus() == cb::mcbp::Status::Etmpfail &&
                    auto_retry_tmpfail) {
                    return false;
                }

                int counter = 0;
                while (rsp.getStatus() == cb::mcbp::Status::Success &&
                       (!rsp.getKey().empty() || !rsp.getDataView().empty())) {
                    std::string key{rsp.getKey()};
                    if (key.empty()) {
                        key = std::to_string(counter++);
                    }

                    callback(key,
                             std::string{rsp.getDataView()},
                             cb::mcbp::Datatype(rsp.getDatatype()));
                    recvResponse(rsp);
                }
                return true;
            },
            fmt::format("{}({})", cmd.getOp(), cmd.getKey()));

    if (!rsp.isSuccess()) {
        throw ConnectionError("Stats failed", rsp);
    }
}

void MemcachedConnection::configureEwouldBlockEngine(const EWBEngineMode& mode,
                                                     cb::engine_errc err_code,
                                                     uint32_t value,
                                                     const std::string& key) {
    cb::mcbp::request::EWB_Payload payload;
    payload.setMode(uint32_t(mode));
    payload.setValue(uint32_t(value));
    payload.setInjectError(uint32_t(err_code));
    auto buf = payload.getBuffer();

    BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::EwouldblockCtl, key};
    cmd.setExtras({reinterpret_cast<const char*>(buf.data()), buf.size()});

    auto response = execute(cmd);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to configure ewouldblock engine",
                              response);
    }
}

void MemcachedConnection::reloadAuditConfiguration(
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::AuditConfigReload);
    applyFrameInfos(command, getFrameInfo);
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to reload audit configuration", response);
    }
}

void MemcachedConnection::applyFeatures(const Featureset& featureset) {
    BinprotHelloCommand command(agentInfo.dump());
    for (const auto& feature : featureset) {
        command.enableFeature(cb::mcbp::Feature(feature), true);
    }

    const auto response = BinprotHelloResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to say hello", response);
    }

    effective_features.clear();
    for (const auto& feature : response.getFeatures()) {
        effective_features.insert(uint16_t(feature));
    }
}

void MemcachedConnection::setFeatures(
        const std::vector<cb::mcbp::Feature>& features) {
    BinprotHelloCommand command(agentInfo.dump());
    for (const auto& feature : features) {
        command.enableFeature(cb::mcbp::Feature(feature), true);
    }

    const auto response = BinprotHelloResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to say hello", response);
    }

    effective_features.clear();
    for (const auto& feature : response.getFeatures()) {
        effective_features.insert(uint16_t(feature));
    }

    // Verify that I was able to set all of them
    nlohmann::json missing = nlohmann::json::array();

    for (const auto& feature : features) {
        if (!hasFeature(feature)) {
            missing.emplace_back(feature);
        }
    }

    if (!missing.empty()) {
        throw std::runtime_error("Failed to enable: " + missing.dump());
    }
}

void MemcachedConnection::setFeature(cb::mcbp::Feature feature, bool enabled) {
    Featureset currFeatures = effective_features;
    if (enabled) {
        currFeatures.insert(uint16_t(feature));
    } else {
        currFeatures.erase(uint16_t(feature));
    }

    applyFeatures(currFeatures);

    if (enabled && !hasFeature(feature)) {
        throw std::runtime_error(fmt::format("Failed to enable {}", feature));
    }
    if (!enabled && hasFeature(feature)) {
        throw std::runtime_error(fmt::format("Failed to disable {}", feature));
    }
}

std::string MemcachedConnection::getSaslMechanisms() {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::SaslListMechs);
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to fetch sasl mechanisms", response);
    }

    return std::string{response.getDataView()};
}

std::string MemcachedConnection::ioctl_get(
        const std::string& key, const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::IoctlGet, key);
    applyFrameInfos(command, getFrameInfo);

    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("ioctl_get '" + key + "' failed", response);
    }
    return std::string{response.getDataView()};
}

void MemcachedConnection::ioctl_set(const std::string& key,
                                    const std::string& value,
                                    const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::IoctlSet, key, value);
    applyFrameInfos(command, getFrameInfo);
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("ioctl_set '" + key + "' failed", response);
    }
}

uint64_t MemcachedConnection::increment(
        const std::string& key,
        uint64_t delta,
        uint64_t initial,
        rel_time_t exptime,
        MutationInfo* info,
        const GetFrameInfoFunction& getFrameInfo) {
    return incr_decr(cb::mcbp::ClientOpcode::Increment,
                     key,
                     delta,
                     initial,
                     exptime,
                     info,
                     getFrameInfo);
}

uint64_t MemcachedConnection::decrement(
        const std::string& key,
        uint64_t delta,
        uint64_t initial,
        rel_time_t exptime,
        MutationInfo* info,
        const GetFrameInfoFunction& getFrameInfo) {
    return incr_decr(cb::mcbp::ClientOpcode::Decrement,
                     key,
                     delta,
                     initial,
                     exptime,
                     info,
                     getFrameInfo);
}

uint64_t MemcachedConnection::incr_decr(
        cb::mcbp::ClientOpcode opcode,
        const std::string& key,
        uint64_t delta,
        uint64_t initial,
        rel_time_t exptime,
        MutationInfo* info,
        const GetFrameInfoFunction& getFrameInfo) {
    const char* opcode_name =
            (opcode == cb::mcbp::ClientOpcode::Increment) ? "incr" : "decr";

    BinprotIncrDecrCommand command(
            opcode, key, Vbid{0}, delta, initial, exptime);
    applyFrameInfos(command, getFrameInfo);

    const auto response = BinprotIncrDecrResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError(
                std::string(opcode_name) + " \"" + key + "\" failed.",
                response.getStatus());
    }

    if (response.getDatatype() != PROTOCOL_BINARY_RAW_BYTES) {
        throw ValidationError(
                std::string(opcode_name) + " \"" + key +
                "\"invalid - response has incorrect datatype (" +
                cb::mcbp::datatype::to_string(response.getDatatype()) + ")");
    }

    if (info != nullptr) {
        *info = response.getMutationInfo();
    }
    return response.getValue();
}

MutationInfo MemcachedConnection::remove(
        const std::string& key,
        Vbid vbucket,
        uint64_t cas,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotRemoveCommand command{key, vbucket, cas};
    applyFrameInfos(command, getFrameInfo);

    const auto response = BinprotRemoveResponse(execute(command));

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to remove: " + key, response.getStatus());
    }

    return response.getMutationInfo();
}

Document MemcachedConnection::get_and_lock(
        const std::string& id,
        Vbid vbucket,
        uint32_t lock_timeout,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGetAndLockCommand command(id, vbucket, lock_timeout);
    applyFrameInfos(command, getFrameInfo);

    const auto response = BinprotGetAndLockResponse(execute(command));

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to get: " + id, response.getStatus());
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = id;
    ret.info.datatype = response.getResponse().getDatatype();
    ret.value = response.getDataView();
    return ret;
}

BinprotResponse MemcachedConnection::getFailoverLog(
        Vbid vbucket, const GetFrameInfoFunction& getFrameInfo) {
    BinprotGetFailoverLogCommand command;
    command.setVBucket(vbucket);
    applyFrameInfos(command, getFrameInfo);

    return execute(command);
}

void MemcachedConnection::unlock(const std::string& id,
                                 Vbid vbucket,
                                 uint64_t cas,
                                 const GetFrameInfoFunction& getFrameInfo) {
    BinprotUnlockCommand command(id, vbucket, cas);
    applyFrameInfos(command, getFrameInfo);

    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("unlock(): " + id, response.getStatus());
    }
}

void MemcachedConnection::dropPrivilege(
        cb::rbac::Privilege privilege,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::DropPrivilege,
                                  format_as(privilege));
    applyFrameInfos(command, getFrameInfo);

    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError(
                fmt::format("dropPrivilege \"{}\" failed.", privilege),
                response.getStatus());
    }
}

MutationInfo MemcachedConnection::mutateWithMeta(
        Document& doc,
        Vbid vbucket,
        uint64_t cas,
        uint64_t seqno,
        uint32_t metaOption,
        std::vector<uint8_t> metaExtras,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotSetWithMetaCommand swm(
            doc, vbucket, cas, seqno, metaOption, std::move(metaExtras));
    applyFrameInfos(swm, getFrameInfo);

    const auto response = BinprotMutationResponse(execute(swm));
    if (!response.isSuccess()) {
        throw ConnectionError(fmt::format("Failed to mutateWithMeta {} {}",
                                          doc.info.id,
                                          response.getDataView()),
                              response.getStatus());
    }

    return response.getMutationInfo();
}

ObserveInfo MemcachedConnection::observeSeqno(
        Vbid vbid, uint64_t uuid, const GetFrameInfoFunction& getFrameInfo) {
    BinprotObserveSeqnoCommand observe(vbid, uuid);
    applyFrameInfos(observe, getFrameInfo);

    const auto response = BinprotObserveSeqnoResponse(execute(observe));
    if (!response.isSuccess()) {
        throw ConnectionError(std::string("Failed to observeSeqno for ") +
                                      vbid.to_string() +
                                      " uuid:" + std::to_string(uuid),
                              response.getStatus());
    }
    return response.getInfo();
}

void MemcachedConnection::enablePersistence(
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::StartPersistence);
    applyFrameInfos(command, getFrameInfo);

    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to enablePersistence ", response);
    }
}

void MemcachedConnection::disablePersistence(
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::StopPersistence);
    applyFrameInfos(command, getFrameInfo);
    const auto response = execute(command);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to disablePersistence ", response);
    }
}

GetMetaPayload MemcachedConnection::getMeta(
        const std::string& key,
        Vbid vbucket,
        GetMetaVersion version,
        const GetFrameInfoFunction& getFrameInfo) {
    BinprotGetMetaCommand cmd{key, vbucket, version};
    applyFrameInfos(cmd, getFrameInfo);
    auto resp = BinprotGetMetaResponse{execute(cmd)};
    if (!resp.isSuccess()) {
        throw ConnectionError("GetMeta() failed", resp);
    }
    return resp.getMetaPayload();
}

Document MemcachedConnection::getRandomKey(Vbid vbucket) {
    BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::GetRandomKey};
    if (hasFeature(cb::mcbp::Feature::GetRandomKeyIncludeXattr)) {
        cb::mcbp::request::GetRandomKeyPayloadV2 randomKey(0, true);
        cmd.setExtras(randomKey.getBuffer());
    } else if (hasFeature(cb::mcbp::Feature::Collections)) {
        // Currently just request random default collection key
        cb::mcbp::request::GetRandomKeyPayload randomKey;
        cmd.setExtras(randomKey.getBuffer());
    }
    cmd.setVBucket(vbucket);
    const auto response = BinprotGetResponse(execute(cmd));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed getRandomKey", response.getStatus());
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = response.getKey();
    ret.info.datatype = response.getResponse().getDatatype();
    ret.value = response.getDataView();
    return ret;
}

void MemcachedConnection::dcpOpenProducer(std::string_view nm) {
    BinprotDcpOpenCommand open{std::string{nm},
                               cb::mcbp::DcpOpenFlag::Producer};
    const auto response = BinprotResponse(execute(open));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed dcpOpenProducer", response);
    }
}

void MemcachedConnection::dcpOpenConsumer(std::string_view nm) {
    BinprotDcpOpenCommand open{std::string{nm}};
    const auto response = BinprotResponse(execute(open));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed dcpOpenConsumer", response);
    }
}

void MemcachedConnection::dcpControl(std::string_view key,
                                     std::string_view value) {
    BinprotDcpControlCommand control;
    control.setKey(std::string{key});
    control.setValue(std::string{value});
    const auto response = BinprotResponse(execute(control));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed dcpControl", response);
    }
}

void MemcachedConnection::dcpStreamRequest(Vbid vbid,
                                           cb::mcbp::DcpAddStreamFlag flags,
                                           uint64_t startSeq,
                                           uint64_t endSeq,
                                           uint64_t vbUuid,
                                           uint64_t snapStart,
                                           uint64_t snapEnd) {
    BinprotDcpStreamRequestCommand stream(
            vbid, flags, startSeq, endSeq, vbUuid, snapStart, snapEnd);
    const auto response = BinprotResponse(execute(stream));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed dcpStreamRequest", response);
    }
}

void MemcachedConnection::dcpStreamRequest(Vbid vbid,
                                           cb::mcbp::DcpAddStreamFlag flags,
                                           uint64_t startSeq,
                                           uint64_t endSeq,
                                           uint64_t vbUuid,
                                           uint64_t snapStart,
                                           uint64_t snapEnd,
                                           const nlohmann::json& value) {
    BinprotDcpStreamRequestCommand stream(
            vbid, flags, startSeq, endSeq, vbUuid, snapStart, snapEnd, value);
    const auto response = BinprotResponse(execute(stream));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed dcpStreamRequest", response);
    }
}

void MemcachedConnection::dcpAddStream(Vbid vbid,
                                       cb::mcbp::DcpAddStreamFlag flags) {
    sendCommand(BinprotDcpAddStreamCommand{flags, vbid});
}

void MemcachedConnection::dcpStreamRequestResponse(
        uint32_t opaque,
        const std::vector<std::pair<uint64_t, uint64_t>>& failovers) {
    BinprotCommandResponse rsp{cb::mcbp::ClientOpcode::DcpStreamReq, opaque};

    // Turn the vector of pairs into a protocol failover table (in a string
    // so we can attach to the response)
    std::string table;
    for (const auto& entry : failovers) {
        auto wireUuid = htonll(entry.first);
        auto wireSeqno = htonll(entry.second);

        std::copy_n(reinterpret_cast<uint8_t*>(&wireUuid),
                    sizeof(uint64_t),
                    std::back_inserter(table));

        std::copy_n(reinterpret_cast<uint8_t*>(&wireSeqno),
                    sizeof(uint64_t),
                    std::back_inserter(table));
    }

    rsp.setValue(table);
    sendCommand(rsp);
}

size_t MemcachedConnection::dcpSnapshotMarkerV2(
        uint32_t opaque,
        uint64_t start,
        uint64_t end,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags) {
    constexpr auto size =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_2Value);
    Frame buffer;
    buffer.payload.resize(size);

    cb::mcbp::FrameBuilder<cb::mcbp::Request> builder(
            {buffer.payload.data(), buffer.payload.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    builder.setOpaque(opaque);

    cb::mcbp::DcpSnapshotMarker marker(start, end, flags, {}, {}, end, 0);
    marker.encode(builder);
    sendFrame(buffer);
    return buffer.payload.size();
}

size_t MemcachedConnection::dcpMutation(const Document& doc,
                                        uint32_t opaque,
                                        uint64_t seqno,
                                        uint64_t revSeqno,
                                        uint32_t lockTime,
                                        uint8_t nru) {
    // No reply expected
    return sendCommand(BinprotDcpMutationCommand{doc.info.id,
                                                 doc.value,
                                                 opaque,
                                                 uint8_t(doc.info.datatype),
                                                 doc.info.expiration,
                                                 doc.info.cas,
                                                 seqno,
                                                 revSeqno,
                                                 doc.info.flags,
                                                 lockTime,
                                                 nru});
}

size_t MemcachedConnection::dcpDeletionV2(const Document& doc,
                                          uint32_t opaque,
                                          uint64_t seqno,
                                          uint64_t revSeqno,
                                          uint32_t deleteTime) {
    // No reply expected
    return sendCommand(BinprotDcpDeletionV2Command{doc.info.id,
                                                   doc.value,
                                                   opaque,
                                                   uint8_t(doc.info.datatype),
                                                   doc.info.cas,
                                                   seqno,
                                                   revSeqno,
                                                   deleteTime});
}

void MemcachedConnection::recvDcpBufferAck(uint32_t expected) {
    Frame frame;
    recvFrame(frame);
    const auto* request = frame.getRequest();
    if (request->getClientOpcode() !=
        cb::mcbp::ClientOpcode::DcpBufferAcknowledgement) {
        throw std::logic_error(
                "MemcachedConnection::recvDcpBufferAck not a buffer ack "
                "opcode request:" +
                request->to_json(request->isValid()).dump());
    }
    const auto& dcpBufferAck = request->getCommandSpecifics<
            cb::mcbp::request::DcpBufferAckPayload>();

    if (dcpBufferAck.getBufferBytes() != expected) {
        throw std::logic_error(
                fmt::format("MemcachedConnection::recvDcpBufferAck: Unexpected "
                            "buffer bytes:{} expected:{}",
                            dcpBufferAck.getBufferBytes(),
                            expected));
    }
}

cb::mcbp::request::GetCollectionIDPayload MemcachedConnection::getCollectionId(
        std::string_view path) {
    BinprotGenericCommand command(
            cb::mcbp::ClientOpcode::CollectionsGetID, {}, std::string(path));
    const auto response = BinprotResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed getCollectionId", response);
    }

    auto extras = response.getResponse().getExtdata();
    if (extras.size() != sizeof(cb::mcbp::request::GetCollectionIDPayload)) {
        throw std::logic_error("getCollectionId invalid extra length");
    }
    cb::mcbp::request::GetCollectionIDPayload payload;
    std::copy_n(
            extras.data(), extras.size(), reinterpret_cast<uint8_t*>(&payload));
    return payload;
}

cb::mcbp::request::GetScopeIDPayload MemcachedConnection::getScopeId(
        std::string_view path) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::CollectionsGetScopeID,
                                  {},
                                  std::string(path));
    const auto response = BinprotResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed getScopeId", response);
    }

    auto extras = response.getResponse().getExtdata();
    if (extras.size() != sizeof(cb::mcbp::request::GetScopeIDPayload)) {
        throw std::logic_error("getScopeId invalid extra length");
    }
    cb::mcbp::request::GetScopeIDPayload payload;
    std::copy_n(
            extras.data(), extras.size(), reinterpret_cast<uint8_t*>(&payload));
    return payload;
}

nlohmann::json MemcachedConnection::getCollectionsManifest() {
    BinprotGenericCommand command(
            cb::mcbp::ClientOpcode::CollectionsGetManifest, {}, {});
    const auto response = BinprotResponse(execute(command));
    if (!response.isSuccess()) {
        throw ConnectionError("Failed getCollectionsManifest", response);
    }
    return nlohmann::json::parse(response.getDataView());
}

void MemcachedConnection::setUnorderedExecutionMode(ExecutionMode mode) {
    switch (mode) {
    case ExecutionMode::Ordered:
        setFeature(cb::mcbp::Feature::UnorderedExecution, false);
        return;
    case ExecutionMode::Unordered:
        setFeature(cb::mcbp::Feature::UnorderedExecution, true);
        return;
    }
    throw std::invalid_argument("setUnorderedExecutionMode: Invalid mode");
}

BinprotResponse MemcachedConnection::execute(
        const BinprotCommand& command, std::chrono::milliseconds readTimeout) {
    BinprotResponse response;
    std::string context;
    try {
        // we have unit tests which test invalid opcodes..
        context = ::to_string(command.getOp());
    } catch (const std::exception& e) {
        context = e.what();
    }
    backoff_execute(
            [&command, &response, &readTimeout, this]() -> bool {
                sendCommand(command);
                recvResponse(response, command.getOp(), readTimeout);

                if (response.getStatus() == cb::mcbp::Status::AuthStale &&
                    tokenBuilder) {
                    if (command.getOp() == cb::mcbp::ClientOpcode::SaslAuth) {
                        // we don't want to recursively do sasl auth
                        return true;
                    }
                    authenticateWithToken();
                    return false;
                }

                bool retry_by_tmpfail =
                        auto_retry_tmpfail &&
                        response.getStatus() == cb::mcbp::Status::Etmpfail;
                bool retry_by_enomem =
                        auto_retry_enomem &&
                        response.getStatus() == cb::mcbp::Status::Enomem;

                return !(retry_by_tmpfail || retry_by_enomem);
            },
            context);
    return response;
}

void MemcachedConnection::backoff_execute(const std::function<bool()>& executor,
                                          const std::string& context,
                                          std::chrono::milliseconds backoff,
                                          std::chrono::seconds executeTimeout) {
    using std::chrono::steady_clock;
    const auto wait_timeout = steady_clock::now() + executeTimeout;
    do {
        if (executor()) {
            return;
        }
        std::this_thread::sleep_for(backoff);
    } while (steady_clock::now() < wait_timeout);
    throw TimeoutException(
            "MemcachedConnection::backoff_executor: Timed out after waiting "
            "more than " +
                    std::to_string(executeTimeout.count()) + " seconds for " +
                    context,
            cb::mcbp::ClientOpcode::Invalid,
            executeTimeout);
}

void MemcachedConnection::evict(const std::string& key,
                                Vbid vbucket,
                                const GetFrameInfoFunction& getFrameInfo) {
    auto context = "evict " + key;
    backoff_execute(
            [this, &key, &vbucket]() -> bool {
                BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::EvictKey,
                                          key);
                cmd.setVBucket(vbucket);
                const auto rsp = execute(cmd);
                if (rsp.isSuccess()) {
                    // Evicted
                    return true;
                }
                if (rsp.getStatus() == cb::mcbp::Status::KeyEexists) {
                    return false;
                }

                throw ConnectionError(
                        "evict: Failed to evict key \"" + key + "\"",
                        rsp.getStatus());
            },
            context);
}

void MemcachedConnection::setVbucket(Vbid vbid,
                                     vbucket_state_t state,
                                     const nlohmann::json& payload,
                                     const GetFrameInfoFunction& getFrameInfo) {
    BinprotSetVbucketCommand command{vbid, state, payload};
    applyFrameInfos(command, getFrameInfo);

    auto rsp = execute(command);
    if (!rsp.isSuccess()) {
        throw ConnectionError("setVbucket: Failed to set state", rsp);
    }
}

vbucket_state_t MemcachedConnection::getVbucket(
        Vbid vbid, const GetFrameInfoFunction& getFrameInfo) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetVbucket);
    cmd.setVBucket(vbid);
    applyFrameInfos(cmd, getFrameInfo);

    auto rsp = execute(cmd);
    if (!rsp.isSuccess()) {
        if (rsp.getStatus() == cb::mcbp::Status::NotMyVbucket) {
            return vbucket_state_dead;
        }
        throw ConnectionError("getVbucket: Failed to get vbucket state", rsp);
    }
    if (rsp.getDataView().size() != sizeof(uint32_t)) {
        throw std::runtime_error("getVbucket: Unexpected response size");
    }
    uint32_t ret;
    std::copy_n(rsp.getDataView().data(),
                sizeof(ret),
                reinterpret_cast<char*>(&ret));

    return static_cast<vbucket_state_t>(ntohl(ret));
}

void MemcachedConnection::waitForSeqnoToPersist(Vbid vbid, uint64_t seqno) {
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::SeqnoPersistence);
    command.setVBucket(vbid);
    command.setExtrasValue(htonll(seqno));
    auto rsp = execute(command);

    if (!rsp.isSuccess()) {
        throw ConnectionError(fmt::format("waitForSeqnoToPersist: Faled to "
                                          "persist up to seqno {}",
                                          seqno),
                              rsp);
    }
}

void MemcachedConnection::applyFrameInfos(
        BinprotCommand& command, const GetFrameInfoFunction& getFrameInfo) {
    if (getFrameInfo) {
        auto frame_info = getFrameInfo();
        for (const auto& fi : frame_info) {
            command.addFrameInfo(*fi);
        }
    }
}
const std::string& MemcachedConnection::getServerInterfaceUuid() const {
    return serverInterfaceUuid;
}

void MemcachedConnection::setServerInterfaceUuid(std::string value) {
    serverInterfaceUuid = std::move(value);
}

void MemcachedConnection::adjustMemcachedClock(
        int64_t clock_shift,
        cb::mcbp::request::AdjustTimePayload::TimeType timeType) {
    cb::mcbp::request::AdjustTimePayload payload;
    payload.setOffset(uint64_t(clock_shift));
    payload.setTimeType(timeType);
    auto buf = payload.getBuffer();
    std::vector<uint8_t> extras;
    std::ranges::copy(buf, std::back_inserter(extras));

    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::AdjustTimeofday);
    cmd.setExtras(extras);

    auto rsp = execute(cmd);
    if (!rsp.isSuccess()) {
        throw ConnectionError(
                "adjustMemcachedClock: Failed to adjust server time", rsp);
    }
}

BinprotGetAllVbucketSequenceNumbersResponse
MemcachedConnection::getAllVBucketSequenceNumbers() {
    BinprotGetAllVbucketSequenceNumbers command;
    return BinprotGetAllVbucketSequenceNumbersResponse(execute(command));
}

BinprotGetAllVbucketSequenceNumbersResponse
MemcachedConnection::getAllVBucketSequenceNumbers(uint32_t state,
                                                  CollectionID collection) {
    BinprotGetAllVbucketSequenceNumbers command(state, collection);
    return BinprotGetAllVbucketSequenceNumbersResponse(execute(command));
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the ConnectionError class
/////////////////////////////////////////////////////////////////////////

// Generates error msgs like ``<prefix>: ["<context>", ]<reason> (#<reason>)``
static std::string formatMcbpExceptionMsg(std::string_view prefix,
                                          cb::mcbp::Status reason,
                                          std::string_view context = {}) {
    // Format the error message
    return fmt::format("{}: {}{}{}{} ({})",
                       prefix,
                       context.empty() ? "" : "'",
                       context,
                       context.empty() ? "" : "', ",
                       reason,
                       uint16_t(reason));
}

static std::string formatMcbpExceptionMsg(std::string_view prefix,
                                          const BinprotResponse& response) {
    std::string context;
    // If the response was not a success and the datatype is json then there's
    // probably a JSON error context that's been included with the response body
    if (cb::mcbp::datatype::is_json(response.getDatatype()) &&
        !response.isSuccess()) {
        try {
            auto json = response.getDataJson();
            if (json.type() == nlohmann::json::value_t::object) {
                if (json.contains("error") &&
                    json["error"].contains("context") &&
                    json["error"]["context"].is_string()) {
                    context = json["error"]["context"].get<std::string>();
                }
            }
        } catch (const nlohmann::json::exception&) {
        }
    }
    return formatMcbpExceptionMsg(prefix, response.getStatus(), context);
}

ConnectionError::ConnectionError(const std::string& prefix,
                                 cb::mcbp::Status reason)
    : std::runtime_error(formatMcbpExceptionMsg(prefix, reason).c_str()),
      reason(reason) {
}

ConnectionError::ConnectionError(const std::string& prefix,
                                 const BinprotResponse& response)
    : std::runtime_error(formatMcbpExceptionMsg(prefix, response).c_str()),
      reason(response.getStatus()),
      payload(response.getDataView()) {
}

std::string ConnectionError::getErrorContext() const {
    const auto decoded = nlohmann::json::parse(payload);
    return decoded["error"]["context"];
}

nlohmann::json ConnectionError::getErrorJsonContext() const {
    return nlohmann::json::parse(payload);
}

/**
 * The GetFileFragmentAsyncReadCallback class is an implementation of the
 * async reader interface to be used by the getFileFragment method.
 * Given that the received message is a file fragment and may be extremely
 * large (hundred of MBs) we need to read the data in chunks and write it to
 * disk as we receive it rather than spool the entire frame into memory.
 */
class GetFileFragmentAsyncReadCallback
    : public folly::AsyncReader::ReadCallback {
public:
    GetFileFragmentAsyncReadCallback(
            folly::EventBase& base,
            cb::io::FileSink& sink,
            std::function<void(std::size_t)> stats_collect_callback)
        : base(base),
          buffer(2_MiB),
          sink(sink),
          stats_collect_callback(std::move(stats_collect_callback)) {
    }

    ~GetFileFragmentAsyncReadCallback() override = default;
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
        *bufReturn = buffer.data() + offset;
        *lenReturn = buffer.size() - offset;
    }

    void readDataAvailable(size_t len) noexcept override {
        offset += len;
        onDataReceived({reinterpret_cast<const char*>(buffer.data()), offset});
    }

    void readEOF() noexcept override {
        eof = true;
        base.terminateLoopSoon();
    }

    void readErr(const folly::AsyncSocketException& ex) noexcept override {
        exception = ex;
        base.terminateLoopSoon();
    }

    void onDataReceived(std::string_view view) {
        if (stats_collect_callback && !view.empty()) {
            stats_collect_callback(view.size());
        }
        if (!header.has_value()) {
            if (view.size() < sizeof(cb::mcbp::Header)) {
                return;
            }
            header = *reinterpret_cast<const cb::mcbp::Header*>(view.data());
            if (!header->isValid()) {
                exception = folly::AsyncSocketException(
                        folly::AsyncSocketException::INTERNAL_ERROR,
                        "Invalid packet received");
                base.terminateLoopSoon();
                return;
            }

            view.remove_prefix(sizeof(cb::mcbp::Header));
        }

        if (header->getResponse().getStatus() != cb::mcbp::Status::Success) {
            if (offset == sizeof(cb::mcbp::Header) + header->getBodylen()) {
                base.terminateLoopSoon();
            }
            return;
        }

        if (!view.empty()) {
            storeData(view);
        }
        offset = 0;
    }

    void handlePotentialNetworkException() {
        if (exception) {
            handleFollyAsyncSocketException(*exception);
        }
    }

    /// Set to true once we see EOF
    bool eof = false;
    /// Contains the exception if we encountered a read error
    std::optional<folly::AsyncSocketException> exception;
    /// The EventBase we're bound to so that we can jump out of the loop
    folly::EventBase& base;
    /// The current offset in the buffer to place new data
    std::size_t offset = 0;
    /// A buffer to hold data
    std::vector<uint8_t> buffer;
    cb::io::FileSink& sink;
    std::size_t bytes_written = 0;

    /// The header we received
    std::optional<cb::mcbp::Header> header;

    /// The callback to collect stats
    std::function<void(std::size_t)> stats_collect_callback;

    /// Store the view to the file
    void storeData(std::string_view view) {
        sink.sink(view);
        bytes_written += view.size();
        if (sink.getBytesWritten() == header->getBodylen()) {
            base.terminateLoopSoon();
        }
    }
};

uint64_t MemcachedConnection::getFileFragment(
        std::string_view uuid,
        uint64_t id,
        uint64_t offset,
        uint64_t length,
        cb::io::FileSink& sink,
        std::function<void(std::size_t)> stats_collect_callback) {
    // This command cannot be used if there is pending data!
    Expects(asyncReadCallback->input_bytes == 0);

    nlohmann::json file_meta{{"id", id},
                             {"offset", std::to_string(offset)},
                             {"length", std::to_string(length)}};

    sendCommand(BinprotGenericCommand{cb::mcbp::ClientOpcode::GetFileFragment,
                                      std::string{uuid},
                                      file_meta.dump()});
    GetFileFragmentAsyncReadCallback callback(
            *eventBase, sink, std::move(stats_collect_callback));
    asyncSocket->setReadCB(&callback);
    eventBase->loop();
    asyncSocket->setReadCB(nullptr);
    callback.handlePotentialNetworkException();
    if (!isStatusSuccess(callback.header->getResponse().getStatus())) {
        callback.buffer.resize(sizeof(cb::mcbp::Header) +
                               callback.header->getBodylen());
        BinprotResponse response;
        response.assign(std::move(callback.buffer));
        throw ConnectionError(
                fmt::format(
                        "getFileFragment: Failed to get file fragment for {}",
                        uuid),
                response);
    }

    return callback.bytes_written;
}
