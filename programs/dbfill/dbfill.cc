/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "platform/strerror.h"

#include <folly/MPMCQueue.h>
#include <folly/io/async/EventBase.h>
#include <libevent/utilities.h>
#include <platform/random.h>
#include <platform/terminal_color.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/cluster_config_map_utils.h>
#include <deque>
#include <iostream>
#ifndef WIN32
#include <csignal>
#endif

extern "C" {
uint32_t crc32buf(const uint8_t* buf, size_t len);
}

/// The document value to store
std::string document_value;

using cb::terminal::TerminalColor;

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: dbfill [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

class Node {
public:
    Node(std::string_view host, in_port_t port, bool tls)
        : host(host), port(port), tls(tls), command_queue(1000) {
    }

    void enque(std::string k, Vbid vbid) {
        command_queue.blockingWrite(
                std::pair<std::string, Vbid>{std::move(k), vbid});
    }

    void start(const McProgramGetopt& getopt, std::string_view bucket) {
        for (int ii = 0; ii < 10; ++ii) {
            using cb::mcbp::Feature;
            auto conn = getopt.createAuthenticatedConnection(
                    host, port, AF_INET, tls);
            conn->selectBucket(bucket);
            conn->setFeatures({Feature::XERROR, Feature::JSON});
            auto sock = conn->releaseSocket();
            evutil_make_socket_nonblocking(sock);

            bevs.emplace_back(bufferevent_socket_new(
                    event_base->getLibeventBase(),
                    sock,
                    BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS));
            bufferevent_setcb(bevs.back().get(),
                              read_callback,
                              write_callback,
                              event_callback,
                              this);
            bufferevent_enable(bevs.back().get(), EV_READ | EV_WRITE);
        }

        thread = std::make_unique<std::thread>([this]() { run(); });
    }

    void stop() const {
        event_base->terminateLoopSoon();
        thread->join();
    }

    bool isIdle() const {
        return command_queue.isEmpty() && active_commands.rlock()->empty();
    }

protected:
    static void event_callback(bufferevent* bev, short event, void* ctx) {
        fmt::println("{}Event error: {}{}",
                     TerminalColor::Red,
                     event,
                     TerminalColor::Reset);
        _Exit(EXIT_FAILURE);
    }

    void onResponseReceived(const cb::mcbp::Response& response) {
        active_commands.withWLock([&response, this](auto& commands) {
            auto iter = commands.find(response.getOpaque());

            if (iter == commands.end()) {
                fmt::println("{}Received response with unknown opaque: {}{}",
                             TerminalColor::Red,
                             response.to_json(false).dump(),
                             TerminalColor::Reset);
                return;
            }

            if (response.getStatus() == cb::mcbp::Status::Etmpfail) {
                sendCommand(iter->second.first,
                            iter->second.second,
                            response.getOpaque());
                return;
            }

            if (response.getStatus() != cb::mcbp::Status::Success) {
                fmt::println("{}Command returned error: {}{}",
                             TerminalColor::Red,
                             response.to_json(false).dump(),
                             TerminalColor::Reset);
            }
            // remove the command
            commands.erase(iter);
        });
    }
    void onFrameReceived(const cb::mcbp::Header& header) {
        if (header.isResponse()) {
            onResponseReceived(header.getResponse());
        } else if (header.isRequest()) {
            fmt::println("{}Received unexpected request: {}{}",
                         TerminalColor::Red,
                         header.getRequest().to_json(false).dump(),
                         TerminalColor::Reset);
        } else {
            throw std::runtime_error("Invalid packet received");
        }
    }

    void read_callback(bufferevent* bev) {
        auto* input = bufferevent_get_input(bev);
        while (isPacketAvailable(input)) {
            auto& frame = *reinterpret_cast<const cb::mcbp::Header*>(
                    evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
            onFrameReceived(frame);
            evbuffer_drain(input,
                           frame.getBodylen() + sizeof(cb::mcbp::Header));
        }

        fill_command_pipeline();
    }

    static void read_callback(bufferevent* bev, void* ctx) {
        static_cast<Node*>(ctx)->read_callback(bev);
    }

    void sendCommand(std::string key, Vbid vb, uint32_t identifier) {
        BinprotMutationCommand cmd;
        cmd.addValueBuffer(document_value);
        cmd.setMutationType(MutationType::Set);
        cmd.setKey(std::move(key));
        cmd.setVBucket(vb);
        cmd.setOpaque(identifier);
        std::vector<uint8_t> bytes;
        cmd.encode(bytes);
        bufferevent_write(
                bevs[round_robin++].get(), bytes.data(), bytes.size());
        if (round_robin == bevs.size()) {
            round_robin = 0;
        }
    }

    static void write_callback(bufferevent*, void* ctx) {
        // we've sent all data; try to refill the pipelines
        static_cast<Node*>(ctx)->fill_command_pipeline();
    }

    void fill_command_pipeline() {
        active_commands.withWLock([this](auto& commands) {
            std::pair<std::string, Vbid> element;
            while (commands.size() < 500 && command_queue.read(element)) {
                sendCommand(element.first, element.second, opaque);
                commands[opaque] = {std::move(element.first), element.second};
                ++opaque;
            }
        });
    }

    void run() {
        event_base->loopForever();
    }

    static bool isPacketAvailable(evbuffer* input) {
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
            std::cerr << header->to_json(false) << std::endl;
            throw std::runtime_error(
                    "isPacketAvailable(): Invalid packet header detected");
        }

        const auto framesize = sizeof(*header) + header->getBodylen();
        if (size >= framesize) {
            // We've got the entire buffer available.. make sure it is
            // continuous
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

    const std::string host;
    const in_port_t port;
    const bool tls;
    folly::MPMCQueue<std::pair<std::string, Vbid>> command_queue;
    folly::Synchronized<
            std::unordered_map<uint32_t, std::pair<std::string, Vbid>>>
            active_commands;
    std::shared_ptr<folly::EventBase> event_base =
            std::make_shared<folly::EventBase>();
    std::unique_ptr<std::thread> thread;

    std::vector<cb::libevent::unique_bufferevent_ptr> bevs;
    uint32_t opaque = 0;
    size_t round_robin = 0;
};

std::unique_ptr<Node> create_node(std::string_view host,
                                  in_port_t port,
                                  bool tls) {
    return std::make_unique<Node>(host, port, tls);
}

int main(int argc, char** argv) {
    std::string bucket;
    size_t size = 256;
    size_t documents = 1000000;
    bool random_value = false;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption({[&bucket](auto value) { bucket = std::string{value}; },
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on"});

    getopt.addOption({[&size](auto value) { size = stoul(std::string{value}); },
                      "size",
                      Argument::Required,
                      "num",
                      "The size of the documents (in bytes)"});

    getopt.addOption({[&documents](auto value) {
                          documents = stoul(std::string{value});
                      },
                      'I',
                      "num-items",
                      Argument::Required,
                      "num",
                      "The number of documents"});

    getopt.addOption({[&random_value](auto) { random_value = true; },
                      'R',
                      "random-body",
                      "Use a random value which don't compress well"});

    getopt.addOption({[&getopt](auto value) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        fmt::println("{}Fatal: failed to ignore SIGPIPE: {}{}",
                     TerminalColor::Red,
                     cb_strerror(),
                     TerminalColor::Reset);
        return EXIT_FAILURE;
    }
#endif

    if (bucket.empty()) {
        fmt::println("{}Specify bucket with --bucket{}",
                     TerminalColor::Red,
                     TerminalColor::Reset);
        return EXIT_FAILURE;
    }

    folly::MPMCQueue<std::unique_ptr<BinprotCommand>> command_queue(1000);

    try {
        getopt.assemble();

        auto connection = getopt.getConnection();
        connection->selectBucket(bucket);
        auto node_locator = NodeLocator<Node>::create(
                *connection, create_node, [](std::string_view key) {
                    return crc32buf(
                            reinterpret_cast<const uint8_t*>(key.data()),
                            key.length());
                });

        node_locator->iterate(
                [&getopt, &bucket](auto& node) { node.start(getopt, bucket); });

        if (random_value) {
            document_value.resize(size);
            cb::RandomGenerator random_generator;
            random_generator.getBytes(document_value.data(),
                                      document_value.size());
        } else {
            document_value = std::string(size, 'a');
        }

        for (size_t ii = 0; ii < documents; ++ii) {
            auto key = fmt::format("key-{}", ii);
            auto [node, vb] = node_locator->lookup(key);
            node.enque(std::move(key), vb);
            if ((ii % 1000) == 0) {
                fmt::print("\rQueued: {}", ii);
            }
        }

        // Now wait for all of them to complete its work,..
        node_locator->iterate([](auto& node) {
            while (!node.isIdle()) {
                std::this_thread::sleep_for(std::chrono::seconds{1});
            }
            node.stop();
        });
    } catch (const std::exception& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
