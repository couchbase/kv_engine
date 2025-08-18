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

/// Set to true when all consumers should drain their spooled data and
/// not expect new data to arrive
static std::atomic_bool done = false;

static std::atomic_uint64_t total_stored = 0;
static std::atomic_uint64_t total_failed = 0;
static std::atomic_uint64_t total_tmpfail = 0;

/// The number of connections to use to each node in the cluster
static std::size_t num_connections = 10;

/// The document value to store
std::string document_value;

enum class RandomValue { Off, On, PerDocument };
RandomValue random_value = RandomValue::Off;

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
        for (std::size_t ii = 0; ii < num_connections; ++ii) {
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

            if (!tls) {
                // Allow bufferevent to pass more than the default 16k in a
                // single write. This doesn't work with TLS (should probably
                // look into why... could be related to the TLS frame size of
                // 16k, but bufferevent should have worked around that
                // internally)
                bufferevent_set_max_single_write(bevs.back().get(), 1_MiB);
            }
            bufferevent_enable(bevs.back().get(), EV_READ | EV_WRITE);
        }

        thread = std::make_unique<std::thread>([this]() { run(); });
    }

    void stop() const {
        event_base->terminateLoopSoon();
        thread->join();
    }

    bool isIdle() const {
        return command_queue.isEmpty() && idle.load();
    }

protected:
    static void event_callback(bufferevent*, short event, void*) {
        fmt::println("{}Event error: {}{}",
                     TerminalColor::Red,
                     event,
                     TerminalColor::Reset);
        _Exit(EXIT_FAILURE);
    }

    void onResponseReceived(const cb::mcbp::Response& response) {
        auto iter = active_commands.find(response.getOpaque());

        if (iter == active_commands.end()) {
            fmt::println("{}Received response with unknown opaque: {}{}",
                         TerminalColor::Red,
                         response.to_json(false).dump(),
                         TerminalColor::Reset);
            return;
        }

        if (response.getStatus() == cb::mcbp::Status::Etmpfail) {
            // Back off to let the server move ahead
            ++total_tmpfail;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            sendCommand(iter->second.first,
                        iter->second.second,
                        response.getOpaque());
            return;
        }

        if (response.getStatus() == cb::mcbp::Status::Success) {
            ++total_stored;
        } else {
            ++total_failed;
            fmt::println("{}Command returned error: {}{}",
                         TerminalColor::Red,
                         response.to_json(false).dump(),
                         TerminalColor::Reset);
        }
        // remove the command
        active_commands.erase(iter);
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
        if (random_value == RandomValue::PerDocument) {
            std::string value;
            value.resize(document_value.size());
            cb::RandomGenerator random_generator;
            random_generator.getBytes(value.data(), value.size());
            cmd.addValueBuffer(value);
        } else {
            cmd.addValueBuffer(document_value);
        }
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
        std::size_t bev_size = 0;
        do {
            std::pair<std::string, Vbid> element;
            bev_size = 0;
            while (active_commands.size() < 100 &&
                   command_queue.read(element) && bev_size < 100_MiB) {
                sendCommand(element.first, element.second, opaque);
                active_commands[opaque] = {std::move(element.first),
                                           element.second};
                ++opaque;
                for (const auto& bev : bevs) {
                    bev_size += evbuffer_get_length(
                            bufferevent_get_output(bev.get()));
                }
            }
            idle = active_commands.empty();
            for (const auto& bev : bevs) {
                bev_size +=
                        evbuffer_get_length(bufferevent_get_output(bev.get()));
            }
        } while (idle && !done && bev_size < 100_MiB);
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
    std::atomic_bool idle = false;
    folly::MPMCQueue<std::pair<std::string, Vbid>> command_queue;
    std::unordered_map<uint32_t, std::pair<std::string, Vbid>> active_commands;
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
    size_t offset = 0;
    std::optional<int> vbucket;

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

    getopt.addOption(
            {[&offset](auto value) { offset = stoul(std::string{value}); },
             "offset",
             Argument::Required,
             "num",
             "The offset for the first key"});

    getopt.addOption(
            {[](auto value) { num_connections = stoul(std::string{value}); },
             "num-connections",
             Argument::Required,
             "num",
             "The number of connections to each node (default: 10)"});

    getopt.addOption(
            {[&vbucket](auto value) { vbucket = stoi(std::string{value}); },
             "vbucket",
             Argument::Required,
             "num",
             "The vbucket to send mutations to (default: use all vbuckets)"});

    getopt.addOption({[](auto value) {
                          if (value == "per-document") {
                              random_value = RandomValue::PerDocument;
                          } else {
                              random_value = RandomValue::On;
                          }
                      },
                      'R',
                      "random-body",
                      Argument::Optional,
                      "per-document",
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

    try {
        getopt.assemble();

        auto connection = getopt.getConnection();
        connection->selectBucket(bucket);
        auto node_locator = NodeLocator<Node>::create(
                *connection, create_node, [&vbucket](std::string_view key) {
                    if (!vbucket) {
                        return crc32buf(
                                reinterpret_cast<const uint8_t*>(key.data()),
                                key.length());
                    }
                    return static_cast<uint32_t>(*vbucket);
                });

        node_locator->iterate(
                [&getopt, &bucket](auto& node) { node.start(getopt, bucket); });

        if (random_value == RandomValue::Off) {
            document_value = std::string(size, 'a');
        } else {
            document_value.resize(size);
            cb::RandomGenerator random_generator;
            random_generator.getBytes(document_value.data(),
                                      document_value.size());
        }

        auto dispatcher =
                [&node_locator, documents, offset](Node& destination) {
                    for (size_t ii = 0; ii < documents; ++ii) {
                        auto key = fmt::format("key-{}", ii + offset);
                        auto [node, vb] = node_locator->lookup(key);
                        if (&node == &destination) {
                            node.enque(std::move(key), vb);
                        }
                    }
                };

        // The various servers may operate with a different speed, and we
        // don't want to have one slow server stop populating the faster
        // nodes
        std::vector<std::thread> generator_threads;
        node_locator->iterate([&generator_threads, &dispatcher](auto& node) {
            generator_threads.emplace_back(
                    [&node, &dispatcher] { dispatcher(node); });
        });

        // Loop and dump statistics until we've stored all documents
        while (total_stored < documents) {
            uint64_t then = total_stored + total_failed + total_tmpfail;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t now = total_stored + total_failed + total_tmpfail;
            fmt::print(
                    stdout,
                    "\r{:>10} stored {:>10} tmpfail {:>4} failed {:>6} ops/s",
                    total_stored,
                    total_tmpfail,
                    total_failed,
                    now - then);
            fflush(stdout);
        }

        fmt::println(stdout, " - done");
        fflush(stdout);

        for (auto& t : generator_threads) {
            t.join();
        }

        done = true;

        // Now wait for all of them to complete its work...
        node_locator->iterate([&documents](auto& node) {
            while (!node.isIdle()) {
                std::this_thread::sleep_for(std::chrono::seconds{1});
            }
            node.stop();
        });
        fmt::println(stdout, " - done");
        fflush(stdout);
    } catch (const std::exception& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
