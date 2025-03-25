/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
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
        std::cerr << header->to_json(false) << std::endl;
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
        McProgramGetopt& instance, const std::string& bucket) {
    auto ret = instance.getConnection();
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
    auto rsp = connection.execute(
            BinprotDcpOpenCommand{"dcpdrain", cb::mcbp::DcpOpenFlag::Producer});
    if (!rsp.isSuccess()) {
        std::cerr << "Failed to open DCP stream: " << rsp.getStatus()
                  << std::endl
                  << "\t" << rsp.getDataView() << std::endl;
        exit(EXIT_FAILURE);
    }

    // we're only going to use a single  vbucket
    BinprotDcpStreamRequestCommand streamRequestCommand;
    streamRequestCommand.setDcpFlags({});
    streamRequestCommand.setDcpReserved(0);
    streamRequestCommand.setDcpStartSeqno(0);
    streamRequestCommand.setDcpEndSeqno(0xffffffff);
    streamRequestCommand.setDcpVbucketUuid(0);
    streamRequestCommand.setDcpSnapStartSeqno(0);
    streamRequestCommand.setDcpSnapEndSeqno(0xfffffff);
    streamRequestCommand.setVBucket(Vbid(0));
    rsp = connection.execute(streamRequestCommand);
    if (!rsp.isSuccess()) {
        std::cerr << "DCP stream request failed: " << rsp.getStatus()
                  << std::endl
                  << "\t" << rsp.getDataView() << std::endl;
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

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: dcplatency [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

int main(int argc, char** argv) {
    McProgramGetopt getopt;
    std::string bucket{};
    std::string name = "dcplatency";
    std::size_t size = 8192;
    using cb::getopt::Argument;
    getopt.addOption({[&bucket](auto value) { bucket = std::string{value}; },
                      'b',
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on"});

    getopt.addOption({[&name](auto value) { name = std::string{value}; },
                      "name",
                      Argument::Required,
                      "dcpname",
                      "The dcp name to use"});

    getopt.addOption({[&size](auto value) {
                          size = strtoul(std::string{value.data()}.c_str());
                      },
                      "size",
                      Argument::Required,
                      "#bytes",
                      "The document size"});

    getopt.addOption({[](auto value) {
                          iterations =
                                  strtoul(std::string{value.data()}.c_str());
                      },
                      "iterations",
                      Argument::Required,
                      "number",
                      "The number of times we should send the document"});

    getopt.addOption(
            {[](auto) { verbose = true; }, "verbose", "Add more output"});

    getopt.addOption({[&getopt](auto value) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    getopt.parse(argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (bucket.empty()) {
        std::cerr << "Please specify bucket with -b" << std::endl;
        return EXIT_FAILURE;
    }

    cb::libevent::unique_event_base_ptr base(event_base_new());
    std::vector<cb::libevent::unique_bufferevent_ptr> events;
    try {
        getopt.assemble();
        auto loadClient = createConnection(getopt, bucket);
        loadClient->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, "key"});

        auto dcpConnection = createConnection(getopt, bucket);
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
    } catch (const std::exception& ex) {
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
