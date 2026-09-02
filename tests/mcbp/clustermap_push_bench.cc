/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mcbp_mock_connection.h"

#include <benchmark/benchmark.h>
#include <cblogger/logger.h>
#include <daemon/bucket_manager.h>
#include <daemon/cluster_config.h>
#include <daemon/front_end_thread.h>
#include <folly/io/IOBuf.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/framebuilder.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
#include <array>
#include <memory>
#include <vector>

class SinkConnection : public McbpMockConnection {
public:
    SinkConnection(FrontEndThread& thr, size_t capacity)
        : McbpMockConnection(thr) {
        sink.reserve(capacity); // just to ensure we dont include
                                // (re)allocs in the benchmark
    }

    using Connection::copyToOutputStream;

    void copyToOutputStream(std::string_view data) override {
        sink.append(data);
    }

    void copyToOutputStream(gsl::span<std::string_view> data) override {
        for (const auto& datum : data) {
            sink.append(datum);
        }
    }

    void reset() {
        sink.clear();
    }

    std::string sink;
};

static constexpr std::array<int64_t, 7> mapSizes{
        {4096, 16384, 65536, 131072, 262144, 524288, 1048576}};
static std::array<ClusterConfiguration, mapSizes.size()> configurations;

static void buildConfiguration(size_t index) {
    std::string uncompressed = R"({"rev":1,"nodes":[)";
    while (uncompressed.size() < size_t(mapSizes[index])) {
        uncompressed.append(
                R"({"hostname":"192.0.2.1","ports":{"direct":11210}},)");
    }
    uncompressed.append(R"({"hostname":"192.0.2.2"}]})");

    const auto iob = cb::compression::deflateSnappy(uncompressed);
    configurations[index].setConfiguration(
            std::make_shared<ClusterConfiguration::Configuration>(
                    ClustermapVersion{1, 1},
                    std::move(uncompressed),
                    std::string{folly::StringPiece(iob->coalesce())}));
}

static ClusterConfiguration& configurationFor(int64_t size) {
    for (size_t ii = 0; ii < mapSizes.size(); ++ii) {
        if (mapSizes[ii] == size) {
            return configurations[ii];
        }
    }
    throw std::invalid_argument("configurationFor: unknown map size");
}

/**
 * the connections a sweep emits into, plus the frame fields derived from the
 * benchmark arguments
 *
 * Arg(0) is the uncompressed size of the map, Arg(1) the number of connections
 * to sweep, and Arg(2) selects compressed vs uncompressed payload
 */
class PushFixture {
public:
    explicit PushFixture(const benchmark::State& state) {
        using namespace cb::mcbp;

        const bool compressed = state.range(2) != 0;
        active = configurationFor(state.range(0))
                         .maybeGetConfiguration(ClustermapVersion{});
        payload = compressed ? active->compressed : active->uncompressed;
        datatype = compressed ? Datatype{PROTOCOL_BINARY_DATATYPE_JSON |
                                         PROTOCOL_BINARY_DATATYPE_SNAPPY}
                              : Datatype{PROTOCOL_BINARY_DATATYPE_JSON};

        version.setEpoch(active->version.getEpoch());
        version.setRevision(active->version.getRevno());
        extras = version.getBuffer();
        frameSize =
                sizeof(Request) + extras.size() + name.size() + payload.size();

        for (int64_t ii = 0; ii < state.range(1); ++ii) {
            sinks.emplace_back(
                    std::make_unique<SinkConnection>(thread, frameSize));
            BucketManager::instance().associateInitialBucket(*sinks.back());
        }
    }

    ~PushFixture() {
        for (const auto& connection : sinks) {
            BucketManager::instance().disassociateBucket(*connection);
        }
    }

    /// The global configuration is pushed with an empty key
    const std::string name;
    std::shared_ptr<ClusterConfiguration::Configuration> active;
    std::string_view payload;
    cb::mcbp::Datatype datatype{};
    cb::mcbp::request::SetClusterConfigPayload version;
    cb::const_byte_buffer extras;
    size_t frameSize = 0;
    FrontEndThread thread;
    std::vector<std::unique_ptr<SinkConnection>> sinks;
};

/**
 * emit a frame built per connection, which is the shape the push had before the
 * payload cache was introduced
 *
 * one iteration is the whole sweep rather than one connection, so that it is
 * directly comparable with ClustermapPushEmitSharedBody
 */
static void ClustermapPushEmitPerConnection(benchmark::State& state) {
    using namespace cb::mcbp;
    PushFixture fixture(state);

    while (state.KeepRunning()) {
        for (const auto& connection : fixture.sinks) {
            // the send buffer is drained by the event loop, so every
            // connection is emitted into an empty buffer
            connection->reset();
            std::string buffer;
            buffer.resize(fixture.frameSize);
            RequestBuilder builder(buffer);
            builder.setMagic(Magic::ServerRequest);
            builder.setOpcode(ServerOpcode::ClustermapChangeNotification);
            builder.setExtras(fixture.extras);
            builder.setKey(fixture.name);
            builder.setDatatype(fixture.datatype);
            builder.setValue(fixture.payload);
            connection->copyToOutputStream(builder.getFrame()->getFrame());
        }
    }
}

/**
 * build the body once per sweep and emit only a header per connection, which is
 * the shape the push has today
 *
 * one iteration is the whole sweep rather than one connection, as the body cost
 * is paid once for the whole sweep
 */
static void ClustermapPushEmitSharedBody(benchmark::State& state) {
    using namespace cb::mcbp;
    PushFixture fixture(state);

    while (state.KeepRunning()) {
        std::string body;
        body.reserve(fixture.extras.size() + fixture.name.size() +
                     fixture.payload.size());
        body.append(reinterpret_cast<const char*>(fixture.extras.data()),
                    fixture.extras.size());
        body.append(fixture.name);
        body.append(fixture.payload);

        for (const auto& connection : fixture.sinks) {
            // the send buffer is drained by the event loop, so every
            // connection is emitted into an empty buffer
            connection->reset();
            Request header = {};
            header.setMagic(Magic::ServerRequest);
            header.setOpcode(ServerOpcode::ClustermapChangeNotification);
            header.setExtlen(gsl::narrow<uint8_t>(fixture.extras.size()));
            header.setKeylen(gsl::narrow<uint16_t>(fixture.name.size()));
            header.setDatatype(uint8_t(fixture.datatype));
            header.setBodylen(gsl::narrow<uint32_t>(body.size()));
            connection->copyToOutputStream(
                    std::string_view{reinterpret_cast<const char*>(&header),
                                     sizeof(header)},
                    body);
        }
    }
}

static void pushArguments(benchmark::internal::Benchmark* bench) {
    bench->ArgsProduct({std::vector<int64_t>{mapSizes.begin(), mapSizes.end()},
                        {1, 100},
                        {0, 1}});
}

BENCHMARK(ClustermapPushEmitPerConnection)->Apply(pushArguments);
BENCHMARK(ClustermapPushEmitSharedBody)->Apply(pushArguments);

int main(int argc, char** argv) {
    cb::logger::createBlackholeLogger();
    cb::rbac::initialize();
    BucketManager::instance();

    for (size_t ii = 0; ii < mapSizes.size(); ++ii) {
        buildConfiguration(ii);
    }

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
    return 0;
}
