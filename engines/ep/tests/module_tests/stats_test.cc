/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit test for stats
 */

#include "stats_test.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "ep_bucket.h"
#include "evp_store_single_threaded_test.h"
#include "file_ops_tracker.h"
#include "item.h"
#include "kv_bucket.h"
#include "memcached/engine_error.h"
#include "tasks.h"
#include "test_helpers.h"
#include "tests/mock/mock_dcp_conn_map.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_function_helper.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/lambda_task.h"
#include "thread_utilities.h"
#include "trace_helpers.h"
#include "utilities/test_manifest.h"
#include "warmup.h"
#include <gtest/gtest.h>
#include <platform/semaphore.h>
#include <statistics/prometheus_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>

#include <folly/portability/GMock.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/units.h>
#include <utilities/logtags.h>

#include <functional>
#include <string_view>
#include <thread>

void StatTest::SetUp() {
    SingleThreadedEPBucketTest::SetUp();
    store->setVBucketState(vbid, vbucket_state_active);
}

std::map<std::string, std::string> StatTest::get_stat(std::string_view statkey,
                                                      std::string_view value,
                                                      bool throw_on_error) {
    std::map<std::string, std::string> stats;
    auto add_stats = [&stats](auto key, auto value, const auto&) {
        stats[std::string{key}] = std::string{value};
    };
    MockCookie cookie;
    // Do get_stats and handle it throttling.
    auto ec = cb::engine_errc::throttled;
    while (ec == cb::engine_errc::throttled) {
        ec = engine->get_stats(cookie, statkey, value, add_stats);
    }

    if (ec != cb::engine_errc::success) {
        if (throw_on_error) {
            throw cb::engine_error(ec, fmt::format("{} {}", statkey, value));
        }
        EXPECT_EQ(cb::engine_errc::success, ec) << "Failed to get stats.";
    }

    return stats;
}

class ParameterizedStatTest
    : public StatTest,
      public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        /**
         * Items left in the WAL or the memtable do not get counted against the
         * logical disk size so we force them items into the LSM tree.
         */
        config_string +=
                "magma_checkpoint_interval=0;"
                "magma_min_checkpoint_interval=0;"
                "magma_sync_every_batch=true";
        StatTest::SetUp();
    }
};

class DatatypeStatTest : public StatTest,
                         public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override {
        config_string += std::string{"item_eviction_policy="} + GetParam();
        StatTest::SetUp();
    }
};

TEST_F(StatTest, vbucket_seqno_stats_test) {
    using namespace testing;
    const std::string vbucket = "vb_" + std::to_string(vbid.get());
    auto vals = get_stat("vbucket-seqno");

    EXPECT_THAT(vals,
                UnorderedElementsAre(
                        Key(vbucket + ":uuid"),
                        Pair(vbucket + ":high_seqno", "0"),
                        Pair(vbucket + ":abs_high_seqno", "0"),
                        Pair(vbucket + ":last_persisted_seqno", "0"),
                        Pair(vbucket + ":purge_seqno", "0"),
                        Pair(vbucket + ":last_persisted_snap_start", "0"),
                        Pair(vbucket + ":last_persisted_snap_end", "0"),
                        Pair(vbucket + ":high_prepared_seqno", "0"),
                        Pair(vbucket + ":high_completed_seqno", "0"),
                        Pair(vbucket + ":max_visible_seqno", "0")));
}

// Test that if we request takeover stats for something that does not exist we
// return some variation of does_not_exist.
TEST_F(StatTest, VBTakeoverStats) {
    // No connection
    const std::string stat =
            "dcp-vbtakeover " + std::to_string(vbid.get()) + " test_producer";

    auto vals = get_stat(stat);
    EXPECT_EQ("connection_does_not_exist", vals["status"]);
    EXPECT_EQ(0, std::stoi(vals["estimate"]));
    EXPECT_EQ(0, std::stoi(vals["chk_items"]));

    // Test with a new connection
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::No, true,
                                      "eq_dcpq:test_producer");

    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.addConn(cookie, producer);

    vals = get_stat(stat);
    EXPECT_EQ("stream_does_not_exist", vals["status"]);
    EXPECT_EQ(0, std::stoi(vals["estimate"]));

    // Test with a stream
    // Need a vBucket to create a stream
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb = store->getVBucket(vbid));

    producer->mockActiveStreamRequest({}, // flags
                                      1, // opaque
                                      *vb,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0); // snap_end_seqno

    vals = get_stat(stat);
    EXPECT_EQ("in-memory", vals["status"]);
    EXPECT_EQ(0, std::stoi(vals["estimate"]));
    EXPECT_EQ(0, std::stoi(vals["chk_items"]));

    // A dead stream should return a different status
    auto stream = producer->findStream(vbid);
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Closed);

    vals = get_stat(stat);
    EXPECT_EQ("stream_is_dead", vals["status"]);
    EXPECT_EQ(0, std::stoi(vals["estimate"]));
}

TEST_F(StatTest, DcpStatTest) {
    auto username =
            cookie_to_mock_cookie(cookie)->getConnection().getUser().name;

    engine->getDcpConnMap().newProducer(
            *cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);

    auto vals = get_stat("dcp");

    EXPECT_EQ(cb::tagUserData(username),
              vals["eq_dcpq:test_producer:user"]);
}

TEST_F(StatTest, FilteredDcpStatTest) {
    auto username =
            cookie_to_mock_cookie(cookie)->getConnection().getUser().name;

    engine->getDcpConnMap().newProducer(
            *cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);

    // Need another connection to test the filtering. We filter by user, so we
    // need to change that, lets use 2i in this case.
    auto* indexCookie = create_mock_cookie();
    auto& indexConn = indexCookie->getConnection();
    indexConn.setUser("2i");
    engine->getDcpConnMap().newProducer(
            *indexCookie, "2i", cb::mcbp::DcpOpenFlag::None);

    nlohmann::json filter = {{"filter", {{"user", username}}}};
    auto vals = get_stat("dcp", filter.dump());

    // Should find our filtered producer
    EXPECT_EQ(cb::tagUserData(username), vals["eq_dcpq:test_producer:user"]);

    // Should not find the 2i one
    EXPECT_EQ(vals.end(), vals.find("eq_dcpq:2i:user"));

    destroy_mock_cookie(indexCookie);
}

TEST_F(StatTest, DcpStreamStatFormatTest) {
    setVBucketState(vbid, vbucket_state_active);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb = store->getVBucket(vbid));

    auto producer = createDcpProducer(
            cookie, IncludeDeleteTime::No, true, "eq_dcpq:test_producer");

    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.addConn(cookie, producer);

    auto stream = producer->mockActiveStreamRequest({}, // flags
                                                    1, // opaque
                                                    *vb,
                                                    0, // start_seqno
                                                    ~0ull, // end_seqno
                                                    0, // vbucket_uuid,
                                                    0, // snap_start_seqno,
                                                    0); // snap_end_seqno

    {
        // "legacy" contains multiple stat keys per stream
        nlohmann::json options = {{"stream_format", "legacy"}};
        auto stats = nlohmann::json(get_stat("dcp", options.dump()));
        EXPECT_FALSE(stats.contains("eq_dcpq:test_producer:stream_0"));
        EXPECT_TRUE(stats.contains("eq_dcpq:test_producer:stream_0_vb_uuid"));
        if (HasFailure()) {
            FAIL() << options.dump() << "\nOutput:\n" << stats.dump();
        }
    }
    {
        // "json" contains 1 JSON value per stream
        nlohmann::json options = {{"stream_format", "json"}};
        auto stats = nlohmann::json(get_stat("dcp", options.dump()));
        // Expect to parse the JSON successfully.
        auto streamJson =
                stats["eq_dcpq:test_producer:stream_0"].get<std::string>();
        EXPECT_NO_THROW(nlohmann::json::parse(streamJson).at("flags"));
        EXPECT_FALSE(stats.contains("eq_dcpq:test_producer:stream_0_vb_uuid"));
        if (HasFailure()) {
            FAIL() << options.dump() << "\nOutput:\n" << stats.dump();
        }
    }
    {
        // "skip" has no stream stats
        nlohmann::json options = {{"stream_format", "skip"}};
        auto stats = nlohmann::json(get_stat("dcp", options.dump()));
        EXPECT_FALSE(stats.contains("eq_dcpq:test_producer:stream_0"));
        EXPECT_FALSE(stats.contains("eq_dcpq:test_producer:stream_0_vb_uuid"));
        if (HasFailure()) {
            FAIL() << options.dump() << "\nOutput:\n" << stats.dump();
        }
    }
}

// MB-32589: Check that _hash-dump stats correctly accounts temporary memory.
TEST_F(StatTest, HashStatsMemUsed) {
    // Add some items to VBucket 0 so the stats call has some data to
    // dump.
    store_item(Vbid(0), makeStoredDocKey("key1"), std::string(100, 'x'));
    store_item(Vbid(0), makeStoredDocKey("key2"), std::string(100, 'y'));

    auto baselineMemory = engine->getEpStats().getPreciseTotalMemoryUsed();

    // Perform the stats call from 'memcached' context
    // (i.e. no engine yet selected).
    ObjectRegistry::onSwitchThread(nullptr);

    std::string_view key{"_hash-dump 0"};
    int addStats_calls = 0;
    auto callback = [&addStats_calls](
                            std::string_view, std::string_view, const auto&) {
        addStats_calls++;
        // The callback runs in the daemon (no-bucket) context so no arena
        // should be assigned to the current thread.
        EXPECT_EQ(cb::NoClientIndex, cb::ArenaMalloc::getCurrentClientIndex());
    };
    MockCookie cookie;
    ASSERT_EQ(cb::engine_errc::success,
              engine->get_stats(cookie, key, {}, callback));

    // Sanity check - should have had at least 1 call to ADD_STATS (otherwise
    // the test isn't valid).
    ASSERT_GT(addStats_calls, 0);

    // Any temporary memory should have been freed by now, and accounted
    // correctly.
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());
}

TEST_F(StatTest, HistogramStatExpansion) {
    // Test that Histograms expand out to the expected stat keys to
    // be returned for CMD_STAT, even after being converted to HistogramData
    Histogram<uint32_t> histogram{size_t(5)};

    // populate the histogram with some dummy valuea
    histogram.add(1 /* value */, 100 /* count */);
    histogram.add(15, 200);
    histogram.add(10000, 6500);

    auto cookie = create_mock_cookie(engine.get());

    using namespace testing;
    using namespace std::literals::string_view_literals;

    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    EXPECT_CALL(cb, Call("test_histogram_mean"sv, "2052741737"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_0,1"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_1,2"sv, "100"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_2,4"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_4,8"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_8,16"sv, "200"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_16,32"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_32,4294967295"sv, "6500"sv, _));

    add_casted_stat("test_histogram"sv, histogram, asStdFunction(cb), *cookie);

    destroy_mock_cookie(cookie);
}

TEST_F(StatTest, HdrHistogramStatExpansion) {
    // Test that HdrHistograms expand out to the expected stat keys to
    // be returned for CMD_STAT, even after being converted to HistogramData
    HdrHistogram histogram{1, 1000000, 1};

    // populate the histogram with some dummy valuea
    histogram.addValueAndCount(1 /* value */, 100 /* count */);
    histogram.addValueAndCount(15, 200);
    histogram.addValueAndCount(10000, 6500);

    auto cookie = create_mock_cookie(engine.get());

    using namespace testing;
    using namespace std::literals::string_view_literals;

    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    EXPECT_CALL(cb, Call("test_histogram_mean"sv, "9544"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_0,1"sv, "100"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_1,15"sv, "200"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_15,9728"sv, "6500"sv, _));

    add_casted_stat("test_histogram"sv, histogram, asStdFunction(cb), *cookie);

    destroy_mock_cookie(cookie);
}

TEST_F(StatTest, UnitNormalisation) {
    // check that metric units simplify the given value to the
    // expected "base" representation
    // e.g., if a stat has the value `456`, and is tracking nanoseconds,
    // units::nanoseconds should normalise that to
    // 0.000000456 - like so
    using namespace cb::stats;
    EXPECT_NEAR(0.000000456, units::nanoseconds.toBaseUnit(456), 0.000000001);
    // This will be used to ensure stats are exposed to Prometheus
    // in consistent base units, as recommended by their best practices
    // https://prometheus.io/docs/practices/naming/#base-units

    // Generic units do not encode a scaling, they're
    // just a stand-in where no better unit applies
    EXPECT_EQ(1, units::none.toBaseUnit(1));
    EXPECT_EQ(1, units::count.toBaseUnit(1));

    // Percent normalises [0,100] to [0.0,1.0]
    EXPECT_EQ(0.5, units::percent.toBaseUnit(50));
    EXPECT_EQ(0.5, units::ratio.toBaseUnit(0.5));

    // time units normalise to seconds
    EXPECT_EQ(1.0 / 1000000000, units::nanoseconds.toBaseUnit(1));
    EXPECT_EQ(1.0 / 1000000, units::microseconds.toBaseUnit(1));
    EXPECT_EQ(1.0 / 1000, units::milliseconds.toBaseUnit(1));
    EXPECT_EQ(1, units::seconds.toBaseUnit(1));
    EXPECT_EQ(60, units::minutes.toBaseUnit(1));
    EXPECT_EQ(60 * 60, units::hours.toBaseUnit(1));
    EXPECT_EQ(60 * 60 * 24, units::days.toBaseUnit(1));

    // bits normalise to bytes, as do all byte units
    EXPECT_EQ(0.5, units::bits.toBaseUnit(4));
    EXPECT_EQ(1, units::bits.toBaseUnit(8));
    EXPECT_EQ(1, units::bytes.toBaseUnit(1));
    EXPECT_EQ(1000, units::kilobytes.toBaseUnit(1));
    EXPECT_EQ(1000000, units::megabytes.toBaseUnit(1));
    EXPECT_EQ(1000000000, units::gigabytes.toBaseUnit(1));
}

TEST_F(StatTest, UnitSuffix) {
    // check that metric units report the correct suffix for their
    // base unit, matching Prometheus recommendations in
    // https://prometheus.io/docs/practices/naming/#metric-names

    using namespace cb::stats;

    for (const auto& unit : {units::none, units::count}) {
        EXPECT_EQ("", unit.getSuffix());
    }

    for (const auto& unit : {units::percent, units::ratio}) {
        EXPECT_EQ("_ratio", unit.getSuffix());
    }

    for (const auto& unit : {units::microseconds,
                             units::milliseconds,
                             units::seconds,
                             units::minutes,
                             units::hours,
                             units::days}) {
        EXPECT_EQ("_seconds", unit.getSuffix());
    }

    for (const auto& unit : {units::bits,
                             units::bytes,
                             units::kilobytes,
                             units::megabytes,
                             units::gigabytes}) {
        EXPECT_EQ("_bytes", unit.getSuffix());
    }
}

std::ostream& operator<<(
        std::ostream& os,
        const std::pair<std::string_view, std::string_view>& mapEntry) {
    os << mapEntry.first << ":" << mapEntry.second;
    return os;
}

TEST_F(StatTest, CollectorForBucketScopeCollection) {
    // Confirm that StatCollector::for{Bucket,Scope,Collection}(...)
    // returns a collector which adds the corresponding labels to every
    // stat added.

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockStatCollector> collector;

    // arbitrary stat key selected which does not have hardcoded labels
    // in stats.def.h
    auto key = cb::stats::Key::bg_wait;
    auto value = 12345.0;

    auto scopeName = "scope-bar";
    auto collectionName = "collection-baz";

    // Create a collector for a bucket
    auto bucket = collector.forBucket("foo");
    auto scope = bucket.forScope(scopeName, ScopeID(0x0));
    auto collection = scope.forCollection(collectionName, CollectionID(0x8));

    InSequence s;

    // base collector has not been modified, adds no labels
    EXPECT_CALL(collector, addStat(_, Matcher<double>(value), ElementsAre()));
    collector.addStat(key, value);

    // adds bucket label
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(value),
                        UnorderedElementsAre(Pair("bucket"sv, "foo"))));
    bucket.addStat(key, value);

    std::string_view scopeNameKey = ScopeStatCollector::scopeNameKey;
    std::string_view scopeIDKey = ScopeStatCollector::scopeIDKey;
    std::string_view collectionNameKey = ColStatCollector::collectionNameKey;
    std::string_view collectionIDKey = ColStatCollector::collectionIDKey;

    // adds scope label
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(value),
                        UnorderedElementsAre(Pair("bucket"sv, "foo"),
                                             Pair(scopeNameKey, scopeName),
                                             Pair(scopeIDKey, "0x0"))));
    scope.addStat(key, value);

    // adds collection label
    EXPECT_CALL(collector,
                addStat(_,
                        Matcher<double>(value),
                        UnorderedElementsAre(
                                Pair("bucket"sv, "foo"),
                                Pair(scopeNameKey, scopeName),
                                Pair(scopeIDKey, "0x0"),
                                Pair(collectionNameKey, collectionName),
                                Pair(collectionIDKey, "0x8"))));
    collection.addStat(key, value);
}

TEST_F(StatTest, CollectorMapsTypesCorrectly) {
    // Confirm that StatCollector::addStat(...) maps arithmetic types to a
    // supported type as expected (e.g., float -> double, uint8_t -> uint64_t).

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockStatCollector> collector;

    InSequence s;

    auto testTypes = [&collector](auto input, auto output) {
        EXPECT_CALL(collector,
                    addStat(_, Matcher<decltype(output)>(output), _));
        collector.addStat("irrelevant_stat_key", input);
    };

    // check that input type is provided to the StatCollector as output type
    // The StatCollector interface does not explicitly handle every possible
    // input type, but implements virtual methods supporting a handful.
    // Other types are either not supported, or are mapped to supported types
    testTypes(uint64_t(), uint64_t());
    testTypes(uint32_t(), uint64_t());
    testTypes(uint16_t(), uint64_t());
    testTypes(uint8_t(), uint64_t());

    testTypes(int64_t(), int64_t());
    testTypes(int32_t(), int64_t());
    testTypes(int16_t(), int64_t());
    testTypes(int8_t(), int64_t());

    testTypes(double(), double());
    testTypes(float(), float());
}

MATCHER_P(MetricFamilyMatcher,
          expectedName,
          "Check the Prometheus metric family of the StatDef matches") {
    return arg.metricFamily == expectedName;
}

TEST_F(StatTest, ConfigStatDefinitions) {
    // Confirm that Configuration.addStats(...) looks up stat definitions
    // and adds the expected value, mapped to the appropriate StatCollector
    // supported type

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockStatCollector> collector;

    // ignore the rest of the calls that we are not specifically interested in.
    // A representative stat will be checked for each config type
    EXPECT_CALL(collector, addStat(_, Matcher<int64_t>(_), _))
            .Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<uint64_t>(_), _))
            .Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<double>(_), _))
            .Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<float>(_), _)).Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<bool>(_), _)).Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<std::string_view>(_), _))
            .Times(AnyNumber());

    auto& config = engine->getConfiguration();
    // confirm that specific stats known to be generated from the config
    // are definitely present

    auto maxSize = config.getMaxSize();
    // test a sssize_t stat
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_max_size"),
                        Matcher<uint64_t>(maxSize),
                        _));

    // test a float stat
    auto threshold = config.getBfilterResidencyThreshold();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_bfilter_residency_threshold"),
                        Matcher<float>(threshold),
                        _));

    // test a bool stat
    auto noop = config.isDcpEnableNoop();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_dcp_enable_noop"),
                        Matcher<bool>(noop),
                        _));

    // test a string stat
    auto flowControlEnabled = config.isDcpConsumerFlowControlEnabled();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_dcp_consumer_flow_control_enabled"),
                        Matcher<bool>(flowControlEnabled),
                        _));

    // config stats are per-bucket, wrap the collector up with a bucket label
    auto bucketC = collector.forBucket("bucket-name");
    config.addStats(bucketC);
}

TEST_F(StatTest, StringStats) {
    // Confirm that the string values are correctly added to StatCollectors.
    // This test checks that "ep_access_scanner_task_time" is added as part of
    // engine->doEngineStats(...).
    // This does not exhaustively test the formatted values of the stat,
    // just that it is correctly received as a string_view.

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockStatCollector> collector;

    // ignore the rest of the calls that we are not specifically interested in.
    // A representative stat will be checked for each config type
    EXPECT_CALL(collector, addStat(_, Matcher<std::string_view>(_), _))
            .Times(AnyNumber());

    // test a string stat
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_access_scanner_task_time"),
                        Matcher<std::string_view>("NOT_SCHEDULED"),
                        _));

    // config stats are per-bucket, wrap the collector up with a bucket label
    auto bucketC = collector.forBucket("bucket-name");
    engine->doEngineStats(bucketC, nullptr);
}

TEST_F(StatTest, WarmupState) {
    // Confirm the warmup state is correctly exposed as a one-hot metric
    //  kv_ep_warmup_status{bucket="default",state="Initialize"} 0.000000
    //  1673626962000
    //  kv_ep_warmup_status{bucket="default",state="CreateVBuckets"} 0.000000
    //  1673626962000
    //  ...
    //  kv_ep_warmup_status{bucket="default",state="Done"} 1.000000
    //  1673626962000

    using namespace std::string_view_literals;
    using namespace testing;

    std::vector<std::string_view> expectedLabels = {
            "Initialize"sv,
            "CreateVBuckets"sv,
            "LoadingCollectionCounts"sv,
            "EstimateDatabaseItemCount"sv,
            "LoadPreparedSyncWrites"sv,
            "PopulateVBucketMap"sv,
            "KeyDump"sv,
            "CheckForAccessLog"sv,
            "LoadingAccessLog"sv,
            "LoadingKVPairs"sv,
            "LoadingData"sv,
            "Done"sv,
    };

    // create a collector to which stats will be added
    StrictMock<MockStatCollector> collector;

    // check for all the expected states
    for (const auto& label : expectedLabels) {
        EXPECT_CALL(collector,
                    addStat(MetricFamilyMatcher("ep_warmup_status"),
                            Matcher<bool>(_),
                            Contains(Pair("state"sv, label))));
    }

    resetEngineAndWarmup();
    engine->getKVBucket()->getPrimaryWarmup()->addStatusMetrics(collector);
}

TEST_F(StatTest, CBStatsScopeCollectionPrefix) {
    // Confirm that CBStatCollector correctly prepends a
    //  scopeID:
    // or
    //  scopeID:collectionID:
    // prefix for scope and collection stats, respectively.

    using namespace std::string_view_literals;
    using namespace testing;

    auto cookie = create_mock_cookie(engine.get());

    // mock addStatFn
    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    auto cbFunc = cb.AsStdFunction();
    // create a collector to which stats will be added
    CBStatCollector collector(cbFunc, *cookie);

    auto bucket = collector.forBucket("BucketName");
    auto scope = bucket.forScope("scope-name", ScopeID(0x0));
    auto collection = scope.forCollection("collection-name", CollectionID(0x8));

    cb::stats::StatDef statDef("foo");

    InSequence s;

    // test a string stat
    EXPECT_CALL(cb, Call("foo"sv, _, _));
    bucket.addStat(statDef, "value");

    EXPECT_CALL(cb, Call("0x0:foo"sv, _, _));
    scope.addStat(statDef, "value");

    EXPECT_CALL(cb, Call("0x0:0x8:foo"sv, _, _));
    collection.addStat(statDef, "value");

    destroy_mock_cookie(cookie);
}

TEST_F(StatTest, CBStatsNameSeparateFromEnum) {
    // Confirm that CBStatCollector uses the correct component
    // of the stat definition for

    using namespace std::string_view_literals;
    using namespace testing;

    auto cookie = create_mock_cookie(engine.get());

    // mock addStatFn
    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    auto cbFunc = cb.AsStdFunction();
    // create a collector to which stats will be added
    CBStatCollector collector(cbFunc, *cookie);

    auto bucket = collector.forBucket("BucketName");
    InSequence s;

    // tests that audit_enabled uses the specified cbstat name rather than
    // "enabled"
    EXPECT_CALL(cb, Call("enabled"sv, _, _));
    bucket.addStat(cb::stats::Key::audit_enabled, "value");

    // tests that total_resp_errors falls back on the enum key
    EXPECT_CALL(cb, Call("total_resp_errors"sv, _, _));
    bucket.addStat(cb::stats::Key::total_resp_errors, "value");

    destroy_mock_cookie(cookie);
}

TEST_F(StatTest, LegacyStatsAreNotFormatted) {
    // Test that adding stats via add_casted_stat (i.e., like code which has not
    // been migrated to directly adding stats to a collector) does not attempt
    // to format the stat key, as it may contain user data with invalid fmt
    // specifiers. It should also not throw if that is the case.

    using namespace std::string_view_literals;
    using namespace testing;

    auto cookie = create_mock_cookie(engine.get());

    // mock addStatFn
    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    auto cbFunc = cb.AsStdFunction();

    CBStatCollector collector(cbFunc, *cookie);

    auto bucket = collector.forBucket("BucketName");
    InSequence s;

    using namespace cb::stats;

    // First, confirm directly adding the stat to the collector _does_
    // format it.
    EXPECT_CALL(cb, Call("BucketName:some_stat_key"sv, _, _));
    bucket.addStat(CBStatsKey("{bucket}:some_stat_key",
                              CBStatsKey::NeedsFormattingTag{}),
                   "value");

    // but add_casted_stat does _not_.
    EXPECT_CALL(cb, Call("{bucket}:some_stat_key"sv, _, _));
    add_casted_stat("{bucket}:some_stat_key", "value", cbFunc, *cookie);

    destroy_mock_cookie(cookie);
}

TEST_F(StatTest, WarmupStats) {
    resetEngineAndWarmup();

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    auto cbFunc = cb.AsStdFunction();

    using namespace cb::stats;
    EXPECT_CALL(cb, Call("ep_warmup"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_thread"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_oom"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_dups"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_time"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_state"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_key_count"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_value_count"sv, _, _));
    EXPECT_CALL(cb, Call("ep_primary_warmup_min_memory_threshold"sv, _, _));
    EXPECT_CALL(cb, Call("ep_primary_warmup_min_items_threshold"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_keys_time"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_estimated_key_count"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_estimated_value_count"sv, _, _));

    // Depending on warmup timings we may/may not see a stat call for
    // ep_warmup_estimate_time.
    EXPECT_CALL(cb, Call("ep_warmup_estimate_time"sv, _, _)).Times(AtMost(1));

    CBStatCollector collector(cbFunc, *cookie);
    store->getPrimaryWarmup()->addStats(collector);
}

TEST_F(StatTest, EngineStatsWarmup) {
    resetEngineAndWarmup();

    using namespace std::string_view_literals;
    using namespace testing;

    // create a collector to which stats will be added
    NiceMock<MockFunction<void(
            std::string_view, std::string_view, CookieIface&)>>
            cb;

    auto cbFunc = cb.AsStdFunction();

    EXPECT_CALL(cb, Call(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(cb, Call("ep_warmup_thread"sv, _, _));
    EXPECT_CALL(cb, Call("ep_warmup_oom"sv, _, _))
            .Times(1)
            .RetiresOnSaturation();
    EXPECT_CALL(cb, Call("ep_warmup_dups"sv, _, _))
            .Times(1)
            .RetiresOnSaturation();
    EXPECT_CALL(cb, Call("ep_warmup_time"sv, _, _))
            .Times(1)
            .RetiresOnSaturation();

    CBStatCollector collector(cbFunc, *cookie);
    auto bucketCollector = collector.forBucket("");
    engine->doEngineStats(bucketCollector, nullptr);
}

TEST_P(ParameterizedStatTest, DiskInfoStatsAfterWarmup) {
    using namespace cb::durability;
    using namespace testing;

    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Write some items
    int num_keys = 2;
    for (int ii = 0; ii < num_keys; ++ii) {
        const auto key = makeStoredDocKey(fmt::format("key {}", ii));
        auto item = make_item(vbid,
                              key,
                              {} /*value*/,
                              0 /*exptime*/,
                              PROTOCOL_BINARY_RAW_BYTES);

        uint64_t cas = 0;
        ASSERT_EQ(cb::engine_errc::success,
                  engine->store(*cookie,
                                item,
                                cas,
                                StoreSemantics::Set,
                                {},
                                DocumentState::Alive,
                                false));

        // Delete some items to bump up the on_disk_deletes
        if (ii % 2 == 0) {
            mutation_descr_t mutInfo;
            ASSERT_EQ(cb::engine_errc::success,
                      engine->remove(*cookie, key, cas, vbid, {}, mutInfo));
        }
    }

    // Write a pending item to bump up ep_db_prepare_size
    auto pendingKey = makeStoredDocKey("pending");
    auto pendingItem = makePendingItem(pendingKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*pendingItem, cookie));

    dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);

    auto stats = get_stat("diskinfo");
    EXPECT_LT(0, std::stoi(stats["ep_db_file_size"]));
    EXPECT_LT(0, std::stoi(stats["ep_db_data_size"]));

    if (!hasMagma()) {
        // Magma does not track on-disk-prepare-bytes, see MB-42900 for details
        EXPECT_LT(0, std::stoi(stats["ep_db_prepare_size"]));
        // Magma always returns 0 from getNumPersistedDeletes
        auto onDiskDeletes = get_stat(fmt::format(
                "dcp-vbtakeover {}", vbid.get()))["on_disk_deletes"];
        EXPECT_LT(0, std::stoi(onDiskDeletes));
    }

    resetEngineAndWarmup();

    // Check the disk stats again after warmup
    // MB-53829: these would be 0 after warmup
    auto newStats = get_stat("diskinfo");
    EXPECT_LT(0, std::stoi(newStats["ep_db_file_size"]));
    EXPECT_LT(0, std::stoi(newStats["ep_db_data_size"]));

    if (!hasMagma()) {
        // Magma does not track on-disk-prepare-bytes, see MB-42900 for details
        EXPECT_LT(0, std::stoi(newStats["ep_db_prepare_size"]));
        // Magma always returns 0 from getNumPersistedDeletes
        auto onDiskDeletes = get_stat(fmt::format(
                "dcp-vbtakeover {}", vbid.get()))["on_disk_deletes"];
        EXPECT_LT(0, std::stoi(onDiskDeletes));
    }
}

TEST_P(ParameterizedStatTest, DiskSlownessArg) {
    EXPECT_NO_THROW(get_stat("disk-slowness 0", {}, true))
            << "disk-slowness accepts an integer argument!";
    EXPECT_NO_THROW(get_stat("disk-slowness 100000000", {}, true))
            << "disk-slowness accepts an integer argument!";
    EXPECT_THROW(get_stat("disk-slowness", {}, true), cb::engine_error)
            << "disk-slowness requires an argument!";
    EXPECT_THROW(get_stat("disk-slowness asdasd", {}, true), cb::engine_error)
            << "disk-slowness requires an integer argument!";
}

TEST_P(ParameterizedStatTest, DiskSlownessNoSlowOp) {
    auto stats = get_stat("disk-slowness 3", {}, true);
    EXPECT_EQ(4, stats.size());
    EXPECT_EQ("0", stats.at("pending_disk_read_num"));
    EXPECT_EQ("0", stats.at("pending_disk_read_slow_num"));
    EXPECT_EQ("0", stats.at("pending_disk_write_num"));
    EXPECT_EQ("0", stats.at("pending_disk_write_slow_num"));
}

TEST_P(ParameterizedStatTest, DiskSlownessSlowRead) {
    // Swap out the global tracker with one local to the test.
    FileOpsTracker tracker;
    engine->fileOpsTracker = &tracker;

    // Simulate read starting on a TaskType::Reader.
    LambdaTask readTask(engine->getTaskable(),
                        TaskId::MultiBGFetcherTask,
                        0,
                        false,
                        [&tracker](auto&) {
                            tracker.start(FileOp::read(11));
                            return false;
                        });
    readTask.execute("");

    {
        auto stats = get_stat("disk-slowness 0", {}, true);
        EXPECT_EQ(4, stats.size());
        EXPECT_EQ("1", stats.at("pending_disk_read_num"));
        EXPECT_EQ("1", stats.at("pending_disk_read_slow_num"));
        EXPECT_EQ("0", stats.at("pending_disk_write_num"));
        EXPECT_EQ("0", stats.at("pending_disk_write_slow_num"));
    }
}

TEST_P(ParameterizedStatTest, DiskSlownessSlowReadAndWrite) {
    // Swap out the global tracker with one local to the test.
    FileOpsTracker tracker;
    engine->fileOpsTracker = &tracker;

    // Simulate reads and writes starting on TaskType::Writer.
    auto fakeOperationOnThread = [this, &tracker](FileOp::Type type) {
        folly::Baton<> waitForStart;
        auto joinGuard = makeLingeringThread([&]() {
            LambdaTask writeTask(engine->getTaskable(),
                                 TaskId::FlusherTask,
                                 0,
                                 false,
                                 [&tracker, &type](auto&) {
                                     tracker.start(FileOp(type));
                                     return false;
                                 });
            writeTask.execute("");
            waitForStart.post();
        });
        waitForStart.wait();
        return joinGuard;
    };

    std::array threads{fakeOperationOnThread(FileOp::Type::Read),
                       fakeOperationOnThread(FileOp::Type::Read),
                       fakeOperationOnThread(FileOp::Type::Write)};

    {
        auto stats = get_stat("disk-slowness 0", {}, true);
        EXPECT_EQ(4, stats.size());
        EXPECT_EQ("2", stats.at("pending_disk_read_num"));
        EXPECT_EQ("2", stats.at("pending_disk_read_slow_num"));
        EXPECT_EQ("1", stats.at("pending_disk_write_num"));
        EXPECT_EQ("1", stats.at("pending_disk_write_slow_num"));
    }
}

TEST_P(ParameterizedStatTest, DiskSlownessSlowOpNonReadWriteThread) {
    // Swap out the global tracker with one local to the test.
    FileOpsTracker tracker;
    engine->fileOpsTracker = &tracker;

    // Simulate read starting on a TaskType::NonIO. Make sure we don't count
    // those.
    LambdaTask nonIoTask(engine->getTaskable(),
                         TaskId::ItemPager,
                         0,
                         false,
                         [&tracker](auto&) {
                             tracker.start(FileOp::read(11));
                             return false;
                         });
    nonIoTask.execute("");

    {
        auto stats = get_stat("disk-slowness 0", {}, true);
        EXPECT_EQ(4, stats.size());
        EXPECT_EQ("0", stats.at("pending_disk_read_num"))
                << "Expected only the slow operation in the reader task to be "
                   "counted.";
    }
}

TEST_P(DatatypeStatTest, datatypesInitiallyZero) {
    // Check that the datatype stats initialise to 0
    auto vals = get_stat();
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_snappy"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_snappy,json"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_snappy,xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json,xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_raw"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_snappy,json,xattr"]));

    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_snappy"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_snappy,json"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_snappy,xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_json"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_json,xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_raw"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_replica_datatype_snappy,json,xattr"]));
}

static void setDatatypeItem(KVBucket* store,
                            CookieIface* cookie,
                            protocol_binary_datatype_t datatype,
                            std::string_view name,
                            const std::string& val = "[0]") {
    Item item(make_item(
            Vbid(0), {name, DocKeyEncodesCollectionId::No}, val, 0, datatype));
    store->set(item, cookie);
}

TEST_P(DatatypeStatTest, datatypeJsonToXattr) {
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_JSON, "jsonDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json"]));

    // Check that updating an items datatype works
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR, "jsonDoc");
    vals = get_stat();

    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json"]));
}

TEST_P(DatatypeStatTest, datatypeRawStatTest) {
    setDatatypeItem(store, cookie, 0, "rawDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_raw"]));
}

TEST_P(DatatypeStatTest, datatypeXattrStatTest) {
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR, "xattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
    // Update the same key with a different value. The datatype stat should
    // stay the same
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR,
                    "xattrDoc", "[2]");
    vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedStatTest) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_SNAPPY,
                    "compressedDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedJson) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY,
            "jsonCompressedDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy,json"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedXattr) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_XATTR |
                            PROTOCOL_BINARY_DATATYPE_SNAPPY,
                    "xattrCompressedDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeJsonXattr) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeDeletion) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    store->deleteItem({"jsonXattrDoc", DocKeyEncodesCollectionId::No},
                      cas,
                      Vbid(0),
                      cookie,
                      {},
                      nullptr,
                      mutation_descr);
    vals = get_stat();
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedJsonXattr) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_JSON |
                            PROTOCOL_BINARY_DATATYPE_SNAPPY |
                            PROTOCOL_BINARY_DATATYPE_XATTR,
                    "jsonCompressedXattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy,json,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeExpireItem) {
    Item item(make_item(Vbid(0),
                        {"expiryDoc", DocKeyEncodesCollectionId::No},
                        "[0]",
                        1,
                        PROTOCOL_BINARY_DATATYPE_JSON));
    store->set(item, cookie);
    store->get({"expiryDoc", DocKeyEncodesCollectionId::No},
               Vbid(0),
               cookie,
               NONE);
    auto vals = get_stat();

    //Should be 0, becuase the doc should have expired
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json"]));
}


TEST_P(DatatypeStatTest, datatypeEviction) {
    const DocKeyView key = {"jsonXattrDoc", DocKeyEncodesCollectionId::No};
    Vbid vbid = Vbid(0);
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    getEPBucket().flushVBucket(vbid);
    const char* msg;
    store->evictKey(key, vbid, &msg);
    vals = get_stat();
    if (GetParam() == "value_only"){
        // Should still be 1 as only value is evicted
        EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    } else {
        // Should be 0 as everything is evicted
        EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json,xattr"]));
    }

    store->get(key, vbid, cookie, QUEUE_BG_FETCH);
    if (GetParam() == "full_eviction") {
        // Run the bgfetch to restore the item from disk
        runBGFetcherTask();
    }
    vals = get_stat();
    // The item should be restored to memory, hence added back to the stats
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

class EPStatsIntrospector {
public:
    static void public_setMaxDataSize(EPStats& stats, int value) {
       stats.maxDataSize.store(value);
    }

    static size_t public_getMaxDataSize(EPStats& stats) {
        return stats.maxDataSize;
    }
};
class EPStatsTest : public ::testing::Test {
protected:
    EPStats stats;

    // Non-default positive counter value to set before checking if reset
    static constexpr int nonDefaultCounterValue = 10;
    // Value to add to histograms before we check if they're reset
    static constexpr int datapoint = 20;
    // Non-default boolean value
    static constexpr bool nonDefaultBool = true;

    // Default/reset value to compare all counters to (inc. those not explicitly
    // of type Counter), except for histograms
    static constexpr int initializedValue = 0;
    static constexpr bool initializedBool = false;

    // Variables that hold the appropriate value depending on whether we are
    // testing equality after initialization or testing that they were not reset
    // (the current value below)
    int nonStatValue = nonDefaultCounterValue;
    bool nonStatBool = nonDefaultBool;

    void setStatsToNonDefaultValues() {
        stats.warmedUpKeys.store(nonDefaultCounterValue);
        stats.warmedUpValues.store(nonDefaultCounterValue);
        stats.warmedUpPrepares.store(nonDefaultCounterValue);
        stats.warmupItemsVisitedWhilstLoadingPrepares.store(
                nonDefaultCounterValue);
        stats.warmDups.store(nonDefaultCounterValue);
        stats.warmOOM.store(nonDefaultCounterValue);
        stats.flusher_todo.store(nonDefaultCounterValue);
        stats.flusherCommits.store(nonDefaultCounterValue);
        stats.cumulativeFlushTime.store(nonDefaultCounterValue);
        stats.cumulativeCommitTime.store(nonDefaultCounterValue);
        stats.totalPersisted.store(nonDefaultCounterValue);
        stats.flushFailed.store(nonDefaultCounterValue);
        stats.flushExpired.store(nonDefaultCounterValue);
        stats.expired_access.store(nonDefaultCounterValue);
        stats.expired_compactor.store(nonDefaultCounterValue);
        stats.expired_pager.store(nonDefaultCounterValue);
        stats.beginFailed.store(nonDefaultCounterValue);
        stats.commitFailed.store(nonDefaultCounterValue);
        stats.vbucketDeletions.store(nonDefaultCounterValue);
        stats.vbucketDeletionFail.store(nonDefaultCounterValue);
        stats.mem_low_wat.store(nonDefaultCounterValue);
        stats.mem_high_wat.store(nonDefaultCounterValue);
        stats.memFreedByCheckpointItemExpel.store(nonDefaultCounterValue);
        stats.forceShutdown.store(nonDefaultBool);
        stats.bg_meta_fetched.store(nonDefaultCounterValue);
        stats.numRemainingBgItems.store(nonDefaultCounterValue);
        stats.numRemainingBgJobs.store(nonDefaultCounterValue);
        stats.numOpsStore.store(nonDefaultCounterValue);
        stats.numOpsDelete.store(nonDefaultCounterValue);
        stats.numOpsGet.store(nonDefaultCounterValue);
        stats.numOpsGetMeta.store(nonDefaultCounterValue);
        stats.numOpsSetMeta.store(nonDefaultCounterValue);
        stats.numOpsDelMeta.store(nonDefaultCounterValue);
        stats.numOpsSetMetaResolutionFailed.store(nonDefaultCounterValue);
        stats.numOpsDelMetaResolutionFailed.store(nonDefaultCounterValue);
        stats.numOpsSetRetMeta.store(nonDefaultCounterValue);
        stats.numOpsDelRetMeta.store(nonDefaultCounterValue);
        stats.numOpsGetMetaOnSetWithMeta.store(nonDefaultCounterValue);
        stats.numInvalidCas.store(nonDefaultCounterValue);
        stats.numCasRegenerated.store(nonDefaultCounterValue);
        stats.alogNumItems.store(nonDefaultCounterValue);
        stats.alogTime.store(nonDefaultCounterValue);
        stats.alogRuntime.store(nonDefaultCounterValue);
        stats.expPagerTime.store(nonDefaultCounterValue);
        stats.isShutdown.store(nonDefaultBool);
        stats.rollbackCount.store(nonDefaultCounterValue);
        stats.defragStoredValueNumMoved.store(nonDefaultCounterValue);

        stats.dirtyAgeHisto.addValue(datapoint);
        stats.diskCommitHisto.addValue(datapoint);

        stats.timingLog = std::make_unique<std::stringstream>();
        EPStatsIntrospector::public_setMaxDataSize(stats,
                                                   nonDefaultCounterValue);

        stats.totalPersistVBState.store(nonDefaultCounterValue);
        stats.commit_time.store(nonDefaultCounterValue);
        stats.cursorsDropped.store(nonDefaultCounterValue);
        stats.memFreedByCheckpointRemoval.store(nonDefaultCounterValue);
        stats.pagerRuns.store(nonDefaultCounterValue);
        stats.expiryPagerRuns.store(nonDefaultCounterValue);
        stats.freqDecayerRuns.store(nonDefaultCounterValue);
        stats.itemsExpelledFromCheckpoints.store(nonDefaultCounterValue);
        stats.itemsRemovedFromCheckpoints.store(nonDefaultCounterValue);
        stats.numValueEjects.store(nonDefaultCounterValue);
        stats.numFailedEjects.store(nonDefaultCounterValue);
        stats.numNotMyVBuckets.store(nonDefaultCounterValue);
        stats.inactiveHTMemory.store(nonDefaultCounterValue);
        stats.inactiveCheckpointOverhead.store(nonDefaultCounterValue);
        stats.bg_fetched.store(nonDefaultCounterValue);
        stats.bgNumOperations.store(nonDefaultCounterValue);
        stats.bgWait.store(nonDefaultCounterValue);
        stats.bgLoad.store(nonDefaultCounterValue);
        stats.oom_errors.store(nonDefaultCounterValue);
        stats.tmp_oom_errors.store(nonDefaultCounterValue);
        stats.pendingOps.store(nonDefaultCounterValue);
        stats.pendingOpsTotal.store(nonDefaultCounterValue);
        stats.pendingOpsMax.store(nonDefaultCounterValue);
        stats.pendingOpsMaxDuration.store(nonDefaultCounterValue);
        stats.vbucketDelMaxWalltime.store(nonDefaultCounterValue);
        stats.vbucketDelTotWalltime.store(nonDefaultCounterValue);
        stats.alogRuns.store(nonDefaultCounterValue);
        stats.accessScannerSkips.store(nonDefaultCounterValue);
        stats.defragNumVisited.store(nonDefaultCounterValue);
        stats.defragNumMoved.store(nonDefaultCounterValue);
        stats.compressorNumVisited.store(nonDefaultCounterValue);
        stats.compressorNumCompressed.store(nonDefaultCounterValue);

        stats.pendingOpsHisto.addValue(datapoint);
        stats.bgWaitHisto.addValue(datapoint);
        stats.bgLoadHisto.addValue(datapoint);
        stats.setWithMetaHisto.addValue(datapoint);
        stats.accessScannerHisto.addValue(datapoint);
        stats.checkpointRemoverHisto.addValue(datapoint);
        stats.itemPagerHisto.addValue(datapoint);
        stats.expiryPagerHisto.addValue(datapoint);
        stats.getVbucketCmdHisto.addValue(datapoint);
        stats.setVbucketCmdHisto.addValue(datapoint);
        stats.delVbucketCmdHisto.addValue(datapoint);
        stats.getCmdHisto.addValue(datapoint);
        stats.storeCmdHisto.addValue(datapoint);
        stats.arithCmdHisto.addValue(datapoint);
        stats.notifyIOHisto.addValue(datapoint);
        stats.getStatsCmdHisto.addValue(datapoint);
        stats.seqnoPersistenceHisto.addValue(datapoint);
        stats.diskInsertHisto.addValue(datapoint);
        stats.diskUpdateHisto.addValue(datapoint);
        stats.diskDelHisto.addValue(datapoint);
        stats.diskVBDelHisto.addValue(datapoint);
        stats.diskCommitHisto.addValue(datapoint);
        stats.itemAllocSizeHisto.addValue(datapoint);
        stats.getMultiBatchSizeHisto.addValue(datapoint);
        stats.dirtyAgeHisto.addValue(datapoint);
        stats.persistenceCursorGetItemsHisto.addValue(datapoint);
        stats.dcpCursorsGetItemsHisto.addValue(datapoint);

        stats.activeOrPendingFrequencyValuesEvictedHisto.addValue(datapoint);
        stats.replicaFrequencyValuesEvictedHisto.addValue(datapoint);
        stats.activeOrPendingFrequencyValuesSnapshotHisto.addValue(datapoint);
        stats.replicaFrequencyValuesSnapshotHisto.addValue(datapoint);
        for (auto& hist : stats.syncWriteCommitTimes) {
            hist.addValue(datapoint);
        }
    }

    void checkStatsAreDefaultValues(bool initialization) {
        // All non-stats should be equal to the default value ONLY when this is
        // initialization (e.g., non-stats should be initialized to
        // initializedValue/Bool, but *not* reset from nonDefaultValue/Bool)
        if (initialization) {
            nonStatValue = initializedValue;
            nonStatBool = initializedBool;

            EXPECT_EQ(nullptr, stats.timingLog);
            EXPECT_EQ(std::numeric_limits<size_t>::max(),
                      EPStatsIntrospector::public_getMaxDataSize(stats));
        } else {
            EXPECT_NE(nullptr, stats.timingLog);
            EXPECT_EQ(nonStatValue,
                      EPStatsIntrospector::public_getMaxDataSize(stats));
        }

        EXPECT_EQ(nonStatValue, stats.mem_low_wat);
        EXPECT_EQ(nonStatValue, stats.mem_high_wat);
        EXPECT_EQ(nonStatBool, stats.forceShutdown);
        EXPECT_EQ(nonStatValue, stats.alogTime);
        EXPECT_EQ(nonStatValue, stats.expPagerTime);
        EXPECT_EQ(nonStatBool, stats.isShutdown);
        EXPECT_EQ(nonStatValue, stats.pendingOps);
        EXPECT_EQ(nonStatValue, stats.numRemainingBgItems);
        EXPECT_EQ(nonStatValue, stats.numRemainingBgJobs);
        EXPECT_EQ(nonStatValue, stats.inactiveCheckpointOverhead);
        EXPECT_EQ(nonStatValue, stats.inactiveHTMemory);
        EXPECT_EQ(nonStatValue, stats.flusher_todo);

        EXPECT_EQ(initializedValue, stats.warmedUpKeys);
        EXPECT_EQ(initializedValue, stats.warmedUpValues);
        EXPECT_EQ(initializedValue, stats.warmedUpPrepares);
        EXPECT_EQ(initializedValue,
                  stats.warmupItemsVisitedWhilstLoadingPrepares);
        EXPECT_EQ(initializedValue, stats.warmDups);
        EXPECT_EQ(initializedValue, stats.warmOOM);
        EXPECT_EQ(initializedValue, stats.flusherCommits);
        EXPECT_EQ(initializedValue, stats.cumulativeFlushTime);
        EXPECT_EQ(initializedValue, stats.cumulativeCommitTime);
        EXPECT_EQ(initializedValue, stats.totalPersisted);
        EXPECT_EQ(initializedValue, stats.flushFailed);
        EXPECT_EQ(initializedValue, stats.flushExpired);
        EXPECT_EQ(initializedValue, stats.expired_access);
        EXPECT_EQ(initializedValue, stats.expired_compactor);
        EXPECT_EQ(initializedValue, stats.expired_pager);
        EXPECT_EQ(initializedValue, stats.beginFailed);
        EXPECT_EQ(initializedValue, stats.commitFailed);
        EXPECT_EQ(initializedValue, stats.vbucketDeletions);
        EXPECT_EQ(initializedValue, stats.vbucketDeletionFail);
        EXPECT_EQ(initializedValue, stats.memFreedByCheckpointItemExpel);
        EXPECT_EQ(initializedValue, stats.bg_meta_fetched);
        EXPECT_EQ(initializedValue, stats.numOpsStore);
        EXPECT_EQ(initializedValue, stats.numOpsDelete);
        EXPECT_EQ(initializedValue, stats.numOpsGet);
        EXPECT_EQ(initializedValue, stats.numOpsGetMeta);
        EXPECT_EQ(initializedValue, stats.numOpsSetMeta);
        EXPECT_EQ(initializedValue, stats.numOpsDelMeta);
        EXPECT_EQ(initializedValue, stats.numOpsSetMetaResolutionFailed);
        EXPECT_EQ(initializedValue, stats.numOpsDelMetaResolutionFailed);
        EXPECT_EQ(initializedValue, stats.numOpsSetRetMeta);
        EXPECT_EQ(initializedValue, stats.numOpsDelRetMeta);
        EXPECT_EQ(initializedValue, stats.numOpsGetMetaOnSetWithMeta);
        EXPECT_EQ(initializedValue, stats.numInvalidCas);
        EXPECT_EQ(initializedValue, stats.numCasRegenerated);
        EXPECT_EQ(initializedValue, stats.alogNumItems);
        EXPECT_EQ(initializedValue, stats.alogRuntime);
        EXPECT_EQ(initializedValue, stats.rollbackCount);
        EXPECT_EQ(initializedValue, stats.defragStoredValueNumMoved);

        EXPECT_TRUE(stats.dirtyAgeHisto.isEmpty());
        EXPECT_TRUE(stats.diskCommitHisto.isEmpty());

        EXPECT_EQ(initializedValue, stats.totalPersistVBState);
        EXPECT_EQ(initializedValue, stats.commit_time);
        EXPECT_EQ(initializedValue, stats.cursorsDropped);
        EXPECT_EQ(initializedValue, stats.memFreedByCheckpointRemoval);
        EXPECT_EQ(initializedValue, stats.pagerRuns);
        EXPECT_EQ(initializedValue, stats.expiryPagerRuns);
        EXPECT_EQ(initializedValue, stats.freqDecayerRuns);
        EXPECT_EQ(initializedValue, stats.itemsExpelledFromCheckpoints);
        EXPECT_EQ(initializedValue, stats.itemsRemovedFromCheckpoints);
        EXPECT_EQ(initializedValue, stats.numValueEjects);
        EXPECT_EQ(initializedValue, stats.numFailedEjects);
        EXPECT_EQ(initializedValue, stats.numNotMyVBuckets);
        EXPECT_EQ(initializedValue, stats.bg_fetched);
        EXPECT_EQ(initializedValue, stats.bgNumOperations);
        EXPECT_EQ(initializedValue, stats.bgWait);
        EXPECT_EQ(initializedValue, stats.bgLoad);
        EXPECT_EQ(initializedValue, stats.oom_errors);
        EXPECT_EQ(initializedValue, stats.tmp_oom_errors);
        EXPECT_EQ(initializedValue, stats.pendingOpsTotal);
        EXPECT_EQ(initializedValue, stats.pendingOpsMax);
        EXPECT_EQ(initializedValue, stats.pendingOpsMaxDuration);
        EXPECT_EQ(initializedValue, stats.vbucketDelMaxWalltime);
        EXPECT_EQ(initializedValue, stats.vbucketDelTotWalltime);
        EXPECT_EQ(initializedValue, stats.alogRuns);
        EXPECT_EQ(initializedValue, stats.accessScannerSkips);
        EXPECT_EQ(initializedValue, stats.defragNumVisited);
        EXPECT_EQ(initializedValue, stats.defragNumMoved);
        EXPECT_EQ(initializedValue, stats.compressorNumVisited);
        EXPECT_EQ(initializedValue, stats.compressorNumCompressed);

        // Histograms
        EXPECT_TRUE(stats.pendingOpsHisto.isEmpty());
        EXPECT_TRUE(stats.bgWaitHisto.isEmpty());
        EXPECT_TRUE(stats.bgLoadHisto.isEmpty());
        EXPECT_TRUE(stats.setWithMetaHisto.isEmpty());
        EXPECT_TRUE(stats.accessScannerHisto.isEmpty());
        EXPECT_TRUE(stats.checkpointRemoverHisto.isEmpty());
        EXPECT_TRUE(stats.itemPagerHisto.isEmpty());
        EXPECT_TRUE(stats.expiryPagerHisto.isEmpty());
        EXPECT_TRUE(stats.getVbucketCmdHisto.isEmpty());
        EXPECT_TRUE(stats.setVbucketCmdHisto.isEmpty());
        EXPECT_TRUE(stats.delVbucketCmdHisto.isEmpty());
        EXPECT_TRUE(stats.getCmdHisto.isEmpty());
        EXPECT_TRUE(stats.storeCmdHisto.isEmpty());
        EXPECT_TRUE(stats.arithCmdHisto.isEmpty());
        EXPECT_TRUE(stats.notifyIOHisto.isEmpty());
        EXPECT_TRUE(stats.getStatsCmdHisto.isEmpty());
        EXPECT_TRUE(stats.seqnoPersistenceHisto.isEmpty());
        EXPECT_TRUE(stats.diskInsertHisto.isEmpty());
        EXPECT_TRUE(stats.diskUpdateHisto.isEmpty());
        EXPECT_TRUE(stats.diskDelHisto.isEmpty());
        EXPECT_TRUE(stats.diskVBDelHisto.isEmpty());
        EXPECT_TRUE(stats.diskCommitHisto.isEmpty());
        EXPECT_TRUE(stats.itemAllocSizeHisto.isEmpty());
        EXPECT_TRUE(stats.getMultiBatchSizeHisto.isEmpty());
        EXPECT_TRUE(stats.dirtyAgeHisto.isEmpty());
        EXPECT_TRUE(stats.persistenceCursorGetItemsHisto.isEmpty());
        EXPECT_TRUE(stats.dcpCursorsGetItemsHisto.isEmpty());

        EXPECT_TRUE(stats.activeOrPendingFrequencyValuesEvictedHisto.isEmpty());
        EXPECT_TRUE(stats.replicaFrequencyValuesEvictedHisto.isEmpty());
        EXPECT_TRUE(
                stats.activeOrPendingFrequencyValuesSnapshotHisto.isEmpty());
        EXPECT_TRUE(stats.replicaFrequencyValuesSnapshotHisto.isEmpty());

        for (auto& hist : stats.syncWriteCommitTimes) {
            EXPECT_TRUE(hist.isEmpty());
        }

        // These stats come from CoreLocal accumulation and are zero
        EXPECT_EQ(0, stats.getDiskQueueSize());
        EXPECT_EQ(0, stats.getTotalEnqueued());
    }
};

TEST_F(EPStatsTest, testEPStatsInitialized) {
    checkStatsAreDefaultValues(true);
}

TEST_F(EPStatsTest, testEPStatsReset) {
    setStatsToNonDefaultValues();
    stats.reset();
    checkStatsAreDefaultValues(false);
}

TEST_P(DatatypeStatTest, MB23892) {
    // This test checks that updating a document with a different datatype is
    // safe to do after an eviction (where the blob is now null)
    const DocKeyView key = {"jsonXattrDoc", DocKeyEncodesCollectionId::No};
    Vbid vbid = Vbid(0);
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat();
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    getEPBucket().flushVBucket(vbid);
    const char* msg;
    store->evictKey(key, vbid, &msg);
    getEPBucket().flushVBucket(vbid);
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_JSON, "jsonXattrDoc", "[1]");
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         ParameterizedStatTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        FullAndValueEviction,
        DatatypeStatTest,
        ::testing::Values("value_only", "full_eviction"),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });

TEST_F(StatTest, testConnAggStats) {
    using namespace testing;
    using namespace cb::stats;

    std::unordered_map<std::string, ::prometheus::MetricFamily> statsMap;
    PrometheusStatCollector collector(statsMap);
    auto bucketCollector = collector.forBucket("foo");
    engine->getDcpConnMap().newProducer(
            *cookie, "foo_producer", cb::mcbp::DcpOpenFlag::None);

    auto mCookie = create_mock_cookie(engine.get());
    engine->getDcpConnMap().newConsumer(*mCookie, "replication:foo_consumer");

    auto rc = engine->doConnAggStats(bucketCollector, ":");
    EXPECT_EQ(cb::engine_errc::success, rc) << "Failed to get conn agg stats.";

    // Assert all the expected connAgg stats are added.
    std::unordered_map<cb::stats::Key, std::string> expected = {
            {Key::connagg_connection_count, "dcp_connection_count"},
            {Key::connagg_backoff, "dcp_backoff"},
            {Key::connagg_producer_count, "dcp_count"},
            {Key::connagg_consumer_count, "dcp_count"},
            {Key::connagg_activestream_count, "dcp_stream_count"},
            {Key::connagg_passivestream_count, "dcp_stream_count"},
            {Key::connagg_items_backfilled_disk, "dcp_items_backfilled"},
            {Key::connagg_items_backfilled_memory, "dcp_items_backfilled"},
            {Key::connagg_items_sent, "dcp_items_sent"},
            {Key::connagg_items_remaining, "dcp_items_remaining"},
            {Key::connagg_total_uncompressed_data_size,
             "dcp_total_uncompressed_data_size_bytes"},
            {Key::connagg_total_bytes, "dcp_total_data_size_bytes"},
            {Key::connagg_ready_queue_bytes, "dcp_ready_queue_size_bytes"},
            {Key::connagg_paused, "dcp_paused_count"},
            {Key::connagg_unpaused, "dcp_unpaused_count"}};

    for (const auto& [internalKey, externalKey] : expected) {
        const auto& metricFamilyName =
                collector.lookup(internalKey).metricFamily;
        EXPECT_NE(statsMap.end(), statsMap.find(metricFamilyName));

        const auto& metricFamily = statsMap[metricFamilyName];
        EXPECT_EQ(externalKey, metricFamily.name);

        if (internalKey == Key::connagg_connection_count) {
            // One each for the producer and consumer.
            EXPECT_EQ(2, metricFamily.metric.size());
            for (const auto& metric : metricFamily.metric) {
                // The raw value set in the metric.
                EXPECT_EQ(1, metric.gauge.value);
            }
        }
    }

    std::unordered_set<std::string> expectedKeysSet;

    for (const auto& [internalKey, externalKey] : expected) {
        expectedKeysSet.insert(externalKey);
    }

    // Check only the metric families we expect are present in the statsMap.
    // Essentially this makes sure, if a new metric family is added we also
    // remember to include it here in the "expected" map.
    for (const auto& [externalKey, v] : statsMap) {
        EXPECT_NE(expectedKeysSet.end(), expectedKeysSet.find(externalKey));
    }

    destroy_mock_cookie(mCookie);
}

/// Formats the labels into a string.
std::string formatLabels(const ::prometheus::ClientMetric& metric) {
    std::string selector{"{"};
    auto it = std::back_inserter(selector);
    for (auto& label : metric.label) {
        it = fmt::format_to(it, "{}={},", label.name, label.value);
    }
    selector.push_back('}');
    return selector;
}

/// Checks that metrics in the low and high groups are unique.
TEST_F(StatTest, EngineMetricsAreUnique) {
    CollectionsManifest cm;
    // MB-64247: Ensure collection TTL is not duplcated.
    cm.add(CollectionEntry::fruit, cb::ExpiryLimit(10), true);
    setCollections(cookie, cm);

    std::unordered_map<std::string, prometheus::MetricFamily> all;
    PrometheusStatCollector collector(all);
    BucketStatCollector bucketCollector(collector, "bucket");
    engine->get_prometheus_stats(bucketCollector,
                                 cb::prometheus::MetricGroup::High);
    engine->get_prometheus_stats(bucketCollector,
                                 cb::prometheus::MetricGroup::Low);

    for (const auto& [name, family] : all) {
        // Use a set of seen labels to find duplicates.
        std::unordered_set<std::string> seenLabels;
        for (const auto& metric : family.metric) {
            auto labels = formatLabels(metric);
            EXPECT_TRUE(seenLabels.insert(labels).second)
                    << "Duplicate metric " << name << labels;
        }
    }
}

class CheckpointMemQuotaTest : public StatTest {
    void SetUp() override {
        config_string +=
                "checkpoint_memory_ratio=0.2;"
                "dcp_consumer_buffer_ratio=0.05";
        StatTest::SetUp();
    }
};

TEST_F(CheckpointMemQuotaTest, CheckpointMemQuotaStat) {
    using namespace cb::prometheus;
    using namespace cb::stats;
    std::unordered_map<std::string, prometheus::MetricFamily> statsMap;
    PrometheusStatCollector collector(statsMap);
    auto bucketCollector = collector.forBucket("foo");
    auto quota = 5 * 1024 * 1024;
    engine->setMaxDataSize(quota);

    auto rc = engine->get_prometheus_stats(bucketCollector, MetricGroup::Low);
    EXPECT_EQ(rc, cb::engine_errc::success);
    EXPECT_NE(statsMap.end(),
              statsMap.find("ep_checkpoint_consumer_limit_bytes"));
    auto metricValue = statsMap["ep_checkpoint_consumer_limit_bytes"]
                               .metric[0]
                               .gauge.value;
    auto checkpointMemRatio = store->getCheckpointMemoryRatio();
    auto consumerBufferRatio = engine->getDcpConsumerBufferRatio();
    EXPECT_EQ(quota * (checkpointMemRatio + consumerBufferRatio), metricValue);
}
