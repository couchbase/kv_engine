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
#include "item.h"
#include "kv_bucket.h"
#include "tasks.h"
#include "test_helpers.h"
#include "tests/mock/mock_stat_collector.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "thread_gate.h"
#include "trace_helpers.h"
#include <tests/mock/mock_function_helper.h>

#include <folly/portability/GMock.h>
#include <memcached/server_cookie_iface.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/units.h>

#include <functional>
#include <string_view>
#include <thread>

void StatTest::SetUp() {
    SingleThreadedEPBucketTest::SetUp();
    store->setVBucketState(vbid, vbucket_state_active);
}

std::map<std::string, std::string> StatTest::get_stat(const char* statkey) {
    // Define a lambda to use as the AddStatFn callback. Note we cannot use
    // a capture for the statistics map (as it's a C-style callback), so
    // instead pass via the cookie.
    struct StatMap : cb::tracing::Traceable {
        std::map<std::string, std::string> map;
    };
    StatMap stats;
    auto add_stats = [](std::string_view key,
                        std::string_view value,
                        gsl::not_null<const void*> cookie) {
        auto* stats =
                reinterpret_cast<StatMap*>(const_cast<void*>(cookie.get()));
        std::string k(key.data(), key.size());
        std::string v(value.data(), value.size());
        stats->map[k] = v;
    };

    EXPECT_EQ(
            cb::engine_errc::success,
            engine->get_stats(&stats,
                              {statkey, statkey == NULL ? 0 : strlen(statkey)},
                              {},
                              add_stats))
            << "Failed to get stats.";

    return stats.map;
}

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

// Test that if we request takeover stats for stream that does not exist we
// return does_not_exist.
TEST_F(StatTest, vbucket_takeover_stats_no_stream) {
    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(cookie);
    engine->getDcpConnMap().newProducer(cookie,
                                        "test_producer",
                                        /*flags*/ 0);

    const std::string stat =
            "dcp-vbtakeover " + std::to_string(vbid.get()) + " test_producer";
    ;
    auto vals = get_stat(stat.c_str());
    EXPECT_EQ("does_not_exist", vals["status"]);
    EXPECT_EQ(0, std::stoi(vals["estimate"]));
    EXPECT_EQ(0, std::stoi(vals["backfillRemaining"]));
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
    struct Cookie : public cb::tracing::Traceable {
        int addStats_calls = 0;
    } state;

    auto callback = [](std::string_view key,
                       std::string_view value,
                       gsl::not_null<const void*> cookie) {
        Cookie& state =
                *reinterpret_cast<Cookie*>(const_cast<void*>(cookie.get()));
        state.addStats_calls++;

        // This callback should run in the memcached-context so no engine should
        // be assigned to the current thread.
        EXPECT_FALSE(ObjectRegistry::getCurrentEngine());
    };

    ASSERT_EQ(cb::engine_errc::success,
              engine->get_stats(&state, key, {}, callback));

    // Sanity check - should have had at least 1 call to ADD_STATS (otherwise
    // the test isn't valid).
    ASSERT_GT(state.addStats_calls, 0);

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
            std::string_view, std::string_view, gsl::not_null<const void*>)>>
            cb;

    EXPECT_CALL(cb, Call("test_histogram_mean"sv, "2052741737"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_0,1"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_1,2"sv, "100"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_2,4"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_4,8"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_8,16"sv, "200"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_16,32"sv, "0"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_32,4294967295"sv, "6500"sv, _));

    add_casted_stat("test_histogram"sv, histogram, asStdFunction(cb), cookie);

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
            std::string_view, std::string_view, gsl::not_null<const void*>)>>
            cb;

    EXPECT_CALL(cb, Call("test_histogram_mean"sv, "9544"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_0,1"sv, "100"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_1,15"sv, "200"sv, _));
    EXPECT_CALL(cb, Call("test_histogram_15,9728"sv, "6500"sv, _));

    add_casted_stat("test_histogram"sv, histogram, asStdFunction(cb), cookie);

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
    testTypes(float(), double());
}

MATCHER_P(StatDefNameMatcher,
          expectedName,
          "Check the unique name of the StatDef matches") {
    return arg.cbstatsKey.key == expectedName;
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

    EXPECT_CALL(collector, addStat(_, Matcher<bool>(_), _)).Times(AnyNumber());

    EXPECT_CALL(collector, addStat(_, Matcher<std::string_view>(_), _))
            .Times(AnyNumber());

    auto& config = engine->getConfiguration();
    // confirm that specific stats known to be generated from the config
    // are definitely present

    // test a ssize_t stat
    auto readerThreads = config.getNumReaderThreads();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_num_reader_threads"),
                        Matcher<int64_t>(readerThreads),
                        _));

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
                        Matcher<double>(threshold),
                        _));

    // test a bool stat
    auto noop = config.isDcpEnableNoop();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_dcp_enable_noop"),
                        Matcher<bool>(noop),
                        _));

    // test a string stat
    auto policy = config.getDcpFlowControlPolicy();
    EXPECT_CALL(collector,
                addStat(StatDefNameMatcher("ep_dcp_flow_control_policy"),
                        Matcher<std::string_view>(policy),
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
    engine->doEngineStats(bucketC);
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
            std::string_view, std::string_view, gsl::not_null<const void*>)>>
            cb;

    auto cbFunc = cb.AsStdFunction();
    // create a collector to which stats will be added
    CBStatCollector collector(cbFunc, cookie, engine->getServerApi());

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
            std::string_view, std::string_view, gsl::not_null<const void*>)>>
            cb;

    auto cbFunc = cb.AsStdFunction();
    // create a collector to which stats will be added
    CBStatCollector collector(cbFunc, cookie, nullptr /* server API */);

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
            std::string_view, std::string_view, gsl::not_null<const void*>)>>
            cb;

    auto cbFunc = cb.AsStdFunction();

    CBStatCollector collector(cbFunc, cookie, nullptr /* server api */);

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
    add_casted_stat("{bucket}:some_stat_key", "value", cbFunc, cookie);

    destroy_mock_cookie(cookie);
}

TEST_P(DatatypeStatTest, datatypesInitiallyZero) {
    // Check that the datatype stats initialise to 0
    auto vals = get_stat(nullptr);
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

void setDatatypeItem(KVBucket* store,
                     const void* cookie,
                     protocol_binary_datatype_t datatype,
                     std::string name, std::string val = "[0]") {
    Item item(make_item(
            Vbid(0), {name, DocKeyEncodesCollectionId::No}, val, 0, datatype));
    store->set(item, cookie);
}

TEST_P(DatatypeStatTest, datatypeJsonToXattr) {
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_JSON, "jsonDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json"]));

    // Check that updating an items datatype works
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR, "jsonDoc");
    vals = get_stat(nullptr);

    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json"]));
}

TEST_P(DatatypeStatTest, datatypeRawStatTest) {
    setDatatypeItem(store, cookie, 0, "rawDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_raw"]));
}

TEST_P(DatatypeStatTest, datatypeXattrStatTest) {
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR, "xattrDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
    // Update the same key with a different value. The datatype stat should
    // stay the same
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_XATTR,
                    "xattrDoc", "[2]");
    vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_xattr"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedStatTest) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_SNAPPY,
                    "compressedDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedJson) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY,
            "jsonCompressedDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy,json"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedXattr) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_XATTR |
                            PROTOCOL_BINARY_DATATYPE_SNAPPY,
                    "xattrCompressedDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_snappy,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeJsonXattr) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeDeletion) {
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat(nullptr);
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
    vals = get_stat(nullptr);
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

TEST_P(DatatypeStatTest, datatypeCompressedJsonXattr) {
    setDatatypeItem(store,
                    cookie,
                    PROTOCOL_BINARY_DATATYPE_JSON |
                            PROTOCOL_BINARY_DATATYPE_SNAPPY |
                            PROTOCOL_BINARY_DATATYPE_XATTR,
                    "jsonCompressedXattrDoc");
    auto vals = get_stat(nullptr);
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
    auto vals = get_stat(nullptr);

    //Should be 0, becuase the doc should have expired
    EXPECT_EQ(0, std::stoi(vals["ep_active_datatype_json"]));
}


TEST_P(DatatypeStatTest, datatypeEviction) {
    const DocKey key = {"jsonXattrDoc", DocKeyEncodesCollectionId::No};
    Vbid vbid = Vbid(0);
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    getEPBucket().flushVBucket(vbid);
    const char* msg;
    store->evictKey(key, vbid, &msg);
    vals = get_stat(nullptr);
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
    vals = get_stat(nullptr);
    // The item should be restored to memory, hence added back to the stats
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
}

TEST_P(DatatypeStatTest, MB23892) {
    // This test checks that updating a document with a different datatype is
    // safe to do after an eviction (where the blob is now null)
    const DocKey key = {"jsonXattrDoc", DocKeyEncodesCollectionId::No};
    Vbid vbid = Vbid(0);
    setDatatypeItem(
            store,
            cookie,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
            "jsonXattrDoc");
    auto vals = get_stat(nullptr);
    EXPECT_EQ(1, std::stoi(vals["ep_active_datatype_json,xattr"]));
    getEPBucket().flushVBucket(vbid);
    const char* msg;
    store->evictKey(key, vbid, &msg);
    getEPBucket().flushVBucket(vbid);
    setDatatypeItem(store, cookie, PROTOCOL_BINARY_DATATYPE_JSON, "jsonXattrDoc", "[1]");
}

INSTANTIATE_TEST_SUITE_P(FullAndValueEviction,
                         DatatypeStatTest,
                         ::testing::Values("value_only", "full_eviction"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });
