/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * Suite of performance tests for ep-engine.
 *
 * Uses the same engine_testapp infrastructure as ep_testsuite.
 *
 * Tests print their performance metrics to stdout; to see this output when
 * run via do:
 *
 *     make test ARGS="--verbose"
 *
 * Note this is designed as a relatively quick micro-benchmark suite; tests
 * are tuned to complete in <2 seconds to maintain the quick turnaround.
**/
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "mock/mock_dcp.h"
#include "module_tests/test_helpers.h"
#include "module_tests/thread_gate.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <platform/cbassert.h>
#include <platform/platform_thread.h>
#include <platform/platform_time.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <iterator>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <type_traits>
#include <unordered_map>

// Default number of iterations for tests. Individual tests may
// override this, but is generally desirable for them to scale the
// default iteration count instead of blindly overriding it.
const size_t ITERATIONS =
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER) || defined(DEBUG)
        // Reduced iteration count for Address/ThreadSanitizer, as it runs ~20x
        // slower than without TSan.  Note: We don't actually track
        // performance when run under TSan, however the workloads of this
        // testsuite are still useful to run under TSan to expose any data
        // race issues.
        // Similary for DEBUG builds; we don't track performance, just want
        // to run to check functionality.
        100000 / 20;
#else
    // Set to a value a typical ~2015 laptop can run Baseline in 3s.
    100000;
#endif

// Key of the sentinel document, used to detect the end of a run.
const char SENTINEL_KEY[] = "__sentinel__";

enum class StatRuntime {
   Slow = true,
   Fast = false
 };

enum class BackgroundWork {
   // Performing bitwise operations and so explicitly assigning values
   None = 0x0,
   Sets = 0x1,
   Dcp = 0x2
 };

inline BackgroundWork operator | (BackgroundWork lhs, BackgroundWork rhs) {
    return static_cast<BackgroundWork> (
            std::underlying_type<BackgroundWork>::type(lhs) |
            std::underlying_type<BackgroundWork>::type(rhs));
}

inline BackgroundWork operator & (BackgroundWork lhs, BackgroundWork rhs) {
    return static_cast<BackgroundWork> (
            std::underlying_type<BackgroundWork>::type(lhs) &
            std::underlying_type<BackgroundWork>::type(rhs));
}

template<typename T>
struct Stats {
    std::string name;
    double mean;
    double median;
    double stddev;
    double pct5;
    double pct95;
    double pct99;
    std::vector<T>* values;
};

static const int iterations_for_fast_stats = 100;
static const int iterations_for_slow_stats = 10;


struct StatProperties {
     const std::string key;
     const StatRuntime runtime;
     std::vector<hrtime_t> timings;
};

const auto& make_stat_pair = std::make_pair<std::string, StatProperties>;

/**
  * The following table is used as input for each of the stats test.
  * Each map entry specifies the test name, and then a StatProperities struct,
  * which contains a key (used as input into the get_stats call),
  * an enum stating whether the stat takes a long time to return (this
  * is used to determine the number of iterations of get_stats to invoke),
  * and finally a vector containing the latency timings for the stat.
  */
std::unordered_map<std::string, StatProperties> stat_tests = {
        make_stat_pair("engine", {"", StatRuntime::Slow, {}}),
        make_stat_pair("dcpagg", {"dcpagg _", StatRuntime::Fast, {}}),
        make_stat_pair("dcp", {"dcp", StatRuntime::Fast, {}}),
        make_stat_pair("hash", {"hash", StatRuntime::Slow, {}}),
        make_stat_pair("vbucket", {"vbucket", StatRuntime::Fast, {}}),
        make_stat_pair("vb-details",
                       {"vbucket-details", StatRuntime::Slow, {}}),
        make_stat_pair("vb-details_vb0",
                       {"vbucket-details 0", StatRuntime::Fast, {}}),
        make_stat_pair("vb-seqno", {"vbucket-seqno", StatRuntime::Slow, {}}),
        make_stat_pair("vb-seqno_vb0",
                       {"vbucket-seqno 0", StatRuntime::Fast, {}}),
        make_stat_pair("prev-vbucket", {"prev-vbucket", StatRuntime::Fast, {}}),
        make_stat_pair("checkpoint", {"checkpoint", StatRuntime::Slow, {}}),
        make_stat_pair("checkpoint_vb0",
                       {"checkpoint 0", StatRuntime::Fast, {}}),
        make_stat_pair("timings", {"timings", StatRuntime::Fast, {}}),
        make_stat_pair("dispatcher", {"dispatcher", StatRuntime::Slow, {}}),
        make_stat_pair("scheduler", {"scheduler", StatRuntime::Fast, {}}),
        make_stat_pair("runtimes", {"runtimes", StatRuntime::Fast, {}}),
        make_stat_pair("memory", {"memory", StatRuntime::Fast, {}}),
        make_stat_pair("uuid", {"uuid", StatRuntime::Fast, {}}),
        // We add a document with the key __sentinel__ to vbucket 0 at the
        // start of the test and hence it is used for the key_vb0 stat.
        make_stat_pair("key_vb0", {"key example_doc 0", StatRuntime::Fast, {}}),
        make_stat_pair("kvtimings", {"kvtimings", StatRuntime::Slow, {}}),
        make_stat_pair("kvstore", {"kvstore", StatRuntime::Fast, {}}),
        make_stat_pair("info", {"info", StatRuntime::Fast, {}}),
        make_stat_pair("allocator", {"allocator", StatRuntime::Slow, {}}),
        make_stat_pair("config", {"config", StatRuntime::Fast, {}}),
        make_stat_pair("dcp-vbtakeover",
                       {"dcp-vbtakeover 0 DCP", StatRuntime::Fast, {}}),
        make_stat_pair("workload", {"workload", StatRuntime::Fast, {}}),
        make_stat_pair("failovers_vb0", {"failovers 0", StatRuntime::Fast, {}}),
        make_stat_pair("failovers", {"failovers", StatRuntime::Slow, {}}),
};

static void fillLineWith(const char c, int spaces) {
    for (int i = 0; i < spaces; ++i) {
        putchar(c);
    }
}

// Render the specified value stats, in human-readable text format.
template<typename T>
void renderToText(const std::string& name,
                  const std::string& description,
                  const std::vector<Stats<T> >& value_stats,
                  const std::string& unit) {

    printf("%s", description.c_str());
    fillLineWith('=', 88 - description.length());

    // From these find the start and end for the spark graphs which covers the
    // a "reasonable sample" of each value set. We define that as from the 5th
    // to the 95th percentile, so we ensure *all* sets have that range covered.
    T spark_start = std::numeric_limits<T>::max();
    T spark_end = 0;
    for (const auto& stats : value_stats) {
        spark_start = (stats.pct5 < spark_start) ? stats.pct5 : spark_start;
        spark_end = (stats.pct95 > spark_end) ? stats.pct95 : spark_end;
    }

    printf("\n\n                                Percentile           \n");
    printf("  %-22s Median     95th     99th  Std Dev  Histogram of samples\n\n", "");
    // Finally, print out each set.
    for (const auto& stats : value_stats) {
        if (stats.median/1e6 < 1) {
            printf("%-22s %8.03f %8.03f %8.03f %8.03f  ",
                    stats.name.c_str(), stats.median/1e3, stats.pct95/1e3,
                    stats.pct99/1e3, stats.stddev/1e3);
        } else {
            printf("%-15s (x1e3) %8.03f %8.03f %8.03f %8.03f  ",
                    stats.name.c_str(), stats.median/1e6, stats.pct95/1e6,
                    stats.pct99/1e6, stats.stddev/1e6);
        }

        // Calculate and render Sparkline (requires UTF-8 terminal).
        const int nbins = 32;
        int prev_distance = 0;
        std::vector<size_t> histogram;
        for (unsigned int bin = 0; bin < nbins; bin++) {
            const T max_for_bin = (spark_end / nbins) * bin;
            auto it = std::lower_bound(stats.values->begin(),
                                       stats.values->end(),
                                       max_for_bin);
            const int distance = std::distance(stats.values->begin(), it);
            histogram.push_back(distance - prev_distance);
            prev_distance = distance;
        }

        const auto minmax = std::minmax_element(histogram.begin(), histogram.end());
        const size_t range = *minmax.second - *minmax.first + 1;
        const int levels = 8;
        for (const auto& h : histogram) {
            int bar_size = ((h - *minmax.first + 1) * (levels - 1)) / range;
            putchar('\xe2');
            putchar('\x96');
            putchar('\x81' + bar_size);
        }
        putchar('\n');
    }
    printf("%58s  %-14d %s %14d\n\n", "",
           int(spark_start/1e3), unit.c_str(), int(spark_end/1e3));
}

template<typename T>
void renderToXML(const std::string& name, const std::string& description,
                 const std::vector<Stats<T> >& value_stats,
                 const std::string& unit) {
    std::string test_name = testHarness->output_file_prefix;
    test_name += name;
    std::ofstream file(test_name + ".xml");

    time_t now;
    time(&now);
    char timebuf[256];
    // Ideally would use 'put_time' here, but it is not supported until GCC 5
    strftime(timebuf, sizeof timebuf, "%FT%T%z", gmtime(&now));

    file << "<testsuites timestamp=\"" << timebuf << "\">\n";

    std::string classname = "ep-perfsuite";

    if (testHarness->bucket_type == "") {
        file << "  <testsuite name=\"ep-perfsuite\">\n";
    } else {
        file << "  <testsuite name=\"ep-perfsuite-" << testHarness->bucket_type
             << "\">\n";
        classname += "-" + testHarness->bucket_type;
    }

    for (const auto& stats : value_stats) {
        file << "    <testcase name=\"" << name << "." << stats.name
             << ".median\" time=\"" << stats.median / 1e3 << "\" classname=\""
             << classname << "\"/>\n"
             << "    <testcase name=\"" << name << "." << stats.name
             << ".pct95\" time=\"" << stats.pct95 / 1e3 << "\" classname=\""
             << classname << "\"/>\n"
             << "    <testcase name=\"" << name << "." << stats.name
             << ".pct99\" time=\"" << stats.pct99 / 1e3 << "\" classname=\""
             << classname << "\"/>\n";
    }
    file << "  </testsuite>\n";
    file << "</testsuites>\n";
}

// Given a vector of values (each a vector<T>) calculate metrics on them
// and print in the format specified by {testHarness->output_format}.
template<typename T>
void output_result(const std::string& name,
                   const std::string& description,
                   std::vector<std::pair<std::string, std::vector<T>* >> values,
                   std::string unit) {
    // First, calculate mean, median, standard deviation and percentiles of
    // each set of values, both for printing and to derive what the range of
    // the graphs should be.
    std::string new_name = name;
    std::replace(new_name.begin(), new_name.end(), ' ', '_');
    std::vector<Stats<T>> value_stats;
    for (const auto &t : values) {
        Stats<T> stats;
        stats.name = t.first;
        stats.values = t.second;
        std::vector <T> &vec = *t.second;

        // Calculate latency percentiles
        std::sort(vec.begin(), vec.end());
        stats.median = vec[(vec.size() * 50) / 100];
        stats.pct5 = vec[(vec.size() * 5) / 100];
        stats.pct95 = vec[(vec.size() * 95) / 100];
        stats.pct99 = vec[(vec.size() * 99) / 100];

        const double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
        stats.mean = sum / vec.size();
        double accum = 0.0;
        std::for_each (vec.begin(), vec.end(),[&](const double d) {
            accum += (d - stats.mean) * (d - stats.mean);
        });
        stats.stddev = sqrt(accum / (vec.size() - 1));

        value_stats.push_back(stats);
    }

    // Now render in the given format.
    switch (testHarness->output_format) {
    case OutputFormat::Text:
        renderToText(new_name, description, value_stats, unit);
        break;

    case OutputFormat::XML:
        renderToXML(new_name, description, value_stats, unit);
        break;
    }
}
/* Add a sentinel document (one with a the key SENTINEL_KEY).
 * This can be used by DCP streams to reliably detect the end of
 * a run (sequence numbers are only supported by DCP, and
 * de-duplication complicates simply counting mutations).
 */
static void add_sentinel_doc(EngineIface* h, Vbid vbid) {
    // Use ADD instead of SET as we only expect to mutate the sentinel
    // doc once per run.
    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         OPERATION_ADD,
                         SENTINEL_KEY,
                         nullptr,
                         0,
                         /*flags*/ 0,
                         0,
                         vbid)
                    .first,
            "Failed to add sentinel document.");
}

/*****************************************************************************
 ** Testcases
 *****************************************************************************/

/*
 * The perf_latency_core performs add/get/replace/delete against the bucket
 * associated with h/h1 parameters.
 *
 * key_prefix is used to enable multiple threads to operate on a single bucket
 * in their own key-spaces.
 * num_docs controls how many of each operation is performed.
 *
 * The elapsed time of each operation is pushed to the vector parameters.
 */
static void perf_latency_core(EngineIface* h,
                              int key_prefix,
                              int num_docs,
                              std::vector<hrtime_t>& add_timings,
                              std::vector<hrtime_t>& get_timings,
                              std::vector<hrtime_t>& replace_timings,
                              std::vector<hrtime_t>& delete_timings) {
    const void* cookie = testHarness->create_cookie(h);
    const std::string data(100, 'x');

    // Build vector of keys
    std::vector<std::string> keys;
    for (int i = 0; i < num_docs; i++) {
        keys.push_back(std::to_string(key_prefix) + std::to_string(i));
    }

    // Create (add)
    for (auto& key : keys) {
        const auto start = std::chrono::steady_clock::now();
        checkeq(cb::engine_errc::success,
                storeCasVb11(h,
                             cookie,
                             OPERATION_ADD,
                             key.c_str(),
                             data.c_str(),
                             data.length(),
                             0,
                             0,
                             Vbid(0),
                             0,
                             0)
                        .first,
                "Failed to add a value");
        const auto end = std::chrono::steady_clock::now();
        add_timings.push_back((end - start).count());
    }

    // Get
    for (auto& key : keys) {
        const auto start = std::chrono::steady_clock::now();
        auto ret = get(h, cookie, key, Vbid(0));
        checkeq(cb::engine_errc::success, ret.first, "Failed to get a value");
        const auto end = std::chrono::steady_clock::now();
        get_timings.push_back((end - start).count());
    }

    // Update (Replace)
    for (auto& key : keys) {
        const auto start = std::chrono::steady_clock::now();
        checkeq(cb::engine_errc::success,
                storeCasVb11(h,
                             cookie,
                             OPERATION_REPLACE,
                             key.c_str(),
                             data.c_str(),
                             data.length(),
                             0,
                             0,
                             Vbid(0),
                             0,
                             0)
                        .first,
                "Failed to replace a value");
        const auto end = std::chrono::steady_clock::now();
        replace_timings.push_back((end - start).count());
    }

    // Delete
    for (auto& key : keys) {
        const auto start = std::chrono::steady_clock::now();
        checkeq(ENGINE_SUCCESS,
                del(h, key.c_str(), 0, Vbid(0), cookie),
                "Failed to delete a value");
        const auto end = std::chrono::steady_clock::now();
        delete_timings.push_back((end - start).count());
    }

    testHarness->destroy_cookie(cookie);
}

static enum test_result perf_latency(EngineIface* h,
                                     const char* title,
                                     size_t num_docs) {
    // Only timing front-end performance, not considering persistence.
    stop_persistence(h);

    std::vector<hrtime_t> add_timings, get_timings,
                          replace_timings, delete_timings;
    add_timings.reserve(num_docs);
    get_timings.reserve(num_docs);
    replace_timings.reserve(num_docs);
    delete_timings.reserve(num_docs);

    std::string description(std::string("Latency [") + title + "] - " +
                            std::to_string(num_docs) + " items (µs)");

    // run and measure on this thread.
    perf_latency_core(h,
                      0,
                      num_docs,
                      add_timings,
                      get_timings,
                      replace_timings,
                      delete_timings);

    add_sentinel_doc(h, Vbid(0));

    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    all_timings.push_back(std::make_pair("Add", &add_timings));
    all_timings.push_back(std::make_pair("Get", &get_timings));
    all_timings.push_back(std::make_pair("Replace", &replace_timings));
    all_timings.push_back(std::make_pair("Delete", &delete_timings));
    output_result(title, description, all_timings, "µs");
    return SUCCESS;
}

/* Benchmark the baseline latency (without any tasks running) of ep-engine.
 */
static enum test_result perf_latency_baseline(EngineIface* h) {
    return perf_latency(h, "1_bucket_1_thread_baseline", ITERATIONS);
}

/* Benchmark the baseline latency with the defragmenter enabled.
 */
static enum test_result perf_latency_defragmenter(EngineIface* h) {
    return perf_latency(h, "With constant defragmention", ITERATIONS);
}

/* Benchmark the baseline latency with the defragmenter enabled.
 */
static enum test_result perf_latency_expiry_pager(EngineIface* h) {
    return perf_latency(h, "With constant Expiry pager", ITERATIONS);
}

class ThreadArguments {
public:
    void reserve(int n) {
        add_timings.reserve(n);
        get_timings.reserve(n);
        replace_timings.reserve(n);
        delete_timings.reserve(n);
    }

    void clear() {
        add_timings.clear();
        get_timings.clear();
        replace_timings.clear();
        delete_timings.clear();
    }

    EngineIface* h;
    int key_prefix;
    int num_docs;
    std::vector<hrtime_t> add_timings;
    std::vector<hrtime_t> get_timings;
    std::vector<hrtime_t> replace_timings;
    std::vector<hrtime_t> delete_timings;
};

extern "C" {
    static void perf_latency_thread(void *arg) {
        ThreadArguments* threadArgs = static_cast<ThreadArguments*>(arg);
        // run and measure on this thread.
        perf_latency_core(threadArgs->h,
                          threadArgs->key_prefix,
                          threadArgs->num_docs,
                          threadArgs->add_timings,
                          threadArgs->get_timings,
                          threadArgs->replace_timings,
                          threadArgs->delete_timings);
    }
}

//
// Test performance of many buckets/threads
//
static enum test_result perf_latency_baseline_multi_thread_bucket(engine_test_t* test,
                                                                  int n_buckets,
                                                                  int n_threads,
                                                                  int num_docs) {
    if (n_buckets > n_threads) {
        // not supporting...
        fprintf(stderr, "Returning FAIL because n_buckets(%d) > n_threads(%d)\n",
                n_buckets, n_threads);
        return FAIL;
    }

    std::vector<BucketHolder> buckets;

    printf("\n\n");
    int printed = printf("=== Latency(%d - bucket(s) %d - thread(s)) - %u items(µs)",
                         n_buckets, n_threads, num_docs);

    fillLineWith('=', 88-printed);

    if (create_buckets(test->cfg, n_buckets, buckets) != n_buckets) {
        destroy_buckets(buckets);
        return FAIL;
    }

    for (int ii = 0; ii < n_buckets; ii++) {
        // re-use test_setup to wait for ready
        test_setup(buckets[ii].h);
        // Only timing front-end performance, not considering persistence.
        stop_persistence(buckets[ii].h);
    }

    std::vector<ThreadArguments> thread_args(n_threads);
    std::vector<cb_thread_t> threads(n_threads);

    // setup the arguments each thread will use.
    // just round robin allocate buckets to threads
    int bucket = 0;
    for (int ii = 0; ii < n_threads; ii++) {
        thread_args[ii].h = buckets[bucket].h;
        thread_args[ii].reserve(num_docs);
        thread_args[ii].num_docs = num_docs;
        thread_args[ii].key_prefix = ii;
        if ((++bucket) == n_buckets) {
            bucket = 0;
        }
    }

    // Now drive bucket(s) from thread(s)
    for (int i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], perf_latency_thread, &thread_args[i], 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    // destroy the buckets and rm the db path
    for (int ii = 0; ii < n_buckets; ii++) {
        testHarness->destroy_bucket(buckets[ii].h, false);
        rmdb(buckets[ii].dbpath.c_str());
    }

    // For the results, bring all the bucket timings into a single array
    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    std::vector<hrtime_t> add_timings, get_timings, replace_timings,
                          delete_timings;
    for (int ii = 0; ii < n_threads; ii++) {
        add_timings.insert(add_timings.end(),
                           thread_args[ii].add_timings.begin(),
                           thread_args[ii].add_timings.end());
        get_timings.insert(get_timings.end(),
                           thread_args[ii].get_timings.begin(),
                           thread_args[ii].get_timings.end());
        replace_timings.insert(replace_timings.end(),
                               thread_args[ii].replace_timings.begin(),
                               thread_args[ii].replace_timings.end());
        delete_timings.insert(delete_timings.end(),
                              thread_args[ii].delete_timings.begin(),
                              thread_args[ii].delete_timings.end());
        // done with these arrays now
        thread_args[ii].clear();
    }
    all_timings.push_back(std::make_pair("Add", &add_timings));
    all_timings.push_back(std::make_pair("Get", &get_timings));
    all_timings.push_back(std::make_pair("Replace", &replace_timings));
    all_timings.push_back(std::make_pair("Delete", &delete_timings));
    std::stringstream title;
    title << n_buckets << "_buckets_" << n_threads << "_threads_baseline";
    output_result(title.str(), "Timings", all_timings, "µs");

    return SUCCESS;
}

static enum test_result perf_latency_baseline_multi_bucket_2(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     2, /* buckets */
                                                     2, /* threads */
                                                     10000/* documents */);
}

static enum test_result perf_latency_baseline_multi_bucket_4(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     4, /* buckets */
                                                     4, /* threads */
                                                     10000/* documents */);
}

enum class Doc_format {
    JSON_PADDED,
    JSON_RANDOM,
    BINARY_RANDOM
};

struct Handle_args {
    Handle_args(EngineIface* _h,
                EngineIface* _h1,
                int _count,
                Doc_format _type,
                std::string _name,
                uint32_t _opaque,
                Vbid _vb,
                bool _getCompressed)
        : h(_h),
          h1(_h1),
          itemCount(_count),
          typeOfData(_type),
          name(_name),
          opaque(_opaque),
          vb(_vb),
          retrieveCompressed(_getCompressed) {
        timings.reserve(_count);
        bytes_received.reserve(_count);
    }

    Handle_args(struct Handle_args const &ha) :
        h(ha.h), h1(ha.h1), itemCount(ha.itemCount),
        typeOfData(ha.typeOfData), name(ha.name), opaque(ha.opaque),
        vb(ha.vb), retrieveCompressed(ha.retrieveCompressed),
        timings(ha.timings), bytes_received(ha.bytes_received)
    { }

    EngineIface* h;
    EngineIface* h1;
    int itemCount;
    Doc_format typeOfData;
    std::string name;
    uint32_t opaque;
    Vbid vb;
    bool retrieveCompressed;
    std::vector<hrtime_t> timings;
    std::vector<size_t> bytes_received;
};

/* Generates random strings of characters based on the input alphabet.
 */
class UniformCharacterDistribution {
public:
    UniformCharacterDistribution(const std::string& alphabet_)
    : alphabet(alphabet_),
      uid(0, alphabet.size()) {}

    template< class Generator >
    char operator()(Generator& g) {
        return alphabet[uid(g)];
    }

private:
    // Set of characters to randomly select from
    std::string alphabet;

    // Underlying integer distribution used to select character.
    std::uniform_int_distribution<> uid;
};


/* Generates a random string of the given length.
 */
template< class Generator>
static std::string make_random_string(UniformCharacterDistribution& dist,
                                      Generator& gen,
                                      size_t len) {
    std::string result(len, 0);
    std::generate_n(result.begin(), len, [&]() {
        return dist(gen);
    });
    return result;
}

std::vector<std::string> genVectorOfValues(Doc_format type,
                                           size_t count, size_t maxSize) {
    static const char alphabet[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";

    size_t len = 0;

    std::random_device ran;
    std::mt19937 dre(ran());
    UniformCharacterDistribution alpha_dist(alphabet);

    std::vector<std::string> vals;
    vals.reserve(count);
    switch (type) {
        case Doc_format::JSON_PADDED:
            for (size_t i = 0; i < count; ++i) {
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length
                std::string str(len, alphabet[i % (sizeof(alphabet) - 1)]);
                vals.push_back("{"
                               "\"one\":\"" + std::to_string(i) + "\", "
                               "\"two\":\"" + "TWO\", "
                               "\"three\":\"" + std::to_string(i) + "\", "
                               "\"four\":\"" + "FOUR\", "
                               "\"five\":\"" + str + "\""
                               "}");
            }
            break;
        case Doc_format::JSON_RANDOM:
            for (size_t i = 0; i < count; ++i) {
                // Generate a fixed-format document with random field values.
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length

                vals.push_back(
                        "{"
                        "\"one\":\"" + std::to_string(i) + "\", "
                        "\"two\":\"" +
                        make_random_string(alpha_dist, dre, len * 0.003) + "\", "
                        "\"three\":\"" +
                        make_random_string(alpha_dist, dre, len * 0.001) + "\", "
                        "\"four\": \"" +
                        make_random_string(alpha_dist, dre, len * 0.002) + "\", "
                        "\"five\":\"" +
                        make_random_string(alpha_dist, dre, len * 0.05) + "\", "
                        "\"six\":\"{1, 2, 3, 4, 5}\", "
                        "\"seven\":\"" +
                        make_random_string(alpha_dist, dre, len * 0.01) + "\", "
                        "\"eight\":\"" +
                        make_random_string(alpha_dist, dre, len * 0.01) + "\", "
                        "\"nine\":{'abc', 'def', 'ghi'}\", "
                        "\"ten\":\"0.123456789\""
                        "}");
            }
            break;
        case Doc_format::BINARY_RANDOM:
            for (size_t i = 0; i < count; ++i) {
                len = ((i + 1) * 10) % maxSize; // Set field length
                len = (len == 0) ? 10 : len;    // Adjust field length
                std::string str(len, 0);
                std::generate_n(str.begin(), len, [&]() {
                    return dre();
                });
                vals.push_back(str);
            }
            break;
        default:
            check(false, "Unknown DATA requested!");
    }
    return vals;
}

/* Function which loads documents into a bucket */
static void perf_load_client(EngineIface* h,
                             Vbid vbid,
                             int count,
                             Doc_format typeOfData,
                             std::vector<hrtime_t>& insertTimes) {
    std::vector<std::string> keys;
    for (int i = 0; i < count; ++i) {
        keys.push_back("key" + std::to_string(i));
    }

    std::vector<std::string> vals =
            genVectorOfValues(typeOfData, count, ITERATIONS);

    for (int i = 0; i < count; ++i) {
        checkeq(cb::engine_errc::success,
                storeCasVb11(h,
                             nullptr,
                             OPERATION_SET,
                             keys[i].c_str(),
                             vals[i].data(),
                             vals[i].size(),
                             /*flags*/ 9258,
                             0,
                             vbid)
                        .first,
                "Failed set.");
        insertTimes.push_back(
                std::chrono::steady_clock::now().time_since_epoch().count());
    }

    add_sentinel_doc(h, vbid);

    wait_for_flusher_to_settle(h);
}

/* Function which loads documents into a bucket until told to stop*/
static void perf_background_sets(EngineIface* h,
                                 Vbid vbid,
                                 int count,
                                 Doc_format typeOfData,
                                 std::vector<hrtime_t>& insertTimes,
                                 ThreadGate& setup_benchmark,
                                 std::atomic<bool>& running_benchmark) {
    std::vector<std::string> keys;
    const void* cookie = testHarness->create_cookie(h);

    for (int ii = 0; ii < count; ++ii) {
        keys.push_back("key" + std::to_string(ii));
    }

    const std::vector<std::string> vals =
            genVectorOfValues(typeOfData, count, ITERATIONS);

    int ii = 0;

    // update atomic stating we are ready to run the benchmark
    setup_benchmark.threadUp();

    while (running_benchmark) {
        if (ii == count) {
            ii = 0;
        }
        const auto start = std::chrono::steady_clock::now();
        checkeq(storeCasVb11(h,
                             cookie,
                             OPERATION_SET,
                             keys[ii].c_str(),
                             vals[ii].data(),
                             vals[ii].size(),
                             /*flags*/ 9258,
                             0,
                             vbid)
                        .first,
                cb::engine_errc::success,
                "Failed set.");
        const auto end = std::chrono::steady_clock::now();
        insertTimes.push_back((end - start).count());
        ++ii;
    }

    testHarness->destroy_cookie(cookie);
}

/*
 * Function which implements a DCP client sinking mutations from an ep-engine
 * DCP Producer (i.e. simulating the replica side of a DCP pairing).
 */
static void perf_dcp_client(EngineIface* h,
                            int itemCount,
                            const std::string& name,
                            uint32_t opaque,
                            Vbid vbid,
                            bool retrieveCompressed,
                            std::vector<hrtime_t>& recv_timings,
                            std::vector<size_t>& bytes_received) {
    const void* cookie = testHarness->create_cookie(h);

    std::string uuid("vb_" + std::to_string(vbid.get()) + ":0:id");
    uint64_t vb_uuid = get_ull_stat(h, uuid.c_str(), "failovers");
    uint32_t streamOpaque = opaque;

    auto& dcp = dynamic_cast<DcpIface&>(*h);
    checkeq(dcp.open(cookie,
                     ++streamOpaque,
                     0,
                     cb::mcbp::request::DcpOpenPayload::Producer,
                     name),
            ENGINE_SUCCESS,
            "Failed dcp producer open connection");

    checkeq(dcp.control(
                    cookie, ++streamOpaque, "connection_buffer_size", "1024"),
            ENGINE_SUCCESS,
            "Failed to establish connection buffer");

    if (retrieveCompressed) {
        testHarness->set_datatype_support(cookie,
                                          PROTOCOL_BINARY_DATATYPE_SNAPPY);

        checkeq(dcp.control(cookie,
                            ++streamOpaque,
                            "force_value_compression",
                            "true"),
                ENGINE_SUCCESS,
                "Failed to force value compression");
    }

    // We create a stream from 0 to MAX(seqno), and then rely on encountering the
    // sentinel document to know when to finish.
    uint64_t rollback = 0;
    checkeq(dcp.stream_req(cookie,
                           0,
                           streamOpaque,
                           vbid,
                           0,
                           std::numeric_limits<uint64_t>::max(),
                           vb_uuid,
                           0,
                           0,
                           &rollback,
                           mock_dcp_add_failover_log,
                           {}),
            ENGINE_SUCCESS,
            "Failed to initiate stream request");

    MockDcpMessageProducers producers(h);

    bool done = false;
    uint32_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    do {
        if (bytes_read > 512) {
            checkeq(ENGINE_SUCCESS,
                    dcp.buffer_acknowledgement(
                            cookie, ++streamOpaque, vbid, bytes_read),
                    "Failed to acknowledge buffer");
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = dcp.step(cookie, &producers);
        switch (err) {
        case ENGINE_EWOULDBLOCK:
            // No data currently available - wait to be notified when
            // more available.
            testHarness->lock_cookie(cookie);
            testHarness->waitfor_cookie(cookie);
            testHarness->unlock_cookie(cookie);
            break;

        case ENGINE_SUCCESS:
            switch (producers.last_op) {
            case cb::mcbp::ClientOpcode::DcpMutation:
            case cb::mcbp::ClientOpcode::DcpDeletion:
                // Check for sentinel (before adding to timings).
                if (producers.last_key == SENTINEL_KEY) {
                    done = true;
                    break;
                }
                recv_timings.push_back(std::chrono::steady_clock::now()
                                               .time_since_epoch()
                                               .count());
                bytes_received.push_back(producers.last_value.length());
                bytes_read += producers.last_packet_size;
                if (pending_marker_ack &&
                    producers.last_byseqno == marker_end) {
                    sendDcpAck(h,
                               cookie,
                               cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                               cb::mcbp::Status::Success,
                               producers.last_opaque);
                }

                break;

            case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
                if (producers.last_flags & 8) {
                    pending_marker_ack = true;
                    marker_end = producers.last_snap_end_seqno;
                }
                bytes_read += producers.last_packet_size;
                break;

            case cb::mcbp::ClientOpcode::Invalid:
                /* Consider case where no messages were ready on the last
                 * step call so we will just ignore this case. Note that we
                 * check for 0 because we clear the last_op value below.
                 */
                break;
            default:
                fprintf(stderr,
                        "Unexpected DCP event type received: %s\n",
                        to_string(producers.last_op).c_str());
                abort();
            }
            producers.last_op = cb::mcbp::ClientOpcode::Invalid;
            break;

        default:
            fprintf(stderr, "Unhandled dcp->step() result: %d\n", err);
            abort();
        }
    } while (!done);

    testHarness->destroy_cookie(cookie);
}

struct Ret_vals {
    Ret_vals(struct Handle_args _ha, size_t n) :
        ha(_ha)
    {
        timings.reserve(n);
        received.reserve(n);
    }
    struct Handle_args ha;
    std::vector<hrtime_t> timings;
    std::vector<size_t> received;
};

/*
 * Performs a single DCP latency / bandwidth test with the given parameters.
 * Returns vectors of item timings and recived bytes.
 */
static std::pair<std::vector<hrtime_t>, std::vector<size_t>>
single_dcp_latency_bw_test(EngineIface* h,
                           Vbid vb,
                           size_t item_count,
                           Doc_format typeOfData,
                           const std::string& name,
                           uint32_t opaque,
                           bool retrieveCompressed) {
    std::vector<size_t> received;

    check(set_vbucket_state(h, vb, vbucket_state_active),
          "Failed set_vbucket_state for vbucket");
    wait_for_flusher_to_settle(h);

    std::vector<hrtime_t> insert_times;

    std::thread load_thread{perf_load_client,
                            h,
                            vb,
                            item_count,
                            typeOfData,
                            std::ref(insert_times)};

    std::vector<hrtime_t> recv_times;
    std::thread dcp_thread{perf_dcp_client,
                           h,
                           item_count,
                           name,
                           opaque,
                           vb,
                           retrieveCompressed,
                           std::ref(recv_times),
                           std::ref(received)};
    load_thread.join();
    dcp_thread.join();

    std::vector<hrtime_t> timings;
    for (size_t j = 0; j < insert_times.size(); ++j) {
        if (insert_times[j] < recv_times[j]) {
            timings.push_back(recv_times[j] - insert_times[j]);
        } else {
            // Since there is no network overhead at all, it is seen
            // that sometimes the DCP client actually received the
            // mutation before the store from the load client returned
            // a SUCCESS.
            timings.push_back(0);
        }
    }

    return {timings, received};
}

static enum test_result perf_dcp_latency_and_bandwidth(EngineIface* h,
                                                       std::string title,
                                                       Doc_format typeOfData,
                                                       size_t item_count) {
    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    std::vector<std::pair<std::string, std::vector<size_t>*> > all_sizes;

    std::vector<struct Ret_vals> iterations;

    // For Loader & DCP client to get documents as is from vbucket 0
    auto as_is_results = single_dcp_latency_bw_test(h,
                                                    Vbid(0),
                                                    item_count,
                                                    typeOfData,
                                                    "As_is",
                                                    /*opaque*/ 0xFFFFFF00,
                                                    false);
    all_timings.push_back({"As_is", &as_is_results.first});
    all_sizes.push_back({"As_is", &as_is_results.second});

    // For Loader & DCP client to get documents compressed from vbucket 1
    auto compress_results = single_dcp_latency_bw_test(h,
                                                       Vbid(1),
                                                       item_count,
                                                       typeOfData,
                                                       "Compress",
                                                       /*opaque*/ 0xFF000000,
                                                       true);
    all_timings.push_back({"Compress", &compress_results.first});
    all_sizes.push_back({"Compress", &compress_results.second});

    printf("\n\n");

    int printed = printf("=== %s KB Rcvd. - %zu items (KB)", title.c_str(),
                         item_count);
    fillLineWith('=', 86-printed);

    output_result(title, "Size", all_sizes, "KB");

    fillLineWith('=', 86);

    printed = printf("=== %s Latency - %zu items(µs)", title.c_str(),
                     item_count);
    fillLineWith('=', 88-printed);

    output_result(title, "Latency", all_timings, "µs");
    printf("\n\n");

    return SUCCESS;
}

static enum test_result perf_dcp_latency_with_padded_json(EngineIface* h) {
    return perf_dcp_latency_and_bandwidth(
            h,
            "DCP In-memory (JSON-PADDED) [As_is vs. Compress]",
            Doc_format::JSON_PADDED,
            ITERATIONS / 20);
}

static enum test_result perf_dcp_latency_with_random_json(EngineIface* h) {
    return perf_dcp_latency_and_bandwidth(
            h,
            "DCP In-memory (JSON-RAND) [As_is vs. Compress]",
            Doc_format::JSON_RANDOM,
            ITERATIONS / 20);
}

static enum test_result perf_dcp_latency_with_random_binary(EngineIface* h) {
    return perf_dcp_latency_and_bandwidth(
            h,
            "DCP In-memory (BINARY-RAND) [As_is vs. Compress]",
            Doc_format::BINARY_RANDOM,
            ITERATIONS / 20);
}

/*
 * The test simulates the real scenario of a bulk load where the
 * ActiveStreamCheckpointProcessorTask runs fast on the Producer.
 * The Consumer receives many 1-item snapshots and executes
 * PassiveStream::handleSnapshotEnd at every incoming mutation.
 * Note that my fix for MB-30019 has been reverted for MB-30234, so currently
 * we are skipping the call to CheckpointManager::checkAndAddNewCheckpoint
 * (most times in real executions, always in this test).
 */
static enum test_result perf_dcp_consumer_snap_end_mutation_latency(
        EngineIface* h) {
    const Vbid vbid = Vbid(0);
    const uint32_t opaque = 1;

    check(set_vbucket_state(h, vbid, vbucket_state_replica),
          "set_vbucket_state failed");

    auto& dcp = dynamic_cast<DcpIface&>(*h);

    // To simulate a scenario where the number of checkpoints is high, I
    // need to prevent the CheckpointRemoverTask from releasing checkpoints.
    // To achieve that, in the following first few steps I create a Producer
    // and an ActiveStream on the replica-vbucket. That will create a cursor
    // for the ActiveStream.
    // Also, given that I never call 'dcp.step(activeCookie)', the stream never
    // switches from Backfill to In-memory, so the new cursor will be stuck on
    // the first checkpoint. That will prevent any checkpoint from being closed
    // and released.

    // Create a DCP Producer connection and add an active stream on vbid
    const void* activeCookie = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp.open(activeCookie,
                     opaque,
                     0 /*seqno*/,
                     cb::mcbp::request::DcpOpenPayload::Producer /*flags*/,
                     "test_producer"),
            "dcp.open failed");

    uint64_t rollbackSeqno = 0;
    checkeq(ENGINE_SUCCESS,
            dcp.stream_req(activeCookie,
                           0 /*flags*/,
                           opaque,
                           vbid,
                           0 /*startSeqno*/,
                           std::numeric_limits<int>::max() /*endSeqno*/,
                           0 /*vbUuid*/,
                           0 /*snap_start_seqno*/,
                           0 /*snap_end_seqno*/,
                           &rollbackSeqno,
                           mock_dcp_add_failover_log,
                           {}),
            "dcp.stream_req failed");

    // We have done with preventing checkpoints from being closed and released.
    // Now we can proceed with our measurement.

    // Create a DCP Consumer connection and add a passive stream on vbid
    const void* passiveCookie = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp.open(passiveCookie,
                     opaque,
                     0 /*seqno*/,
                     0 /*flags*/,
                     "test_consumer"),
            "dcp.open failed");
    checkeq(ENGINE_SUCCESS,
            dcp.add_stream(passiveCookie, opaque, vbid, 0 /*flags*/),
            "dcp.add_stream failed");

    std::vector<hrtime_t> timings;
    // Note: trying to keep the runtime below 1sec on local environment as it
    //     will increase on ASan, TSan and UBSan runs.
    const size_t numItems = ITERATIONS / 5;
    // Note: here we send many 1-item snapshots. At every iteration:
    //     1) we send the snapshot-marker with snapStart=snapEnd=seqno
    //     2) we send the seqno-mutation, which is the snapshot-end mutation
    for (size_t seqno = 1; seqno <= numItems; seqno++) {
        // 1) snapshot-marker
        checkeq(ENGINE_SUCCESS,
                dcp.snapshot_marker(passiveCookie,
                                    opaque,
                                    vbid,
                                    seqno /*snapStart*/,
                                    seqno /*snapEnd*/,
                                    dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                    {} /*HCS*/,
                                    {} /*maxVisibleSeqno*/),
                "dcp.snapshot_marker failed");

        auto begin = std::chrono::steady_clock::now();
        std::string key = "key_" + std::to_string(seqno);
        // 2) snapshot-end mutation
        checkeq(ENGINE_SUCCESS,
                dcp.mutation(
                        passiveCookie,
                        opaque,
                        DocKey(key, DocKeyEncodesCollectionId::No),
                        cb::const_byte_buffer(
                                reinterpret_cast<const uint8_t*>("value"), 5),
                        0 /*priv_bytes*/,
                        PROTOCOL_BINARY_RAW_BYTES,
                        0 /*cas*/,
                        vbid,
                        0 /*flags*/,
                        seqno,
                        0 /*revSeqno*/,
                        0 /*expiration*/,
                        0 /*lockTime*/,
                        {} /*meta*/,
                        0 /*nru*/),
                "dcp.mutation failed");

        timings.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::steady_clock::now() - begin)
                                  .count());
    }

    std::vector<std::pair<std::string, std::vector<hrtime_t>*>> result;
    result.push_back({"Datatype::Raw", &timings});

    std::string title = "DCP Consumer snapshot-end mutation";
    // Note: I need to print the title here as it is ignore by output_result
    //     for stdout
    printf("\n=== %s - %zu items === ", title.c_str(), numItems);
    output_result(title, "Latency (ns) ", result, "ns");
    printf("\n");

    testHarness->destroy_cookie(passiveCookie);
    testHarness->destroy_cookie(activeCookie);

    return SUCCESS;
}

static enum test_result perf_multi_thread_latency(engine_test_t* test) {
    return perf_latency_baseline_multi_thread_bucket(test,
                                                     1, /* bucket */
                                                     4, /* threads */
                                                     10000/* documents */);
}

static enum test_result perf_latency_dcp_impact(EngineIface* h) {
    // Spin up a DCP replication background thread, then start the normal
    // latency test.
    const size_t num_docs = ITERATIONS;
    // Perform 3 DCP-visible operations - add, replace, delete:
    const size_t num_dcp_ops = num_docs * 3;

    // Don't actually care about send times & bytes for this test.
    std::vector<hrtime_t> ignored_send_times;
    std::vector<size_t> ignored_send_bytes;
    std::thread dcp_thread{perf_dcp_client,
                           h,
                           num_dcp_ops,
                           "DCP",
                           /*opaque*/ 0x1,
                           Vbid(0),
                           /*compressed*/ false,
                           std::ref(ignored_send_times),
                           std::ref(ignored_send_bytes)};

    enum test_result result = perf_latency(h, "With background DCP", num_docs);

    dcp_thread.join();

    return result;
}

static void perf_stat_latency_core(EngineIface* h,
                                   int key_prefix,
                                   StatRuntime statRuntime) {
    const int iterations = (statRuntime == StatRuntime::Slow) ?
            iterations_for_slow_stats : iterations_for_fast_stats;

    const void* cookie = testHarness->create_cookie(h);
    // For some of the stats we need to have a document stored
    checkeq(cb::engine_errc::success,
            storeCasVb11(h,
                         nullptr,
                         OPERATION_ADD,
                         "example_doc",
                         nullptr,
                         0,
                         /*flags*/ 0,
                         0,
                         Vbid(0))
                    .first,
            "Failed to add example document.");

    if (isWarmupEnabled(h)) {
        // Include warmup-specific stats
        stat_tests.insert({"warmup", {"warmup", StatRuntime::Fast, {}} });
    }

    if (isPersistentBucket(h)) {
        // Include persistence-specific stats
        stat_tests.insert(
                {make_stat_pair("diskinfo",
                                {"diskinfo", StatRuntime::Fast, {}}),
                 make_stat_pair("diskinfo-detail",
                                {"diskinfo-detail", StatRuntime::Slow, {}}),
                 make_stat_pair(
                         "vkey_vb0",
                         {"vkey example_doc 0", StatRuntime::Fast, {}})});
    }

    for (auto& stat : stat_tests) {
        if (stat.second.runtime == statRuntime) {
            for (int ii = 0; ii < iterations; ii++) {
                auto start = std::chrono::steady_clock::now();
                if (stat.first.compare("engine") == 0) {
                    checkeq(ENGINE_SUCCESS,
                            h->get_stats(cookie, {}, {}, add_stats),
                            "Failed to get engine stats");
                } else {
                    checkeq(ENGINE_SUCCESS,
                            h->get_stats(cookie,
                                         {stat.second.key.c_str(),
                                          stat.second.key.length()},
                                         {},
                                         add_stats),
                            (std::string("Failed to get stat:") +
                             stat.second.key)
                                    .c_str());
                }

                auto end = std::chrono::steady_clock::now();
                stat.second.timings.push_back((end - start).count());
            }
        }
    }
    testHarness->destroy_cookie(cookie);
}

static enum test_result perf_stat_latency(EngineIface* h,
                                          const char* title,
                                          StatRuntime statRuntime,
                                          BackgroundWork backgroundWork,
                                          int active_vbuckets) {
    ThreadGate setup_benchmark(2);
    std::atomic<bool> running_benchmark { true };
    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    std::vector<hrtime_t> insert_timings;
    std::vector<hrtime_t> ignored_send_times;
    std::vector<size_t> ignored_send_bytes;
    std::thread dcp_thread;

    insert_timings.reserve(iterations_for_fast_stats);

    for (int vb = 0; vb < active_vbuckets; vb++) {
        check(set_vbucket_state(h, Vbid(vb), vbucket_state_active),
              "Failed set_vbucket_state for vbucket");
    }
    if (isPersistentBucket(h)) {
        wait_for_stat_to_be(h, "ep_persist_vbstate_total", active_vbuckets);
    }

    // Only timing front-end performance, not considering persistence.
    stop_persistence(h);

    if ((backgroundWork & BackgroundWork::Sets) == BackgroundWork::Sets) {
        std::thread load_thread{perf_background_sets,
                                h,
                                Vbid(0),
                                iterations_for_fast_stats,
                                Doc_format::JSON_RANDOM,
                                std::ref(insert_timings),
                                std::ref(setup_benchmark),
                                std::ref(running_benchmark)};

        if ((backgroundWork & BackgroundWork::Dcp) == BackgroundWork::Dcp) {
            std::thread local_dcp_thread{perf_dcp_client,
                                         h,
                                         0,
                                         "DCP",
                                         /*opaque*/ 0x1,
                                         Vbid(0),
                                         /*compressed*/ false,
                                         std::ref(ignored_send_times),
                                         std::ref(ignored_send_bytes)};
            dcp_thread.swap(local_dcp_thread);
        }

        setup_benchmark.threadUp();

        // run and measure on this thread.
        perf_stat_latency_core(h, 0, statRuntime);

        // Need to tell the thread performing sets to stop
        running_benchmark = false;
        load_thread.join();
        if ((backgroundWork & BackgroundWork::Dcp) == BackgroundWork::Dcp) {
            // Need to tell the thread performing DCP to stop
            add_sentinel_doc(h, Vbid(0));
            dcp_thread.join();
            all_timings.emplace_back("Sets and DCP (bg)", &insert_timings);
        } else {
            all_timings.emplace_back("Sets (bg)", &insert_timings);
        }
    } else {
        // run and measure on this thread.
        perf_stat_latency_core(h, 0, statRuntime);
    }

    for (auto& stat : stat_tests) {
        if (statRuntime == stat.second.runtime) {
            all_timings.emplace_back(stat.first,
                                     &stat.second.timings);
        }
    }

    const std::string iterations = (statRuntime == StatRuntime::Slow) ?
    std::to_string(iterations_for_slow_stats)
    : std::to_string(iterations_for_fast_stats);

    std::string description(std::string("Latency [") + title + "] - " +
                            iterations + " items (µs)");
    output_result(title, description, all_timings, "µs");
    return SUCCESS;
}

/* Benchmark the baseline stats (without any tasks running) of ep-engine */
static enum test_result perf_stat_latency_baseline(EngineIface* h) {
    return perf_stat_latency(
            h, "Baseline Stats", StatRuntime::Fast, BackgroundWork::None, 1);
}

/* Benchmark the stats with 100 active vbuckets */
static enum test_result perf_stat_latency_100vb(EngineIface* h) {
    return perf_stat_latency(h,
                             "Stats with 100 vbuckets",
                             StatRuntime::Fast,
                             BackgroundWork::None,
                             100);
}

/*
 * Benchmark the stats with 100 active vbuckets.  And sets and DCP running on
 * background thread.
 */
static enum test_result perf_stat_latency_100vb_sets_and_dcp(EngineIface* h) {
    return perf_stat_latency(
            h,
            "Stats with 100 vbuckets and background sets and DCP",
            StatRuntime::Fast,
            (BackgroundWork::Sets | BackgroundWork::Dcp),
            100);
}

/* Benchmark the baseline slow stats (without any tasks running) of ep-engine */
static enum test_result perf_slow_stat_latency_baseline(EngineIface* h) {
    return perf_stat_latency(h,
                             "Baseline Slow Stats",
                             StatRuntime::Slow,
                             BackgroundWork::None,
                             1);
}

/* Benchmark the slow stats with 100 active vbuckets */
static enum test_result perf_slow_stat_latency_100vb(EngineIface* h) {
    return perf_stat_latency(h,
                             "Slow Stats with 100 vbuckets",
                             StatRuntime::Slow,
                             BackgroundWork::None,
                             100);
}

/*
 * Benchmark the slow stats with 100 active vbuckets.  And sets and DCP running
 * on background thread.
 */
static enum test_result perf_slow_stat_latency_100vb_sets_and_dcp(
        EngineIface* h) {
    return perf_stat_latency(
            h,
            "Slow Stats with 100 vbuckets and background sets and DCP",
            StatRuntime::Slow,
            (BackgroundWork::Sets | BackgroundWork::Dcp),
            100);
}

/*****************************************************************************
 * List of testcases
 *****************************************************************************/

const char *default_dbname = "./perf_test";

BaseTestCase testsuite_testcases[] = {
        TestCase("Baseline latency", perf_latency_baseline,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209",
                 prepare, cleanup),
        TestCase("Defragmenter latency", perf_latency_defragmenter,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209"
                 // Run defragmenter constantly.
                 ";defragmenter_interval=0",
                 prepare, cleanup),
        TestCase("Expiry pager latency", perf_latency_expiry_pager,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209"
                 // Run expiry pager constantly.
                 ";exp_pager_stime=0",
                 prepare, cleanup),
        TestCaseV2("Multi bucket latency", perf_latency_baseline_multi_bucket_2,
                   NULL, NULL,
                   "backend=couchdb;ht_size=393209",
                   prepare, cleanup),
        TestCaseV2("Multi bucket latency", perf_latency_baseline_multi_bucket_4,
                   NULL, NULL,
                   "backend=couchdb;ht_size=393209",
                   prepare, cleanup),

        TestCase("DCP latency (Padded JSON)",
                 perf_dcp_latency_with_padded_json,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCase("DCP latency (Random JSON)", perf_dcp_latency_with_random_json,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209",
                 prepare, cleanup),

        TestCase("DCP latency (Random BIN)",
                 perf_dcp_latency_with_random_binary,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCaseV2("Multi thread latency", perf_multi_thread_latency,
                   NULL, NULL,
                   "backend=couchdb;ht_size=393209",
                   prepare, cleanup),

        TestCase("DCP impact on front-end latency", perf_latency_dcp_impact,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209",
                 prepare, cleanup),

        TestCase("DCP Consumer snapshot-end mutation latency",
                perf_dcp_consumer_snap_end_mutation_latency,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209",
                 prepare,
                 cleanup),

        TestCase("Baseline Stat latency", perf_stat_latency_baseline,
                 test_setup, teardown,
                 "backend=couchdb;ht_size=393209",
                 prepare, cleanup),

        TestCase("Stat latency with 100 active vbuckets",
                 perf_stat_latency_100vb,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCase("Stat latency with 100 vbuckets. Also sets & DCP traffic on "
                 "separate thread",
                 perf_stat_latency_100vb_sets_and_dcp,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCase("Baseline Slow Stat latency", perf_slow_stat_latency_baseline,
                 test_setup, teardown, "backend=couchdb;ht_size=393209",
                 prepare, cleanup),

        TestCase("Stat latency with 100 active vbuckets",
                 perf_slow_stat_latency_100vb,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCase("Stat latency with 100 vbuckets. Also sets & DCP traffic on "
                 "separate thread",
                 perf_slow_stat_latency_100vb_sets_and_dcp,
                 test_setup,
                 teardown,
                 "backend=couchdb;ht_size=393209"
                 // Test requires ~1GB bucket quota
                 ";max_size=1000000000",
                 prepare,
                 cleanup),

        TestCase(NULL, NULL, NULL, NULL,
                 "backend=couchdb", prepare, cleanup)
};
