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
**/

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include "ep_testsuite_common.h"
#include "ep_test_apis.h"

struct Stats {
    std::string name;
    double mean;
    double median;
    double stddev;
    double pct5;
    double pct95;
    double pct99;
    std::vector<hrtime_t>* vec;
};

// Given a vector of timings (each a vector<hrtime_t>) calcuate metrics on them
// and print to stdout.
void print_timings(std::vector<std::pair<std::string, std::vector<hrtime_t>*> > timings)
{
    // First, calculate mean, median, standard deviation and percentiles of
    // each set of timings, both for printing and to derive what the range of
    // the graphs should be.
    std::vector<Stats> timing_stats;
    for (const auto& t : timings) {
        Stats stats;
        stats.name = t.first;
        stats.vec = t.second;
        std::vector<hrtime_t>& vec = *t.second;

        // Calculate latency percentiles
        std::sort(vec.begin(), vec.end());
        stats.median = vec[(vec.size() * 50) / 100];
        stats.pct5 = vec[(vec.size() * 5) / 100];
        stats.pct95 = vec[(vec.size() * 95) / 100];
        stats.pct99 = vec[(vec.size() * 99) / 100];

        const double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
        stats.mean = sum / vec.size();
        double accum = 0.0;
        std::for_each (vec.begin(), vec.end(), [&](const double d) {
            accum += (d - stats.mean) * (d - stats.mean);
        });
        stats.stddev = sqrt(accum / (vec.size() - 1));

        timing_stats.push_back(stats);
    }

    // From these find the start and end for the spark graphs which covers the
    // a "reasonable sample" of each timing set. We define that as from the 9th
    // to the 95th percentile, so we ensure *all* sets have that range covered.
    hrtime_t spark_start = std::numeric_limits<hrtime_t>::max();
    hrtime_t spark_end = 0;
    for (const auto& stats : timing_stats) {
        spark_start = (stats.pct5 < spark_start) ? stats.pct5 : spark_start;
        spark_end = (stats.pct95 > spark_end) ? stats.pct95 : spark_end;
    }

    printf("\n\n                         Percentile           \n");
    printf("  %-8s Median     95th     99th  Std Dev  Histogram of samples\n\n", "");
    // Finally, print out each set.
    for (const auto& stats : timing_stats) {
        printf("%-8s %8.03f %8.03f %8.03f %8.03f  ",
               stats.name.c_str(), stats.median/1e3, stats.pct95/1e3,
               stats.pct99/1e3, stats.stddev/1e3);

        // Calculate and render Sparkline (requires UTF-8 terminal).
        const int nbins = 32;
        int prev_distance = 0;
        std::vector<size_t> histogram;
        for (unsigned int bin = 0; bin < nbins; bin++) {
            const hrtime_t max_for_bin = (spark_end / nbins) * bin;
            auto it = std::lower_bound(stats.vec->begin(), stats.vec->end(), max_for_bin);
            const int distance = std::distance(stats.vec->begin(), it);
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
    printf("%44s  %-14d µs %14d\n\n", "",
           int(spark_start/1e3), int(spark_end/1e3));
}


/*****************************************************************************
 ** Testcases
 *****************************************************************************/

static enum test_result perf_latency(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1,
                                     const char* title) {
    // Only timing front-end performance, not considering persistence.
    stop_persistence(h, h1);

    const void *cookie = testHarness.create_cookie();

    const unsigned int num_docs = 500000;
    const std::string data(100, 'x');

    std::vector<hrtime_t> add_timings, get_timings, replace_timings, delete_timings;
    add_timings.reserve(num_docs);
    get_timings.reserve(num_docs);
    replace_timings.reserve(num_docs);
    delete_timings.reserve(num_docs);

    int printed = 0;
    printf("\n\n=== Latency [%s] - %u items (µs) %n", title, num_docs, &printed);
    for (int i = 0; i < 81-printed; i++) {
        putchar('=');
    }

    // Build vector of keys
    std::vector<std::string> keys;
    for (unsigned int i = 0; i < num_docs; i++) {
        keys.push_back(std::to_string(i));
    }

    // Create (add)
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_ADD, key.c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
              "Failed to add a value");
        const hrtime_t end = gethrtime();
        add_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Get
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(h1->get(h, cookie, &item, key.c_str(), key.size(), 0) == ENGINE_SUCCESS,
              "Failed to get a value");
        const hrtime_t end = gethrtime();
        get_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Update (Replace)
    for (auto& key : keys) {
        item* item = NULL;
        const hrtime_t start = gethrtime();
        check(storeCasVb11(h, h1, cookie, OPERATION_REPLACE, key.c_str(),
                           data.c_str(), data.length(), 0, &item, 0,
                           /*vBucket*/0, 0, 0) == ENGINE_SUCCESS,
              "Failed to replace a value");
        const hrtime_t end = gethrtime();
        replace_timings.push_back(end - start);
        h1->release(h, cookie, item);
    }

    // Delete
    for (auto& key : keys) {
        const hrtime_t start = gethrtime();
        check(del(h, h1, key.c_str(), 0, 0, cookie) == ENGINE_SUCCESS,
              "Failed to delete a value");
        const hrtime_t end = gethrtime();
        delete_timings.push_back(end - start);
    }

    std::vector<std::pair<std::string, std::vector<hrtime_t>*> > all_timings;
    all_timings.push_back(std::make_pair("Add", &add_timings));
    all_timings.push_back(std::make_pair("Get", &get_timings));
    all_timings.push_back(std::make_pair("Replace", &replace_timings));
    all_timings.push_back(std::make_pair("Delete", &delete_timings));
    print_timings(all_timings);

    return SUCCESS;
}

/* Benchmark the baseline latency (without any tasks running) of ep-engine.
 */
static enum test_result perf_latency_baseline(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    return perf_latency(h, h1, "Baseline");
}

/* Benchmark the baseline latency with the defragmenter enabled.
 */
static enum test_result perf_latency_defragmenter(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    return perf_latency(h, h1, "With constant defragmention");
}


/*****************************************************************************
 * List of testcases
 *****************************************************************************/

TestCase testsuite_testcases[] = {
        TestCase("Baseline latency", perf_latency_baseline,
                 test_setup, teardown,
                 "ht_size=393209", prepare, cleanup),
        TestCase("Defragmenter latency", perf_latency_defragmenter,
                 test_setup, teardown,
                 "ht_size=393209"
                 // Run defragmenter constantly.
                 ";defragmenter_interval=0",
                 prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
