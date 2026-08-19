/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "timing_histogram_printer.h"
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <cstdio>

class TimingHistogramPrinterTest : public ::testing::Test {
protected:
    static std::string dumpToString(TimingHistogramPrinter& printer,
                                    std::string_view name) {
        const auto tempFileName = cb::io::mktemp("timing_histo_test");
        FILE* fp = std::fopen(tempFileName.c_str(), "w");
        if (!fp) {
            throw std::runtime_error("Failed to open temp file: " +
                                     tempFileName);
        }
        printer.dumpHistogram(name, fp);
        std::fclose(fp);

        const auto output = cb::io::loadFile(tempFileName);
        cb::io::rmrf(tempFileName);
        return output;
    }
};

TEST_F(TimingHistogramPrinterTest, GetHistogramType) {
    using Type = TimingHistogramPrinter::HistogramType;

    // Count histograms
    EXPECT_EQ(Type::Count,
              TimingHistogramPrinter::getHistogramType("readIOAmpCount"));
    EXPECT_EQ(Type::Count,
              TimingHistogramPrinter::getHistogramType("rw_0:readIOAmpCount"));
    EXPECT_EQ(Type::Count,
              TimingHistogramPrinter::getHistogramType("saveDocCount"));
    EXPECT_EQ(Type::Count,
              TimingHistogramPrinter::getHistogramType("bg_batch_size"));
    EXPECT_EQ(Type::Count,
              TimingHistogramPrinter::getHistogramType(
                      "rw_0:ep_active_or_pending_eviction_values_evicted"));

    // Size histograms
    EXPECT_EQ(Type::Size, TimingHistogramPrinter::getHistogramType("readSize"));
    EXPECT_EQ(Type::Size,
              TimingHistogramPrinter::getHistogramType("rw_0:writeSize"));
    EXPECT_EQ(Type::Size,
              TimingHistogramPrinter::getHistogramType("fsReadSeek"));
    EXPECT_EQ(Type::Size,
              TimingHistogramPrinter::getHistogramType("item_alloc_sizes"));

    // Ratio histograms
    EXPECT_EQ(Type::Ratio,
              TimingHistogramPrinter::getHistogramType("compressionRatio"));
    EXPECT_EQ(
            Type::Ratio,
            TimingHistogramPrinter::getHistogramType("rw_0:compressionRatio"));

    // Time in seconds
    EXPECT_EQ(Type::TimeSeconds,
              TimingHistogramPrinter::getHistogramType("paged_out_time"));
    EXPECT_EQ(Type::TimeSeconds,
              TimingHistogramPrinter::getHistogramType("rw_0:paged_out_time"));

    // Default: Time in microseconds
    EXPECT_EQ(Type::TimeMicroseconds,
              TimingHistogramPrinter::getHistogramType("readTime"));
    EXPECT_EQ(Type::TimeMicroseconds,
              TimingHistogramPrinter::getHistogramType("rw_0:flushTime"));
    EXPECT_EQ(Type::TimeMicroseconds,
              TimingHistogramPrinter::getHistogramType("GET"));
    EXPECT_EQ(Type::TimeMicroseconds,
              TimingHistogramPrinter::getHistogramType("unknown"));
}

TEST_F(TimingHistogramPrinterTest, DumpCountHistogram) {
    const nlohmann::json json = {{"data", {{12, 100, 50.0}, {24, 100, 100.0}}},
                                 {"bucketsLow", 0},
                                 {"total", 200},
                                 {"overflowed", 5},
                                 {"max_trackable", 100}};

    TimingHistogramPrinter printer(json);
    EXPECT_EQ(205, printer.getTotal());

    const auto output = dumpToString(printer, "rw_0:readIOAmpCount");
    EXPECT_NE(std::string::npos,
              output.find("The following data is collected for "
                          "\"rw_0:readIOAmpCount\""));
    // Count buckets must not contain 'us', 'ms', or 's'
    EXPECT_NE(std::string::npos, output.find("[  0.00 -  12.00] (50.0000%)"));
    EXPECT_NE(std::string::npos, output.find("[ 12.00 -  24.00] (100.0000%)"));
    EXPECT_NE(std::string::npos, output.find("[100.00 - inf   ] (overflowed)"));
    EXPECT_EQ(std::string::npos, output.find("]us"));
    EXPECT_EQ(std::string::npos, output.find("]ms"));
    EXPECT_EQ(std::string::npos, output.find("]s"));
    EXPECT_NE(std::string::npos, output.find("Total: 205 operations"));
}

TEST_F(TimingHistogramPrinterTest, DumpSizeHistogram) {
    const nlohmann::json json = {{"data",
                                  {{512, 10, 25.0},
                                   {2048, 10, 50.0},
                                   {2097152, 10, 75.0},
                                   {2147483648ULL, 10, 100.0}}},
                                 {"bucketsLow", 0},
                                 {"total", 40},
                                 {"overflowed", 2},
                                 {"max_trackable", 1073741824ULL}};

    TimingHistogramPrinter printer(json);
    const auto output = dumpToString(printer, "rw_0:readSize");
    EXPECT_NE(std::string::npos, output.find("[  0.00 - 512.00]B (25.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  0.50 -   2.00]KiB (50.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  0.00 -   2.00]MiB (75.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  0.00 -   2.00]GiB (100.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  1.00 - inf   ]GiB (overflowed)"));
}

TEST_F(TimingHistogramPrinterTest, DumpRatioHistogram) {
    const nlohmann::json json = {{"data", {{12, 50, 50.0}, {88, 50, 100.0}}},
                                 {"bucketsLow", 0},
                                 {"total", 100},
                                 {"overflowed", 1},
                                 {"max_trackable", 100}};

    TimingHistogramPrinter printer(json);
    const auto output = dumpToString(printer, "compressionRatio");
    EXPECT_NE(std::string::npos, output.find("[  0.00 -   1.20] (50.0000%)"));
    EXPECT_NE(std::string::npos, output.find("[  1.20 -   8.80] (100.0000%)"));
    EXPECT_NE(std::string::npos, output.find("[ 10.00 - inf   ] (overflowed)"));
}

TEST_F(TimingHistogramPrinterTest, DumpTimeMicrosecondsHistogram) {
    const nlohmann::json json = {
            {"data", {{500, 10, 50.0}, {50000, 10, 100.0}}},
            {"bucketsLow", 0},
            {"total", 20},
            {"overflowed", 1},
            {"max_trackable", 2000000}};

    TimingHistogramPrinter printer(json);
    const auto output = dumpToString(printer, "rw_0:flushTime");
    EXPECT_NE(std::string::npos, output.find("[  0.00 - 500.00]us (50.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  0.50 -  50.00]ms (100.0000%)"));
    EXPECT_NE(std::string::npos,
              output.find("[  2.00 - inf   ]s (overflowed)"));
}

TEST_F(TimingHistogramPrinterTest, DumpTimeSecondsHistogram) {
    const nlohmann::json json = {{"data", {{10, 10, 100.0}}},
                                 {"bucketsLow", 0},
                                 {"total", 10},
                                 {"overflowed", 0}};

    TimingHistogramPrinter printer(json);
    const auto output = dumpToString(printer, "paged_out_time");
    EXPECT_NE(std::string::npos, output.find("[  0.00 -  10.00]s (100.0000%)"));
}
