/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "utilities/readfile.h"

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <getopt.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#include <stdexcept>
#include <unordered_map>

// leading text to be included in the header and source files
constexpr const char* preamble = R"(/*
 *     Copyright 2022 Couchbase, Inc
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*********************************
 ** Generated file, do not edit **
 *********************************/
)";

constexpr const char* metricFamilyRegexStr = "[a-zA-Z_][a-zA-Z0-9_]*";

static void usage() {
    fmt::print(stderr,
               "Usage: gen_stat_definitions -j statJSON -C configJSON -c cfile "
               "-h headerfile\n"
               "\tThe JSON file will be read to generate the c and h file.\n");
    exit(EXIT_FAILURE);
}

/**
 * Validate that a metric family string literal name meets the
 * regex:
 *
 *   [a-zA-Z_][a-zA-Z0-9_]*
 *
 * Note: metric names _may_ also include colons, but these are reserved for
 * user defined recording rules and should _not_ be included in any names
 * exposed by KV.
 */
bool isValidMetricFamily(std::string_view name) {
    static const std::regex metricFamilyRegex(metricFamilyRegexStr,
                                              std::regex_constants::ECMAScript);
    return std::regex_match(name.begin(), name.end(), metricFamilyRegex);
}

struct Spec {
    std::string enumKey;
    std::string cbstat;
    std::string unit;

    struct Prometheus {
        std::string family;
        std::unordered_map<std::string, std::string> labels;
    } prometheus;

    bool cbstatEnabled = true;
    bool prometheusEnabled = true;

    void validate() const {
        if (!cbstatEnabled && !prometheusEnabled) {
            throw std::runtime_error(fmt::format(
                    "Stat:{} is not exposed for either of cbstat or prometheus",
                    enumKey));
        }
        if (!prometheus.family.empty() &&
            !isValidMetricFamily(prometheus.family)) {
            throw std::runtime_error(fmt::format(
                    "Stat:{} has invalid prometheus metric family name. Must "
                    "match regex:{}",
                    enumKey,
                    metricFamilyRegexStr));
        }
    }
};

void from_json(const nlohmann::json& j, Spec::Prometheus& p) {
    if (auto itr = j.find("family"); itr != j.end()) {
        const auto& family = *itr;
        if (!family.is_string()) {
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid prometheus.family type "
                                "{}, must be a string",
                                j.at("key"),
                                family.type_name()));
        }
        family.get_to(p.family);
    }

    if (auto itr = j.find("labels"); itr != j.end()) {
        const auto& labels = *itr;
        if (!labels.is_object()) {
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid prometheus.labels type "
                                "{}, must be an object",
                                j.at("key"),
                                labels.type_name()));
        }
        labels.get_to(p.labels);
    }
}

void from_json(const nlohmann::json& j, Spec& s) {
    using value_t = nlohmann::json::value_t;
    j.at("key").get_to(s.enumKey);
    j.at("unit").get_to(s.unit);
    if (auto itr = j.find("cbstat"); itr != j.end()) {
        auto cbstat = *itr;
        switch (cbstat.type()) {
        case value_t::boolean:
            if (!cbstat.get<bool>()) {
                s.cbstatEnabled = false;
            }
            break;
        case value_t::string:
            cbstat.get_to(s.cbstat);
            break;
        default:
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid cbstat field type {}, "
                                "must be bool or string",
                                s.enumKey,
                                cbstat.type_name()));
        }
    }
    if (auto itr = j.find("prometheus"); itr != j.end()) {
        const auto& prometheus = *itr;
        switch (prometheus.type()) {
        case value_t::boolean:
            if (!prometheus.get<bool>()) {
                s.prometheusEnabled = false;
            }
            break;
        case value_t::object:
            // parse prometheus stuff from json
            s.prometheus = prometheus;
            break;
        default:
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid prometheus field type {}, "
                                "must be bool or object",
                                s.enumKey,
                                prometheus.type_name()));
        }
    }
}

/**
 * Format a map of labels as initialiser list for code generation.
 *
 * results in:
 *
 *  {{"foo", "bar"}, {"baz", "qux"}}
 */
std::string formatLabels(
        const std::unordered_map<std::string, std::string>& labels) {
    using namespace std::string_view_literals;
    fmt::memory_buffer buf;
    buf.append("{"sv);
    for (const auto& [k, v] : labels) {
        fmt::format_to(std::back_inserter(buf), R"({{"{}","{}"}},)", k, v);
    }
    buf.append("}"sv);
    return fmt::to_string(buf);
}

/**
 * Generate a "StatDef(...)" string for a given stat from provided json.
 */
std::ostream& operator<<(std::ostream& os, const Spec& spec) {
    // TODO: followup to simplify construction - no need for several overloads
    //       now it's not being constructed from macros. This separation of
    //       output for prometheus vs cbstats is inherited from x-macro version.

    auto cbstat =
            "\"" + (!spec.cbstat.empty() ? spec.cbstat : spec.enumKey) + "\"sv";
    auto prom = !spec.prometheus.family.empty() ? spec.prometheus.family
                                                : spec.enumKey;

    // the cbstat key may need formatting at runtime, check if this is the case
    if (cbstat.find('{') != std::string::npos) {
        // output a stat key with a tag tracking that it needs formatting.
        // There's no point checking the key each time at runtime.
        cbstat = fmt::format(
                "CBStatsKey({}, CBStatsKey::NeedsFormattingTag{{}})", cbstat);
    }

    if (spec.prometheusEnabled && spec.cbstatEnabled) {
        fmt::print(os,
                   R"(StatDef({}, {}, "{}", {}))",
                   cbstat,
                   spec.unit,
                   prom,
                   formatLabels(spec.prometheus.labels));
    } else if (spec.prometheusEnabled) {
        fmt::print(os,
                   R"(StatDef("{}"sv, {}, {}, {}))",
                   prom,
                   spec.unit,
                   formatLabels(spec.prometheus.labels),
                   "cb::stats::StatDef::PrometheusOnlyTag{}");
    } else if (spec.cbstatEnabled) {
        fmt::print(os,
                   R"(StatDef({}, {}))",
                   cbstat,
                   "cb::stats::StatDef::CBStatsOnlyTag{}");
    }

    return os;
}

nlohmann::json readJsonFile(const char* filename) {
    try {
        // allow comments (/* */, //) in the parsed json
        return nlohmann::json::parse(readFile(filename),
                                     /* callback */ nullptr,
                                     /* allow exceptions */ true,
                                     /* ignore_comments */ true);
    } catch (const std::system_error& e) {
        fmt::print(stderr, "Failed to open file: {}\n", e.what());
        exit(EXIT_FAILURE);
    } catch (const nlohmann::json::exception& e) {
        fmt::print(stderr, "Failed to parse JSON: {}\n", e.what());
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    int cmd;
    const char* statjsonfile = nullptr;
    const char* configjsonfile = nullptr;
    const char* hfile = nullptr;
    const char* cfile = nullptr;

    while ((cmd = getopt(argc, argv, "j:C:c:h:")) != -1) {
        switch (cmd) {
        case 'j':
            statjsonfile = optarg;
            break;
        case 'C':
            configjsonfile = optarg;
            break;
        case 'c':
            cfile = optarg;
            break;
        case 'h':
            hfile = optarg;
            break;
        default:
            usage();
        }
    }

    if (statjsonfile == nullptr || configjsonfile == nullptr ||
        hfile == nullptr || cfile == nullptr) {
        usage();
    }

    nlohmann::json stats = readJsonFile(statjsonfile);
    nlohmann::json config = readJsonFile(configjsonfile);

    fmt::memory_buffer enumKeysBuf;
    fmt::memory_buffer statDefsBuf;

    for (const auto& statJson : stats) {
        // parse fields from json
        Spec spec = statJson;
        // check basic requirements are met
        spec.validate();
        // format the enum key for the .h
        fmt::format_to(std::back_inserter(enumKeysBuf), "{},\n", spec.enumKey);
        // format the whole stat def for the .cc
        fmt::format_to(std::back_inserter(statDefsBuf), "{},\n", spec);
    }

    for (const auto& configParam : config.at("params").items()) {
        // config params use only the key currently, no units or description
        std::vector<std::string> keys;
        // get any aliases
        if (configParam.value().count("aliases")) {
            configParam.value()["aliases"].get_to(keys);
        }

        // get the actual config key
        keys.push_back(configParam.key());

        // add a stat def for each name this config is known by
        for (const auto& key : keys) {
            Spec spec;
            spec.enumKey = "ep_" + key;
            spec.unit = "none";
            // format the enum key for the .h
            fmt::format_to(
                    std::back_inserter(enumKeysBuf), "{},\n", spec.enumKey);
            // format the whole stat def for the .cc
            fmt::format_to(std::back_inserter(statDefsBuf), "{},\n", spec);
        }
    }

    // Great! All the definitions were parsed fine. Now write the .h and .cc

    // generate header, containing enum keys and definition array decl
    std::ofstream headerfile(hfile);
    if (!headerfile.is_open()) {
        fmt::print(stderr, "Unable to create header file:{}\n", hfile);
        return 1;
    }
    fmt::print(headerfile, "{}#pragma once\n", preamble);

    fmt::print(headerfile,
               R"(
#include <statistics/statdef.h>

#include <array>

namespace cb::stats {{
enum class Key {{
{}
enum_max
}};

extern const std::array<StatDef, size_t(Key::enum_max)> statDefinitions;

}} // end namespace cb::stats

)",
               std::string_view(enumKeysBuf.data(), enumKeysBuf.size()));
    headerfile.close();

    // generate source file containing all StatDefs
    std::ofstream sourcefile(cfile);
    if (!sourcefile.is_open()) {
        fmt::print(stderr, "Unable to create source file:{}\n", cfile);
        return 1;
    }

    fmt::print(sourcefile, preamble);

    fmt::print(sourcefile,
               R"(
#include "generated_stats.h"

#include <string_view>

using namespace std::string_view_literals;

namespace cb::stats {{
using namespace units;
const std::array<StatDef, size_t(Key::enum_max)> statDefinitions{{{{
{}
}}}};
}} // end namespace cb::stats

)",
               std::string_view(statDefsBuf.data(), statDefsBuf.size()));

    return 0;
}
