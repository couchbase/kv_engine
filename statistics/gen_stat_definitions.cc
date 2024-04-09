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
#include <platform/terminal_color.h>
#include <prometheus/metric_type.h>
#include <statistics/units.h>
#include <array>
#include <fstream>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#include <stdexcept>
#include <unordered_map>

namespace prometheus {
// inject json deserialisation def as it needs to be in the same namespace
// as MetricType.
// maps MetricType enum to/from json strings
// default to "untyped"
// ignore linting, can't correct the macro.
// NOLINTNEXTLINE(modernize-avoid-c-arrays)
NLOHMANN_JSON_SERIALIZE_ENUM(MetricType,
                             {{MetricType::Untyped, "untyped"},
                              {MetricType::Counter, "counter"},
                              {MetricType::Gauge, "gauge"},
                              {MetricType::Histogram, "histogram"},
                              {MetricType::Info, "histogram"},
                              {MetricType::Summary, "summary"}})

std::string to_string(MetricType type) {
    switch (type) {
    case MetricType::Untyped:
        return "MetricType::Untyped";
    case MetricType::Counter:
        return "MetricType::Counter";
    case MetricType::Gauge:
        return "MetricType::Gauge";
    case MetricType::Histogram:
        return "MetricType::Histogram";
    case MetricType::Info:
        return "MetricType::Info";
    case MetricType::Summary:
        return "MetricType::Summary";
    }
    return "MetricType::Invalid(" + std::to_string(int(type)) + ")";
}

std::ostream& operator<<(std::ostream& os, const MetricType& type) {
    return os << to_string(type);
}
inline auto format_as(const MetricType& type) {
    return to_string(type);
}

} // namespace prometheus

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

/**
 * Filter out any config keys which should not be in public documentation. The
 * metadata for anything that matches here will be emitted as
 * stability=internal.
 */
constexpr std::array<std::string_view, 1> excludedConfigKeys{
        // Nexus is strictly for testing
        "nexus_"};

constexpr std::string_view configVersionAdded = "7.0.0";

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

bool shouldDocumentConfigKey(std::string_view name) {
    for (auto& matcher : excludedConfigKeys) {
        if (name.find(matcher) != std::string::npos) {
            return false;
        }
    }
    return true;
}

enum DefinitionType {
    /// Definition for a compile-time generated stat
    /// { key: string, family: object?, added: string, unit: string,
    ///   description: string }
    StatDefinition,
    /// Description for a metric family (for generating documentation)
    /// { family: string, unit: string, description: string }
    FamilyDescription,
};

DefinitionType getDefinitionType(const nlohmann::json& statDef) {
    if (statDef.contains("family")) {
        return DefinitionType::FamilyDescription;
    }
    return DefinitionType::StatDefinition;
}

/**
 * Specification for emitting a stat definition.
 */
struct Spec {
    std::string enumKey;
    std::string cbstat;
    std::string unit;
    ::prometheus::MetricType type = ::prometheus::MetricType::Untyped;

    struct Prometheus {
        std::string family;
        std::unordered_map<std::string, std::string> labels;
    } prometheus;

    bool cbstatEnabled = true;
    bool prometheusEnabled = true;
    std::string stability = "committed";
    std::string added = "";

    [[nodiscard]] std::string_view getName() const {
        return prometheus.family.empty() ? enumKey : prometheus.family;
    }

    [[nodiscard]] bool validate() const {
        bool success = true;
        auto fail = [&](const std::string_view msg) {
            success = false;
            fmt::print(stderr,
                       "{}genstats: {}{}",
                       cb::terminal::TerminalColor::Red,
                       msg,
                       cb::terminal::TerminalColor::Reset);
        };
        if (!cbstatEnabled && !prometheusEnabled) {
            fail(fmt::format(
                    "'{}' is not exposed for either of cbstat or prometheus",
                    enumKey));
        }
        if (!prometheus.family.empty() &&
            !isValidMetricFamily(prometheus.family)) {
            fail(fmt::format(
                    "'{}' has invalid prometheus metric family name. Must "
                    "match regex:{}",
                    enumKey,
                    metricFamilyRegexStr));
        }

        if (!(stability == "committed" || stability == "volatile" ||
              stability == "internal")) {
            fail(fmt::format("'{}' invalid value for field: stability: '{}'\n",
                             enumKey,
                             stability));
        }
        if (added.empty()) {
            fail(fmt::format("'{}' missing required field: added\n", enumKey));
        }
        return success;
    }
};

void from_json(const nlohmann::json& j, Spec::Prometheus& p) {
    if (auto itr = j.find("family"); itr != j.end()) {
        const auto& family = *itr;
        if (!family.is_string()) {
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid prometheus.family type "
                                "{}, must be a string",
                                j.at("key").get<std::string>(),
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
                                j.at("key").get<std::string>(),
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

    if (auto itr = j.find("type"); itr != j.end()) {
        const auto& type = *itr;
        if (!type.is_string()) {
            throw std::runtime_error(
                    fmt::format("Stat:{} has invalid type field type "
                                "{}, must be a string",
                                j.at("family").get<std::string>(),
                                type.type_name()));
        }
        type.get_to(s.type);
    }

    if (auto itr = j.find("stability"); itr != j.end()) {
        s.stability = itr.value();
    }

    if (auto itr = j.find("added"); itr != j.end()) {
        s.added = itr.value();
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
                   R"(StatDef({}, {}, "{}", {}, {}))",
                   cbstat,
                   spec.unit,
                   prom,
                   spec.type,
                   formatLabels(spec.prometheus.labels));
    } else if (spec.prometheusEnabled) {
        fmt::print(os,
                   R"(StatDef("{}"sv, {}, {}, {}, {}))",
                   prom,
                   spec.unit,
                   spec.type,
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

#if FMT_VERSION >= 100000
template <>
struct fmt::formatter<Spec> : ostream_formatter {};
#endif

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

/**
 * Generate a metrics documentation entry from the spec.
 * @return A tuple of the exported metric name and the doc entry.
 */
std::pair<std::string, nlohmann::json> generateDocEntry(const Spec& spec) {
    using namespace nlohmann;

    // documentation specification does not allow for untyped metrics.
    // If a type has not been provided, assume a gauge - this is the
    // most generic option.
    auto type = spec.type == prometheus::MetricType::Untyped
                        ? prometheus::MetricType::Gauge
                        : spec.type;

    // begin building json representation matching the format
    // documentation requires
    json statDoc{{"type", type}, {"stability", spec.stability}};

    if (!spec.added.empty()) {
        statDoc["added"] = spec.added;
    }

    // work out the full name
    // TODO: This duplicates logic done in StatDef, but that is
    // used in memcached itself, not here for code generation.
    // consider whether this can be consolidated.
    std::string_view suffix;
    if (!spec.unit.empty() && spec.unit != "none") {
        auto unit = cb::stats::Unit::from_string(spec.unit);
        auto baseUnitStr = to_string(unit.getBaseUnit());
        suffix = unit.getSuffix();
        statDoc["unit"] = baseUnitStr;
    }
    auto name =
            "kv_" + (spec.prometheus.family.empty() ? spec.enumKey
                                                    : spec.prometheus.family);
    name += suffix;

    return {std::move(name), std::move(statDoc)};
}

/**
 * Adds a documentation entry for metric family described by the stat
 * definition.
 *
 * If an entry for that metric family already exists (because there
 * are multiple stat definitions emitting the same metric family), the entry
 * becomes an array storing all of the conflicting definitions. Those must then
 * be coalesce into a single documentation entry.
 *
 * @see resolveMetricFamilyConflicts
 */
void addDocumentation(const Spec& spec,
                      const nlohmann::json& statJson,
                      nlohmann::json& documentation) {
    using namespace nlohmann;
    if (!spec.prometheusEnabled) {
        return;
    }

    auto [statName, statDoc] = generateDocEntry(spec);
    statDoc["help"] = statJson.value("description", "");

    if (statJson.contains("/prometheus/labels"_json_pointer)) {
        auto labels = json::array();
        for (const auto& elem :
             statJson["/prometheus/labels"_json_pointer].items()) {
            labels.push_back(elem.key());
        }

        if (!labels.empty()) {
            statDoc["labels"] = std::move(labels);
        }
    }

    if (!documentation.contains(statName)) {
        documentation[statName] = std::move(statDoc);
        return;
    } else if (documentation[statName].type() == json::value_t::array) {
        documentation[statName].push_back(std::move(statDoc));
        return;
    }

    auto existingEntry = documentation[statName];
    documentation[statName] =
            json::array({std::move(existingEntry), std::move(statDoc)});
}

/**
 * The stat_defnitions.json format is such that we might have multiple entries
 * describing the same metric family with different labels. For example,
 * kv_ops{op=get} and kv_ops{op=set} have two distinct definitions, but are the
 * same metric family.
 *
 * @return true on success, false if any conflicts occurred
 *
 * @see addDocumentation
 */
bool resolveMetricFamilyConflicts(
        nlohmann::json& documentation,
        const std::unordered_map<std::string, nlohmann::json>&
                familyDescriptions) {
    using namespace nlohmann;

    bool anyConflicts = false;

    for (auto& entry : documentation.items()) {
        if (entry.value().type() != json::value_t::array) {
            continue;
        }

        if (entry.value().size() < 2) {
            throw std::runtime_error(
                    fmt::format("Unexpected array, only {} variant for {}",
                                entry.value().size(),
                                entry.key()));
        }

        // For now, we just pick the first metadata entry that we happen to have
        // in the list. They should all be identical except for having different
        // descriptions.
        auto outputDocEntry = entry.value().at(0);

        // We have multiple definitions for the metric family in entry.key().
        // Need to pick one and a description.
        auto familyIt = familyDescriptions.find(entry.key());
        if (familyIt == familyDescriptions.end()) {
            anyConflicts = true;
            // This is not great to read but allows clearer output messages.
            auto suffix = cb::stats::Unit::from_string(
                                  outputDocEntry.value("unit", ""))
                                  .getSuffix();
            auto baseKey =
                    entry.key()
                            .substr(0, entry.key().size() - suffix.size())
                            .substr(3);
            fmt::print(stderr,
                       "Multiple definitions emit the same metric "
                       "family '{}' with different labels. To fix, ensure "
                       "there is a family description:\n"
                       "{{ \"family\": \"{}\", \"unit\": \"{}\", "
                       "\"description\": \"...\" }}\n",
                       entry.key(),
                       baseKey,
                       suffix.empty() ? "count" : suffix.substr(1));
            continue;
        }

        outputDocEntry["help"] = familyIt->second.value("description", "");
        entry.value() = outputDocEntry;
    }

    return !anyConflicts;
}

void addConfigDocumentation(const Spec& spec,
                            std::string_view helpText,
                            nlohmann::json& documentation) {
    auto [statName, statDoc] = generateDocEntry(spec);
    statDoc["help"] = helpText;

    documentation[statName] = std::move(statDoc);
}

int main(int argc, char** argv) {
    int cmd;
    const char* statjsonfile = nullptr;
    const char* configjsonfile = nullptr;
    const char* docoutputjsonfile = nullptr;
    const char* hfile = nullptr;
    const char* cfile = nullptr;

    while ((cmd = getopt(argc, argv, "j:C:d:c:h:")) != -1) {
        switch (cmd) {
        case 'j':
            statjsonfile = optarg;
            break;
        case 'C':
            configjsonfile = optarg;
            break;
        case 'd':
            docoutputjsonfile = optarg;
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
        docoutputjsonfile == nullptr || hfile == nullptr || cfile == nullptr) {
        usage();
    }

    nlohmann::json stats = readJsonFile(statjsonfile);
    nlohmann::json config = readJsonFile(configjsonfile);

    fmt::memory_buffer enumKeysBuf;
    fmt::memory_buffer statDefsBuf;

    // json output with a structure common to all components for consumption
    // by docs
    nlohmann::json documentation;

    bool anyFailedRequirements = false;

    // List of the family descriptions we've seen
    std::unordered_map<std::string, nlohmann::json> familyDescriptions;

    for (const auto& statJson : stats) {
        auto type = getDefinitionType(statJson);
        if (type == DefinitionType::FamilyDescription) {
            auto baseName = statJson.at("family").get<std::string>();
            auto unit = cb::stats::Unit::from_string(
                    statJson.value("unit", "none"));
            auto family = fmt::format("kv_{}{}", baseName, unit.getSuffix());
            familyDescriptions.insert(std::make_pair(family, statJson));
            continue;
        }
        if (type != DefinitionType::StatDefinition) {
            throw std::runtime_error("Unexpected definition type!");
        }

        // parse fields from json
        Spec spec = statJson;
        // check basic requirements are met
        if (!spec.validate()) {
            // Don't stop at the first failure. This means multiple issues
            // can be found in a single build rather than fail-fix-rebuild
            // for each instance.
            anyFailedRequirements = true;
            continue;
        }
        // format the enum key for the .h
        fmt::format_to(std::back_inserter(enumKeysBuf), "{},\n", spec.enumKey);
        // format the whole stat def for the .cc
        fmt::format_to(std::back_inserter(statDefsBuf), "{},\n", spec);

        addDocumentation(spec, statJson, documentation);
    }

    bool conflictsResolved =
            resolveMetricFamilyConflicts(documentation, familyDescriptions);

    if (!conflictsResolved || anyFailedRequirements) {
        // requirements failures will have been logged to stderr already
        // just exit and fail this build.
        return 1;
    }

    // generate header, containing enum keys and definition array decl
    std::ofstream docfile(docoutputjsonfile);
    if (!docfile.is_open()) {
        fmt::print(stderr,
                   "Unable to create documentation JSON file:{}\n",
                   docoutputjsonfile);
        return 1;
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
            spec.added = configVersionAdded;
            // We don't have a stability field for our config params, so for now
            // we decide what it should be here.
            spec.stability =
                    shouldDocumentConfigKey(key) ? "volatile" : "internal";
            // format the enum key for the .h
            fmt::format_to(
                    std::back_inserter(enumKeysBuf), "{},\n", spec.enumKey);
            // format the whole stat def for the .cc
            fmt::format_to(std::back_inserter(statDefsBuf), "{},\n", spec);

            auto type = configParam.value()["type"].get<std::string>();
            // String config keys will never be exposed via Prometheus.
            if (type == "std::string") {
                continue;
            }
            auto description = configParam.value().value("descr", "");
            addConfigDocumentation(spec, description, documentation);
        }
    }

    // Documentation contains all keys exposed via Prometheus, including
    // stats and config.
    docfile << documentation.dump(/* indent */ 4);
    docfile.close();

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

#include <prometheus/metric_type.h>

#include <string_view>

using namespace std::string_view_literals;

namespace cb::stats {{
using namespace units;
using prometheus::MetricType;
const std::array<StatDef, size_t(Key::enum_max)> statDefinitions{{{{
{}
}}}};
}} // end namespace cb::stats

)",
               std::string_view(statDefsBuf.data(), statDefsBuf.size()));

    return 0;
}
