/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <mcbp/mcbp.h>
#include <mcbp/protocol/opcode.h>

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/timeutils.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <gsl/gsl>
#include <iostream>
#include <unordered_map>

namespace cb::mcbp::sla {

/**
 * Merge the content of document 2 into document 1 by overwriting
 * all values in document 1 with the value found in document 2.
 *
 * @param doc1 the resulting document
 * @param doc2 the document to remove the values from
 */
static void merge_docs(nlohmann::json& doc1, const nlohmann::json& doc2);

/**
 * The backing store for all of the thresholds. In order to make it easy
 * for ourself without any locking, just create a fixed array of atomics
 * and read out of it. It means that during "reinitializaiton" we might
 * return incorrect values, but let's just ignore that. In a deployed
 * system we'll initialize this during startup, and run with that
 * configuration until we stop.
 */
static std::array<std::atomic<std::chrono::nanoseconds>, 0x100> threshold;

/**
 * Convert the time to a textual representation which may be used
 * to generate the JSON representation of the SLAs
 */
static std::string time2text(std::chrono::nanoseconds time2convert) {
    const char* const extensions[] = {" ns", " us", " ms", " s", nullptr};
    int id = 0;
    auto time = time2convert.count();

    while (time > 999 && (time % 1000) == 0) {
        ++id;
        time /= 1000;
        if (extensions[id + 1] == nullptr) {
            break;
        }
    }

    return std::to_string(time) + extensions[id];
}

/**
 * Determine what to use as the "default" value in the JSON.
 *
 * To do that we'll count the number of times each timeout is specified,
 * and use the one with the highest count.
 *
 * Given that the functions is rarely called we don't care to try to optimize
 * it ;)
 */
static std::chrono::nanoseconds getDefaultValue() {
    std::unordered_map<uint64_t, size_t> counts;
    for (auto& ts : threshold) {
        counts[ts.load(std::memory_order_relaxed).count()]++;
    }
    auto result =
            std::max_element(counts.begin(),
                             counts.end(),
                             [](std::pair<const uint64_t, size_t> a,
                                std::pair<const uint64_t, size_t> b) -> bool {
                                 return a.second < b.second;
                             });
    return std::chrono::nanoseconds(result->first);
}

nlohmann::json to_json() {
    nlohmann::json ret;
    ret["version"] = 1;
    ret["comment"] = "Current MCBP SLA configuration";

    // Add a default entry:
    const auto def = getDefaultValue();
    ret["default"]["slow"] = time2text(def);

    for (unsigned int ii = 0; ii < threshold.size(); ++ii) {
        try {
            auto opcode = cb::mcbp::ClientOpcode(ii);
            std::string cmd = ::to_string(opcode);
            const auto ns = threshold[ii].load(std::memory_order_relaxed);
            if (ns != def) {
                // It differs from the default value
                ret[cmd]["slow"] = time2text(ns);
            }
        } catch (const std::exception&) {
            // unknown command. ignore
        }
    }

    return ret;
}

std::chrono::nanoseconds getSlowOpThreshold(cb::mcbp::ClientOpcode opcode) {
    // This isn't really safe, but we don't want to use proper synchronization
    // in this case as it is part of the command execution for _all_ commands.
    // The _worst case_ scenario is that our reporting is incorrect while
    // we're reconfiguring the system.
    //
    // During reconfiguration we'll first try to look up the default value,
    // then initialize all of the entries with the default value. We'll then
    // apply the value for each of the individual entries.
    return threshold[uint8_t(opcode)].load(std::memory_order_relaxed);
}

/**
 * Read and merge all of the files specified in the system default locations:
 *
 *     /etc/couchbase/kv/opcode-attributes.json
 *     /etc/couchbase/kv/opcode-attributes.d/<*.json>
 *
 * @param root the root directory (prepend to the paths above)
 * @return The merged all of the on-disk files
 */
static nlohmann::json mergeFilesOnDisk(const std::string& root) {
    // First try to read the system default
    auto system = cb::io::sanitizePath(
            root + "/etc/couchbase/kv/opcode-attributes.json");
    nlohmann::json configuration;

    if (cb::io::isFile(system)) {
        const auto content = cb::io::loadFile(system);
        nlohmann::json doc;
        try {
            doc = nlohmann::json::parse(content);
        } catch (const nlohmann::json::exception&) {
            throw std::invalid_argument(
                    "cb::mcbp::sla::reconfigure: Invalid json in '" + system +
                    "'");
        }
        reconfigure(doc, false);
        configuration = std::move(doc);
    }

    // Replace .json with .d
    system.resize(system.size() - 4);
    system.push_back('d');

    if (cb::io::isDirectory(system)) {
        auto files = cb::io::findFilesWithPrefix(system, "");
        std::sort(files.begin(), files.end());
        for (const auto& file : files) {
            // Skip files which don't end with ".json"
            if (file.find(".json") != file.size() - 5) {
                continue;
            }

            const auto content = cb::io::loadFile(file);
            nlohmann::json doc;
            try {
                doc = nlohmann::json::parse(content);
            } catch (const nlohmann::json::exception&) {
                throw std::invalid_argument(
                        "cb::mcbp::sla::reconfigure: Invalid json in '" +
                        system + "'");
            }
            reconfigure(doc, false);
            if (configuration.empty()) {
                configuration = std::move(doc);
            } else {
                merge_docs(configuration, doc);
            }
        }
    }

    return configuration;
}

void reconfigure(const std::string& root) {
    auto configuration = mergeFilesOnDisk(root);

    if (!configuration.empty()) {
        reconfigure(configuration);
    }
}

void reconfigure(const std::string& root, const nlohmann::json& override) {
    auto configuration = mergeFilesOnDisk(root);

    if (!configuration.empty()) {
        merge_docs(configuration, override);
        reconfigure(configuration);
    } else {
        reconfigure(override);
    }
}

/**
 * Reconfigure the system with the provided JSON document by first
 * trying to look up the default entry. If found we'll be setting
 * all of the entries in our map to that value before iterating over
 * all of the entries in the JSON document and try to update that
 * single command.
 *
 * The format of the JSON document looks like:
 *
 *     {
 *       "version": 1,
 *       "default": {
 *         "slow": 500
 *       },
 *       "get": {
 *         "slow": 100
 *       },
 *       "compact_db": {
 *         "slow": "30 m"
 *       }
 *     }
 */
void reconfigure(const nlohmann::json& doc, bool apply) {
    // Check the version!
    auto version = doc.find("version");
    if (version == doc.end()) {
        throw std::invalid_argument(
                "cb::mcbp::sla::reconfigure: Missing mandatory element "
                "'version'");
    }

    if (!version->is_number()) {
        throw std::invalid_argument(
                "cb::mcbp::sla::reconfigure: 'version' should be a number");
    }

    if (version->get<int>() != 1) {
        throw std::invalid_argument(
                "cb::mcbp::sla::reconfigure: Unsupported version: " +
                std::to_string(version->get<int>()));
    }

    // Check if we've got a default entry:
    auto obj = doc.find("default");
    if (obj != doc.end()) {
        // Handle default entry
        auto val = getSlowOpThreshold(*obj);
        if (apply) {
            for (auto& t : threshold) {
                t.store(val, std::memory_order_relaxed);
            }
        }
    }

    for (auto it = doc.cbegin(); it != doc.cend(); ++it) {
        if (it.key() == "version" || it.key() == "default" ||
            it.key() == "comment") {
            // Ignore these entries
            continue;
        }

        cb::mcbp::ClientOpcode opcode;
        try {
            opcode = to_opcode(it.key());
        } catch (const std::invalid_argument&) {
            throw std::invalid_argument(
                    std::string{
                            "cb::mcbp::sla::reconfigure: Unknown command '"} +
                    it.key() + "'");
        }
        auto value = getSlowOpThreshold(it.value());
        if (apply) {
            threshold[uint8_t(opcode)].store(value, std::memory_order_relaxed);
        }
    }
}

std::chrono::nanoseconds getSlowOpThreshold(const nlohmann::json& doc) {
    if (!doc.is_object()) {
        throw std::invalid_argument(
                "cb::mcbp::sla::getSlowOpThreshold: Entry is not an object");
    }

    auto val = doc.find("slow");
    if (val == doc.end()) {
        throw std::invalid_argument(
                "cb::mcbp::sla::getSlowOpThreshold: Entry does not contain a "
                "mandatory 'slow' entry");
    }

    if (val->is_number()) {
        return std::chrono::milliseconds(val->get<uint64_t>());
    }

    if (!val->is_string()) {
        throw std::invalid_argument(
                "cb::mcbp::sla::getSlowOpThreshold: Entry is not a value or a "
                "string");
    }

    try {
        return cb::text2time(val->get<std::string>());
    } catch (const std::invalid_argument&) {
        throw std::invalid_argument(
                "cb::mcbp::sla::getSlowOpThreshold: Entry contains an unknown "
                "time unit");
    }
}

static void merge_docs(nlohmann::json& doc1, const nlohmann::json& doc2) {
    for (auto it = doc2.cbegin(); it != doc2.cend(); ++it) {
        if (it.key() == "version" || it.key() == "comment") {
            // Ignore these entries
            continue;
        }

        // For some reason we don't have a slow entry!
        auto slow = it.value().find("slow");
        if (slow == it.value().end()) {
            continue;
        }

        doc1[it.key()] = it.value();
    }
}

} // namespace cb::mcbp::sla
