/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "utilities/readfile.h"

#include <fmt/format.h>
#include <json_utilities.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <fstream>
#include <map>
#include <string>

std::string prototypes;
std::string initialization;
std::string implementation;
std::string addStatImplementation;

using getValidatorCode = std::string (*)(const std::string&,
                                         const nlohmann::json&);

std::map<std::string, getValidatorCode> validators;
std::map<std::string, std::string> datatypes;

static std::string getDatatype(const std::string& key,
                               const nlohmann::json& json) {
    auto ret = json["type"].get<std::string>();
    auto iter = datatypes.find(ret);
    if (iter == datatypes.end()) {
        fmt::print(stderr,
                   "Invalid datatype specified for \"{}\": {}\n",
                   key,
                   ret);
        exit(1);
    }

    return iter->second;
}

static std::string getRangeValidatorCode(const std::string& key,
                                         const nlohmann::json& json) {
    // We've already made the checks to verify that these objects exist
    auto validator = json["validator"];
    auto first = validator.begin();

    auto min = first->find("min");
    auto max = first->find("max");
    if (min == first->end() && max == first->end()) {
        fmt::print(
                stderr,
                "Incorrect syntax for a range validator specified for\"{}\".\n"
                "You need at least one of a min or a max clause.\n",
                key);
        exit(1);
    }

    // If min exists and is not a numeric type
    if (min != first->end() &&
        !(min->type() == nlohmann::json::value_t::number_integer ||
          min->type() == nlohmann::json::value_t::number_unsigned ||
          min->type() == nlohmann::json::value_t::number_float)) {
        fmt::print(stderr,
                   "Incorrect datatype for the range validator specified for "
                   "\"{}\"\nOnly numbers are supported.\n",
                   key);
        exit(1);
    }

    // If max exists and is not of the correct type
    if (max != first->end() &&
        !(max->type() == nlohmann::json::value_t::number_integer ||
          max->type() == nlohmann::json::value_t::number_unsigned ||
          max->type() == nlohmann::json::value_t::number_float ||
          (max->type() == nlohmann::json::value_t::string &&
           max->get<std::string>() == "NUM_CPU"))) {
        fmt::print(stderr,
                   "Incorrect datatype for the range validator specified for "
                   "\"{}\"\nOnly numbers are supported.\n",
                   key);
        exit(1);
    }

    std::string validator_type;
    std::string mins;
    std::string maxs;

    if (getDatatype(key, json) == "float") {
        validator_type = "FloatRangeValidator";
        if (min != first->end()) {
            mins = std::to_string(min->get<float>());
        } else {
            mins = "std::numeric_limits<float>::min()";
        }
        if (max != first->end()) {
            maxs = std::to_string(max->get<float>());
        } else {
            maxs = "std::numeric_limits<float>::max()";
        }
    } else if (getDatatype(key, json) == "ssize_t") {
        validator_type = "SSizeRangeValidator";
        if (min != first->end()) {
            mins = std::to_string(min->get<int64_t>());
        } else {
            mins = "std::numeric_limits<ssize_t>::min()";
        }
        if (max != first->end()) {
            maxs = std::to_string(max->get<int64_t>());
        } else {
            maxs = "std::numeric_limits<ssize_t>::max()";
        }
    } else {
        validator_type = "SizeRangeValidator";
        if (min != first->end()) {
            mins = std::to_string(min->get<uint64_t>());
        } else {
            mins = "std::numeric_limits<size_t>::main()";
        }
        if (max != first->end() &&
            max->type() == nlohmann::json::value_t::string &&
            max->get<std::string>() == "NUM_CPU") {
            maxs = "Couchbase::get_available_cpu_count()";
        } else if (max != first->end()) {
            maxs = std::to_string(max->get<uint64_t>());
        } else {
            maxs = "std::numeric_limits<size_t>::max()";
        }
    }

    return fmt::format(
            "(new {}())->min({})->max({})", validator_type, mins, maxs);
}

static std::string getEnumValidatorCode(const std::string& key,
                                        const nlohmann::json& json) {
    // We've already made the checks to verify if these objects exist
    auto validator = json["validator"];
    auto first = validator.begin();

    if (first->type() != nlohmann::json::value_t::array) {
        fmt::print(
                stderr,
                "Incorrect enum value for {}.  Array of values is required.\n",
                key);
        exit(1);
    }

    if (first->empty()) {
        fmt::print(stderr,
                   "At least one validator enum element is required ({})\n",
                   key);
        exit(1);
    }

    std::string out("(new EnumValidator())");

    for (auto& obj : *first) {
        if (obj.type() != nlohmann::json::value_t::string) {
            fmt::print(stderr,
                       "Incorrect validator for {}, all enum entries must be "
                       "strings.\n",
                       key);
            exit(1);
        }
        out += fmt::format("\n\t\t->add(\"{}\")", obj.get<std::string>());
    }
    return out;
}

static void initialize() {
    std::string_view header(R"(/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// ###########################################
// # DO NOT EDIT! THIS IS A GENERATED FILE
// ###########################################
)");

    prototypes += header;
    implementation += header;
    implementation += std::string_view{R"(
#include "configuration.h"
#include "configuration_impl.h"
#include <platform/sysinfo.h>
#include <statistics/labelled_collector.h>
#include <limits>

using namespace std::string_literals;

)"};

    validators["range"] = getRangeValidatorCode;
    validators["enum"] = getEnumValidatorCode;
    datatypes["bool"] = "bool";
    datatypes["size_t"] = "size_t";
    datatypes["ssize_t"] = "ssize_t";
    datatypes["float"] = "float";
    datatypes["string"] = "std::string";
    datatypes["std::string"] = "std::string";
}

static bool isReadOnly(const nlohmann::json& json) {
    try {
        return !cb::jsonGet<bool>(json, "dynamic");
    } catch (const nlohmann::json::exception& e) {
        fmt::print(stderr, "{}\n", e.what());
        exit(1);
    }
    return false;
}

static bool hasAliases(const nlohmann::json& json) {
    auto aliases = json.find("aliases");
    if (aliases == json.end()) {
        return false;
    }

    if (aliases->type() == nlohmann::json::value_t::string ||
        aliases->type() == nlohmann::json::value_t::array) {
        return true;
    }

    return false;
}

static std::vector<std::string> getAliases(const nlohmann::json& json) {
    auto aliases = json.find("aliases");

    std::vector<std::string> output;

    if (aliases->type() == nlohmann::json::value_t::string) {
        output.emplace_back(aliases->get<std::string>());
    } else if (aliases->type() == nlohmann::json::value_t::array) {
        for (auto elem : *aliases) {
            output.emplace_back(elem.get<std::string>());
        }
    }

    return output;
}

static std::string getValidator(const std::string& key,
                                const nlohmann::json& json) {
    auto validator = json.find("validator");
    if (validator == json.end()) {
        // No validator found
        return "";
    }

    // Abort early if the validator is bad
    if (validator->size() != 1) {
        fmt::print(stderr, "Only one validator can be specified for {}\n", key);
        exit(1);
    }

    // Get the validator json (first element)
    auto first = validator->begin();

    // Lookup the correct function from the map
    std::map<std::string, getValidatorCode>::iterator iter;
    iter = validators.find(first.key());
    if (iter == validators.end()) {
        fmt::print(stderr,
                   "Unknown validator specified for \"{}\": \"{}\"\n",
                   key,
                   first->get<std::string>());
        exit(1);
    }

    return (iter->second)(key, json);
}

/**
 * Generates code from the requirements field.
 *
 * Generates code to be used in generated_configuration.cc constructing the
 * Requirement object and adding the appropriate requirements.
 * @param key key to generate requirements for
 * @param json json object representing the config parameter
 * @param params json object of all parameters, required to determine the
 * intended type of the required parameter.
 * @return string of the code constructing a Requirement object.
 */
static std::string getRequirements(const std::string& key,
                                   const nlohmann::json& json,
                                   const nlohmann::json& params) {
    auto requirements = json.find("requires");
    if (requirements == json.end() || requirements->empty()) {
        return "";
    }

    std::string out("(new Requirement)\n");

    for (auto req : requirements->items()) {
        auto reqKey = req.key();

        auto reqParam = params.find(key);
        if (reqParam == params.end()) {
            fmt::print(stderr,
                       "Required parameter \"{}\" for parameter \"{}\" does "
                       "not exist\n",
                       reqKey,
                       key);
            exit(1);
        }

        auto type = getDatatype(reqKey, params[reqKey]);
        std::string value;

        switch (req.value().type()) {
        case nlohmann::json::value_t::string:
            value = std::string("\"") + req.value().get<std::string>() + "\"";
            break;
        case nlohmann::json::value_t::number_unsigned:
            value = std::to_string(req.value().get<uint64_t>());
            break;
        case nlohmann::json::value_t::number_integer:
            value = std::to_string(req.value().get<int64_t>());
            break;
        case nlohmann::json::value_t::number_float:
            value = std::to_string(req.value().get<float_t>());
            break;
        case nlohmann::json::value_t::boolean:
            value = req.value().get<bool>() ? "true" : "false";
            break;
        case nlohmann::json::value_t::array:
        case nlohmann::json::value_t::discarded:
        case nlohmann::json::value_t::null:
        case nlohmann::json::value_t::object:
            break;
        case nlohmann::json::value_t::binary:
            throw std::runtime_error(
                    "getRequirements(): binary value not supported");
        }

        out += fmt::format("        ->add(\"{}\",({}){})", reqKey, type, value);
    }

    return out;
}

static std::string getGetterPrefix(const std::string& str) {
    return str == "bool" ? "is" : "get";
}

static std::string getCppName(const std::string& str) {
    std::string out;
    bool doUpper = true;

    for (const char& c : str) {
        if (c == '_') {
            doUpper = true;
        } else {
            if (doUpper) {
                out += toupper(c);
                doUpper = false;
            } else {
                out += c;
            }
        }
    }
    return out;
}

static std::string formatValue(const std::string value,
                               const std::string& type) {
    if (type == "std::string") {
        return fmt::format("\"{}\"s", value);
    } else {
        return fmt::format("static_cast<{}>({})", type, value);
    }
}

static void generate(const nlohmann::json& params, const std::string& key) {
    auto cppName = getCppName(key);

    auto json = params[key];
    auto type = getDatatype(key, json);

    auto defaultVal = json["default"];

    std::string defaultValStr;
    std::string defaultValServerless;
    if (defaultVal.is_object()) {
        if (defaultVal.find("on-prem") == defaultVal.end()) {
            fmt::print(stderr,
                       "Default is an object but no \"on-prem\" key found for "
                       "entry:'{}' raw_json:'{}'\n",
                       key,
                       json.dump());
            exit(1);
        }
        if (defaultVal.find("serverless") == defaultVal.end()) {
            fmt::print(stderr,
                       "Default is an object but no \"serverless\" key found "
                       "for entry:'{}' raw_json:'{}'\n",
                       key,
                       json.dump());
            exit(1);
        }

        defaultValStr = defaultVal["on-prem"].get<std::string>();
        defaultValServerless = defaultVal["serverless"].get<std::string>();
    } else {
        defaultValStr = defaultVal.get<std::string>();
    }

    if (type != "std::string") {
        if (defaultValStr == "max" || defaultValStr == "min") {
            defaultValStr = fmt::format(
                    "std::numeric_limits<{}>::{}()", type, defaultValStr);
        }
        if (defaultValServerless == "max" || defaultValServerless == "min") {
            defaultValServerless = fmt::format("std::numeric_limits<{}>::{}()",
                                               type,
                                               defaultValServerless);
        }
    }

    auto validator = getValidator(key, json);
    auto requirements = getRequirements(key, json, params);

    // Generate prototypes
    prototypes += fmt::format(
            "    {} {}{}() const;\n", type, getGetterPrefix(type), cppName);
    const auto dynamic = !isReadOnly(json);

    if (dynamic) {
        prototypes +=
                fmt::format("    void set{}(const {} &nval);\n", cppName, type);
    }

    // Generate initialization code
    if (defaultVal.is_object()) {
        initialization += fmt::format("    addParameter(\"{}\", {}, {}, {});\n",
                                      key,
                                      formatValue(defaultValStr, type),
                                      formatValue(defaultValServerless, type),
                                      dynamic);
    } else {
        initialization += fmt::format("    addParameter(\"{}\", {}, {});\n",
                                      key,
                                      formatValue(defaultValStr, type),
                                      dynamic);
    }

    if (!validator.empty()) {
        initialization += fmt::format(
                "    setValueValidator(\"{}\", {});\n", key, validator);
    }
    if (!requirements.empty()) {
        initialization += fmt::format(
                "    setRequirements(\"{}\", {});\n", key, requirements);
    }
    if (hasAliases(json)) {
        for (const auto& alias : getAliases(json)) {
            initialization +=
                    fmt::format("    addAlias(\"{}\", \"{}\");\n", key, alias);
        }
    }

    // Generate the getter
    implementation += fmt::format(
            "{} Configuration::{}{}() const {{\n    "
            "return getParameter<{}>(\"{}\");\n}}\n",
            type,
            getGetterPrefix(type),
            cppName,
            datatypes[type],
            key);
    if (!isReadOnly(json)) {
        // generate the setter
        implementation += fmt::format(
                "void Configuration::set{}(const {} &nval) {{\n"
                "    setParameter(\"{}\", nval);\n}}\n",
                cppName,
                type,
                key);
    }

    // collect all the aliases
    std::vector<std::string> names;
    if (hasAliases(json)) {
        names = getAliases(json);
    }
    // and the main config param name
    names.push_back(key);
    // add to the definition of Configuration::addStat
    for (const auto& name : names) {
        addStatImplementation += fmt::format(
                "    maybeAddStat(collector, Key::ep_{}, \"{}\"sv);\n",
                name,
                name);
    }
}

/**
 * Read "configuration.json" and generate getters and setters
 * for the parameters in there.
 */
int main(int argc, char **argv) {
    if (argc < 4) {
        fmt::print(stderr,
                   "Usage: {} <input config file> <header> <source>\n",
                   argv[0]);
        return 1;
    }

    const std::string file = argv[1];
    const std::string header = argv[2];
    const std::string source = argv[3];

    initialize();

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(readFile(file));
    } catch (const nlohmann::json::exception& e) {
        fmt::print(stderr, "Failed to parse JSON. e.what()=\n", e.what());
        return 1;
    }

    auto params = json.find("params");
    if (params == json.end()) {
        fmt::print(stderr, "FATAL: could not find \"params\" section\n");
        return 1;
    }

    for (const auto& obj : params->items()) {
        generate(*params, obj.key());
    }

    std::ofstream headerfile(header);
    if (!headerfile.is_open()) {
        fmt::print(stderr, "Unable to create header file : {}\n", header);
        return 1;
    }
    headerfile << prototypes;
    headerfile.close();

    std::ofstream implfile(source);
    if (!implfile.is_open()) {
        fmt::print(stderr, "Unable to create source file : {}\n", header);
        return 1;
    }
    implfile << implementation << std::endl
             << "void Configuration::initialize() {" << std::endl
             << initialization << "}" << std::endl
             << std::endl
             << "void Configuration::addStats(const BucketStatCollector& "
                "collector) const {"
             << std::endl
             << "    using namespace cb::stats;" << std::endl
             << "    using namespace std::string_view_literals;" << std::endl
             << "    std::lock_guard<std::mutex> lh(mutex);" << std::endl
             << addStatImplementation << "}" << std::endl;
    implfile.close();

    return 0;
}
