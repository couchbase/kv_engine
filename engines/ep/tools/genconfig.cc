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

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <utilities/json_utilities.h>
#include <utilities/readfile.h>
#include <fstream>
#include <map>
#include <string>

std::string prototypes;
std::string initialization;
std::string conditionalInitialization;
std::string implementation;
std::string addStatImplementation;
std::string enumPrototypes;
std::string enumImplementation;

using getValidatorCode = std::string (*)(const std::string&,
                                         const nlohmann::json&);

std::map<std::string, getValidatorCode> validators;
std::map<std::string, std::string> datatypes;

static std::string getCppName(const std::string& str);

static std::string formatValue(const std::string value,
                               const std::string& type);

static std::string getDatatype(const std::string& key,
                               const nlohmann::json& json) {
    auto ret = json["type"].get<std::string>();
    auto iter = datatypes.find(ret);
    if (iter == datatypes.end()) {
        fmt::print(stderr,
                   "Invalid datatype specified for \"{}\": {}\n",
                   key,
                   ret);
        exit(EXIT_FAILURE);
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
        exit(EXIT_FAILURE);
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
        exit(EXIT_FAILURE);
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
        exit(EXIT_FAILURE);
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
        exit(EXIT_FAILURE);
    }

    if (first->empty()) {
        fmt::print(stderr,
                   "At least one validator enum element is required ({})\n",
                   key);
        exit(EXIT_FAILURE);
    }

    return fmt::format("(new TypedEnumValidator<cb::config::{}>())",
                       getCppName(key));
}

static void initialize() {
    std::string_view header(R"(/*
 *     Copyright 2024-Present Couchbase, Inc.
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
    enumPrototypes += header;
    enumImplementation += header;

    implementation += std::string_view{R"(
#include "configuration.h"
#include "configuration_impl.h"
#include <platform/sysinfo.h>
#include <statistics/labelled_collector.h>
#include <limits>

using namespace std::string_literals;

)"};

    enumImplementation += std::string_view{R"(
#include "configuration.h"
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
        exit(EXIT_FAILURE);
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

/**
 * Returns the validator and the generated code for the validator.
 */
static std::pair<std::string, std::string> getValidatorAndCode(
        const std::string& key, const nlohmann::json& json) {
    auto validator = json.find("validator");
    if (validator == json.end()) {
        // No validator found
        return {"", ""};
    }

    // Abort early if the validator is bad
    if (validator->size() != 1) {
        fmt::print(stderr, "Only one validator can be specified for {}\n", key);
        exit(EXIT_FAILURE);
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
        exit(EXIT_FAILURE);
    }

    return {first.key(), (iter->second)(key, json)};
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
            exit(EXIT_FAILURE);
        }

        auto type = datatypes[getDatatype(reqKey, params[reqKey])];
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

static std::string getEnumDefinitionCode(const std::string& key,
                                         const nlohmann::json& enumValidator) {
    std::string out(fmt::format("/// Possible values for \"{}\".\n", key));
    auto typeName = getCppName(key);
    out += fmt::format("enum class {} {{\n", typeName);
    for (auto& value : enumValidator) {
        out += std::string(4, ' ');
        out += getCppName(value.get<std::string>());
        out += ",\n";
    }

    out += "};\n";
    out += fmt::format("std::string_view format_as({});\n", typeName);
    out += fmt::format(
            "void from_string({}&, std::string_view);\n", typeName, typeName);
    return out;
}

static std::string getEnumImplementationCode(
        const std::string& key, const nlohmann::json& enumValidator) {
    auto typeName = getCppName(key);

    // Formatting
    std::string out;
    out += fmt::format("std::string_view cb::config::format_as({} value) {{\n",
                       typeName);

    out += "    switch (value) {\n";
    for (auto& value : enumValidator) {
        out += fmt::format("    case {}::{}: return \"{}\";\n",
                           typeName,
                           getCppName(value.get<std::string>()),
                           value.get<std::string>());
    }
    out += "    }\n";

    out += fmt::format(
            "    throw std::range_error(\"Invalid value for {}: \" + "
            "std::to_string(static_cast<int>(value)));\n",
            key);
    out += "}\n";

    // Parsing
    out += fmt::format(
            "void cb::config::from_string({}& out, std::string_view input) "
            "{{\n",
            typeName);
    for (auto& value : enumValidator) {
        out += fmt::format(
                "    if (input == \"{}\") {{ out = {}::{}; return; }}\n",
                value.get<std::string>(),
                typeName,
                getCppName(value.get<std::string>()));
    }
    out += fmt::format(
            "    throw std::range_error(\"Invalid value for {}: \" + "
            "std::string(input));\n",
            key);
    out += "}\n";

    return out;
}

static std::string formatValue(const std::string value,
                               const std::string& type) {
    if (type == "std::string" || type.find("Configuration::") == 0) {
        return fmt::format("\"{}\"s", value);
    }
    return fmt::format("static_cast<{}>({})", type, value);
}

/**
 * Remove leading characters.
 */
static std::string_view trimLeft(std::string_view input,
                                 std::string_view chars = "\t\n ") {
    const auto idx = input.find_first_not_of(chars);
    if (idx == std::string_view::npos) {
        return input;
    }
    return input.substr(idx);
}

static bool isOnPremOrServerless(const nlohmann::json& defaultVal) {
    // Both keys must be defined when defaulting via on-prem/serverless
    if (defaultVal.count("on-prem") && defaultVal.count("serverless")) {
        return true;
    }

    return false;
}

struct KeyMatch {
    std::string key;
    nlohmann::json keyObject;
    std::string matches;
    std::string then;
};

struct ConditionalDefault {
    KeyMatch ifMatch;
    std::vector<KeyMatch> elseIfs;
    std::string elseValue;
};

static ConditionalDefault getConditionalDefault(
        const nlohmann::json& params, const nlohmann::json& defaultVal) {
    auto conditional = defaultVal.find("if");
    if (conditional == defaultVal.end() || defaultVal.size() > 1) {
        fmt::print(stderr,
                   "Error: default object does not define a valid \"if\": {}\n",
                   defaultVal.dump());
        exit(EXIT_FAILURE);
    }

    // Build the conditional up, which has the if condition, an optional series
    // of elif statements and a final else.
    ConditionalDefault rv;
    auto entry = conditional->begin();
    if (params.count(entry.key()) == 0) {
        fmt::print(stderr,
                   "if condition {} refers to an unknown parameter {}\n",
                   conditional->dump(),
                   entry.key());
        exit(EXIT_FAILURE);
    }

    rv.ifMatch = KeyMatch{entry.key(),
                          *params.find(entry.key()),
                          entry.value().at("equals").get<std::string>(),
                          entry.value().at("then").get<std::string>()};

    // Look for the optional array of elif conditions
    if (entry.value().count("elif")) {
        for (const auto& elif : entry.value().at("elif")) {
            if (elif.size() != 1) {
                fmt::print(stderr,
                           "Error: \"elif\" must have 1 key {}\n",
                           elif.dump());
                exit(EXIT_FAILURE);
            }

            // The elif is a key:value where key is the parameter to test.
            auto parameter = elif.begin();
            if (params.count(parameter.key()) == 0) {
                fmt::print(stderr,
                           "Error: elif {} refers to an unknown "
                           "parameter {}\n",
                           elif.dump(),
                           parameter.key());
                exit(EXIT_FAILURE);
            }

            rv.elseIfs.emplace_back(
                    KeyMatch{parameter.key(),
                             *params.find(parameter.key()),
                             parameter.value().at("equals").get<std::string>(),
                             parameter.value().at("then").get<std::string>()});
        }
    }

    // finally require an else
    if (conditional->count("else")) {
        rv.elseValue = conditional->at("else").get<std::string>();
    } else {
        fmt::print(stderr,
                   "Error: else condition is not found {}\n",
                   conditional->dump());
        exit(EXIT_FAILURE);
    }

    return rv;
}

static void generate(const nlohmann::json& params, const std::string& key) {
    auto cppName = getCppName(key);

    auto json = params[key];
    auto type = getDatatype(key, json);

    auto defaultVal = json["default"];

    std::string defaultValStr;
    std::string defaultValServerless;
    std::optional<std::string> defaultValTSAN;
    std::optional<ConditionalDefault> conditionalDefault;
    if (defaultVal.is_object()) {
        if (auto tsanFound = defaultVal.find("tsan");
            tsanFound != defaultVal.end()) {
            defaultValTSAN = tsanFound->get<std::string>();
        }
        if (isOnPremOrServerless(defaultVal)) {
            defaultValStr = defaultVal["on-prem"].get<std::string>();
            defaultValServerless = defaultVal["serverless"].get<std::string>();
        }
        if (defaultVal.count("if")) {
            // Either a conditional if or serverless/tsan
            if (defaultValTSAN || defaultValStr.size() ||
                defaultValServerless.size()) {
                fmt::println(stderr,
                             "Error: if condition with tsan/server/on-prem "
                             "keys not supported {}",
                             defaultVal.dump());
                exit(EXIT_FAILURE);
            }
            conditionalDefault = getConditionalDefault(params, defaultVal);
        }
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
        if (defaultValTSAN.has_value() &&
            (*defaultValTSAN == "max" || *defaultValTSAN == "min")) {
            defaultValTSAN = fmt::format(
                    "std::numeric_limits<{}>::{}()", type, *defaultValTSAN);
        }
    }

    auto [validator, validatorCode] = getValidatorAndCode(key, json);
    auto requirements = getRequirements(key, json, params);

    const bool isEnum = validator == "enum";
    std::string getterSuffix = isEnum ? "String" : "";

    // Generate prototypes
    prototypes += fmt::format("    {} {}{}{}() const;\n",
                              type,
                              getGetterPrefix(type),
                              cppName,
                              getterSuffix);
    const auto dynamic = !isReadOnly(json);

    if (dynamic) {
        prototypes +=
                fmt::format("    void set{}(const {} &nval);\n", cppName, type);
    }

    if (isEnum) {
        enumPrototypes += getEnumDefinitionCode(key, json["validator"]["enum"]);
        prototypes += fmt::format(
                "    cb::config::{} get{}() const;\n", cppName, cppName);

        if (dynamic) {
            prototypes +=
                    fmt::format("    void set{}(const cb::config::{} &nval);\n",
                                cppName,
                                cppName);
        }
    }
    // Generate initialization code
    if (conditionalDefault) {
        // Generates if/else if/else code to set the parameter based on the
        // value of other parameters. This code will be emitted last to ensure
        // it reads the current value of the input parameters.
        auto& conditional = *conditionalDefault;
        conditionalInitialization += fmt::format(
                "    if (getParameter<{}>(\"{}\") == {})",
                conditional.ifMatch.keyObject["type"].get<std::string>(),
                conditional.ifMatch.key,
                formatValue(conditional.ifMatch.matches,
                            conditional.ifMatch.keyObject["type"]
                                    .get<std::string>()));
        conditionalInitialization += "{\n    ";
        conditionalInitialization +=
                fmt::format("    setParameter(\"{}\", {});\n",
                            key,
                            formatValue(conditional.ifMatch.then, type));

        for (const auto& elif : conditional.elseIfs) {
            conditionalInitialization += "    } else if (";
            conditionalInitialization += fmt::format(
                    "getParameter<{}>(\"{}\") == {})",
                    elif.keyObject["type"].get<std::string>(),
                    elif.key,
                    formatValue(elif.matches,
                                elif.keyObject["type"].get<std::string>()));
            conditionalInitialization += "{\n    ";
            conditionalInitialization +=
                    fmt::format("    setParameter(\"{}\", {});\n",
                                key,
                                formatValue(elif.then, type));
        }

        if (!conditional.elseValue.empty()) {
            conditionalInitialization += "    } else {\n    ";
            conditionalInitialization +=
                    fmt::format("    setParameter(\"{}\", {});\n",
                                key,
                                formatValue(conditional.elseValue, type));
            conditionalInitialization += "    }\n";
        } else {
            conditionalInitialization += "    }\n    ";
        }

        // And still need to add "this" parameter, otherwise it cannot be
        // written to by the conditional init
        initialization +=
                fmt::format("   /*1*/ addParameter(\"{}\", {}, {});\n",
                            key,
                            formatValue(conditional.elseValue, type),
                            dynamic);

    } else if (defaultVal.is_object()) {
        initialization += fmt::format(
                "    addParameter(\"{}\", {}, {}, {{{}}}, {});\n",
                key,
                formatValue(defaultValStr, type),
                formatValue(defaultValServerless, type),
                (defaultValTSAN ? formatValue(*defaultValTSAN, type) : ""),
                dynamic);
    } else {
        initialization += fmt::format("    addParameter(\"{}\", {}, {});\n",
                                      key,
                                      formatValue(defaultValStr, type),
                                      dynamic);
    }

    if (!validatorCode.empty()) {
        initialization += fmt::format(
                "    setValueValidator(\"{}\", {});\n", key, validatorCode);
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
            "{} Configuration::{}{}{}() const {{\n    "
            "return getParameter<{}>(\"{}\");\n}}\n",
            type,
            getGetterPrefix(type),
            cppName,
            getterSuffix,
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

    if (isEnum) {
        implementation += fmt::format(fmt::runtime(trimLeft(R"#(
cb::config::{} Configuration::{}{}() const {{
    auto str = getParameter<{}>("{}");
    cb::config::{} val;
    from_string(val, str);
    return val;
}}
)#")),
                                      cppName,
                                      getGetterPrefix(type),
                                      cppName,
                                      datatypes[type],
                                      key,
                                      cppName,
                                      cppName);
        if (!isReadOnly(json)) {
            // generate the setter
            implementation += fmt::format(fmt::runtime(trimLeft(R"#(
void Configuration::set{}(const cb::config::{} &nval) {{
    setParameter("{}", std::string(format_as(nval)));
}}
)#")),
                                          cppName,
                                          cppName,
                                          key);
        }

        enumImplementation +=
                getEnumImplementationCode(key, json["validator"]["enum"]);
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
    if (argc < 6) {
        fmt::print(stderr,
                   "Usage: {} <input config file> <header> <source> "
                   "<enum-header> <enum-source>\n",
                   argv[0]);
        return 1;
    }

    const std::string file = argv[1];
    const std::string header = argv[2];
    const std::string source = argv[3];
    const std::string enumHeader = argv[4];
    const std::string enumSource = argv[5];

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

    {
        std::ofstream headerfile(header);
        if (!headerfile.is_open()) {
            fmt::print(stderr, "Unable to create header file : {}\n", header);
            return 1;
        }
        headerfile << prototypes;
        headerfile.close();
    }

    {
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
                 << "    using namespace std::string_view_literals;"
                 << std::endl
                 << addStatImplementation << "}" << std::endl;

        implfile << "void Configuration::runConditionalInitialize() {"
                 << std::endl
                 << conditionalInitialization << "}" << std::endl;
        implfile.close();
    }

    {
        std::ofstream enumHeaderfile(enumHeader);
        if (!enumHeaderfile.is_open()) {
            fmt::print(
                    stderr, "Unable to create header file : {}\n", enumHeader);
            return 1;
        }
        enumHeaderfile << enumPrototypes;
        enumHeaderfile.close();
    }

    {
        std::ofstream enumImplfile(enumSource);
        if (!enumImplfile.is_open()) {
            fmt::print(
                    stderr, "Unable to create source file : {}\n", enumSource);
            return 1;
        }
        enumImplfile << enumImplementation;
        enumImplfile.close();
    }

    return 0;
}
