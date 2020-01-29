/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include <json_utilities.h>
#include <platform/dirutils.h>

#include <nlohmann/json.hpp>

#include <sys/stat.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <map>

std::stringstream prototypes;
std::stringstream initialization;
std::stringstream implementation;

typedef std::string (*getValidatorCode)(const std::string&,
                                        const nlohmann::json&);

std::map<std::string, getValidatorCode> validators;
std::map<std::string, std::string> datatypes;

static std::string getDatatype(const std::string& key,
                               const nlohmann::json& json) {
    auto ret = json["type"].get<std::string>();
    auto iter = datatypes.find(ret);
    if (iter == datatypes.end()) {
        std::cerr << "Invalid datatype specified for \"" << key << "\": " << ret
                  << std::endl;
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
        std::cerr << "Incorrect syntax for a range validator specified for"
                  << "\"" << key << "\"." << std::endl
                  << "You need at least one of a min or a max clause."
                  << std::endl;
        exit(1);
    }

    // If min exists and is not a numeric type
    if (min != first->end() &&
        !(min->type() == nlohmann::json::value_t::number_integer ||
          min->type() == nlohmann::json::value_t::number_unsigned ||
          min->type() == nlohmann::json::value_t::number_float)) {
        std::cerr << "Incorrect datatype for the range validator specified for "
                  << "\"" << key << "\"." << std::endl
                  << "Only numbers are supported." << std::endl;
        exit(1);
    }

    // If max exists and is not of the correct type
    if (max != first->end() &&
        !(max->type() == nlohmann::json::value_t::number_integer ||
          max->type() == nlohmann::json::value_t::number_unsigned ||
          max->type() == nlohmann::json::value_t::number_float ||
          (max->type() == nlohmann::json::value_t::string &&
           max->get<std::string>() == "NUM_CPU"))) {
        std::cerr << "Incorrect datatype for the range validator specified for "
                  << "\"" << key << "\"." << std::endl
                  << "Only numbers are supported." << std::endl;
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

    std::string out = "(new " + validator_type + "())->min(" + mins +
                      ")->max(" + maxs + ")";
    return out;
}

static std::string getEnumValidatorCode(const std::string& key,
                                        const nlohmann::json& json) {
    // We've already made the checks to verify if these objects exist
    auto validator = json["validator"];
    auto first = validator.begin();

    if (first->type() != nlohmann::json::value_t::array) {
        std::cerr << "Incorrect enum value for " << key
                  << ".  Array of values is required." << std::endl;
        exit(1);
    }

    if (first->size() < 1) {
        std::cerr << "At least one validator enum element is required (" << key
                  << ")" << std::endl;
        exit(1);
    }

    std::stringstream ss;
    ss << "(new EnumValidator())";

    for (auto& obj : *first) {
        if (obj.type() != nlohmann::json::value_t::string) {
            std::cerr << "Incorrect validator for " << key
                      << ", all enum entries must be strings." << std::endl;
            exit(1);
        }
        ss << "\n\t\t->add(\"" << obj.get<std::string>() << "\")";
    }
    return ss.str();
}

static void initialize() {
    const char* header = R"(/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0"
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// ###########################################
// # DO NOT EDIT! THIS IS A GENERATED FILE
// ###########################################
)";

    prototypes << header << R"(
#pragma once

#include <string>
)";

    implementation << header << R"(
#include "configuration.h"
#include "configuration_impl.h"
#include <platform/sysinfo.h>
#include <limits>

using namespace std::string_literals;

)";

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
        std::cerr << e.what() << std::endl;
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
        std::cerr << "Only one validator can be specified for " << key
                  << std::endl;
        exit(1);
    }

    // Get the validator json (first element)
    auto first = validator->begin();

    // Lookup the correct function from the map
    std::map<std::string, getValidatorCode>::iterator iter;
    iter = validators.find(first.key());
    if (iter == validators.end()) {
        std::cerr << "Unknown validator specified for \"" << key << "\": \""
                  << first->get<std::string>() << "\"" << std::endl;
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
    if (requirements == json.end() || requirements->size() <= 0) {
        return "";
    }

    std::ostringstream ss;

    ss << "(new Requirement)\n";

    for (auto req : requirements->items()) {
        auto reqKey = req.key();

        auto reqParam = params.find(key);
        if (reqParam == params.end()) {
            std::cerr << "Required parameter \"" << reqKey
                      << "\" for parameter \"" << key << "\" does not exist"
                      << std::endl;
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
        }

        ss << "        ->add(\"" << reqKey << "\", (" << type << ")" << value
           << ")";
    }

    return ss.str();
}

static std::string getGetterPrefix(const std::string& str) {
    if (str.compare("bool") == 0) {
        return "is";
    } else {
        return "get";
    }
}

static std::string getCppName(const std::string& str) {
    std::stringstream ss;
    bool doUpper = true;

    std::string::const_iterator iter;
    for (iter = str.begin(); iter != str.end(); ++iter) {
        if (*iter == '_') {
            doUpper = true;
        } else {
            if (doUpper) {
                ss << (char)toupper(*iter);
                doUpper = false;
            } else {
                ss << (char)*iter;
            }
        }
    }
    return ss.str();
}

static void generate(const nlohmann::json& params, const std::string& key) {
    std::string cppName = getCppName(key);

    auto json = params[key];
    std::string type = getDatatype(key, json);
    std::string defaultVal = json["default"].get<std::string>();

    if (defaultVal.compare("max") == 0 || defaultVal.compare("min") == 0) {
        if (type.compare("std::string") != 0) {
            std::stringstream ss;
            ss << "std::numeric_limits<" << type << ">::" << defaultVal << "()";
            defaultVal = ss.str();
        }
    }

    std::string validator = getValidator(key, json);
    std::string requirements = getRequirements(key, json, params);

    // Generate prototypes
    prototypes << "    " << type << " " << getGetterPrefix(type) << cppName
               << "() const;" << std::endl;
    const auto dynamic = !isReadOnly(json);

    if (dynamic) {
        prototypes << "    void set" << cppName << "(const " << type
                   << " &nval);" << std::endl;
    }

    // Generate initialization code
    initialization << "    addParameter(\"" << key << "\", " << std::boolalpha;
    if (type == "std::string") {
        initialization << "\"" << defaultVal << "\"s, ";
    } else {
        initialization << type << "(" << defaultVal << "), ";
    }
    initialization << dynamic << ");" << std::endl;

    if (!validator.empty()) {
        initialization << "    setValueValidator(\"" << key << "\", "
                       << validator << ");" << std::endl;
    }
    if (!requirements.empty()) {
        initialization << "    setRequirements(\"" << key << "\", "
                       << requirements << ");" << std::endl;
    }
    if (hasAliases(json)) {
        for (std::string alias : getAliases(json)) {
            initialization << "    addAlias(\"" << key << "\", \"" << alias
                           << "\");" << std::endl;
        }
    }

    // Generate the getter
    implementation << type << " Configuration::" << getGetterPrefix(type)
                   << cppName << "() const {" << std::endl
                   << "    return "
                   << "getParameter<" << datatypes[type] << ">(\"" << key
                   << "\");" << std::endl
                   << "}" << std::endl;

    if (!isReadOnly(json)) {
        // generate the setter
        implementation << "void Configuration::set" << cppName << "(const "
                       << type << " &nval) {" << std::endl
                       << "    setParameter(\"" << key << "\", nval);"
                       << std::endl
                       << "}" << std::endl;
    }
}

/**
 * Read "configuration.json" and generate getters and setters
 * for the parameters in there
 */
int main(int argc, char **argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " "
                  << "<input config file> <header> <source>\n";
        return 1;
    }

    const char* file = argv[1];
    const char* header = argv[2];
    const char* source = argv[3];

    initialize();

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(cb::io::loadFile(file));
    } catch (const nlohmann::json::exception& e) {
        std::cerr << "Failed to parse JSON. e.what()=" << e.what() << std::endl;
        return 1;
    }

    auto params = json.find("params");
    if (params == json.end()) {
        std::cerr << "FATAL: could not find \"params\" section" << std::endl;
        return 1;
    }

    for (const auto& obj : params->items()) {
        generate(*params, obj.key());
    }

    std::ofstream headerfile(header);
    if (!headerfile.is_open()) {
        std::cerr << "Unable to create header file : " << header << std::endl;
        return 1;
    }
    headerfile << prototypes.str();
    headerfile.close();

    std::ofstream implfile(source);
    if (!implfile.is_open()) {
        std::cerr << "Unable to create source file : " << header << std::endl;
        return 1;
    }
    implfile << implementation.str() << std::endl
             << "void Configuration::initialize() {" << std::endl
             << initialization.str() << "}" << std::endl;
    implfile.close();
    return 0;
}
