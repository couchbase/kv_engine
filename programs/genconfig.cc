/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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
#include <cassert>
#include <cstdio>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <cerrno>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <map>

#include <ctype.h>

#include "cJSON.h"

using namespace std;

stringstream prototypes;
stringstream initialization;
stringstream implementation;

typedef string (*getValidatorCode)(const std::string &, cJSON*);

std::map<string, getValidatorCode> validators;
map<string, string> getters;
map<string, string> datatypes;


static std::ostream& operator <<(std::ostream &out, const cJSON *o) {
    switch (o->type) {
    case cJSON_Number:
        if (o->valueint != o->valuedouble) {
            out << (float)o->valuedouble;
        } else {
            out << o->valueint;
        }
        break;
    case cJSON_String:
        out << '"' << o->valuestring << '"';
        break;
    default:
        cerr << "Internal error.. unknow json code" << endl;
        abort();
    }
    return out;
}

static bool isFloat(const cJSON *o) {
    return o->valueint != o->valuedouble;
}

static string getRangeValidatorCode(const std::string &key, cJSON *o) {
    // the range validator should contain a "min" and "max" element
    cJSON *min = cJSON_GetObjectItem(o, "min");
    cJSON *max = cJSON_GetObjectItem(o, "max");

    if (min == 0 || max == 0) {
        cerr << "Incorrect syntax for a range validator specified for"
             << "\"" << key << "\"." << endl
             <<"You need both a min and max clause." << endl;
        exit(1);
    }

    if (min->type != max->type || min->type != cJSON_Number) {
        cerr << "Incorrect datatype for the range validator specified for "
             << "\"" << key << "\"." << endl
             << "Only numbers are supported." << endl;
        exit(1);
    }

    stringstream ss;
    if (isFloat(min) || isFloat(max)) {
        ss << "(new FloatRangeValidator())->min((float)" << min << ")->max((float)" << max << ")";
    } else {
        ss << "(new SizeRangeValidator())->min(" << min << ")->max(" << max << ")";
    }

    return ss.str();
}

static string getEnumValidatorCode(const std::string &key, cJSON *o) {

    if (o->type != cJSON_Array) {
        cerr << "Incorrect enum value for " << key
             << ".  Array of values is required." << endl;
        exit(1);
    }

    if (cJSON_GetArraySize(o) < 1) {
        cerr << "At least one validator enum element is required ("
             << key << ")" << endl;
        exit(1);
    }

    stringstream ss;
    ss << "(new EnumValidator())";

    for (cJSON *p(o->child); p; p = p->next) {
        if (p->type != cJSON_String) {
            cerr << "Incorrect validator for " << key
                 << ", all enum entries must be strings." << endl;
            exit(1);
        }
        char *value = cJSON_Print(p);
        ss << "\n\t\t->add(" << value << ")";
        free(value);
    }
    return ss.str();
}

static void initialize() {
    prototypes
        << "/*" << endl
        << " *     Copyright 2011 Couchbase, Inc" << endl
        << " *" << endl
        << " *   Licensed under the Apache License, Version 2.0 (the \"License\");" << endl
        << " *   you may not use this file except in compliance with the License." << endl
        << " *   You may obtain a copy of the License at" << endl
        << " *" << endl
        << " *       http://www.apache.org/licenses/LICENSE-2.0" << endl
        << " *" << endl
        << " *   Unless required by applicable law or agreed to in writing, software" << endl
        << " *   distributed under the License is distributed on an \"AS IS\" BASIS," << endl
        << " *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." << endl
        << " *   See the License for the specific language governing permissions and" << endl
        << " *   limitations under the License." << endl
        << " */" << endl
        << endl
        << "// ###########################################" << endl
        << "// # DO NOT EDIT! THIS IS A GENERATED FILE " << endl
        << "// ###########################################" << endl
        << "#ifndef SRC_GENERATED_CONFIGURATION_H_" << endl
        << "#define SRC_GENERATED_CONFIGURATION_H_ 1" << endl
        << endl
        << "#include \"config.h\"" << endl
        << endl
        << "#include <string>" << endl;

    implementation
        << "/*" << endl
        << " *     Copyright 2011 Couchbase, Inc" << endl
        << " *" << endl
        << " *   Licensed under the Apache License, Version 2.0 (the \"License\");" << endl
        << " *   you may not use this file except in compliance with the License." << endl
        << " *   You may obtain a copy of the License at" << endl
        << " *" << endl
        << " *       http://www.apache.org/licenses/LICENSE-2.0" << endl
        << " *" << endl
        << " *   Unless required by applicable law or agreed to in writing, software" << endl
        << " *   distributed under the License is distributed on an \"AS IS\" BASIS," << endl
        << " *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." << endl
        << " *   See the License for the specific language governing permissions and" << endl
        << " *   limitations under the License." << endl
        << " */" << endl
        << endl
        << "// ###########################################" << endl
        << "// # DO NOT EDIT! THIS IS A GENERATED FILE " << endl
        << "// ###########################################" << endl
        << endl
        << "#include \"config.h\"" << endl
        << "#include \"configuration.h\"" << endl;
    validators["range"] = getRangeValidatorCode;
    validators["enum"] = getEnumValidatorCode;
    getters["std::string"] = "getString";
    getters["bool"] = "getBool";
    getters["size_t"] = "getInteger";
    getters["ssize_t"] = "getSignedInteger";
    getters["float"] = "getFloat";
    datatypes["bool"] = "bool";
    datatypes["size_t"] = "size_t";
    datatypes["ssize_t"] = "ssize_t";
    datatypes["float"] = "float";
    datatypes["string"] = "std::string";
    datatypes["std::string"] = "std::string";
}

static string getString(cJSON *i) {
    if (i == NULL) {
        return "";
    }
    assert(i->type == cJSON_String);
    return i->valuestring;
}

static bool isReadOnly(cJSON *o) {
    cJSON *i = cJSON_GetObjectItem(o, "dynamic");
    if (i == NULL || i->type == cJSON_False) {
        return false;
    }

    assert(i->type == cJSON_True);
    return true;
}

static string getDatatype(const std::string &key, cJSON *o) {
    cJSON *i = cJSON_GetObjectItem(o, "type");
    assert(i != NULL && i->type == cJSON_String);
    string ret = i->valuestring;

    map<string, string>::iterator iter = datatypes.find(ret);
    if (iter == datatypes.end()) {
        cerr << "Invalid datatype specified for \"" << key << "\": "
             << i->valuestring << endl;
        exit(1);
    }

    return iter->second;
}

static string getValidator(const std::string &key, cJSON *o) {
    if (o == NULL) {
        return "";
    }

    cJSON *n = cJSON_GetArrayItem(o, 0);
    if (n == NULL) {
        return "";
    }

    std::map<string, getValidatorCode>::iterator iter;
    iter = validators.find(string(n->string));
    if (iter == validators.end()) {
        cerr << "Unknown validator specified for \"" << key
             << "\": \"" << n->string << "\""
             << endl;
        exit(1);
    }

    return (iter->second)(key, n);
}

static string getGetterPrefix(const string &str) {
    if (str.compare("bool") == 0) {
        return "is";
    } else {
        return "get";
    }
}

static string getCppName(const string &str) {
    stringstream ss;
    bool doUpper = true;

    string::const_iterator iter;
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

static void generate(cJSON *o) {
    assert(o != NULL);

    string config_name = o->string;
    string cppname = getCppName(config_name);
    string type = getDatatype(config_name, o);
    string defaultVal = getString(cJSON_GetObjectItem(o, "default"));

    if (defaultVal.compare("max") == 0 || defaultVal.compare("min") == 0) {
        if (type.compare("std::string") != 0) {
            stringstream ss;
            ss << "std::numeric_limits<" << type << ">::" << defaultVal << "()";
            defaultVal = ss.str();
        }
    }

    string validator = getValidator(config_name,
                                    cJSON_GetObjectItem(o, "validator"));

    // Generate prototypes
    prototypes << "    " << type
               << " " << getGetterPrefix(type)
               << cppname << "() const;" << endl;
    if  (!isReadOnly(o)) {
        prototypes << "    void set" << cppname << "(const " << type
                   << " &nval);" << endl;
    }

    // Generate initialization code
    initialization << "    setParameter(\"" << config_name << "\", ";
    if (type.compare("std::string") == 0) {
        initialization << "(const char*)\"" << defaultVal << "\");" << endl;
    } else {
        initialization << "(" << type << ")" << defaultVal << ");" << endl;
    }
    if (!validator.empty()) {
        initialization << "    setValueValidator(\"" << config_name
                       << "\", " << validator << ");" << endl;
    }


    // Generate the getter
    implementation << type << " Configuration::" << getGetterPrefix(type)
                   << cppname << "() const {" << endl
                   << "    return " << getters[type] << "(\""
                   << config_name << "\");" << endl << "}" << endl;

    if  (!isReadOnly(o)) {
        // generate the setter
        implementation << "void Configuration::set" << cppname
                       << "(const " << type << " &nval) {" << endl
                       << "    setParameter(\"" << config_name
                       << "\", nval);" << endl
                       << "}" << endl;
    }
}

/**
 * Read "configuration.json" and generate getters and setters
 * for the parameters in there
 */
int main(int argc, char **argv) {
    const char *file = "configuration.json";
    if (argc == 2) {
        file = argv[1];
    }

    initialize();

    struct stat st;
    if (stat(file, &st) == -1) {
        cerr << "Failed to look up " << file << ": "
             << strerror(errno) << endl;
        return 1;
    }

    char *data = new char[st.st_size + 1];
    data[st.st_size] = 0;
    ifstream input(file);
    input.read(data, st.st_size);
    input.close();

    cJSON *c = cJSON_Parse(data);
    if (c == NULL) {
        cerr << "Failed to parse JSON.. probably syntax error" << endl;
        return 1;
    }

    cJSON *params = cJSON_GetObjectItem(c, "params");
    if (params == NULL) {
        cerr << "FATAL: could not find \"params\" section" << endl;
        return 1;
    }

    int num = cJSON_GetArraySize(params);
    for (int ii = 0; ii < num; ++ii) {
        generate(cJSON_GetArrayItem(params, ii));
    }
    prototypes << "#endif  // SRC_GENERATED_CONFIGURATION_H_" << endl;

    ofstream headerfile("src/generated_configuration.h");
    headerfile << prototypes.str();
    headerfile.close();

    ofstream implfile("src/generated_configuration.cc");
    implfile << implementation.str() << endl
             << "void Configuration::initialize() {" << endl
             << initialization.str()
             << "}" << endl;
    implfile.close();

    cJSON_Delete(c);
    delete []data;

    return 0;
}
