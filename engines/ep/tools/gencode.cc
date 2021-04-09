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

#include <nlohmann/json.hpp>
#include <getopt.h>
#include <iostream>
#include <fstream>

static void usage() {
    std::cerr << "Usage: gencode -j JSON -c cfile -h headerfile -f function"
              << std::endl
              << "\tThe JSON file will be read to generate the c and h file."
              << std::endl;
    exit(EXIT_FAILURE);
}

static std::string escapeQuotes(const std::string& str) {
    std::string escaped;

    for (auto& c : str) {
        switch (c) {
        case '"':
            escaped += '\\';
            // Fall through.
        default:
            escaped += c;
        }
    }

    return escaped;
}

int main(int argc, char **argv) {
    int cmd;
    const char* json = nullptr;
    const char* hfile = nullptr;
    const char* cfile = nullptr;
    const char* function = nullptr;

    while ((cmd = getopt(argc, argv, "j:c:h:f:")) != -1) {
        switch (cmd) {
        case 'j':
            json = optarg;
            break;
        case 'c':
            cfile = optarg;
            break;
        case 'h':
            hfile = optarg;
            break;
        case 'f':
            function = optarg;
            break;
        default:
            usage();
        }
    }

    if (json == nullptr || hfile == nullptr || cfile == nullptr ||
        function == nullptr) {
        usage();
    }

    // Parsing the json data will prettify the output easily.
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(readFile(json));
    } catch (const std::system_error& e) {
        std::cerr << "Failed to open file. " << e.what() << std::endl;
        exit(EXIT_FAILURE);
    } catch (const nlohmann::json::exception& e) {
        std::cerr << "Failed to parse JSON. " << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }

    auto escaped = escapeQuotes(parsed.dump());

    std::ofstream headerfile(hfile);
    if (!headerfile.is_open()) {
        std::cerr << "Unable to create header file : " << hfile << std::endl;
        return 1;
    }
    headerfile << "/*" << std::endl
               << " *     Copyright 2020 Couchbase, Inc" << std::endl
               << " *" << std::endl
               << " *   Use of this software is governed by the Business "
                  "Source License included"
               << std::endl
               << " *   in the file licenses/BSL-Couchbase.txt.  As"
                  " of the Change Date specified"
               << std::endl
               << " *   in that file, in accordance with the Business"
                  "Source License, use of this"
               << std::endl
               << " *   software will be governed by the Apache License, "
                  "Version 2.0, included in"
               << std::endl
               << " *   the file licenses/APL2.txt." << std::endl
               << " */" << std::endl
               << std::endl
               << "/********************************" << std::endl
               << "** Generated file, do not edit **" << std::endl
               << "*********************************/" << std::endl
               << "#pragma once" << std::endl
               << std::endl
               << "#ifdef __cplusplus" << std::endl
               << "extern \"C\" {" << std::endl
               << "#endif" << std::endl
               << std::endl
               << "const char *" << function << "(void);" << std::endl
               << std::endl
               << "#ifdef __cplusplus" << std::endl
               << "}" << std::endl
               << "#endif" << std::endl;
    headerfile.close();

    std::ofstream sourcefile(cfile);
    if (!sourcefile.is_open()) {
        std::cerr << "Unable to create source file : " << cfile << std::endl;
        return 1;
    }
    sourcefile << "/*" << std::endl
               << " *     Copyright 2020 Couchbase, Inc" << std::endl
               << " *" << std::endl
               << " *   Use of this software is governed by the Business "
                  "Source License included"
               << std::endl
               << " *   in the file licenses/BSL-Couchbase.txt.  As"
                  " of the Change Date specified"
               << std::endl
               << " *   in that file, in accordance with the Business"
                  "Source License, use of this"
               << std::endl
               << " *   software will be governed by the Apache License, "
                  "Version 2.0, included in"
               << std::endl
               << " *   the file licenses/APL2.txt." << std::endl
               << " */" << std::endl
               << std::endl
               << "/********************************" << std::endl
               << "** Generated file, do not edit **" << std::endl
               << "*********************************/" << std::endl
               << "#include \"" << hfile << "\"" << std::endl
               << std::endl
               << "const char *" << function << "(void)" << std::endl
               << "{" << std::endl
               << "    return \"" << escaped << "\";" << std::endl
               << "}" << std::endl;
    sourcefile.close();

    return 0;
}
