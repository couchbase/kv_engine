/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "auditevent_generator.h"
#include "generator_event.h"
#include "generator_module.h"
#include "generator_utilities.h"

#include <errno.h>
#include <getopt.h>
#include <nlohmann/json.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

int main(int argc, char **argv) {
    std::string input_file;
    std::string output_file;
    std::string srcroot;
    std::string objroot;
    std::filesystem::path header;
    int cmd;

    while ((cmd = getopt(argc, argv, "i:r:b:o:f:")) != -1) {
        switch (cmd) {
        case 'r': /* root */
            srcroot.assign(optarg);
            break;
        case 'b': /* binary root */
            objroot.assign(optarg);
            break;
        case 'o': /* output file */
            output_file.assign(optarg);
            break;
        case 'i': /* input file */
            input_file.assign(optarg);
            break;
        case 'f':
            header = std::filesystem::path(optarg);
            break;
        default:
            fprintf(stderr,
                    "usage: %s -r PATH -i FILE -o FILE -f headerfile\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    try {
        auto json = load_file(input_file);
        std::list<std::unique_ptr<Module>> modules;
        parse_module_descriptors(json, modules, srcroot, objroot);
        create_master_file(modules, output_file);

        std::ofstream file;
        file.open(header);
        if (!file.is_open()) {
            throw std::system_error(
                    errno,
                    std::system_category(),
                    "Failed to open " + header.generic_string());
        }

        file << "// This is a generated file, do not edit" << std::endl
             << "#pragma once" << std::endl;

        for (const auto& module : modules) {
            module->createHeaderFile(file);
        }
        file.close();
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }

    exit(EXIT_SUCCESS);
}
