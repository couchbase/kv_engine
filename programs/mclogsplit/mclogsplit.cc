/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <getopt.h>
#include <platform/dirutils.h>
#include <utilities/terminate_handler.h>

#include <gsl/gsl-lite.hpp>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <system_error>
#include <vector>

void usage() {
    using std::endl;
    std::cerr << "usage: mclogsplit [options] [input-file]" << endl
              << "\t--input filename    Input file to process (default stdin)"
              << endl
              << "\t--output dirname    Create output files in the named dir"
              << endl;
    exit(EXIT_FAILURE);
}

void process_stream(FILE* stream, const std::string& output) {
    // By default dump everything outside the markers to stderr
    FILE* ofs = stderr;
    int idx = 0;

    try {
        cb::io::mkdirp(output);
    } catch (const std::system_error& error) {
        throw std::system_error(error.code(),
                                "Failed to create directory " + output);
    }

    const std::string start_marker{"---------- Opening logfile: "};
    const std::string end_marker{"---------- Closing logfile"};

    std::vector<char> line(2048); // we don't have lines that long
    while (fgets(line.data(), gsl::narrow<int>(line.size()), stream) !=
           nullptr) {
        // Check if this is a begin line..
        const char* pos = std::strstr(line.data(), start_marker.c_str());
        if (pos) {
            if (ofs != stderr) {
                fclose(ofs);
            }

            // This is a new logfile..
            std::stringstream ss;
            ss << output << "/memcached.log." << std::setw(6)
               << std::setfill('0') << idx++ << ".txt";
            const auto filename = cb::io::sanitizePath(ss.str());
            ofs = fopen(filename.c_str(), "w");
            if (ofs == nullptr) {
                throw std::system_error(errno,
                                        std::system_category(),
                                        "Filed to open " + filename);
            }
            fprintf(stdout, "Writing to logfile: %s\n", filename.c_str());
        }

        // Write the log line
        fprintf(ofs, "%s", line.data());

        // Check if this is an END line
        if (std::strstr(line.data(), end_marker.c_str()) != nullptr) {
            if (ofs != stderr) {
                fclose(ofs);
            }
            ofs = stderr;
        }
    }

    if (ofs != stderr) {
        fclose(ofs);
    }
}

void process_file(const std::string& filename, const std::string& output) {
    FILE* fp = fopen(filename.c_str(), "r");
    if (fp == nullptr) {
        throw std::system_error(
                errno, std::system_category(), "Can't open " + filename);
    }
    process_stream(fp, output);
    fclose(fp);
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();

    std::vector<option> long_options = {
            {"input", required_argument, nullptr, 'i'},
            {"output", required_argument, nullptr, 'o'},
            {nullptr, 0, nullptr, 0}};

    char* input = nullptr;
    std::string output{"."};

    int cmd;
    while ((cmd = getopt_long(argc, argv, "", long_options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'i':
            input = optarg;
            break;
        case 'o':
            output = optarg;
            break;
        default:
            usage();
        }
    }

    try {
        if (argc == optind && input == nullptr) {
            // No files specified. use stdin!
            process_stream(stdin, output);
            return EXIT_SUCCESS;
        }

        if (input != nullptr) {
            process_file(input, output);
        }

        for (; optind < argc; ++optind) {
            process_file(argv[optind], output);
        }

        return EXIT_SUCCESS;
    } catch (std::exception& exception) {
        std::cerr << exception.what() << std::endl;
    }

    return EXIT_FAILURE;
}
