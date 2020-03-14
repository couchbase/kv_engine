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

#include <getopt.h>
#include <platform/dirutils.h>
#include <utilities/terminate_handler.h>

#include <cstdio>
#include <cstring>
#include <gsl/gsl>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <system_error>

void usage() {
    using std::endl;
    std::cerr << "usage: mclogsplit [options] [input-file]" << endl
              << "\t--input filename    Input file to process (default stdin)"
              << endl
              << "\t--output dirname    Create output files in the named dir"
              << endl
              << "\t--preserve          Preseve the filenames" << endl
              << endl;
    exit(EXIT_FAILURE);
}

void process_stream(FILE* stream, const std::string& output, bool preserve) {
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
        const char* pos;
        if ((pos = std::strstr(line.data(), start_marker.c_str())) != nullptr) {
            if (ofs != stderr) {
                fclose(ofs);
            }

            // This is a new logfile..
            std::string filename;
            if (preserve) {
                filename.assign(pos + start_marker.length());
                filename.pop_back();
                filename = output + "/" + cb::io::basename(filename);
            } else {
                std::stringstream ss;
                ss << output << "/memcached.log." << std::setw(6)
                   << std::setfill('0') << idx++ << ".txt";
                filename = ss.str();
            }

            filename = cb::io::sanitizePath(filename);
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

void process_file(const std::string& filename,
                  const std::string& output,
                  bool preserve) {
    FILE* fp = fopen(filename.c_str(), "r");
    if (fp == nullptr) {
        throw std::system_error(
                errno, std::system_category(), "Can't open " + filename);
    }
    process_stream(fp, output, preserve);
    fclose(fp);
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();

    struct option long_options[] = {{"input", required_argument, nullptr, 'i'},
                                    {"output", required_argument, nullptr, 'o'},
                                    {"preserve", no_argument, nullptr, 'p'},
                                    {nullptr, 0, nullptr, 0}};

    char* input = nullptr;
    std::string output{"."};
    bool preserve = false;

    int cmd;
    while ((cmd = getopt_long(argc, argv, "i:o:p", long_options, nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'i':
            input = optarg;
            break;
        case 'o':
            output = optarg;
            break;
        case 'p':
            preserve = true;
            break;

        default:
            usage();
        }
    }

    try {
        if (argc == optind && input == nullptr) {
            // No files specified. use stdin!
            process_stream(stdin, output, preserve);
            return EXIT_SUCCESS;
        }

        if (input != nullptr) {
            process_file(input, output, preserve);
        }

        for (; optind < argc; ++optind) {
            process_file(argv[optind], output, preserve);
        }

        return EXIT_SUCCESS;
    } catch (std::exception& exception) {
        std::cerr << exception.what() << std::endl;
    }

    return EXIT_FAILURE;
}
