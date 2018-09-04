/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "input_couchfile.h"
#include "output_couchfile.h"

#include <getopt.h>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

/**
 * couchfile_upgrade is a tool for making couchstore files collection-aware.
 * This is essentially a set of steps which result in a new couchstore file
 * that has each document assigned to the DefaultCollection.
 */

struct ProgramOptions {
    OptionsSet options;
    const char* inputFilename;
    const char* outputFilename;
};

static bool runUpgrade(const ProgramOptions& options,
                       Collections::InputCouchFile& input) {
    using PreflightStatus = Collections::InputCouchFile::PreflightStatus;
    switch (input.preflightChecks(std::cerr)) {
    case PreflightStatus::ReadyForUpgrade:
        break;
    case PreflightStatus::InputFileCannotBeProcessed:
    case PreflightStatus::UpgradePartial:
    case PreflightStatus::UpgradeCompleteAndPartial: {
        std::cerr << "Pre-upgrade checks have failed\n";
        return false;
    }
    case PreflightStatus::UpgradeComplete: {
        return options.options.test(Options::Tolerate);
    }
    }

    // Open the output file now that the input file is ok for processing
    Collections::OutputCouchFile output(
            options.options, options.outputFilename, CollectionID::Default);

    // Perform the upgrade steps
    // 1. Write out a new file tagged in a way we can determine upgrade has
    //    started but not finished
    output.writeUpgradeBegin(input);
    output.commit();

    // 2. Now run the upgrade, docs are copied from in to out and moved to
    //    the default collection
    input.upgrade(output);
    output.commit();

    // 3. Write out to the new file that the upgrade is done, KV can now warmup
    //    from this file.
    output.writeUpgradeComplete(input);
    output.commit();
    return true;
}

static void runStatus(Collections::InputCouchFile& input) {
    if (input.preflightChecks(std::cout) ==
        Collections::InputCouchFile::PreflightStatus::ReadyForUpgrade) {
        std::cout << "filename: " << input.getFilename()
                  << " is ready for upgrade\n";
    }
}

static void usage() {
    std::cout <<
            R"(Usage:
    -v or --verbose        Optional: Run with verbose output to stdout.
    -s or --status         Optional: Print upgrade status of input file.
    -t or --tolerate       Optional: Tolerate upgraded files - exit 0 if file is already marked as upgraded.
    -i or --input <name>   Required: Input filename.
    -o or --output <name>  Required (only if not -s): Output filename to be created.)"
              << std::endl;
}

static ProgramOptions parseArguments(int argc, char** argv) {
    int cmd = 0;
    ProgramOptions pOptions{};

    struct option long_options[] = {{"tolerate", no_argument, nullptr, 't'},
                                    {"status", no_argument, nullptr, 's'},
                                    {"verbose", no_argument, nullptr, 'v'},
                                    {"input", required_argument, nullptr, 'i'},
                                    {"output", required_argument, nullptr, 'o'},
                                    {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc, argv, "tsvi:o:", long_options, nullptr)) !=
           -1) {
        switch (cmd) {
        case 'v': {
            pOptions.options.set(Options::Verbose);
            std::cout << "Enabling Verbose\n";
            break;
        }
        case 's': {
            pOptions.options.set(Options::Status);
            std::cout << "Status\n";
            break;
        }
        case 't': {
            pOptions.options.set(Options::Tolerate);
            std::cout << "exit(0) for already upgraded files\n";
            break;
        }
        case 'i': {
            pOptions.inputFilename = optarg;
            std::cout << "Input:" << optarg << "\n";
            break;
        }
        case 'o': {
            pOptions.outputFilename = optarg;
            std::cout << "Output:" << optarg << "\n";
            break;
        }
        case ':':
        case '?': {
            usage();
            throw std::invalid_argument("Invalid Argument");
            break;
        }
        }
    }

    if (!pOptions.inputFilename) {
        usage();
        throw std::invalid_argument("Missing -i");
    }
    if (!pOptions.outputFilename && !pOptions.options.test(Options::Status)) {
        usage();
        throw std::invalid_argument("Missing -o");
    }
    if (pOptions.outputFilename && pOptions.options.test(Options::Status)) {
        usage();
        throw std::invalid_argument("-o with -s is not allowed");
    }

    return pOptions;
}

int main(int argc, char** argv) {
    bool success = true;
    try {
        auto options = parseArguments(argc, argv);
        Collections::InputCouchFile input(options.options,
                                          options.inputFilename);

        if (options.options.test(Options::Status)) {
            runStatus(input);
        } else {
            success = runUpgrade(options, input);
        }
    } catch (const std::exception& e) {
        success = false;
        std::cerr << "An exception occurred: " << e.what() << std::endl;
    }

    if (!success) {
        std::cerr << "Terminating with exit code 1\n";
    }

    return success ? EXIT_SUCCESS : EXIT_FAILURE;
}