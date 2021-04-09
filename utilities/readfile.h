/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <fstream>
#include <sstream>

// Simple method to read a file. Ideally we should have used cb::io::loadFile,
// but that adds a dependency to libdirutils (which in turn depends on
// libplatform due to malloc etc) causing us to build too much code when
// running in the context of clang-tidy to generate header files
inline std::string readFile(const std::string& fname) {
    std::ifstream file;
    file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    file.open(fname);
    std::stringstream out;
    out << file.rdbuf();
    return out.str();
}
