/*
 *     Copyright 2020 Couchbase, Inc.
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
