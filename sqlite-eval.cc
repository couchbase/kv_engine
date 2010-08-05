/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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

#include "config.h"
#include <iostream>
#include <fstream>
#include <stdexcept>

#include "sqlite-eval.hh"
#include "sqlite-pst.hh"
#include "ep.hh"

void SqliteEvaluator::eval(const std::string &filename) {
    std::ifstream in(filename.c_str(), std::ios_base::in);
    std::string query;

    if (!in.good()) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Failed to open eval file\n");
    }

    while (in.good() && !in.eof()) {
        char c;
        in.get(c);

        if (c == ';') {
            execute(query);
            query.clear();
        } else {
            query.append(1, c);
        }
    }

    execute(query);
}

void SqliteEvaluator::execute(std::string &query) {
    trim(query);
    if (!query.empty()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Executing query: ``%s''\n", query.c_str());
        PreparedStatement st(db, query.c_str());
        st.execute();
    }
}

void SqliteEvaluator::trim(std::string &str) {
    static const std::string whitespaces(" \r\n\t");

    while (!str.empty() && whitespaces.find(str[0]) != std::string::npos) {
        str = str.substr(1);
    }

    while (!str.empty() && whitespaces.find(str[str.size()-1]) != std::string::npos) {
        str = str.substr(0, str.size()-1);
    }
}
