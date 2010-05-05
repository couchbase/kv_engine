/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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
