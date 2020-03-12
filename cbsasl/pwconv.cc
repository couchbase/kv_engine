/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <cbsasl/logging.h>
#include <cbsasl/pwdb.h>
#include "user.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>

void cb::sasl::pwdb::convert(std::istream& is, std::ostream& os) {
    nlohmann::json json;
    nlohmann::json users = nlohmann::json::array();

    std::vector<char> up(1024);

    while (is.getline(up.data(), up.size()).good()) {
        if (up.front() == '#') {
            // comment line
            continue;
        }

        std::string username(up.data());
        // strip off potential carrige returns
        auto index = username.find('\r');
        if (index != username.npos) {
            username.resize(index);
        }

        if (username.empty()) {
            // empty line
            continue;
        }

        std::string password;
        index = username.find(' ');
        if (index != username.npos) {
            password = username.substr(index + 1);
            username.resize(index);
        }

        cb::sasl::logging::log(cb::sasl::logging::Level::Trace,
                               "Create user entry for [" + username + "]");

        auto u = UserFactory::create(username, password);
        users.push_back(u.to_json());
    }

    json["users"] = users;

    os << json.dump(4) << std::endl;
}

void cb::sasl::pwdb::convert(const std::string& ifile,
                             const std::string& ofile) {
    std::stringstream inputstream(read_password_file(ifile));
    std::stringstream outputstream;

    convert(inputstream, outputstream);

    write_password_file(ofile, outputstream.str());
}

std::string cb::sasl::pwdb::read_password_file(const std::string& filename) {
    if (filename == "-") {
        std::string contents;
        try {
            contents.assign(std::istreambuf_iterator<char>{std::cin},
                            std::istreambuf_iterator<char>());
        } catch (const std::exception& ex) {
            std::cerr << "Failed to read password database from stdin: "
                      << ex.what() << std::endl;
            exit(EXIT_FAILURE);
        }
        return contents;
    }

    auto ret = cb::io::loadFile(filename, std::chrono::seconds{5});

    // The password file may be encrypted
    auto* env = getenv("COUCHBASE_CBSASL_SECRETS");
    if (env == nullptr) {
        return ret;
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(env);
    } catch (const nlohmann::json::exception& e) {
        std::stringstream ss;
        ss << "cbsasl_read_password_file: Invalid json specified in "
              "COUCHBASE_CBSASL_SECRETS. Parse failed with: '"
           << e.what() << "'";
        throw std::runtime_error(ss.str());
    }

    return cb::crypto::decrypt(json, ret);
}

void cb::sasl::pwdb::write_password_file(const std::string& filename,
                                         const std::string& content) {
    if (filename == "-") {
        std::cout.write(content.data(), content.size());
        std::cout.flush();
        return;
    }

    std::ofstream of(filename, std::ios::binary);

    auto* env = getenv("COUCHBASE_CBSASL_SECRETS");
    if (env == nullptr) {
        of.write(content.data(), content.size());
    } else {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(env);
        } catch (const nlohmann::json::exception& e) {
            std::stringstream ss;
            ss << "cbsasl_write_password_file: Invalid json specified in "
                  "COUCHBASE_CBSASL_SECRETS. Parse failed with: '"
               << e.what() << "'";
            throw std::runtime_error(ss.str());
        }

        auto enc = cb::crypto::encrypt(json, content);
        of.write(enc.data(), enc.size());
    }

    of.flush();
    of.close();
}
