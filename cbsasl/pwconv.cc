/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <cbsasl/logging.h>
#include <cbsasl/pwdb.h>
#include <cbsasl/user.h>
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
