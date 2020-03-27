/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "pwfile.h"
#include "password_database.h"

#include <cbsasl/logging.h>
#include <platform/timeutils.h>
#include <chrono>
#include <mutex>
#include <sstream>

class PasswordDatabaseManager {
public:
    PasswordDatabaseManager() : db(new cb::sasl::pwdb::PasswordDatabase) {
    }

    void swap(std::unique_ptr<cb::sasl::pwdb::PasswordDatabase>& ndb) {
        std::lock_guard<std::mutex> lock(dbmutex);
        db.swap(ndb);
    }

    cb::sasl::pwdb::User find(const std::string& username) {
        std::lock_guard<std::mutex> lock(dbmutex);
        return db->find(username);
    }

private:
    std::mutex dbmutex;
    std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> db;
};

static PasswordDatabaseManager pwmgr;

bool find_user(const std::string& username, cb::sasl::pwdb::User& user) {
    user = pwmgr.find(username);
    return !user.isDummy();
}

cb::sasl::Error parse_user_db(const std::string content, bool file) {
    try {
        auto start = std::chrono::steady_clock::now();
        std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> db(
                new cb::sasl::pwdb::PasswordDatabase(content, file));

        std::string logmessage(
                "Loading [" + content + "] took " +
                cb::time2text(std::chrono::steady_clock::now() - start));
        cb::sasl::logging::log(cb::sasl::logging::Level::Debug, logmessage);
        pwmgr.swap(db);
    } catch (std::exception& e) {
        std::string message("Failed loading [");
        message.append(content);
        message.append("]: ");
        message.append(e.what());
        cb::sasl::logging::log(cb::sasl::logging::Level::Error, message);
        return cb::sasl::Error::FAIL;
    } catch (...) {
        std::string message("Failed loading [");
        message.append(content);
        message.append("]: Unknown error");
        cb::sasl::logging::log(cb::sasl::logging::Level::Error, message);
        return cb::sasl::Error::FAIL;
    }

    return cb::sasl::Error::OK;
}

/**
 * The isasl pwfile is the old style format of this file.
 *
 * Let's just parse it and build up the JSON needed from the
 * new style password database as documented in CBSASL.md
 */
static cb::sasl::Error load_isasl_user_db() {
    const char* filename = getenv("ISASL_PWFILE");

    if (!filename) {
        cb::sasl::logging::log(cb::sasl::logging::Level::Debug,
                               "No password file specified");
        return cb::sasl::Error::OK;
    }

    std::string content;

    try {
        std::stringstream input(cb::sasl::pwdb::read_password_file(filename));
        std::stringstream output;

        cb::sasl::pwdb::convert(input, output);
        content = output.str();
    } catch (std::runtime_error& e) {
        cb::sasl::logging::log(
                cb::sasl::logging::Level::Error,
                std::string{"load_isasl_user_db() received exception: "} +
                        e.what());
        return cb::sasl::Error::FAIL;
    }

    auto ret = parse_user_db(content, false);

    return ret;
}

cb::sasl::Error load_user_db() {
    try {
        const char* filename = getenv("CBSASL_PWFILE");

        if (filename) {
            return parse_user_db(filename, true);
        }

        return load_isasl_user_db();
    } catch (std::bad_alloc&) {
        return cb::sasl::Error::NO_MEM;
    }
}
