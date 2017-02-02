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
#include "pwconv.h"

#include <mutex>
#include <platform/timeutils.h>
#include <sstream>

class PasswordDatabaseManager {
public:
    PasswordDatabaseManager()
        : db(new cb::sasl::PasswordDatabase) {

    }

    void swap(std::unique_ptr<cb::sasl::PasswordDatabase>& ndb) {
        std::lock_guard<std::mutex> lock(dbmutex);
        db.swap(ndb);
    }

    cb::sasl::User find(const std::string& username) {
        std::lock_guard<std::mutex> lock(dbmutex);
        return db->find(username);
    }

private:
    std::mutex dbmutex;
    std::unique_ptr<cb::sasl::PasswordDatabase> db;
};

static PasswordDatabaseManager pwmgr;

void free_user_ht(void) {
    std::unique_ptr<cb::sasl::PasswordDatabase> ndb(
        new cb::sasl::PasswordDatabase);
    pwmgr.swap(ndb);
}

bool find_user(const std::string& username, cb::sasl::User& user) {
    user = pwmgr.find(username);
    return !user.isDummy();
}

cbsasl_error_t parse_user_db(const std::string content, bool file) {
    try {
        auto start = gethrtime();
        std::unique_ptr<cb::sasl::PasswordDatabase> db(
            new cb::sasl::PasswordDatabase(content, file));

        std::string logmessage(
            "Loading [" + content + "] took " +
            Couchbase::hrtime2text(gethrtime() - start));
            cbsasl_log(nullptr, cbsasl_loglevel_t::Debug, logmessage);
        pwmgr.swap(db);
    } catch (std::exception& e) {
        std::string message("Failed loading [");
        message.append(content);
        message.append("]: ");
        message.append(e.what());
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, message);
        return CBSASL_FAIL;
    } catch (...) {
        std::string message("Failed loading [");
        message.append(content);
        message.append("]: Unknown error");
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, message);
    }

    return CBSASL_OK;
}

/**
 * The isasl pwfile is the old style format of this file.
 *
 * Let's just parse it and build up the JSON needed from the
 * new style password database as documented in CBSASL.md
 */
static cbsasl_error_t load_isasl_user_db(void) {
    const char* filename = getenv("ISASL_PWFILE");

    if (!filename) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Debug,
                   "No password file specified");
        return CBSASL_OK;
    }

    std::string content;

    try {
        std::stringstream input(cbsasl_read_password_file(filename));
        std::stringstream output;

        cbsasl_pwconv(input, output);
        content = output.str();
    } catch (std::runtime_error &e) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, e.what());
        return CBSASL_FAIL;
    }

    auto ret = parse_user_db(content, false);

    return ret;
}

cbsasl_error_t load_user_db(void) {
    try {
        const char* filename = getenv("CBSASL_PWFILE");

        if (filename) {
            return parse_user_db(filename, true);
        }

        return load_isasl_user_db();
    } catch (std::bad_alloc&) {
        return CBSASL_NOMEM;
    }
}
