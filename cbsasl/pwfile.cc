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
#include "cbsasl_internal.h"
#include "user.h"
#include "pwconv.h"

#include <cstring>
#include <iterator>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <platform/strerror.h>
#include <platform/timeutils.h>

class PasswordDatabaseManager {
public:
    PasswordDatabaseManager()
        : db(new Couchbase::PasswordDatabase) {

    }

    void swap(std::unique_ptr<Couchbase::PasswordDatabase>& ndb) {
        std::lock_guard<std::mutex> lock(dbmutex);
        db.swap(ndb);
    }

    Couchbase::User find(const std::string& username) {
        std::lock_guard<std::mutex> lock(dbmutex);
        return db->find(username);
    }

private:
    std::mutex dbmutex;
    std::unique_ptr<Couchbase::PasswordDatabase> db;
};

static PasswordDatabaseManager pwmgr;

void free_user_ht(void) {
    std::unique_ptr<Couchbase::PasswordDatabase> ndb(
        new Couchbase::PasswordDatabase);
    pwmgr.swap(ndb);
}

bool find_pw(const std::string& user, std::string& password) {
    Couchbase::User u = pwmgr.find(user);
    if (!u.isDummy()) {
        try {
            const auto& meta = u.getPassword(Mechanism::PLAIN);
            password.assign(meta.getPassword());
            return true;
        } catch (...) { ;
        }
    }
    return false;
}

bool find_user(const std::string& username, Couchbase::User& user) {
    user = pwmgr.find(username);
    return !user.isDummy();
}

cbsasl_error_t parse_user_db(const std::string content) {
    try {
        using namespace Couchbase;
        auto start = gethrtime();
        std::unique_ptr<PasswordDatabase> db(
            new PasswordDatabase(content, true));

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

    std::string ofile(filename);
    ofile.append(".pwconv");
    try {
        cbsasl_pwconv(filename, ofile);
    } catch (std::runtime_error &e) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, e.what());
        return CBSASL_FAIL;
    }

    auto ret = parse_user_db(ofile);
    remove(ofile.c_str());

    return ret;
}

cbsasl_error_t load_user_db(void) {
    try {
        const char* filename = getenv("CBSASL_PWFILE");

        if (filename) {
            return parse_user_db(filename);
        }

        return load_isasl_user_db();
    } catch (std::bad_alloc&) {
        return CBSASL_NOMEM;
    }
}
