/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "pwfile.h"
#include "password_database.h"

#include <cbsasl/logging.h>
#include <folly/Synchronized.h>
#include <platform/timeutils.h>
#include <chrono>
#include <memory>
#include <mutex>

class PasswordDatabaseManager {
public:
    void swap(std::unique_ptr<cb::sasl::pwdb::PasswordDatabase>& ndb) {
        db.swap(ndb);
    }

    cb::sasl::pwdb::User find(const std::string& username) {
        return (*db.rlock())->find(username);
    }

    void iterate(
            std::function<void(const cb::sasl::pwdb::User&)> usercallback) {
        (*db.rlock())->iterate(usercallback);
    }

    static PasswordDatabaseManager& instance() {
        static PasswordDatabaseManager singleton;
        return singleton;
    }

protected:
    PasswordDatabaseManager() {
        *db.wlock() = std::make_unique<cb::sasl::pwdb::PasswordDatabase>();
    }

    folly::Synchronized<std::unique_ptr<cb::sasl::pwdb::PasswordDatabase>> db;
};

bool find_user(const std::string& username, cb::sasl::pwdb::User& user) {
    user = PasswordDatabaseManager::instance().find(username);
    return !user.isDummy();
}

cb::sasl::Error parse_user_db(
        const std::string content,
        bool file,
        std::function<void(const cb::sasl::pwdb::User&)> usercallback) {
    try {
        auto start = std::chrono::steady_clock::now();
        std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> db(
                new cb::sasl::pwdb::PasswordDatabase(content, file));
        std::string logmessage(
                "Loading [" + content + "] took " +
                cb::time2text(std::chrono::steady_clock::now() - start));
        cb::sasl::logging::log(cb::sasl::logging::Level::Debug, logmessage);
        PasswordDatabaseManager::instance().swap(db);
        if (usercallback) {
            PasswordDatabaseManager::instance().iterate(usercallback);
        }
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
static cb::sasl::Error load_isasl_user_db(
        std::function<void(const cb::sasl::pwdb::User&)> usercallback) {
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

    auto ret = parse_user_db(content, false, usercallback);

    return ret;
}

cb::sasl::Error load_user_db(
        std::function<void(const cb::sasl::pwdb::User&)> usercallback) {
    try {
        const char* filename = getenv("CBSASL_PWFILE");

        if (filename) {
            return parse_user_db(filename, true, std::move(usercallback));
        }

        return load_isasl_user_db(std::move(usercallback));
    } catch (std::bad_alloc&) {
        return cb::sasl::Error::NO_MEM;
    }
}
