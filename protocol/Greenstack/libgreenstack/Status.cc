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
#include <libgreenstack/Status.h>
#include <map>

const std::map<const Greenstack::Status, std::string> mappings = {
    {Greenstack::Status::Success,              "Success"},
    {Greenstack::Status::InvalidArguments,     "Invalid arguments"},
    {Greenstack::Status::InternalError,        "Internal error"},
    {Greenstack::Status::AuthenticationError,  "Authentication error"},
    {Greenstack::Status::AuthenticationStale,  "Authentication stale"},
    {Greenstack::Status::NotInitialized,       "Not initialized"},
    {Greenstack::Status::InvalidState,         "Invalid state"},
    {Greenstack::Status::NoAccess,             "No access"},
    {Greenstack::Status::NotFound,             "Not found"},
    {Greenstack::Status::UnknownCommand,       "Unknown command"},
    {Greenstack::Status::UserAgentBlacklisted, "User agent blacklisted"},
    {Greenstack::Status::NotImplemented,       "Not implemented"},
    {Greenstack::Status::NoMemory,             "No memory"},
    {Greenstack::Status::AlreadyExists,        "Already exists"},
    {Greenstack::Status::ObjectTooBig,         "Object too big"},
    {Greenstack::Status::TooBusy,              "Too busy"},
    {Greenstack::Status::IllegalRange,         "Illegal range"},
    {Greenstack::Status::TmpFailure,           "Temporary failure"},

    // Memcached specific error codes
    {Greenstack::Status::NotMyVBucket,         "Not my VBucket"},
    {Greenstack::Status::NoBucket,             "No Bucket"},
    {Greenstack::Status::Rollback,             "Rollback"},
    {Greenstack::Status::NotStored,            "Not Stored"}
};

std::string Greenstack::to_string(const Greenstack::Status& status) {
    const auto iter = mappings.find(status);
    if (iter == mappings.cend()) {
        return std::to_string(uint16_t(status));
    } else {
        return iter->second;
    }
}

Greenstack::Status Greenstack::to_status(uint16_t value) {
    Status code = Status(value);
    const auto iter = mappings.find(code);
    if (iter == mappings.cend()) {
        return Status::InvalidStatusCode;
    }
    return code;
}
