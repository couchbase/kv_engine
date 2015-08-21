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
#pragma once

#include <cstdint>
#include <string>

namespace Greenstack {
    enum class Status : uint16_t {
        // Generic status codes
        Success = 0x0000,
        InvalidArguments = 0x0001,
        InternalError = 0x0002,
        AuthenticationError = 0x0003,
        AuthenticationStale = 0x0004,
        NotInitialized = 0x0005,
        InvalidState = 0x0006,
        NoAccess = 0x0007,
        NotFound = 0x0008,
        UnknownCommand = 0x0009,
        UserAgentBlacklisted = 0x000a,
        NotImplemented = 0x000b,
        NoMemory = 0x000c,
        AlreadyExists = 0x000d,
        ObjectTooBig = 0x000e,
        TooBusy = 0x000f,
        IllegalRange = 0x0010,
        TmpFailure = 0x0011,

        // Memcached status codes
        NotMyVBucket = 0x0400,
        NoBucket = 0x0401,
        Rollback = 0x0402,
        NotStored = 0x403,

        // Invalid status
        InvalidStatusCode = 0xffff
    };

    /**
     * Convert a status code to a textual representation
     */
    std::string to_string(const Greenstack::Status& status);

    /**
     * Convert a uint16_t to a Status code. If the received value isn't
     * a legal status code, InvalidStatusCode is returned
     */
    Status to_status(uint16_t value);
}
