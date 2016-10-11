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
#include "buckets.h"

Bucket::Bucket(const Bucket& other)
{
    cb_mutex_enter(&other.mutex);

    cb_mutex_initialize(&mutex);
    cb_cond_initialize(&cond);
    clients = other.clients;
    state = other.state.load();
    type = other.type;
    std::copy(std::begin(other.name), std::end(other.name),
              std::begin(name));
    engine_event_handlers = other.engine_event_handlers;
    engine = other.engine;
    stats = other.stats;
    timings = other.timings;
    subjson_operation_times = other.subjson_operation_times;
    topkeys = other.topkeys;

    cb_mutex_exit(&other.mutex);
}

namespace BucketValidator {
    bool validateBucketName(const std::string& name, std::string& errors) {
        if (name.empty()) {
            errors.assign("BucketValidator::validateBucketName: "
                              "Name can't be empty");
            return false;
        }

        if (name.length() > MAX_BUCKET_NAME_LENGTH) {
            errors.assign("BucketValidator::validateBucketName: Name"
                              " too long (exceeds " +
                          std::to_string(MAX_BUCKET_NAME_LENGTH) +
                          ")");
            return false;
        }

        // Verify that the bucket name only consists of legal characters
        for (const uint8_t ii : name) {
            if (!(isupper(ii) || islower(ii) || isdigit(ii))) {
                switch (ii) {
                case '_':
                case '-':
                case '.':
                case '%':
                    break;
                default:
                    errors.assign("BucketValidator::validateBucketName: "
                                      "name contains invalid characters");
                    return false;
                }
            }
        }

        return true;
    }

    bool validateBucketType(const BucketType& type, std::string& errors) {
        if (type == BucketType::Unknown) {
            errors.assign("BucketValidator::validateBucketType: "
                              "Unsupported bucket type");
            return false;
        }

        return true;
    }
}
