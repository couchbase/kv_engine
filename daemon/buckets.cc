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
#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "stats.h"
#include "topkeys.h"
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <platform/scope_timer.h>

Bucket::Bucket() = default;

void Bucket::reset() {
    std::lock_guard<std::mutex> guard(mutex);
    engine.reset();
    state = Bucket::State::None;
    name[0] = '\0';
    setEngine(nullptr);
    topkeys.reset();
    clusterConfiguration.reset();
    max_document_size = default_max_item_size;
    supportedFeatures = {};
    for (auto& c : responseCounters) {
        c.reset();
    }
    subjson_operation_times.reset();
    timings.reset();
    for (auto& s : stats) {
        s.reset();
    }
    type = BucketType::Unknown;
}

bool Bucket::supports(cb::engine::Feature feature) {
    return supportedFeatures.find(feature) != supportedFeatures.end();
}

DcpIface* Bucket::getDcpIface() const {
    return bucketDcp;
}

EngineIface& Bucket::getEngine() const {
    return *engine;
}

void Bucket::destroyEngine(bool force) {
    engine.get_deleter().force = force;
    engine.reset();
}

void Bucket::setEngine(unique_engine_ptr engine_) {
    engine = std::move(engine_);
    bucketDcp = dynamic_cast<DcpIface*>(engine.get());
}

namespace BucketValidator {
std::string validateBucketName(std::string_view name) {
    if (name.empty()) {
        return "Name can't be empty";
    }

    if (name.length() > MAX_BUCKET_NAME_LENGTH) {
        return "Name too long (exceeds " +
               std::to_string(MAX_BUCKET_NAME_LENGTH) + ")";
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
                return "Name contains invalid characters";
            }
        }
    }

    return {};
}
}

std::string to_string(Bucket::State state) {
    switch (state) {
    case Bucket::State::None:
        return "none";
    case Bucket::State::Creating:
        return "creating";
    case Bucket::State::Initializing:
        return "initializing";
    case Bucket::State::Ready:
        return "ready";
    case Bucket::State::Stopping:
        return "stopping";
    case Bucket::State::Destroying:
        return "destroying";
    }
    throw std::invalid_argument("Invalid bucket state: " +
                                std::to_string(int(state)));
}

bool mayAccessBucket(Cookie& cookie, const std::string& bucket) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer({cookie, Code::CreateRbacContext});
    return cb::rbac::mayAccessBucket(cookie.getConnection().getUser(), bucket);
}
