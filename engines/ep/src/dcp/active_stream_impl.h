/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "active_stream.h"
#include "bucket_logger.h"
#include "producer.h"

/**
 * Separate implementation class for the ActiveStream logging method.
 *
 * When logging from an ActiveStream context we use the logger associated
 * with the stream specific DcpProducer (ConnHandler). In order to access the
 * logger of the DcpProducer we must include the necessary producer header
 * file, a forward declaration is not enough. As users of ActiveStream
 * in some cases use the ActiveStream::log method, we must keep this method
 * public, and, as this method is public and templated, in the header file.
 * However, not all users of ActiveStream require knowledge of the DcpProducer
 * or ConnHandler classes. To minimise header inclusions, which speeds up build
 * times, this method has been defined in a separate header file. Users of
 * ActiveStream that wish to use the logging functionality can include the
 * active_stream_impl.h file instead of the active_stream.h file.
 */

extern std::shared_ptr<BucketLogger> globalActiveStreamBucketLogger;

template <typename... Args>
void ActiveStream::log(spdlog::level::level_enum severity,
                       const char* fmt,
                       Args... args) const {
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().log(severity, fmt, args...);
    } else {
        getBucketLogger()->log(severity, fmt, args...);
    }
}
