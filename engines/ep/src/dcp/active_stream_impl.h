/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "active_stream.h"
#include "bucket_logger.h"
#include "producer.h"

const std::string activeStreamLoggingPrefix =
        "DCP (Producer): **Deleted conn**";

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
template <typename... Args>
void ActiveStream::log(spdlog::level::level_enum severity,
                       const char* fmt,
                       Args... args) const {
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().log(severity, fmt, args...);
    } else {
        if (getGlobalBucketLogger()->should_log(severity)) {
            getGlobalBucketLogger()->log(
                    severity,
                    std::string{activeStreamLoggingPrefix}.append(fmt).data(),
                    args...);
        }
    }
}
