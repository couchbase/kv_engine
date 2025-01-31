/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/producer_stream.h"

#include "bucket_logger.h"
#include "dcp/producer.h"
#include "dcp/response.h"

std::unique_ptr<DcpResponse> ProducerStream::makeEndStreamResponse(
        cb::mcbp::DcpStreamEndStatus reason) {
    return std::make_unique<StreamEndResponse>(opaque_, reason, vb_, sid);
}

void ProducerStream::updateAggStats(StreamAggStats& stats) {
    stats.itemsRemaining += getItemsRemaining();
    stats.readyQueueMemory += getReadyQueueMemory();
}

void ProducerStream::notifyStreamReady(bool force, DcpProducer* producer) {
    bool inverse = false;
    if (force || itemsReady.compare_exchange_strong(inverse, true)) {
        /**
         * The below block of code exists to reduce the amount of times that we
         * have to promote the producerPtr (weak_ptr<DcpProducer>). Callers that
         * have already done so can supply a raw ptr for us to use instead.
         */
        if (producer) {
            // Caller supplied a producer to call this on, use that
            producer->notifyStreamReady(vb_);
            return;
        }

        // No producer supplied, promote the weak_ptr and use that
        auto lkProducer = producerPtr.lock();
        if (!lkProducer) {
            return;
        }
        lkProducer->notifyStreamReady(vb_);
    }
}

void ProducerStream::logWithContext(spdlog::level::level_enum severity,
                                    std::string_view msg,
                                    cb::logger::Json ctx) const {
    // Format: {"vb:"vb:X", "sid": "sid:none", ...}
    auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
    if (sid) {
        object.insert(object.begin(), {"sid", sid.to_string()});
    }
    object.insert(object.begin(), {"vb", getVBucket()});

    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().logWithContext(severity, msg, std::move(ctx));
    } else {
        if (getGlobalBucketLogger()->should_log(severity)) {
            getGlobalBucketLogger()->logWithContext(
                    severity, msg, std::move(ctx));
        }
    }
}

void ProducerStream::logWithContext(spdlog::level::level_enum severity,
                                    std::string_view msg) const {
    logWithContext(severity, msg, cb::logger::Json::object());
}