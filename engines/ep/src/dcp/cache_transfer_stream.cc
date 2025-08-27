/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/cache_transfer_stream.h"

#include "bucket_logger.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "ep_engine.h"
#include "ep_task.h"
#include "ep_time.h"
#include "executor/executorpool.h"
#include "hash_table.h"
#include "progress_tracker.h"
#include "stored-value.h"
#include "vbucket.h"
#include <folly/ScopeGuard.h>
#include <statistics/cbstat_collector.h>

#include <memory>

/**
 * A visting implementation which will calls into the
 * CacheTransferStream::maybeQueueItem() method for each StoredValue in the
 * hash-table which will decide if the StoredValue should be copied onto the
 * readyQ (and sent over DCP).
 *
 * This visitor uses a ProgressTracker to control the duration of the visit.
 */
class CacheTransferHashTableVisitor : public HashTableVisitor {
public:
    CacheTransferHashTableVisitor(std::shared_ptr<CacheTransferStream> stream)
        : weakStream(std::move(stream)) {
    }

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    /**
     * Setups the shared_ptr<CacheTransferStream> to the stream that the visitor
     * should write to.
     * @returns true if all objects could be obtained
     */
    bool setUpHashTableVisit(VBucket& vb);

    /**
     * Called when the visitor has finished visiting the hash table, drops the
     * reference to the stream.
     */
    void tearDownHashTableVisit();

    /**
     * @returns true if the stream is active and the visitor should continue.
     */
    bool setUpHashBucketVisit() override;

    CacheTransferStream& getStream() const {
        Expects(stream);
        return *stream;
    }

    /// @returns the number of items queued for this stream.
    uint64_t getQueuedCount() const {
        return queuedCount;
    }

    /// @returns the number of items visited for this stream.
    uint64_t getVisitedCount() const {
        return visitedCount;
    }

    CacheTransferStream::Status getStatus() const {
        return status;
    }

private:
    uint64_t queuedCount{0};
    uint64_t visitedCount{0};
    ProgressTracker progressTracker;
    CacheTransferStream::Status status{
            CacheTransferStream::Status::KeepVisiting};

    // The visitor holds a weak reference to the stream.
    std::weak_ptr<CacheTransferStream> weakStream;

    // When visiting, a strong reference to the stream is held in this member.
    // setUpHashTableVisit/tearDownHashTableVisit are used to manage this.
    std::shared_ptr<CacheTransferStream> stream;

    // For collection drop check. The readHandle is only valid during the run
    // and is acquired/released by setUpHashTableVisit/tearDownHashTableVisit
    Collections::VB::ReadHandle readHandle;
};

class CacheTransferTask : public EpTask {
public:
    CacheTransferTask(EventuallyPersistentEngine& e,
                      Vbid vbid,
                      std::shared_ptr<CacheTransferStream> stream,
                      std::string_view descriptionDetail)
        : EpTask(e, TaskId::CacheTransferTask),
          vbid(vbid),
          visitor(std::move(stream)),
          descriptionDetail(descriptionDetail),
          startTime(cb::time::steady_clock::now()) {
    }

    bool run() override;

    std::string getDescription() const override {
        return fmt::format(
                "{} CacheTransferTask for {}", descriptionDetail, vbid);
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::milliseconds(30);
    }

private:
    Vbid vbid;
    CacheTransferHashTableVisitor visitor;
    HashTable::Position position;
    std::string descriptionDetail;
    uint64_t queuedCount{0};
    uint64_t visitedCount{0};
    cb::time::steady_clock::time_point startTime;
};

bool CacheTransferHashTableVisitor::visit(const HashTable::HashBucketLock& lh,
                                          StoredValue& v) {
    // Track total visit count.
    ++visitedCount;

    // Pass v to the stream which may queue the item, ignore it or request that
    // visiting yields/stops.
    switch (stream->maybeQueueItem(v, readHandle)) {
    case CacheTransferStream::Status::OOM:
    case CacheTransferStream::Status::Stop:
        return false;
    case CacheTransferStream::Status::QueuedItem:
        ++queuedCount;
        [[fallthrough]];
    case CacheTransferStream::Status::KeepVisiting:
        return progressTracker.shouldContinueVisiting(visitedCount);
    }

    folly::assume_unreachable();
}

bool CacheTransferHashTableVisitor::setUpHashTableVisit(VBucket& vb) {
    stream = weakStream.lock();
    if (!stream || !stream->isActive()) {
        // No stream, no visit.
        // Not active, no visit.
        return false;
    }

    readHandle = vb.lockCollections();

    // Have a stream and it is active, so we can visit.
    queuedCount = 0;
    visitedCount = 0;
    progressTracker.setDeadline(cb::time::steady_clock::now() +
                                std::chrono::milliseconds(25));
    return true;
}

void CacheTransferHashTableVisitor::tearDownHashTableVisit() {
    // Drop our reference to the stream.
    stream.reset();
    readHandle.unlock();
}

bool CacheTransferHashTableVisitor::setUpHashBucketVisit() {
    status = CacheTransferStream::Status::QueuedItem;
    return stream->isActive();
}

// Run visits the single vbucket the CacheTransferStream is associated with
bool CacheTransferTask::run() {
    Expects(engine);
    auto vb = engine->getVBucket(vbid);
    if (!vb || !visitor.setUpHashTableVisit(*vb)) {
        // No VB or no viable stream, stop running.
        return false;
    }

    // Ensure we always teardown the hash table visit (drops stream reference)
    auto guard = folly::makeGuard([this] { visitor.tearDownHashTableVisit(); });

    // Visit the vbucket from the last position.
    position = vb->ht.pauseResumeVisit(visitor, position);
    // Nofify the producer if something was queued
    bool notify = visitor.getQueuedCount() > 0;
    auto& stream = visitor.getStream();
    // Accumulate stats for the overall HT visit
    visitedCount += visitor.getVisitedCount();
    queuedCount += visitor.getQueuedCount();

    // Helper function to try and notify the producer and select the correct
    // return value for the tasks continuation
    auto notifyAndGetRescheduleValue =
            [&notify, &stream, this](bool reschedule) {
                if (!stream.isActive()) {
                    // Stream is dead, but we may still need to notify.
                    // The task should not reschedule
                    reschedule = false;
                }
                if (notify) {
                    // Notify producer to ship the queued data
                    auto producer = stream.getProducer();
                    if (producer) {
                        producer->notifyStreamReady(vbid);
                    } else {
                        // No producer, stop the task.
                        return false;
                    }
                }
                return reschedule;
            };

    if (position == vb->ht.endPosition()) {
        // Calculate total runtime
        auto endTime = cb::time::steady_clock::now();
        auto totalRuntimeMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                      startTime)
                        .count();

        // Reached end of HT
        stream.logWithContext(
                spdlog::level::info,
                "CacheTransferTask::run: Reached HT end.",
                {{"vbid", vbid},
                 {"visited_count", visitedCount},
                 {"queued_count", queuedCount},
                 {"total_runtime_ms", totalRuntimeMs},
                 {"total_bytes_queued", stream.getTotalBytesQueued()}});
        stream.setDead(cb::mcbp::DcpStreamEndStatus::Ok);
        // Notify the producer as we have queued a stream end.
        notify = true;
        return notifyAndGetRescheduleValue(false);
    }

    if (visitor.getStatus() == CacheTransferStream::Status::OOM) {
        // Ideally there should be a wake-up when memory reduces.
        // We may of queued nothing so cannot use producer wakeup
        // @todo: configurable snooze time may be useful for testing
        snooze(0.5);
    } else {
        // Cannot recall if this is needed - most of the time we want to yield
        // and run immediately, but ignore any previous snooze(0.1)
        snooze(0.0);
    }

    return notifyAndGetRescheduleValue(true);
}

CacheTransferStream::CacheTransferStream(std::shared_ptr<DcpProducer> p,
                                         const std::string& name,
                                         uint32_t opaque,
                                         uint64_t maxSeqno,
                                         uint64_t uuid,
                                         Vbid vbid,
                                         EventuallyPersistentEngine& engine,
                                         IncludeValue includeValue,
                                         Collections::VB::Filter filter)
    : ProducerStream(filter.getStreamId()
                             ? name + filter.getStreamId().to_string() + ":cts"
                             : name + ":cts",
                     p,
                     filter.getStreamId(),
                     cb::mcbp::DcpAddStreamFlag::None,
                     opaque,
                     vbid,
                     maxSeqno,
                     uuid),
      engine(engine),
      includeValue(includeValue),
      filter(std::move(filter)) {
    Expects(p);
    OBJ_LOG_INFO_CTX(p->getLogger(),
                     "Creating CacheTransferStream",
                     {"max_seqno", maxSeqno},
                     {"vbid", vbid},
                     {"vbucket_uuid", uuid},
                     {"include_value", includeValue});
}

void CacheTransferStream::setActive() {
    auto producer = getProducer();
    if (!producer) {
        logWithContext(
                spdlog::level::warn,
                "CacheTransferStream::scheduleTask: Producer cannot be locked",
                {"vbid", getVBucket()});
        return;
    }
    std::lock_guard<std::mutex> lh(streamMutex);
    tid = ExecutorPool::get()->schedule(std::make_unique<CacheTransferTask>(
            engine, getVBucket(), shared_from_this(), producer->logHeader()));
}

void CacheTransferStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    ExecutorPool::get()->cancel(tid);
    std::lock_guard<std::mutex> lh(streamMutex);
    if (state == State::Dead) {
        return;
    }
    state = State::Dead;
    pushToReadyQ(makeEndStreamResponse(status));
}

bool CacheTransferStream::endIfRequiredPrivilegesLost(DcpProducer& producer) {
    // Does this stream still have the appropriate privileges to operate?
    if (filter.checkPrivileges(*producer.getCookie(), engine) !=
        cb::engine_errc::success) {
        std::unique_lock lh(streamMutex);
        pushToReadyQ(makeEndStreamResponse(
                cb::mcbp::DcpStreamEndStatus::LostPrivileges));
        lh.unlock();
        notifyStreamReady(false, &producer);
        return true;
    }
    return false;
}

void CacheTransferStream::setDeadWithLock(
        cb::mcbp::DcpStreamEndStatus status,
        std::unique_lock<folly::SharedMutex>&) {
    setDead(status);
    if (status != cb::mcbp::DcpStreamEndStatus::Disconnected) {
        notifyStreamReady();
    }
}

void CacheTransferStream::addTakeoverStats(const AddStatFn& add_stat,
                                           CookieIface& c,
                                           const VBucket& vb) {
    // @todo: figure out if we need to provide takeover stats.
    // We certainly need to provide some stats if the CTS is created during
    // takeover.
}

std::string CacheTransferStream::getStreamTypeName() const {
    return "CacheTransfer";
}

std::string CacheTransferStream::getStateName() const {
    switch (state) {
    case State::Active:
        return "Active";
    case State::Dead:
        return "Dead";
    }
    folly::assume_unreachable();
}

bool CacheTransferStream::isActive() const {
    std::lock_guard<std::mutex> lh(streamMutex);
    return state != State::Dead;
}

std::unique_ptr<DcpResponse> CacheTransferStream::next(DcpProducer& producer) {
    std::lock_guard<std::mutex> lh(streamMutex);
    if (readyQ.empty()) {
        return nullptr;
    }

    auto& response = readyQ.front();
    if (!producer.bufferLogInsert(response->getMessageSize())) {
        return nullptr;
    }

    return popFromReadyQ();
}

size_t CacheTransferStream::getItemsRemaining() {
    std::lock_guard<std::mutex> lh(streamMutex);
    return readyQ.size();
}

size_t CacheTransferStream::getMemoryUsed() const {
    return engine.getEpStats().getEstimatedTotalMemoryUsed();
}

CacheTransferStream::Status CacheTransferStream::maybeQueueItem(
        const StoredValue& sv, Collections::VB::ReadHandle& readHandle) {
    preQueueCallback(sv);

    const auto debugLogSv = [this](const std::string_view msg,
                                   const StoredValue& logSv) {
        if (shouldLog(spdlog::level::debug)) {
            logWithContext(
                    spdlog::level::debug, msg, {{"sv", nlohmann::json{logSv}}});
        }
    };

    // Check if the sv is eligible for transfer.
    // 1. Temporary/Deleted/Pending StoredValues are not eligible.
    if (sv.isTempItem() || sv.isDeleted() || sv.isPending()) {
        debugLogSv("CacheTransferStream skipping temp/deleted/pending", sv);
        return Status::KeepVisiting;
    }

    // 2. Dropped collection items are not eligible.
    if (readHandle.isLogicallyDeleted(sv.getKey(), sv.getBySeqno())) {
        debugLogSv("CacheTransferStream skipping as in dropped collection", sv);
        return Status::KeepVisiting;
    }

    // 3. StoredValues with a sequence number greater than the stream's maxSeqno
    // are not eligible.
    if (uint64_t(sv.getBySeqno()) > getMaxSeqno()) {
        debugLogSv("CacheTransferStream skipping sv with seqno > maxSeqno", sv);
        return Status::KeepVisiting;
    }

    // 4. Do checks for a value transfer, these don't apply if the only a
    // key/meta transfer is requested.
    if (includeValue == IncludeValue::Yes) {
        // 4.1 If not resident, it is not eligible.
        if (!sv.isResident()) {
            debugLogSv("CacheTransferStream skipping non-resident", sv);
            return Status::KeepVisiting;
        }

        // 4.2 If the sv is expired, it is not eligible.
        if (sv.isExpired(ep_real_time())) {
            debugLogSv("CacheTransferStream skipping expired", sv);
            return Status::KeepVisiting;
        }
    }

    // If the item is not allowed by the filter, skip it.
    if (!filter.check(sv.getKey())) {
        debugLogSv("CacheTransferStream skipping as not allowed by filter", sv);
        return Status::KeepVisiting;
    }

    debugLogSv("CacheTransferStream queuing", sv);

    // Generate a MutationResponse. It carries all the required information to
    // transfer the cache. Later we will tweak this so that a DcpMutation isn't
    // the final output message. Note that for IncludeXattrs etc.. we enable
    // everything unconditionally. A consumer which enables CacheTransfer knows
    // all these features and there is no desire to negoiate a cache transfer
    // without xattr/collecions etc... The Delete... options actually have no
    // meaning as we're not generating anything that is "deleted".
    auto response = std::make_unique<MutationResponse>(
            queued_item{sv.toItem(getVBucket(),
                                  StoredValue::HideLockedCas::No,
                                  includeValue == IncludeValue::Yes
                                          ? StoredValue::IncludeValue::Yes
                                          : StoredValue::IncludeValue::No)},
            opaque_,
            includeValue,
            IncludeXattrs::Yes,
            IncludeDeleteTime::Yes,
            IncludeDeletedUserXattrs::Yes,
            DocKeyEncodesCollectionId::Yes,
            EnableExpiryOutput::Yes,
            sid,
            DcpResponse::Event::CachedValue);
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        if (state != State::Active) {
            return Status::Stop;
        }
        totalBytesQueued += response->getMessageSize();
        pushToReadyQ(std::move(response));
    }

    // Backoff off if over HWM
    const auto hwm = engine.getEpStats().mem_high_wat.load();
    const auto memoryUsed = getMemoryUsed();
    if (memoryUsed > hwm) {
        if (shouldLog(spdlog::level::debug)) {
            logWithContext(spdlog::level::debug,
                           "CacheTransferStream OOM:",
                           {{"mem_used", memoryUsed}, {"hwm", hwm}});
        }
        return Status::OOM;
    }

    return Status::QueuedItem;
}
