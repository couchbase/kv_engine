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
#include "kv_bucket.h"
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
    CacheTransferHashTableVisitor(std::shared_ptr<CacheTransferStream> stream,
                                  const Configuration& config)
        : weakStream(std::move(stream)),
          visitDurationMs(std::chrono::milliseconds(
                  config.getDcpCacheTransferVisitDurationMs())),
          oneVisitPerStep(config.isDcpCacheTransferOneVisitPerStep()) {
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
    const std::chrono::milliseconds visitDurationMs;
    const bool oneVisitPerStep{false};
};

class CacheTransferTask : public EpTask {
public:
    CacheTransferTask(EventuallyPersistentEngine& e,
                      Vbid vbid,
                      std::shared_ptr<CacheTransferStream> stream,
                      std::string_view descriptionDetail)
        : EpTask(e, TaskId::CacheTransferTask),
          vbid(vbid),
          visitor(std::move(stream), e.getConfiguration()),
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
    status = stream->maybeQueueItem(v, readHandle);
    switch (status) {
    case CacheTransferStream::Status::OOM:
        ++queuedCount; // When OOM, one item was queued
        [[fallthrough]];
    case CacheTransferStream::Status::Stop:
    case CacheTransferStream::Status::ReachedClientMemoryLimit:
        return false;
    case CacheTransferStream::Status::QueuedItem:
        ++queuedCount;
        [[fallthrough]];
    case CacheTransferStream::Status::KeepVisiting:
        if (oneVisitPerStep) {
            return false;
        }
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
                                visitDurationMs);
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

    if (position == vb->ht.endPosition() ||
        CacheTransferStream::isFinished(visitor.getStatus())) {
        // Calculate total runtime
        auto endTime = cb::time::steady_clock::now();
        auto totalRuntimeMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                      startTime)
                        .count();

        // Reached end of HT
        OBJ_LOG_INFO_CTX(
                stream,
                "CacheTransferTask::run: completed.",
                {{"vb", vbid},
                 {"ht_end", position == vb->ht.endPosition()},
                 {"status", visitor.getStatus()},
                 {"visited_count", visitedCount},
                 {"queued_count", queuedCount},
                 {"ht_num_items", vb->ht.getNumItems()},
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
                                         const StreamRequestInfo& req,
                                         Vbid vbid,
                                         EventuallyPersistentEngine& engine,
                                         Collections::VB::Filter filter)
    : ProducerStream(filter.getStreamId()
                             ? name + filter.getStreamId().to_string() + ":cts"
                             : name + ":cts",
                     p,
                     filter.getStreamId(),
                     cb::mcbp::DcpAddStreamFlag::None,
                     opaque,
                     vbid,
                     req.start_seqno,
                     req.vbucket_uuid),
      engine(engine),
      availableBytes(filter.getCacheTransferFreeMemory()),
      filter(std::move(filter)),
      request(req) {
    Expects(p);

    if (this->filter.isCacheTransferAllKeys() &&
        // Client wants all keys, but they signalled no availableBytes. Just
        // jump straight to IncludeValue::No rather than later in the "run loop"
        // of the task.
        availableBytes.value_or(~0ull) == 0) {
        includeValue = IncludeValue::No;
    }

    OBJ_LOG_INFO_CTX(p->getLogger(),
                     "Creating CacheTransferStream",
                     {"max_seqno", request.start_seqno},
                     {"end_seqno", request.end_seqno},
                     {"vb", vbid},
                     {"vbucket_uuid", request.vbucket_uuid},
                     {"all_keys", this->filter.isCacheTransferAllKeys()},
                     {"available_bytes", availableBytes});
}

void CacheTransferStream::setActive() {
    auto producer = getProducer();
    if (!producer) {
        OBJ_LOG_WARN_CTX(
                *this,
                "CacheTransferStream::scheduleTask: Producer cannot be locked",
                {"vb", getVBucket()});
        return;
    }
    std::lock_guard<std::mutex> lh(streamMutex);
    tid = ExecutorPool::get()->schedule(std::make_unique<CacheTransferTask>(
            engine, getVBucket(), shared_from_this(), producer->logHeader()));
}

void CacheTransferStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    ExecutorPool::get()->cancel(tid);
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        if (state != State::Active) {
            return;
        }
        if (status == cb::mcbp::DcpStreamEndStatus::Ok &&
            request.end_seqno > request.start_seqno) {
            state = State::SwitchingToActiveStream;
            pushToReadyQ(std::make_unique<CacheTransferToActiveStreamResponse>(
                    opaque_, getVBucket(), sid));
        } else {
            state = State::Dead;
            pushToReadyQ(makeEndStreamResponse(status));
        }
    }
    notifyStreamReady(false, getProducer().get());
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

void CacheTransferStream::cancelTransfer() {
    size_t bytes = 0;
    bool logMessage = false;
    {
        // Read stream state and determine if we should log a message.
        std::lock_guard<std::mutex> lh(streamMutex);
        bytes = totalBytesQueued;
        // Log only on transition from Active. There could be many in-flight
        // messages triggering the cancel. setDead will change the state of this
        // stream to be !Active.
        logMessage = state == State::Active;
    }
    if (logMessage) {
        OBJ_LOG_INFO_CTX(
                *this,
                "CacheTransferStream::cancelTransfer: Cancelling transfer",
                {"vbid", getVBucket()},
                {"total_bytes_queued", bytes});
    }
    // setDead may next switch over to an ActiveStream.
    setDead(cb::mcbp::DcpStreamEndStatus::Ok);
}

void CacheTransferStream::addTakeoverStats(const AddStatFn& add_stat,
                                           CookieIface& c,
                                           const VBucket& vb) {
    // MB-69678: Since MB-68800 CacheTransfer runs over the initial stream
    // phase (ie non-takeover phase). Still, ns_server pulls takeover-stats
    // during non-takeover phases (eg for backfill information). On a
    // CacheTransferStream we don't need to provide any useful information, but
    // an empty payload wouldd break ns_server.
    add_casted_stat("status", "calculating-item-count", add_stat, c);
    add_casted_stat("estimate", 0, add_stat, c);
    add_casted_stat("chk_items", 0, add_stat, c);
}

void CacheTransferStream::addStats(const AddStatFn& add_stat, CookieIface& c) {
    Stream::addStats(add_stat, c);
    size_t streamTid{0};
    size_t streamTotalBytesQueued{0};
    size_t streamLastSeqno{0};
    auto streamIncludeValue{IncludeValue::Yes};
    std::optional<size_t> streamAvailableBytes;

    {
        std::lock_guard<std::mutex> lh(streamMutex);
        streamTid = tid;
        streamIncludeValue = includeValue;
        streamTotalBytesQueued = totalBytesQueued;
        streamLastSeqno = lastSeqno;
        streamAvailableBytes = availableBytes;
    }
    add_casted_stat("tid", streamTid, add_stat, c);
    add_casted_stat(
            "include_value", to_string(streamIncludeValue), add_stat, c);
    add_casted_stat("total_bytes_queued", streamTotalBytesQueued, add_stat, c);
    add_casted_stat("last_sent_seqno", streamLastSeqno, add_stat, c);
    if (streamAvailableBytes) {
        add_casted_stat("available_bytes", *streamAvailableBytes, add_stat, c);
    }
}

std::string CacheTransferStream::getStreamTypeName() const {
    return "CacheTransfer";
}

std::string CacheTransferStream::getStateName() const {
    switch (state) {
    case State::Active:
        return "Active";
    case State::SwitchingToActiveStream:
        return "SwitchingToActiveStream";
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
    std::unique_lock<std::mutex> lh(streamMutex);

    itemsReady.store(false);

    if (readyQ.empty()) {
        return nullptr;
    }

    auto& response = readyQ.front();
    if (!producer.bufferLogInsert(response->getMessageSize())) {
        return nullptr;
    }

    itemsReady.store(true);
    if (response->getBySeqno()) {
        lastSeqno = *response->getBySeqno();
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

bool CacheTransferStream::skip(const StoredValue& sv,
                               Collections::VB::ReadHandle& readHandle) const {
    // Check if the sv is eligible for transfer.
    // 1. Temporary/Deleted/Pending StoredValues are not eligible.
    if (sv.isTempItem() || sv.isDeleted() || sv.isPending()) {
        OBJ_LOG_DEBUG_CTX(*this,
                          "CacheTransferStream skipping temp/deleted/pending",
                          {"sv", nlohmann::json{sv}});
        return true;
    }

    // 2. Dropped collection items are not eligible.
    if (readHandle.isLogicallyDeleted(sv.getKey(), sv.getBySeqno())) {
        OBJ_LOG_DEBUG_CTX(
                *this,
                "CacheTransferStream skipping as in dropped collection",
                {"sv", nlohmann::json{sv}});
        return true;
    }

    // 3. StoredValues with a sequence number greater than the stream's maxSeqno
    // are not eligible.
    if (uint64_t(sv.getBySeqno()) > getMaxSeqno()) {
        OBJ_LOG_DEBUG_CTX(
                *this,
                "CacheTransferStream skipping sv with seqno > maxSeqno",
                {"sv", nlohmann::json{sv}});
        return true;
    }

    // 4. Do checks for a value transfer, but all key transfers ignore these.
    if (includeValue == IncludeValue::Yes && !filter.isCacheTransferAllKeys()) {
        // 4.1 If not resident, it is not eligible unless this is an all key
        // transfer.
        if (!sv.isResident()) {
            OBJ_LOG_DEBUG_CTX(*this,
                              "CacheTransferStream skipping non-resident",
                              {"sv", nlohmann::json{sv}});
            return true;
        }

        // 4.2 If the sv is expired, it is not eligible.
        if (sv.isExpired(ep_real_time())) {
            OBJ_LOG_DEBUG_CTX(*this,
                              "CacheTransferStream skipping expired",
                              {"sv", nlohmann::json{sv}});
            return true;
        }
    }

    // If the item is not allowed by the filter, skip it.
    if (!filter.check(sv.getKey())) {
        OBJ_LOG_DEBUG_CTX(
                *this,
                "CacheTransferStream skipping as not allowed by filter",
                {"sv", nlohmann::json{sv}});
        return true;
    }
    return false;
}

CacheTransferStream::Status CacheTransferStream::maybeQueueItem(
        const StoredValue& sv, Collections::VB::ReadHandle& readHandle) {
    preQueueCallback(sv);

    // Lots of little checks to make to decide if the item found in the
    // hash-table should be skipped or queued.
    if (skip(sv, readHandle)) {
        return Status::KeepVisiting;
    }

    if (availableBytes && *availableBytes < sv.size()) {
        availableBytes.reset(); // no point re-entering here on every visit
        if (filter.isCacheTransferAllKeys()) {
            // availableBytes is exhausted and this is an all key transfer. Stop
            // sending values and only send keys.
            OBJ_LOG_INFO_CTX(*this,
                             "CacheTransferStream switching to key only as "
                             "have reached "
                             "requested transfer limit",
                             {"bytes_queued", totalBytesQueued},
                             {"sv_size", sv.size()});
            includeValue = IncludeValue::No;
        } else {
            // No memory left, stop the transfer.
            OBJ_LOG_INFO_CTX(*this,
                             "CacheTransferStream stopping as have reached "
                             "requested transfer limit",
                             {"sv_size", sv.size()},
                             {"available_bytes", *availableBytes});
            return Status::ReachedClientMemoryLimit;
        }
    }

    OBJ_LOG_DEBUG_CTX(
            *this, "CacheTransferStream queuing", {"sv", nlohmann::json{sv}});

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
            IncludeDeleteTime::Yes,
            DocKeyEncodesCollectionId::Yes,
            EnableExpiryOutput::Yes,
            sid,
            includeValue == IncludeValue::Yes && sv.isResident()
                    ? DcpResponse::Event::CachedValue
                    : DcpResponse::Event::CachedKeyMeta);
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        if (state != State::Active) {
            return Status::Stop;
        }
        totalBytesQueued += response->getMessageSize();
        if (availableBytes) {
            // reduce to 0 (we may go over by max value but maybe that's ok)
            *availableBytes = (*availableBytes > sv.size())
                                      ? (*availableBytes - sv.size())
                                      : 0;
        }
        pushToReadyQ(std::move(response));
    }

    // Backoff off if over HWM
    const auto hwm = engine.getEpStats().mem_high_wat.load();
    const auto memoryUsed = getMemoryUsed();
    if (memoryUsed > hwm) {
        OBJ_LOG_DEBUG_CTX(*this,
                          "CacheTransferStream OOM:",
                          {{"mem_used", memoryUsed}, {"hwm", hwm}});
        return Status::OOM;
    }

    return Status::QueuedItem;
}

void CacheTransferStream::logWithContext(spdlog::level::level_enum level,
                                         std::string_view msg,
                                         cb::logger::Json ctx) const {
    // Format: {"vb:"vb:X", "sid": "7", ...}
    auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
    if (sid) {
        object.insert(object.begin(), {"sid", sid.to_string()});
    }
    object.insert(object.begin(), {"vb", getVBucket()});

    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().logWithContext(level, msg, std::move(ctx));
    } else {
        if (getGlobalBucketLogger()->should_log(level)) {
            getGlobalBucketLogger()->logWithContext(level, msg, std::move(ctx));
        }
    }
}

std::string to_string(CacheTransferStream::Status status) {
    switch (status) {
    case CacheTransferStream::Status::QueuedItem:
        return "QueuedItem";
    case CacheTransferStream::Status::KeepVisiting:
        return "KeepVisiting";
    case CacheTransferStream::Status::OOM:
        return "OOM";
    case CacheTransferStream::Status::ReachedClientMemoryLimit:
        return "ReachedClientMemoryLimit";
    case CacheTransferStream::Status::Stop:
        return "Stop";
    }
    folly::assume_unreachable();
}
