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

#include "passive_stream.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/events_generated.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "durability/durability_monitor.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/json_utilities.h>
#include <nlohmann/json.hpp>
#include <platform/json_log_conversions.h>
#include <platform/optional.h>
#include <statistics/cbstat_collector.h>

#include <memory>

const std::string passiveStreamLoggingPrefix =
        "DCP (Consumer): **Deleted conn**";

PassiveStream::PassiveStream(EventuallyPersistentEngine* e,
                             std::shared_ptr<DcpConsumer> c,
                             const std::string& name,
                             cb::mcbp::DcpAddStreamFlag flags,
                             uint32_t opaque,
                             Vbid vb,
                             uint64_t st_seqno,
                             uint64_t vb_uuid,
                             uint64_t snap_start_seqno,
                             uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno,
                             const Collections::ManifestUid vb_manifest_uid)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno),
      engine(e),
      consumerPtr(c),
      last_seqno(vb_high_seqno, {*this}),
      cur_snapshot_start(0, {*this}),
      cur_snapshot_end(0, {*this}),
      cur_snapshot_type(Snapshot::None),
      cur_snapshot_ack(false),
      cur_snapshot_prepare(false),
      vb_manifest_uid(vb_manifest_uid),
      flatBuffersSystemEventsEnabled(c->areFlatBuffersSystemEventsEnabled()) {
    std::lock_guard<std::mutex> lh(streamMutex);
    streamRequest_UNLOCKED(vb_uuid);
    itemsReady.store(true);
}

PassiveStream::~PassiveStream() {
    if (state_ != StreamState::Dead) {
        // Destructed a "live" stream, log it.
        OBJ_LOG_INFO_CTX(*this,
                         "Destructing stream",
                         {"last_seqno", last_seqno.load()},
                         {"unacked_bytes", unackedBytes.load()});
    }

    if (auto consumer = consumerPtr.lock()) {
        consumer->incrFlowControlFreedBytes(unackedBytes);
    }
}

void PassiveStream::streamRequest(uint64_t vb_uuid) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);
        streamRequest_UNLOCKED(vb_uuid);
    }
    notifyStreamReady();
}

void PassiveStream::streamRequest_UNLOCKED(uint64_t vb_uuid) {
    auto stream_req_value = createStreamReqValue();

    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    pushToReadyQ(std::make_unique<StreamRequest>(
            vb_,
            opaque_,
            flags_,
            start_seqno_,
            std::numeric_limits<uint64_t>::max(),
            start_seqno_ ? vb_uuid : 0,
            snap_start_seqno_,
            snap_end_seqno_,
            stream_req_value));

    const bool isTakeover =
            isFlagSet(flags_, cb::mcbp::DcpAddStreamFlag::TakeOver);

    OBJ_LOG_INFO_CTX(*this,
                     "Attempting to add stream",
                     {"takeover", isTakeover},
                     {"opaque", opaque_},
                     {"start_seqno", start_seqno_},
                     {"vb_uuid", vb_uuid},
                     {"snapshot", {snap_start_seqno_, snap_end_seqno_}},
                     {"last_seqno", last_seqno.load()},
                     {"stream_req_value",
                      stream_req_value.empty() ? "none" : stream_req_value},
                     {"flags", flags_});
}

void PassiveStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    std::lock_guard<std::mutex> slh(streamMutex);
    if (transitionState(StreamState::Dead)) {
        const auto severity =
                status == cb::mcbp::DcpStreamEndStatus::Disconnected
                        ? spdlog::level::level_enum::warn
                        : spdlog::level::level_enum::info;
        OBJ_LOG_CTX(*this,
                    severity,
                    "Setting stream to dead state",
                    {"last_seqno", last_seqno.load()},
                    {"unacked_bytes", unackedBytes.load()},
                    {"status", cb::mcbp::to_string(status)});
    }
}

std::string PassiveStream::getStreamTypeName() const {
    return "Passive";
}

std::string PassiveStream::getStateName() const {
    return to_string(state_);
}

bool PassiveStream::isActive() const {
    return state_ != StreamState::Dead;
}

bool PassiveStream::isPending() const {
    return state_ == StreamState::Pending;
}

void PassiveStream::acceptStream(cb::mcbp::Status status, uint32_t add_opaque) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::acceptStream(): Unable to "
                         "find VBucket - cannot accept Stream",
                         {"status", status});
        return;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::acceptStream(): Unable to "
                         "lock Consumer - cannot accept Stream",
                         {"status", status});
        return;
    }

    // We use the cur_snapshot_prepare member to determine if we should
    // notify the PDM of any Memory snapshots. It is set when we see a
    // prepare in any snapshot. Consider the following snapshot:
    //
    // [1:Prepare(A), 2:Mutation(B)] Type = Memory
    //
    // If we have only received and persisted the following sequence of events
    // but then restart, we would fail to notify the PDM of the complete
    // snapshot:
    //
    // 1) SnapshotMarker (1-2) Type = Memory
    // 2) Prepare (1)                        <- Persisted to disk
    //
    // To solve this, we can fix the cur_snapshot_prepare state on
    // PassiveStream acceptance. The PDM already avoids acking back the same
    // seqno, so notifying an extra snapshot shouldn't matter, and even if we
    // did ack back the same seqno, the ADM should already deal with weakly
    // monotonic acks as we ack back the HPS on stream connection.
    cur_snapshot_prepare = true;

    // SyncReplication: About to commence accepting data on this stream. Check
    // if the associated consumer supports SyncReplication, so we can later
    // correctly process Snapshot Markers.
    supportsSyncReplication = consumer->isSyncReplicationEnabled();

    // as above, but check if FlatBuffers was enabled
    flatBuffersSystemEventsEnabled =
            consumer->areFlatBuffersSystemEventsEnabled();

    // For SyncReplication streams lookup the highPreparedSeqno to check if
    // we need to re-ACK (after accepting the stream).
    const int64_t highPreparedSeqno =
            supportsSyncReplication ? vb->getHighPreparedSeqno() : 0;

    std::unique_lock<std::mutex> lh(streamMutex);
    if (isPending()) {
        pushToReadyQ(std::make_unique<AddStreamResponse>(
                add_opaque, opaque_, status));
        if (status == cb::mcbp::Status::Success) {
            // Before we receive/process anything else, send a seqno ack if we
            // are a stream for a pre-existing vBucket to ensure that the
            // replica can commit any in-flight SyncWrites if no further
            // SyncWrites are done and no disk snapshots processed by this
            // replica.
            if (highPreparedSeqno) {
                pushToReadyQ(std::make_unique<SeqnoAcknowledgement>(
                        opaque_, vb_, highPreparedSeqno));
            }
            transitionState(StreamState::Reading);
        } else {
            transitionState(StreamState::Dead);
        }
        lh.unlock();
        notifyStreamReady();
    }
}

void PassiveStream::reconnectStream(VBucketPtr& vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    auto uuidToUse = start_seqno ? vb->failovers->getLatestEntry().vb_uuid : 0;

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.getEnd() == info.start) {
        info.range.setStart(info.start);
    }

    auto stream_req_value = createStreamReqValue();

    {
        std::lock_guard<std::mutex> lh(streamMutex);
        vb_uuid_ = uuidToUse;
        snap_start_seqno_ = info.range.getStart();
        start_seqno_ = info.start;
        snap_end_seqno_ = info.range.getEnd();
        last_seqno.reset(start_seqno);
        // The start_seqno & cur_snapshot_end shouldn't be less than start_seqno
        // to set it's starting val to start_seqno
        cur_snapshot_start.reset(start_seqno);
        cur_snapshot_end.reset(start_seqno);

        OBJ_LOG_INFO_CTX(*this,
                         "Attempting to reconnect stream",
                         {"new_opaque", new_opaque},
                         {"start_seqno", start_seqno},
                         {"snapshot", {snap_start_seqno_, snap_end_seqno_}},
                         {"value", stream_req_value},
                         {"vb_uuid", vb_uuid_});

        pushToReadyQ(std::make_unique<StreamRequest>(
                vb_,
                new_opaque,
                flags_,
                start_seqno,
                std::numeric_limits<uint64_t>::max(),
                vb_uuid_,
                snap_start_seqno_,
                snap_end_seqno_,
                stream_req_value));
    }
    notifyStreamReady();
}

cb::engine_errc PassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse, UpdateFlowControl& ufc) {
    if (!dcpResponse) {
        return cb::engine_errc::invalid_arguments;
    }

    if (!isActive()) {
        // If the Stream isn't active, *but* the object is still receiving
        // messages from the DcpConsumer that means the stream is still
        // registered in the streams map and hence we should ignore any
        // messages (until STREAM_END is received and the stream is removed form
        // the map).
        return cb::engine_errc::success;
    }

    auto seqno = dcpResponse->getBySeqno();
    if (seqno) {
        if (uint64_t(*seqno) <= last_seqno.load()) {
            OBJ_LOG_WARN_CTX(
                    *this,
                    "Erroneous (out of sequence) message received, with "
                    "its seqno is not greater than last received "
                    "seqno; Dropping mutation",
                    {"response", dcpResponse->to_string()},
                    {"opaque", opaque_},
                    {"seqno", *seqno},
                    {"last_seqno", last_seqno.load()});
            return cb::engine_errc::out_of_range;
        }
    } else if (dcpResponse->getEvent() == DcpResponse::Event::SnapshotMarker) {
        auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
        uint64_t snapStart = s->getStartSeqno();
        uint64_t snapEnd = s->getEndSeqno();
        if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
            OBJ_LOG_WARN_CTX(
                    *this,
                    "Erroneous snapshot marker received, its start and "
                    "end are less than last received seqno; Dropping marker",
                    {"opaque", opaque_},
                    {"snap_start", snapStart},
                    {"snap_end", snapEnd},
                    {"last_seqno", last_seqno.load()});
            return cb::engine_errc::out_of_range;
        }
    }

    auto& bucket = *engine->getKVBucket();
    switch (bucket.getReplicationThrottleStatus()) {
    case KVBucket::ReplicationThrottleStatus::Disconnect:
        OBJ_LOG_WARN_RAW(
                *this,
                "Disconnecting the connection as there is no memory to "
                "complete replication");
        return cb::engine_errc::disconnect;
    case KVBucket::ReplicationThrottleStatus::Process: {
        return forceMessage(*dcpResponse).getError();
    }
    case KVBucket::ReplicationThrottleStatus::Pause: {
        forceMessage(*dcpResponse);
        // Don't ack the bytes
        unackedBytes += ufc.release();
        return cb::engine_errc::temporary_failure;
    }
    }

    folly::assume_unreachable();
}

ProcessUnackedBytesResult PassiveStream::processUnackedBytes(
        uint32_t& processed_bytes) {
    processUnackedBytes_TestHook();

    // Note: We need to sync on state transitions as setDead() is possibly
    // acking all the remaining unackedBytes. Here we would ack again the bytes
    // twice otherwise, see MB-60468.
    std::lock_guard<std::mutex> lh(streamMutex);

    auto& bucket = *engine->getKVBucket();
    const auto availableBytes = bucket.getMemAvailableForReplication();
    const auto ackableBytes = std::min(unackedBytes.load(), availableBytes);

    // Tell the Consumer how many bytes we can ack back to the Producer
    processed_bytes += ackableBytes;

    unackedBytes -= ackableBytes;
    return unackedBytes > 0 ? more_to_process : all_processed;
}

cb::engine_errc PassiveStream::processMessageInner(
        MutationConsumerMessage* message, EnforceMemCheck enforceMemCheck) {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return cb::engine_errc::disconnect;
    }

    if (uint64_t(*message->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*message->getBySeqno()) > cur_snapshot_end.load()) {
        OBJ_LOG_WARN_CTX(*this,
                         "Erroneous (sequence number does not fall in the "
                         "expected snapshot range); Dropping the seqno",
                         {"message", message->to_string()},
                         {"current_snapshot",
                          {cur_snapshot_start.load(), cur_snapshot_end.load()}},
                         {"seqno", *message->getBySeqno()});
        return cb::engine_errc::out_of_range;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(message->getItem()->getCas())) {
        OBJ_LOG_WARN_CTX(
                *this,
                "Invalid CAS received. Regenerating new CAS",
                {"cas", fmt::format("{:#x}", message->getItem()->getCas())},
                {"message", message->to_string()},
                {"seqno", message->getItem()->getBySeqno()});
        message->getItem()->setCas();
    }

    auto ret = cb::engine_errc::failed;
    DeleteSource deleteSource = DeleteSource::Explicit;

    switch (message->getEvent()) {
    case DcpResponse::Event::Mutation:
        ret = engine->getKVBucket()->setWithMeta(*message->getItem(),
                                                 0,
                                                 nullptr,
                                                 consumer->getCookie(),
                                                 permittedVBStates,
                                                 CheckConflicts::No,
                                                 true,
                                                 GenerateBySeqno::No,
                                                 GenerateCas::No,
                                                 message->getExtMetaData(),
                                                 enforceMemCheck);
        break;
    case DcpResponse::Event::Expiration:
        deleteSource = DeleteSource::TTL;
        // fallthrough with deleteSource updated
    case DcpResponse::Event::Deletion:
        if (message->getItem()->getNBytes() == 0) {
            uint64_t delCas = 0;
            ItemMetaData meta = message->getItem()->getMetaData();
            ret = engine->getKVBucket()->deleteWithMeta(
                    message->getItem()->getKey(),
                    delCas,
                    nullptr,
                    message->getVBucket(),
                    consumer->getCookie(),
                    permittedVBStates,
                    CheckConflicts::No,
                    meta,
                    GenerateBySeqno::No,
                    GenerateCas::No,
                    *message->getBySeqno(),
                    message->getExtMetaData(),
                    deleteSource,
                    enforceMemCheck);
            if (ret == cb::engine_errc::no_such_key) {
                ret = cb::engine_errc::success;
            }
        } else {
            // The deletion has a value, send it through the setWithMeta path to
            // process it correctly
            ret = engine->getKVBucket()->setWithMeta(*message->getItem(),
                                                     0,
                                                     nullptr,
                                                     consumer->getCookie(),
                                                     permittedVBStates,
                                                     CheckConflicts::No,
                                                     true,
                                                     GenerateBySeqno::No,
                                                     GenerateCas::No,
                                                     message->getExtMetaData(),
                                                     enforceMemCheck);
        }
        break;
    case DcpResponse::Event::Prepare:
        ret = engine->getKVBucket()->prepare(
                *message->getItem(), consumer->getCookie(), enforceMemCheck);
        // If the stream has received and successfully processed a pending
        // SyncWrite, then we have to flag that the Replica must notify the
        // DurabilityMonitor at snapshot-end received for the DM to move the
        // HighPreparedSeqno.
        if (ret == cb::engine_errc::success) {
            cur_snapshot_prepare.store(true);
        }
        break;
    case DcpResponse::Event::Commit:
    case DcpResponse::Event::Abort:
    case DcpResponse::Event::SetVbucket:
    case DcpResponse::Event::StreamReq:
    case DcpResponse::Event::StreamEnd:
    case DcpResponse::Event::SnapshotMarker:
    case DcpResponse::Event::AddStream:
    case DcpResponse::Event::SystemEvent:
    case DcpResponse::Event::SeqnoAcknowledgement:
    case DcpResponse::Event::OSOSnapshot:
    case DcpResponse::Event::SeqnoAdvanced:
    case DcpResponse::Event::CachedValue:
        throw std::invalid_argument(
                "PassiveStream::processMessageInner: invalid event " +
                std::string(message->to_string()));
    }

    return ret;
}

void PassiveStream::seqnoAck(int64_t seqno) {
    // Only send a seqnoAck if we have an active stream that the producer has
    // responded with Success to the stream request
    if (!isActive() || isPending()) {
        OBJ_LOG_WARN_CTX(
                *this,
                "Could not ack seqno because stream was in StreamState: "
                "Expected it to be in a different state",
                {"seqno", seqno},
                {"state", to_string(state_.load())},
                {"expected_state", to_string(StreamState::Reading)});
        return;
    }

    {
        std::lock_guard<std::mutex> lh(streamMutex);
        if (!isActive()) {
            return;
        }

        pushToReadyQ(
                std::make_unique<SeqnoAcknowledgement>(opaque_, vb_, seqno));
    }
    notifyStreamReady();
}

std::string PassiveStream::to_string(StreamState st) {
    switch (st) {
    case StreamState::Pending:
        return "pending";
    case StreamState::Reading:
        return "reading";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument("PassiveStream::to_string(StreamState): " +
                                std::to_string(int(st)));
}

cb::engine_errc PassiveStream::processCommit(
        const CommitSyncWriteConsumer& commit) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    // The state of the VBucket should never change during a commit, because
    // VBucket::commit() may generated expired items.
    // NOTE: Theoretically this will never occur, because we kill all streams
    // when changing the VBucket state.
    std::shared_lock rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->commit(rlh,
                      commit.getKey(),
                      commit.getPreparedSeqno(),
                      *commit.getBySeqno(),
                      CommitType::Majority,
                      vb->lockCollections(commit.getKey()));
}

cb::engine_errc PassiveStream::processAbort(
        const AbortSyncWriteConsumer& abort) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    // The state of the VBucket should never change during an abort, because
    // VBucket::abort() may generated expired items.
    // NOTE: Theoretically this will never occur, because we kill all streams
    // when changing the VBucket state.
    std::shared_lock rlh(vb->getStateLock());

    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->abort(rlh,
                     abort.getKey(),
                     abort.getPreparedSeqno(),
                     abort.getAbortSeqno(),
                     vb->lockCollections(abort.getKey()));
}

cb::engine_errc PassiveStream::processSystemEvent(
        const SystemEventMessage& event) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }
    std::shared_lock rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return cb::engine_errc::not_my_vbucket;
    }

    cb::engine_errc rv = cb::engine_errc::success;

    if (flatBuffersSystemEventsEnabled) {
        rv = processSystemEventFlatBuffers(
                *vb, static_cast<const SystemEventConsumerMessage&>(event));
    } else {
        rv = processSystemEvent(*vb, event);
    }

    return rv;
}

cb::engine_errc PassiveStream::processSystemEvent(
        VBucket& vb, const SystemEventMessage& event) {
    Expects(!flatBuffersSystemEventsEnabled);
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    using mcbp::systemevent::id;
    switch (event.getSystemEvent()) {
    case id::BeginCollection:
        return processBeginCollection(vb, CreateCollectionEvent(event));
    case id::EndCollection:
        return processDropCollection(vb, DropCollectionEvent(event));
    case id::CreateScope:
        return processCreateScope(vb, CreateScopeEvent(event));
    case id::DropScope:
        return processDropScope(vb, DropScopeEvent(event));
    case id::ModifyCollection:
        // invalid event for non-FlatBuffers
        return cb::engine_errc::invalid_arguments;
    }
    folly::assume_unreachable();
}

cb::engine_errc PassiveStream::processSystemEventFlatBuffers(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    Expects(flatBuffersSystemEventsEnabled);
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    using mcbp::systemevent::id;
    switch (event.getSystemEvent()) {
    case id::BeginCollection:
        return processBeginCollection(vb, event);
    case id::EndCollection:
        return processDropCollection(vb, event);
    case id::CreateScope:
        return processCreateScope(vb, event);
    case id::DropScope:
        return processDropScope(vb, event);
    case id::ModifyCollection:
        return processModifyCollection(vb, event);
    }
    folly::assume_unreachable();
}

cb::engine_errc PassiveStream::processBeginCollection(
        VBucket& vb, const CreateCollectionEvent& event) {
    try {
        // This creation event comes from a node which didn't support
        // FlatBuffers. The following parameters require FlatBuffers to transfer
        // Metered, CanDeduplicate and flushUid. These parameters are all set to
        // default state as we assume the source node is older than any of these
        // parameters.
        // - No metering
        // - De-duplication is enabled
        // - FlushUid is 0
        vb.replicaBeginCollection(event.getManifestUid(),
                                  {event.getScopeID(), event.getCollectionID()},
                                  event.getKey(),
                                  event.getMaxTtl(),
                                  Collections::Metered::No,
                                  CanDeduplicate::Yes,
                                  Collections::ManifestUid{},
                                  event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::processBeginCollection exception",
                         {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropCollection(
        VBucket& vb, const DropCollectionEvent& event) {
    try {
        // set the isSystemCollection as false. If receiving a non-flatbuffers
        // system event, the producer is not 7.6 and thus does not support
        // system collections.
        vb.replicaDropCollection(event.getManifestUid(),
                                 event.getCollectionID(),
                                 false,
                                 event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::processDropCollection exception",
                         {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processCreateScope(
        VBucket& vb, const CreateScopeEvent& event) {
    try {
        vb.replicaCreateScope(event.getManifestUid(),
                              event.getScopeID(),
                              event.getKey(),
                              event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::processCreateScope exception",
                         {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropScope(VBucket& vb,
                                                const DropScopeEvent& event) {
    try {
        // If processing a !flat-buffer event, cannot be a system scope
        vb.replicaDropScope(event.getManifestUid(),
                            event.getScopeID(),
                            false,
                            event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(*this,
                         "PassiveStream::processDropScope exception",
                         {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processBeginCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        // Decompose the FlatBuffers data.
        // Here we will use defaults when the producer is older.
        // The vbucket is now informed of the collection and it will regenerate
        // a new FlatBuffers system event using *this* system's schema+data.
        const auto& collection =
                Collections::VB::Manifest::getCollectionFlatbuffer(
                        event.getEventData());
        cb::ExpiryLimit maxTtl;
        if (collection.ttlValid()) {
            maxTtl = std::chrono::seconds(collection.maxTtl());
        }

        vb.replicaBeginCollection(
                Collections::ManifestUid{collection.uid()},
                {collection.scopeId(), collection.collectionId()},
                event.getKey(),
                maxTtl,
                Collections::getMetered(collection.metered()),
                getCanDeduplicateFromHistory(collection.history()),
                Collections::ManifestUid{collection.flushUid()},
                *event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(
                *this,
                "PassiveStream::processBeginCollection FlatBuffers exception",
                {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processModifyCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto& collection =
                Collections::VB::Manifest::getCollectionFlatbuffer(
                        event.getEventData());

        cb::ExpiryLimit maxTtl;
        if (collection.ttlValid()) {
            maxTtl = std::chrono::seconds(collection.maxTtl());
        }

        vb.replicaModifyCollection(
                Collections::ManifestUid{collection.uid()},
                collection.collectionId(),
                maxTtl,
                Collections::getMetered(collection.metered()),
                getCanDeduplicateFromHistory(collection.history()),
                *event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(
                *this,
                "PassiveStream::processModifyCollection flatbuffer exception",
                {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropCollection(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* collection =
                Collections::VB::Manifest::getDroppedCollectionFlatbuffer(
                        event.getEventData());
        vb.replicaDropCollection(Collections::ManifestUid{collection->uid()},
                                 collection->collectionId(),
                                 collection->systemCollection(),
                                 *event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(
                *this,
                "PassiveStream::processDropCollection FlatBuffers exception",
                {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processCreateScope(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* scope = Collections::VB::Manifest::getScopeFlatbuffer(
                event.getEventData());
        vb.replicaCreateScope(Collections::ManifestUid{scope->uid()},
                              scope->scopeId(),
                              event.getKey(),
                              *event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(
                *this,
                "PassiveStream::processCreateScope FlatBuffers exception",
                {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc PassiveStream::processDropScope(
        VBucket& vb, const SystemEventConsumerMessage& event) {
    try {
        const auto* scope =
                Collections::VB::Manifest::getDroppedScopeFlatbuffer(
                        event.getEventData());
        vb.replicaDropScope(Collections::ManifestUid{scope->uid()},
                            scope->scopeId(),
                            scope->systemScope(),
                            *event.getBySeqno());
    } catch (std::exception& e) {
        OBJ_LOG_WARN_CTX(
                *this,
                "PassiveStream::processDropScope FlatBuffers exception",
                {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

// Helper function to avoid a Monotonic violation (same end-seqno) for the
// !HISTORY->HISTORY snapshot
static bool mustAssignEndSeqno(SnapshotMarker* marker, uint64_t endSeqno) {
    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Memory)) {
        // Always assign and catch monotonic violations
        return true;
    }

    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::History) &&
        marker->getEndSeqno() == endSeqno) {
        // HISTORY disk snapshot marker can follow !HISTORY disk and they have
        // the same end-seqno. Skip the assignment and avoid the monotonic
        // exception
        return false;
    }

    // Always assign and catch monotonic violations
    return true;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return;
    }
    // Vbucket must be in a permitted state to apply the snapshot
    std::shared_lock rlh(vb->getStateLock());
    if (!permittedVBStates.test(vb->getState())) {
        return;
    }

    // cur_snapshot_start is initialised to 0 so only set it for numbers > 0,
    // as the first snapshot maybe have a snap_start_seqno of 0.
    if (marker->getStartSeqno() > 0) {
        cur_snapshot_start.store(marker->getStartSeqno());
    }

    if (mustAssignEndSeqno(marker, cur_snapshot_end)) {
        cur_snapshot_end.store(marker->getEndSeqno());
    }
    const auto prevSnapType = cur_snapshot_type.load();
    cur_snapshot_type.store(
            isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk)
                    ? Snapshot::Disk
                    : Snapshot::Memory);

    auto checkpointType =
            isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk)
                    ? CheckpointType::Disk
                    : CheckpointType::Memory;

    const auto historical =
            isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::History)
                    ? CheckpointHistorical::Yes
                    : CheckpointHistorical::No;

    // Check whether the snapshot can be considered as an initial disk
    // checkpoint for the replica.
    if (checkpointType == CheckpointType::Disk && vb->getHighSeqno() == 0) {
        checkpointType = CheckpointType::InitialDisk;
    }

    auto& ckptMgr = *vb->checkpointManager;

    // If this stream doesn't support SyncReplication (i.e. the producer
    // is a pre-MadHatter version) then we should consider the HCS to be
    // present but zero for disk snapshot (not possible for any
    // SyncWrites to have completed yet). If SyncReplication is
    // supported then use the value from the marker.
    const std::optional<uint64_t> hcs =
            isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk) &&
                            !supportsSyncReplication
                    ? 0
                    : marker->getHighCompletedSeqno();

    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk) &&
        supportsSyncReplication) {
        const auto hps = marker->getHighPreparedSeqno();
        if (hps > marker->getEndSeqno()) {
            const auto msg = fmt::format(
                    "PassiveStream::processMarker: stream:{} {}, flags:{}, "
                    "snapStart:{}, snapEnd:{}, HPS:{} - HPS is out of range",
                    name_,
                    vb_,
                    marker->getFlags(),
                    marker->getStartSeqno(),
                    marker->getEndSeqno(),
                    to_string_or_none(hps));
            throw std::logic_error(msg);
        }

        // The HPS can be lower than the snapshot start seqno in the following
        // cases:
        // 1. Active is a 8.0 node, but sending HPS in marker v2.2 is disabled
        // on the active; HPS is then encoded as 0 in the marker.
        // 2. Active is a pre-8.0 version node & it sends marker v2.0; HPS is
        // encoded is encoded in the marker.
        // 3. Offline upgrade:
        //    - There is a offline upgrade of the cluster from < 8.0 to 8.0.
        //    - The replica has HPS on disk set to a non-zero value (due to
        //      older logic where we bumped the HPS to snapEnd each time entire
        //      snapshot is consumed, despite not having any durability work
        //      load).
        //    - When the 8.0 cluster starts, since both the consumer & producer
        //      have negotiated marker_version=2.2, active sends out HPS=0 &
        //      replica warms up from disk a non-zero value, which cause a
        //      monotonic violation on the replica, eventually when the active
        //      sent snapshot marker is processed.
        cur_snapshot_hps = hps < marker->getStartSeqno() ? std::nullopt : hps;
    } else {
        cur_snapshot_hps = std::nullopt;
    }

    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk) && !hcs) {
        const auto msg = fmt::format(
                "PassiveStream::processMarker: stream:{} {}, flags:{}, "
                "snapStart:{}, snapEnd:{}, HCS:{} - missing HCS",
                name_,
                vb_,
                marker->getFlags(),
                marker->getStartSeqno(),
                marker->getEndSeqno(),
                to_string_or_none(hcs));
        throw std::logic_error(msg);
    }

    uint64_t purgeSeqno = 0;
    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Disk)) {
        // A replica could receive a duplicate DCP prepare during a disk
        // snapshot if it had previously received an uncompleted prepare.
        // We can receive a disk snapshot when we either:
        //     a) First connect
        //     b) Get cursor dropped by the active
        //
        // We selectively allow these prepares to overwrite the old one by
        // setting a duplicate prepare window in the vBucket. This will
        // allow any currently outstanding prepares to be overwritten, but
        // not any new ones.
        vb->setDuplicatePrepareWindow();

        // Only set the purgeSeqno if the snapshot is a disk snapshot
        purgeSeqno = marker->getPurgeSeqno().value_or(0);

        // Purge can be below the snapshot range, but never ahead.
        if (purgeSeqno > marker->getEndSeqno()) {
            const auto msg = fmt::format(
                    "PassiveStream::processMarker: stream:{} {}, flags:{}, "
                    "snapStart:{}, snapEnd:{}, HCS:{}, purgeSeqno:{} - "
                    "invalid purgeSeqno",
                    name_,
                    vb_,
                    marker->getFlags(),
                    marker->getStartSeqno(),
                    marker->getEndSeqno(),
                    to_string_or_none(hcs),
                    purgeSeqno);
            throw std::logic_error(msg);
        }
    }

    // We could be connected to a non sync-repl, so if the max-visible is
    // not transmitted (optional is false), set visible to snap-end
    const auto visibleSeq =
            marker->getMaxVisibleSeqno().value_or(marker->getEndSeqno());

    if (cur_snapshot_end < visibleSeq) {
        const auto msg = fmt::format(
                "PassiveStream::processMarker: snapEnd:{} < "
                "visibleSnapEnd:{}, snapStart:{}, hcs:{}, "
                "checkpointType:{}, historical:{}",
                cur_snapshot_end.load(),
                visibleSeq,
                cur_snapshot_start.load(),
                hcs ? std::to_string(*hcs) : "nullopt",
                ::to_string(checkpointType),
                ::to_string(historical));
        throw std::logic_error(msg);
    }

    if (checkpointType == CheckpointType::InitialDisk) {
        // Case: receiving the first snapshot in a Disk snapshot.
        // Note that replica may never execute here as the active may switch
        // directly to in-memory and send the first snapshot in a Memory
        // snapshot.
        vb->setReceivingInitialDiskSnapshot(true);
        ckptMgr.createSnapshot(cur_snapshot_start.load(),
                               cur_snapshot_end.load(),
                               hcs,
                               cur_snapshot_hps,
                               checkpointType,
                               visibleSeq,
                               historical,
                               purgeSeqno);
    } else {
        // Case: receiving any type of snapshot (Disk/Memory).
        if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Checkpoint)) {
            ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                   cur_snapshot_end.load(),
                                   hcs,
                                   cur_snapshot_hps,
                                   checkpointType,
                                   visibleSeq,
                                   historical,
                                   purgeSeqno);
        } else {
            // MB-42780: In general we cannot merge multiple snapshots into
            // the same checkpoint. The only exception is for when replica
            // receives multiple Memory checkpoints in a row.
            // Since 6.5.0 the Active behaves correctly with regard to that
            // (ie, the Active always sets the
            // DcpSnapshotMarkerFlag::Checkpoint in a
            // snapshot transition tha involves Disk snapshots), but older
            // Producers may still miss the
            // DcpSnapshotMarkerFlag::Checkpoint.
            if (prevSnapType == Snapshot::Memory &&
                cur_snapshot_type == Snapshot::Memory) {
                ckptMgr.extendOpenCheckpoint(cur_snapshot_end.load(),
                                             visibleSeq);
            } else {
                ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                       cur_snapshot_end.load(),
                                       hcs,
                                       cur_snapshot_hps,
                                       checkpointType,
                                       visibleSeq,
                                       historical,
                                       purgeSeqno);
            }
        }
    }

    if (isFlagSet(marker->getFlags(), DcpSnapshotMarkerFlag::Acknowledge)) {
        cur_snapshot_ack = true;
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getKVBucket()->setVBucketState(
            vb_, state->getState(), {}, TransferVB::Yes);
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        pushToReadyQ(std::make_unique<SetVBucketStateResponse>(
                opaque_, cb::mcbp::Status::Success));
    }
    notifyStreamReady();
}

void PassiveStream::handleSnapshotEnd(uint64_t seqno) {
    auto vb = engine->getVBucket(vb_);
    if (!vb) {
        return;
    }

    if (seqno != cur_snapshot_end.load()) {
        return;
    }

    if (cur_snapshot_type.load() == Snapshot::Disk) {
        vb->setReceivingInitialDiskSnapshot(false);
    }

    if (cur_snapshot_ack) {
        {
            std::lock_guard<std::mutex> lh(streamMutex);
            pushToReadyQ(std::make_unique<SnapshotMarkerResponse>(
                    opaque_, cb::mcbp::Status::Success));
        }
        notifyStreamReady();
        cur_snapshot_ack = false;
    }

    // Notify the PassiveDM that the snapshot-end mutation has been received on
    // PassiveStream, if the snapshot contains at least one Prepare. That is
    // necessary for unblocking the High Prepared Seqno in PassiveDM. Note that
    // the HPS is what the PassiveDM acks back to the Active. See comments in
    // PassiveDM for details.

    // Disk snapshots are subject to deduplication, and may be missing purged
    // aborts. We must notify the PDM even if we have not seen a prepare, to
    // account for possible unseen prepares.

    if (cur_snapshot_prepare || cur_snapshot_type.load() == Snapshot::Disk) {
        const auto hps = cur_snapshot_type.load() == Snapshot::Disk
                                 ? cur_snapshot_hps
                                 : std::nullopt;
        vb->notifyPassiveDMOfSnapEndReceived(seqno, hps);
        cur_snapshot_prepare.store(false);
    }
}

void PassiveStream::addStats(const AddStatFn& add_stat, CookieIface& c) {
    Stream::addStats(add_stat, c);

    try {
        add_casted_stat("unacked_bytes", unackedBytes, add_stat, c);

        add_casted_stat("last_received_seqno", last_seqno.load(), add_stat, c);
        add_casted_stat(
                "ready_queue_memory", getReadyQueueMemory(), add_stat, c);
        add_casted_stat("cur_snapshot_type",
                        ::to_string(cur_snapshot_type.load()),
                        add_stat,
                        c);

        if (cur_snapshot_type.load() != Snapshot::None) {
            add_casted_stat("cur_snapshot_start",
                            cur_snapshot_start.load(),
                            add_stat,
                            c);
            add_casted_stat(
                    "cur_snapshot_end", cur_snapshot_end.load(), add_stat, c);
        }

        add_casted_stat("cur_snapshot_prepare",
                        cur_snapshot_prepare.load(),
                        add_stat,
                        c);

        auto stream_req_value = createStreamReqValue();

        if (!stream_req_value.empty()) {
            add_casted_stat(
                    "vb_manifest_uid", stream_req_value.c_str(), add_stat, c);
        }

    } catch (std::exception& error) {
        OBJ_LOG_INFO_CTX(*this,
                         "PassiveStream::addStats: Failed to build stats",
                         {"error", error.what()});
    }
}

std::unique_ptr<DcpResponse> PassiveStream::next() {
    std::lock_guard<std::mutex> lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return nullptr;
    }

    return popFromReadyQ();
}

bool PassiveStream::transitionState(StreamState newState) {
    OBJ_LOG_DEBUG_CTX(*this,
                      "PassiveStream::transitionState: Transitioning",
                      {"from", to_string(state_.load())},
                      {"to", to_string(newState)});

    if (state_ == newState) {
        return false;
    }

    bool validTransition = false;
    switch (state_.load()) {
    case StreamState::Pending:
        if (newState == StreamState::Reading || newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::Reading:
        if (newState == StreamState::Dead) {
            validTransition = true;
        }
        break;

    case StreamState::Dead:
        // Once 'dead' shouldn't transition away from it.
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "PassiveStream::transitionState:"
                " newState (which is" +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }

    state_ = newState;
    return true;
}

void PassiveStream::notifyStreamReady() {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

std::string PassiveStream::createStreamReqValue() const {
    nlohmann::json stream_req_json;
    std::ostringstream ostr;
    ostr << std::hex << static_cast<uint64_t>(vb_manifest_uid);
    stream_req_json["uid"] = ostr.str();
    return stream_req_json.dump();
}

void PassiveStream::logWithContext(spdlog::level::level_enum severity,
                                   std::string_view msg,
                                   cb::logger::Json ctx) const {
    // Format: {"vb":"vb:X", ...}
    auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
    object.insert(object.begin(), {"vb", getVBucket()});

    auto consumer = consumerPtr.lock();
    if (consumer) {
        consumer->getLogger().logWithContext(severity, msg, std::move(ctx));
    } else {
        if (getGlobalBucketLogger()->should_log(severity)) {
            getGlobalBucketLogger()->logWithContext(
                    severity, msg, std::move(ctx));
        }
    }
}

void PassiveStream::maybeLogMemoryState(cb::engine_errc status,
                                        const std::string& msgType,
                                        int64_t seqno) {
    bool previousNoMem = isNoMemory.load();
    if (status == cb::engine_errc::no_memory && !previousNoMem) {
        OBJ_LOG_WARN_CTX(*this,
                         "Got error while trying to process with seqno",
                         {"status", cb::to_string(status)},
                         {"message_type", msgType},
                         {"seqno", seqno});
        isNoMemory.store(true);
    } else if (status == cb::engine_errc::success && previousNoMem) {
        OBJ_LOG_INFO_RAW(*this,
                         "PassiveStream resuming after no-memory backoff");
        isNoMemory.store(false);
    }
}

std::string PassiveStream::Labeller::getLabel(const char* name) const {
    return fmt::format("PassiveStream({} {})::{}",
                       stream.getVBucket(),
                       stream.getName(),
                       name);
}

PassiveStream::ProcessMessageResult PassiveStream::processMessage(
        gsl::not_null<DcpResponse*> response, EnforceMemCheck enforceMemCheck) {
    auto vb = engine->getVBucket(vb_);
    if (!vb) {
        return {*this, cb::engine_errc::not_my_vbucket, {}};
    }

    cb::engine_errc ret = cb::engine_errc::success;
    auto* resp = response.get();
    switch (resp->getEvent()) {
    case DcpResponse::Event::Mutation:
    case DcpResponse::Event::Deletion:
    case DcpResponse::Event::Expiration:
    case DcpResponse::Event::Prepare:
        ret = processMessageInner(dynamic_cast<MutationConsumerMessage*>(resp),
                                  enforceMemCheck);
        break;
    case DcpResponse::Event::Commit:
        ret = processCommit(dynamic_cast<CommitSyncWriteConsumer&>(*resp));
        break;
    case DcpResponse::Event::Abort:
        ret = processAbort(dynamic_cast<AbortSyncWriteConsumer&>(*resp));
        break;
    case DcpResponse::Event::SnapshotMarker:
        processMarker(dynamic_cast<SnapshotMarker*>(resp));
        break;
    case DcpResponse::Event::SetVbucket:
        processSetVBucketState(dynamic_cast<SetVBucketState*>(resp));
        break;
    case DcpResponse::Event::StreamEnd: {
        streamDeadHook();
        std::lock_guard<std::mutex> lh(streamMutex);
        transitionState(StreamState::Dead);
    } break;
    case DcpResponse::Event::SystemEvent:
        ret = processSystemEvent(dynamic_cast<SystemEventMessage&>(*resp));
        break;
    case DcpResponse::Event::StreamReq:
    case DcpResponse::Event::AddStream:
    case DcpResponse::Event::SeqnoAcknowledgement:
    case DcpResponse::Event::OSOSnapshot:
    case DcpResponse::Event::SeqnoAdvanced:
    case DcpResponse::Event::CachedValue:
        // These are invalid events for this path, they are handled by
        // the DcpConsumer class
        throw std::invalid_argument(
                "PassiveStream::processMessage: invalid event " +
                std::string(resp->to_string()));
    }

    const auto seqno = resp->getBySeqno();

    const auto* mutation = dynamic_cast<MutationConsumerMessage*>(resp);
    if (mutation) {
        Expects(seqno);
        if (ret != cb::engine_errc::success) {
            // ENOMEM logging is handled by maybeLogMemoryState
            if (ret != cb::engine_errc::no_memory) {
                OBJ_LOG_WARN_CTX(
                        *this,
                        "PassiveStream::processMessage: Got error while trying "
                        "to process with seqno: cid",
                        {"vb_", vb_},
                        {"cb::to_stringret", cb::to_string(ret)},
                        {"resp-to_string", resp->to_string()},
                        {"*seqno", *seqno},
                        {"collection_i_d",
                         mutation->getItem()->getKey().getCollectionID()});
            }
        }
        maybeLogMemoryState(ret, resp->to_string(), *seqno);
    } else {
        if (ret != cb::engine_errc::success) {
            OBJ_LOG_WARN_CTX(*this,
                             "PassiveStream::processMessage: Got error while "
                             "trying to process with seqno",
                             {"error", cb::to_string(ret)},
                             {"response", resp->to_string()},
                             {"seqno", seqno});
        }
    }

    return {*this, ret, seqno};
}

PassiveStream::ProcessMessageResult::~ProcessMessageResult() {
    if (err == cb::engine_errc::success && seqno) {
        stream->handleSnapshotEnd(*seqno);
    }
}

size_t PassiveStream::getUnackedBytes() const {
    return unackedBytes;
}

PassiveStream::ProcessMessageResult PassiveStream::forceMessage(
        DcpResponse& resp) {
    auto ret = processMessage(&resp, EnforceMemCheck::No);

    const auto err = ret.getError();
    Expects(err != cb::engine_errc::temporary_failure);
    Expects(err != cb::engine_errc::no_memory);

    const auto seqno = resp.getBySeqno();
    if (err == cb::engine_errc::success && seqno) {
        last_seqno.store(*seqno);
    }

    return ret;
}