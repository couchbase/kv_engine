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

#include "dcp/consumer.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "connhandler_impl.h"
#include "dcp/dcpconnmap.h"
#include "dcp/passive_stream.h"
#include "dcp/response.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "objectregistry.h"
#include "vbucket.h"
#include <executor/executorpool.h>
#include <fmt/chrono.h>
#include <phosphor/phosphor.h>
#include <xattr/utils.h>

#include <charconv>
#include <utility>

const std::string DcpConsumer::noopCtrlMsg = "enable_noop";
const std::string DcpConsumer::noopIntervalCtrlMsg = "set_noop_interval";
const std::string DcpConsumer::connBufferCtrlMsg = "connection_buffer_size";
const std::string DcpConsumer::priorityCtrlMsg = "set_priority";
// from vulcan onwards we only use the _vulcan control message
const std::string DcpConsumer::cursorDroppingCtrlMsg =
        "supports_cursor_dropping_vulcan";
const std::string DcpConsumer::sendStreamEndOnClientStreamCloseCtrlMsg =
        "send_stream_end_on_client_close_stream";
const std::string DcpConsumer::hifiMFUCtrlMsg = "supports_hifi_MFU";
const std::string DcpConsumer::enableOpcodeExpiryCtrlMsg =
        "enable_expiry_opcode";

class DcpConsumerTask : public EpTask {
public:
    DcpConsumerTask(EventuallyPersistentEngine& e,
                    std::shared_ptr<DcpConsumer> c,
                    double sleeptime = 1,
                    bool completeBeforeShutdown = true)
        : EpTask(e, TaskId::DcpConsumerTask, sleeptime, completeBeforeShutdown),
          consumerPtr(c),
          description("DcpConsumerTask, processing buffered items for " +
                      c->getName()) {
    }

    ~DcpConsumerTask() override {
        auto consumer = consumerPtr.lock();
        if (consumer) {
            consumer->taskCancelled();
        }
    }

    bool run() override {
        TRACE_EVENT0("ep-engine/task", "DcpConsumerTask");
        auto consumer = consumerPtr.lock();
        if (!consumer) {
            return false;
        }

        if (consumer->doDisconnect()) {
            return false;
        }

        double sleepFor = 0.0;
        enum ProcessUnackedBytesResult state = consumer->processUnackedBytes();
        switch (state) {
            case all_processed:
                sleepFor = INT_MAX;
                break;
            case more_to_process:
                sleepFor = 0.0;
                break;
            case stop_processing:
                return false;
        }

        // Check if we've been notified of more work to do - if not then sleep;
        // if so then wakeup and re-run the task.
        // Note: The order of the wakeUp / snooze here is *critical* - another
        // thread may concurrently notify us (set processorNotification=true)
        // while we are performing the checks, so we need to ensure we don't
        // loose a wakeup as that would result in this Task sleeping forever
        // (and DCP hanging).
        // To prevent this, we perform an initial check of notifiedProcessor(),
        // which if false we initially sleep, and then check a second time.
        // We could race if the other actor sets processorNotification=true
        // between the second `if(consumer->notifiedProcessor)` and us calling
        // `wakeUp()`; but that's essentially a benign race as it will just
        // result in wakeUp() being called twice which is benign.
        if (consumer->notifiedProcessor(false)) {
            wakeUp();
            state = more_to_process;
        } else {
            snooze(sleepFor);
            // Check if the processor was notified again,
            // in which case the task should wake immediately.
            if (consumer->notifiedProcessor(false)) {
                wakeUp();
                state = more_to_process;
            }
        }

        consumer->setProcessorTaskState(state);

        return true;
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This should be a very fast operation (p50 under 10us), however we
        // have observed long tails: p99.9 of 20ms; so use a threshold of 100ms.
        return std::chrono::milliseconds(100);
    }

private:
    /* we have one task per consumer. the task only needs a reference to the
       consumer object and does not own it. Hence std::weak_ptr should be used*/
    const std::weak_ptr<DcpConsumer> consumerPtr;
    const std::string description;
};

DcpConsumer::DcpConsumer(EventuallyPersistentEngine& engine,
                         CookieIface* cookie,
                         const std::string& name,
                         std::string consumerName_)
    : ConnHandler(engine, cookie, name),
      lastMessageTime(ep_uptime_now()),
      opaqueCounter(0),
      processorTaskId(0),
      processorTaskState(all_processed),
      bufferedVBQueue(engine.getConfiguration().getMaxVbuckets()),
      processorNotification(false),
      backoffs(0),
      dcpNoopTxInterval(engine.getConfiguration().getDcpNoopTxInterval()),
      pendingSendStreamEndOnClientStreamClose(true),
      consumerName(std::move(consumerName_)),
      processorTaskRunning(false),
      flowControl(engine, *this) {
    Configuration& config = engine.getConfiguration();
    setSupportAck(false);
    setLogHeader("DCP (Consumer) " + getName() + " -");

    pendingEnableNoop = config.isDcpEnableNoop();
    getErrorMapState = pendingEnableNoop ? GetErrorMapState::PendingRequest
                                         : GetErrorMapState::Skip;
    noopIntervalNegotiation.state =
            config.isDcpEnableNoop()
                    ? NoopIntervalNegotiation::State::PendingMillisecondsRequest
                    : NoopIntervalNegotiation::State::Completed;
    pendingSetPriority = true;
    pendingSupportCursorDropping = true;
    pendingSupportHifiMFU = true;
    pendingEnableExpiryOpcode = true;

    // If a consumer_name was provided then tell the producer about it. Having
    // a consumer name determines if we should support SyncReplication. If we
    // have not yet received a consumer name then the cluster is in a mixed mode
    // state and ns_server will not have set the topology on any producer nodes.
    // We should NOT attempt to enable SyncReplication if this is the case. When
    // the cluster is fully upgraded to MadHatter+, ns_server will tear down DCP
    // connections and recreate them with the consumer name.
    pendingSendConsumerName = !consumerName.empty();
    syncReplNegotiation.state =
            pendingSendConsumerName
                    ? BlockingDcpControlNegotiation::State::PendingRequest
                    : BlockingDcpControlNegotiation::State::Completed;

    // Consumer needs to know if the Producer supports IncludeDeletedUserXattrs
    deletedUserXattrsNegotiation.state =
            BlockingDcpControlNegotiation::State::PendingRequest;

    // Consumer need to know if the Producer supports v7 DCP status codes
    v7DcpStatusCodesNegotiation.state =
            BlockingDcpControlNegotiation::State::PendingRequest;

    // Consumer unconditionally will attempt to negotiate "FlatBuffers"
    // SystemEvents using DCP Control.
    flatBuffersNegotiation.state =
            BlockingDcpControlNegotiation::State::PendingRequest;

    allowSanitizeValueInDeletion.store(config.isAllowSanitizeValueInDeletion());

    // Enable ChangeStreams on this connection
    changeStreamsNegotiation.state =
            BlockingDcpControlNegotiation::State::PendingRequest;
}

DcpConsumer::~DcpConsumer() {
    // Log runtime / pause information when we destruct.
    const auto now = ep_uptime_now();
    logger->info(
            "Destroying connection. Created {} ago. Last message received "
            "{} ago. {}",
            (now - created),
            (now - lastMessageTime),
            getPausedDetailsDescription());

    cancelTask();
}


void DcpConsumer::cancelTask() {
    bool exp = true;
    if (processorTaskRunning.compare_exchange_strong(exp, false)) {
        ExecutorPool::get()->cancel(processorTaskId);
    }
}

void DcpConsumer::taskCancelled() {
    processorTaskRunning.store(false);
}

std::shared_ptr<PassiveStream> DcpConsumer::makePassiveStream(
        EventuallyPersistentEngine& e,
        std::shared_ptr<DcpConsumer> consumer,
        const std::string& name,
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        Vbid vb,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vb_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        uint64_t vb_high_seqno,
        const Collections::ManifestUid vb_manifest_uid) {
    return std::make_shared<PassiveStream>(&e,
                                           consumer,
                                           name,
                                           flags,
                                           opaque,
                                           vb,
                                           start_seqno,
                                           end_seqno,
                                           vb_uuid,
                                           snap_start_seqno,
                                           snap_end_seqno,
                                           vb_high_seqno,
                                           vb_manifest_uid);
}

cb::engine_errc DcpConsumer::addStream(uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpAddStreamFlag flags) {
    TRACE_EVENT2("DcpConsumer",
                 "addStream",
                 "vbid",
                 vbucket.get(),
                 "flags",
                 static_cast<uint32_t>(flags));

    lastMessageTime = ep_uptime_now();
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) Add stream failed because this vbucket doesn't exist",
                vbucket);
        return cb::engine_errc::not_my_vbucket;
    }

    if (vb->getState() == vbucket_state_active) {
        logger->warn(
                "({}) Add stream failed because this vbucket happens to "
                "be in active state",
                vbucket);
        return cb::engine_errc::not_my_vbucket;
    }

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.getEnd() == info.start) {
        info.range.setStart(info.start);
    }

    uint32_t new_opaque = ++opaqueCounter;
    failover_entry_t entry = vb->failovers->getLatestEntry();
    uint64_t start_seqno = info.start;
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = entry.vb_uuid;
    uint64_t snap_start_seqno = info.range.getStart();
    uint64_t snap_end_seqno = info.range.getEnd();
    uint64_t high_seqno = vb->getHighSeqno();
    const Collections::ManifestUid vb_manifest_uid =
            vb->lockCollections().getManifestUid();

    auto stream = findStream(vbucket);
    if (stream) {
        if(stream->isActive()) {
            logger->warn("({}) Cannot add stream because one already exists",
                         vbucket);
            return cb::engine_errc::key_already_exists;
        }
        removeStream(vbucket);
    }

    /* We need 'Processor' task only when we have a stream. Hence create it
     only once when the first stream is added */
    bool exp = false;
    if (processorTaskRunning.compare_exchange_strong(exp, true)) {
        ExTask task = std::make_shared<DcpConsumerTask>(
                engine_, shared_from_this(), 1);
        processorTaskId = ExecutorPool::get()->schedule(task);
    }

    stream = makePassiveStream(engine_,
                               shared_from_this(),
                               getName(),
                               flags,
                               new_opaque,
                               vbucket,
                               start_seqno,
                               end_seqno,
                               vbucket_uuid,
                               snap_start_seqno,
                               snap_end_seqno,
                               high_seqno,
                               vb_manifest_uid);
    registerStream(stream);
    readyStreamsVBQueue.lock()->push_back(vbucket);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);
    pendingAddStream = false;

    return cb::engine_errc::success;
}

cb::engine_errc DcpConsumer::closeStream(uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    TRACE_EVENT2("DcpConsumer",
                 "closeStream",
                 "opaque",
                 opaque,
                 "vbid",
                 vbucket.get());

    lastMessageTime = ep_uptime_now();
    if (doDisconnect()) {
        removeStream(vbucket);
        return cb::engine_errc::disconnect;
    }

    auto oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        opaqueMap_.erase(oitr);
    }

    auto stream = findStream(vbucket);
    if (!stream) {
        logger->warn(
                "({}) Cannot close stream because no "
                "stream exists for this vbucket",
                vbucket);
        return getNoStreamFoundErrorCode();
    }

    stream->setDead(cb::mcbp::DcpStreamEndStatus::Closed);

    return cb::engine_errc::success;
}

cb::engine_errc DcpConsumer::streamEnd(uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    TRACE_EVENT2("DcpConsumer",
                 "streamEnd",
                 "vbid",
                 vbucket.get(),
                 "status",
                 uint32_t(status));

    lastMessageTime = ep_uptime_now();
    UpdateFlowControl ufc(*this, StreamEndResponse::baseMsgBytes);

    auto stream = findStream(vbucket);
    if (!stream) {
        logger->warn(
                "({}) End stream received with opaque:{} but no such stream "
                "for this "
                "vBucket",
                vbucket,
                opaque);
        return getNoStreamFoundErrorCode();
    }

    if (stream->getOpaque() != opaque) {
        // MB-34951: By the time the DcpConsumer receives the StreamEnd from
        // the DcpProducer it is possible that ns_server has already started
        // a new Stream (with updated opaque) for this vbucket.
        // In which case just ignore this StreamEnd message, returning SUCCESS.
        logger->info(
                "({}) End stream received with opaque {} but current opaque "
                "for that vb is {} - ignoring",
                vbucket,
                opaque,
                stream->getOpaque());
        return cb::engine_errc::success;
    }

    logger->info("({}) End stream received with reason {}",
                 vbucket,
                 cb::mcbp::to_string(status));

    auto msg = std::make_unique<StreamEndResponse>(
            opaque, status, vbucket, cb::mcbp::DcpStreamId{});
    auto res = lookupStreamAndDispatchMessage(
            ufc, vbucket, opaque, std::move(msg));

    if (res == cb::engine_errc::success) {
        // Stream End message successfully passed to stream. Can now remove
        // the stream from the streams map as it has completed its lifetime.
        removeStream(vbucket);
    }

    return res;
}

cb::engine_errc DcpConsumer::processMutationOrPrepare(
        Vbid vbucket,
        uint32_t opaque,
        const DocKeyView& key,
        queued_item item,
        cb::const_byte_buffer meta,
        size_t msgBytes) {
    UpdateFlowControl ufc(*this, msgBytes);

    std::unique_ptr<ExtendedMetaData> emd;
    if (!meta.empty()) {
        emd = std::make_unique<ExtendedMetaData>(meta.data(), meta.size());
        if (emd->getStatus() == cb::engine_errc::invalid_arguments) {
            return cb::engine_errc::invalid_arguments;
        }
    }

    auto msg = std::make_unique<MutationConsumerMessage>(
            std::move(item),
            opaque,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeleteTime::No,
            IncludeDeletedUserXattrs::Yes,
            key.getEncoding(),
            emd.release(),
            cb::mcbp::DcpStreamId{});
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

cb::engine_errc DcpConsumer::mutation(uint32_t opaque,
                                      const DocKeyView& key,
                                      cb::const_byte_buffer value,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint32_t flags,
                                      uint64_t bySeqno,
                                      uint64_t revSeqno,
                                      uint32_t exptime,
                                      uint32_t lock_time,
                                      cb::const_byte_buffer meta,
                                      uint8_t nru) {
    lastMessageTime = ep_uptime_now();

    if (bySeqno == 0) {
        logger->warn("({}) Invalid sequence number(0) for mutation!", vbucket);
        return cb::engine_errc::invalid_arguments;
    }

    queued_item item(new Item(key,
                              flags,
                              exptime,
                              value.data(),
                              value.size(),
                              datatype,
                              cas,
                              bySeqno,
                              vbucket,
                              revSeqno,
                              nru /*freqCounter */));

    return processMutationOrPrepare(vbucket,
                                    opaque,
                                    key,
                                    std::move(item),
                                    meta,
                                    MutationResponse::mutationBaseMsgBytes +
                                            key.size() + meta.size() +
                                            value.size());
}

cb::engine_errc DcpConsumer::deletion(uint32_t opaque,
                                      const DocKeyView& key,
                                      cb::const_byte_buffer value,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint64_t bySeqno,
                                      uint64_t revSeqno,
                                      cb::const_byte_buffer meta) {
    return toMainDeletion(DeleteType::Deletion,
                          opaque,
                          key,
                          value,
                          datatype,
                          cas,
                          vbucket,
                          bySeqno,
                          revSeqno,
                          meta,
                          0);
}

cb::engine_errc DcpConsumer::deletionV2(uint32_t opaque,
                                        const DocKeyView& key,
                                        cb::const_byte_buffer value,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t bySeqno,
                                        uint64_t revSeqno,
                                        uint32_t deleteTime) {
    return toMainDeletion(DeleteType::DeletionV2,
                          opaque,
                          key,
                          value,
                          datatype,
                          cas,
                          vbucket,
                          bySeqno,
                          revSeqno,
                          {},
                          deleteTime);
}

cb::engine_errc DcpConsumer::deletion(uint32_t opaque,
                                      const DocKeyView& key,
                                      cb::const_byte_buffer value,
                                      uint8_t datatype,
                                      uint64_t cas,
                                      Vbid vbucket,
                                      uint64_t bySeqno,
                                      uint64_t revSeqno,
                                      cb::const_byte_buffer meta,
                                      uint32_t deleteTime,
                                      IncludeDeleteTime includeDeleteTime,
                                      DeleteSource deletionCause,
                                      UpdateFlowControl& ufc) {
    lastMessageTime = ep_uptime_now();

    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    if (bySeqno == 0) {
        logger->warn("({}) Invalid sequence number(0) for deletion!", vbucket);
        return cb::engine_errc::invalid_arguments;
    }

    auto stream = findStream(vbucket);
    if (!stream) {
        return getNoStreamFoundErrorCode();
    }

    if (stream->getOpaque() != opaque) {
        return getOpaqueMissMatchErrorCode();
    }

    queued_item item(Item::makeDeletedItem(deletionCause,
                                           key,
                                           0,
                                           deleteTime,
                                           value.data(),
                                           value.size(),
                                           datatype,
                                           cas,
                                           bySeqno,
                                           vbucket,
                                           revSeqno));

    if (includeDeletedUserXattrs == IncludeDeletedUserXattrs::No) {
        // Case pre-6.6 connection: Body and UserXattrs are invalid in deletion

        // MB-29040: Producer may send deleted doc with value that still has
        // the user xattrs and the body. Fix up that mistake by running the
        // expiry hook which will correctly process the document
        if (!value.empty()) {
            if (cb::mcbp::datatype::is_xattr(datatype)) {
                auto vb = engine_.getVBucket(vbucket);
                if (vb) {
                    engine_.getKVBucket()->runPreExpiryHook(*vb, *item);
                }
            } else {
                // MB-31141: Deletes cannot have a value
                item->replaceValue(TaggedPtr<Blob>(Blob::New(0),
                                                   TaggedPtrBase::NoTagValue));
                item->setDataType(PROTOCOL_BINARY_RAW_BYTES);
            }
        }
    } else {
        // Case 6.6 connection: UserXattrs in deletion is legal since MB-37374,
        // Body still invalid.

        if (cb::xattr::get_body_size(
                    datatype,
                    {reinterpret_cast<const char*>(value.data()),
                     value.size()}) > 0) {
            // MB-43205 shows that we cannot unconditionally fail here as 6.6
            // connections may still see Body in deletions (generated by pre-6.6
            // nodes) after an offline upgrade.
            // Note: (allowSanitizeValueInDeletion = true) by default, disabling
            // the sanitizer is left only for test purpose.

            if (!allowSanitizeValueInDeletion) {
                logger->error(
                        "DcpConsumer::deletion: ({}) Value cannot contain a "
                        "body",
                        vbucket);
                return cb::engine_errc::invalid_arguments;
            }

            item->removeBody();
        }
    }

    cb::engine_errc err;
    std::unique_ptr<ExtendedMetaData> emd;
    if (!meta.empty()) {
        emd = std::make_unique<ExtendedMetaData>(meta.data(), meta.size());
        if (emd->getStatus() == cb::engine_errc::invalid_arguments) {
            err = cb::engine_errc::invalid_arguments;
        }
    }

    try {
        err = stream->messageReceived(std::make_unique<MutationConsumerMessage>(
                                              item,
                                              opaque,
                                              IncludeValue::Yes,
                                              IncludeXattrs::Yes,
                                              includeDeleteTime,
                                              IncludeDeletedUserXattrs::Yes,
                                              key.getEncoding(),
                                              emd.release(),
                                              cb::mcbp::DcpStreamId{}),
                                      ufc);
    } catch (const std::bad_alloc&) {
        err = cb::engine_errc::no_memory;
    }

    // The item was buffered and will be processed later
    if (err == cb::engine_errc::temporary_failure) {
        notifyVbucketReady(vbucket);
    }

    return err;
}

cb::engine_errc DcpConsumer::expiration(uint32_t opaque,
                                        const DocKeyView& key,
                                        cb::const_byte_buffer value,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t bySeqno,
                                        uint64_t revSeqno,
                                        uint32_t deleteTime) {
    return toMainDeletion(DeleteType::Expiration,
                          opaque,
                          key,
                          value,
                          datatype,
                          cas,
                          vbucket,
                          bySeqno,
                          revSeqno,
                          {},
                          deleteTime);
}

cb::engine_errc DcpConsumer::toMainDeletion(DeleteType origin,
                                            uint32_t opaque,
                                            const DocKeyView& key,
                                            cb::const_byte_buffer value,
                                            uint8_t datatype,
                                            uint64_t cas,
                                            Vbid vbucket,
                                            uint64_t bySeqno,
                                            uint64_t revSeqno,
                                            cb::const_byte_buffer meta,
                                            uint32_t deleteTime) {
    IncludeDeleteTime includeDeleteTime;
    uint32_t bytes = 0;
    DeleteSource deleteSource;
    switch (origin) {
    case DeleteType::Deletion: {
        includeDeleteTime = IncludeDeleteTime::No;
        deleteTime = 0;
        deleteSource = DeleteSource::Explicit;
        bytes = MutationResponse::deletionBaseMsgBytes + key.size() +
                meta.size() + value.size();
        break;
    }
    case DeleteType::DeletionV2: {
        meta = {};
        includeDeleteTime = IncludeDeleteTime::Yes;
        deleteSource = DeleteSource::Explicit;
        bytes = MutationResponse::deletionV2BaseMsgBytes + key.size() +
                value.size();
        break;
    }
    case DeleteType::Expiration: {
        meta = {};
        includeDeleteTime = IncludeDeleteTime::Yes;
        deleteSource = DeleteSource::TTL;
        bytes = MutationResponse::expirationBaseMsgBytes + key.size() +
                value.size();
        break;
    }
    }
    if (bytes == 0) {
        throw std::logic_error(std::string("DcpConsumer::toMainDeletion: ") +
                               logHeader() +
                               " is using an unexpected deletion type, as bytes"
                               " is uninitialized!");
    }

    UpdateFlowControl ufc(*this, bytes);
    auto err = deletion(opaque,
                        key,
                        value,
                        datatype,
                        cas,
                        vbucket,
                        bySeqno,
                        revSeqno,
                        meta,
                        deleteTime,
                        includeDeleteTime,
                        deleteSource,
                        ufc);

    // TMPFAIL means the stream has buffered the message for later processing
    // so skip flowControl, success or any other error, we still need to ack
    if (err == cb::engine_errc::temporary_failure) {
        // Mask the TMPFAIL
        return cb::engine_errc::success;
    }

    return err;
}

cb::engine_errc DcpConsumer::snapshotMarker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> max_visible_seqno) {
    lastMessageTime = ep_uptime_now();
    uint32_t bytes = SnapshotMarker::baseMsgBytes;
    if (high_completed_seqno || max_visible_seqno) {
        bytes += sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                 sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);
    } else {
        bytes += sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload);
    }
    UpdateFlowControl ufc(*this, bytes);

    if (start_seqno > end_seqno) {
        logger->warn(
                "({}) Invalid snapshot marker "
                "received, snap_start ({}) <= snap_end ({})",
                vbucket,
                start_seqno,
                end_seqno);
        return cb::engine_errc::invalid_arguments;
    }

    auto msg = std::make_unique<SnapshotMarker>(opaque,
                                                vbucket,
                                                start_seqno,
                                                end_seqno,
                                                flags,
                                                high_completed_seqno,
                                                max_visible_seqno,
                                                std::nullopt,
                                                cb::mcbp::DcpStreamId{});
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

cb::engine_errc DcpConsumer::noop(uint32_t opaque) {
    lastMessageTime = ep_uptime_now();
    return cb::engine_errc::success;
}

cb::engine_errc DcpConsumer::setVBucketState(uint32_t opaque,
                                             Vbid vbucket,
                                             vbucket_state_t state) {
    TRACE_EVENT2("DcpConsumer",
                 "setVBucketState",
                 "vbid",
                 vbucket.get(),
                 "state",
                 int(state));

    lastMessageTime = ep_uptime_now();
    UpdateFlowControl ufc(*this, SetVBucketState::baseMsgBytes);

    auto msg = std::make_unique<SetVBucketState>(opaque, vbucket, state);
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

cb::engine_errc DcpConsumer::step(bool throttled,
                                  DcpMessageProducersIface& producers) {
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    if (pendingAddStream) {
        return cb::engine_errc::would_block;
    }

    cb::engine_errc ret;
    if ((ret = flowControl.handleFlowCtl(producers)) !=
        cb::engine_errc::failed) {
        return ret;
    }

    // MB-29441: Send a GetErrorMap to the producer to determine if it
    // is a pre-5.0.0 node. The consumer will set the producer's noop-interval
    // accordingly in 'handleNoop()', so 'handleGetErrorMap()' *must* execute
    // before 'handleNoop()'.
    // Note: We only support mixed-mode cluster one major version apart, so
    // as of 7.x we don't support communicating with v5.x; but we still perform
    // detection of v5 so we can at least report a clean error to the user.
    if ((ret = handleGetErrorMap(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = handleNoop(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = handlePriority(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = supportCursorDropping(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = supportHifiMFU(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = sendStreamEndOnClientStreamClose(producers)) !=
        cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = enableExpiryOpcode(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = enableSynchronousReplication(producers)) !=
        cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = handleDeletedUserXattrs(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = enableV7DcpStatus(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = enableFlatBuffersSystemEvents(producers)) !=
        cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = handleChangeStreams(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    auto resp = getNextItem();
    if (resp == nullptr) {
        return cb::engine_errc::would_block;
    }

    switch (resp->getEvent()) {
        case DcpResponse::Event::AddStream:
        {
            auto* as = static_cast<AddStreamResponse*>(resp.get());
            ret = producers.add_stream_rsp(
                    as->getOpaque(), as->getStreamOpaque(), as->getStatus());
            break;
        }
        case DcpResponse::Event::StreamReq:
        {
            auto* sr = static_cast<StreamRequest*>(resp.get());
            ret = producers.stream_req(sr->getOpaque(),
                                       sr->getVBucket(),
                                       sr->getFlags(),
                                       sr->getStartSeqno(),
                                       sr->getEndSeqno(),
                                       sr->getVBucketUUID(),
                                       sr->getSnapStartSeqno(),
                                       sr->getSnapEndSeqno(),
                                       sr->getRequestValue());
            break;
        }
        case DcpResponse::Event::SetVbucket:
        {
            auto* vs =
                    static_cast<SetVBucketStateResponse*>(resp.get());
            ret = producers.set_vbucket_state_rsp(vs->getOpaque(),
                                                  vs->getStatus());
            break;
        }
        case DcpResponse::Event::SnapshotMarker: {
            auto* mr =
                    static_cast<SnapshotMarkerResponse*>(resp.get());
            ret = producers.marker_rsp(mr->getOpaque(), mr->getStatus());
            break;
        }
        case DcpResponse::Event::SeqnoAcknowledgement: {
            auto* ack = static_cast<SeqnoAcknowledgement*>(resp.get());
            ret = producers.seqno_acknowledged(ack->getOpaque(),
                                               ack->getVbucket(),
                                               ack->getPreparedSeqno());
            break;
        }
        default:
            logger->warn("Unknown consumer event ({}), disconnecting",
                         int(resp->getEvent()));
            ret = cb::engine_errc::disconnect;
    }

    return ret;
}

bool RollbackTask::run() {
    TRACE_EVENT0("ep-engine/task", "RollbackTask");
    if (cons->doDisconnect()) {
        return false;
    }
    if (cons->doRollback(opaque, vbid, rollbackSeqno)) {
        return true;
    }
    ++(engine->getEpStats().rollbackCount);
    return false;
}

bool DcpConsumer::handleResponse(const cb::mcbp::Response& response) {
    if (doDisconnect()) {
        return false;
    }

    const auto opcode = response.getClientOpcode();
    const auto opaque = response.getOpaque();

    logger->debug("handleResponse(): opcode:{}, opqaue:{}, status:{}",
                  opcode,
                  opaque,
                  response.getStatus());

    if (opcode == cb::mcbp::ClientOpcode::DcpStreamReq) {
        auto oitr = opaqueMap_.find(opaque);
        if (oitr == opaqueMap_.end()) {
            EP_LOG_WARN(
                    "Received response with opaque {} and that opaque "
                    "does not exist in opaqueMap",
                    opaque);
            return false;
        }
        if (!isValidOpaque(opaque, oitr->second.second)) {
            EP_LOG_WARN(
                    "Received response with opaque {} and that stream does not "
                    "exist for {}",
                    opaque,
                    Vbid(oitr->second.second));
            return false;
        }

        Vbid vbid = oitr->second.second;
        const auto status = response.getStatus();
        const auto value = response.getValue();

        TRACE_EVENT2("DcpConsumer",
                     "dcp_stream_req response",
                     "opaque",
                     opaque,
                     "status",
                     uint16_t(status));

        if (status == cb::mcbp::Status::Rollback) {
            if (value.size() != sizeof(uint64_t)) {
                logger->warn(
                        "({}) Received rollback "
                        "request with incorrect bodylen of {}, disconnecting",
                        vbid,
                        value.size());
                return false;
            }
            uint64_t rollbackSeqno = 0;
            memcpy(&rollbackSeqno, value.data(), sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);
            return handleRollbackResponse(vbid, opaque, rollbackSeqno);
        }

        if (((value.size() % 16) != 0 || value.empty()) &&
            status == cb::mcbp::Status::Success) {
            logger->warn(
                    "({})Got a stream response with a "
                    "bad failover log (length {}), disconnecting",
                    vbid,
                    value.size());
            return false;
        }

        streamAccepted(opaque, status, value.data(), value.size());
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::DcpBufferAcknowledgement) {
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::DcpControl) {
        // The Consumer-Producer negotiation for Sync Replication, deleted user
        // xattrs and v7 DCP status codes happens over DCP_CONTROL and
        // introduces a blocking step. The blocking DCP_CONTROL request is
        // signed at Consumer by tracking the opaque value sent to the Producer,
        // so here we can identify it and complete the negotiation. Note that a
        // pre-6.5 Producer sends EINVAL as it does not recognize the Sync
        // Replication negotiation-key.
        const auto opaque = response.getOpaque();
        if (opaque == syncReplNegotiation.opaque) {
            syncReplNegotiation.state =
                    BlockingDcpControlNegotiation::State::Completed;
            if (response.getStatus() == cb::mcbp::Status::Success) {
                supportsSyncReplication.store(SyncReplication::SyncReplication);
            }
        } else if (opaque == deletedUserXattrsNegotiation.opaque) {
            deletedUserXattrsNegotiation.state =
                    BlockingDcpControlNegotiation::State::Completed;
            includeDeletedUserXattrs =
                    (response.getStatus() == cb::mcbp::Status::Success
                             ? IncludeDeletedUserXattrs::Yes
                             : IncludeDeletedUserXattrs::No);
        } else if (opaque == v7DcpStatusCodesNegotiation.opaque) {
            v7DcpStatusCodesNegotiation.state =
                    BlockingDcpControlNegotiation::State::Completed;
            isV7DcpStatusEnabled =
                    response.getStatus() == cb::mcbp::Status::Success;
        } else if (response.getOpaque() == flatBuffersNegotiation.opaque) {
            flatBuffersNegotiation.state =
                    BlockingDcpControlNegotiation::State::Completed;
            flatBuffersSystemEventsEnabled =
                    response.getStatus() == cb::mcbp::Status::Success;
        } else if (opaque == changeStreamsNegotiation.opaque) {
            changeStreamsNegotiation.state =
                    BlockingDcpControlNegotiation::State::Completed;
            changeStreams = (response.getStatus() == cb::mcbp::Status::Success);
        } else if (opaque == noopIntervalNegotiation.opaque) {
            using State = NoopIntervalNegotiation::State;
            switch (noopIntervalNegotiation.state) {
            case State::PendingMillisecondsResponse:
                // Producer accepted the value encoded as milliseconds
                // (i.e. v7.6+), negotiation complete, otherwise fallback and
                // attempt integer seconds request.
                noopIntervalNegotiation.state =
                        (response.getStatus() == cb::mcbp::Status::Success)
                                ? State::Completed
                                : State::PendingSecondsRequest;
                break;
            case State::PendingSecondsResponse:
                if (response.getStatus() != cb::mcbp::Status::Success) {
                    logger->error(
                            "Got non-success status {} for "
                            "DcpControl(\"set_noop_interval\") - "
                            "disconnecting",
                            noopIntervalNegotiation.state);
                    return false;
                }
                noopIntervalNegotiation.state = State::Completed;
                break;
            default:
                logger->error(
                        "Unexpected noopIntervalNegotiation state:{} when "
                        "handling DcpControl response - disconnecting",
                        noopIntervalNegotiation.state);
                return false;
            }
        }
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::GetErrorMap) {
        auto status = response.getStatus();
        // GetErrorMap is supported on versions >= 5.0.0.
        // "Unknown Command" is returned on pre-5.0.0 versions.
        // We only support mixed-mode (online upgrade) for the previous major
        // - which of of writing is 6.x - so 5.x is no longer supported. However,
        // there isn't a simple way to detect 5.x - only less than 5 - so for
        // now we are slightly more permissive and still allow 5.x, rejecting
        // 4.x and lower.
        auto producerIsVersion5orHigher =
                status != cb::mcbp::Status::UnknownCommand;
        if (!producerIsVersion5orHigher) {
            logger->error(
                    "Incompatible Producer node version detected - this "
                    "version of CB Server requires version 6 or higher - "
                    "disconnecting. (Producer responded with {} to GetErrorMap "
                    "request indicating version <5.0.0)",
                    status);
            return false;
        }
        getErrorMapState = GetErrorMapState::Skip;
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged) {
        // Seqno ack might respond in a non-success case if the vBucket has gone
        // away on the producer. We don't really care if this happens, the
        // stream has probably already gone away, but we don't want to take down
        // the connection (return false) as it might cause a rebalance to fail.
        return true;
    }

    logger->warn("Trying to handle an unknown response {}, disconnecting",
                 opcode);

    return false;
}

bool DcpConsumer::handleRollbackResponse(Vbid vbid,
                                         uint32_t opaque,
                                         uint64_t rollbackSeqno) {
    auto vb = engine_.getVBucket(vbid);
    auto stream = findStream(vbid);

    if (!(vb && stream)) {
        logger->warn("({}) handleRollbackResponse: {}, stream:{}",
                     vbid,
                     vb.get() ? "ok" : "nullptr",
                     stream.get() ? "ok" : "nullptr");
        return false;
    }

    // Can we avoid rolling back to zero?
    // If another failover entry is available, confirm that the upper bound
    // matches the start seqno of the request, if so we can try that uuid.
    if (rollbackSeqno == 0 && vb->failovers->getNumEntries() > 1) {
        // Get the seqno from the latest entry, this is the 'upper' bound of the
        // next entry. If the seqno equals the stream start point we can use it
        auto entry = vb->failovers->getLatestEntry();
        if (entry.by_seqno == stream->getStartSeqno()) {
            vb->failovers->removeLatestEntry();
            entry = vb->failovers->getLatestEntry();
            logger->info(
                    "({}) Received rollback request. Rollback to 0, have {} "
                    "entries remaining. Retrying with previous failover "
                    "vb_uuid:{}",
                    vbid,
                    vb->failovers->getNumEntries(),
                    entry.vb_uuid);

            stream->streamRequest(entry.vb_uuid);
            return true;
        }
        logger->info(
                "({}) Cannot avoid rollback to 0, vb_uuid:{} cannot be used"
                " as entry.by_seqno:{} does not match stream "
                "start_seqno:{}",
                vbid,
                entry.vb_uuid,
                entry.by_seqno,
                stream->getStartSeqno());
    }

    logger->info("({}) Received rollback request. Rolling back to seqno:{}",
                 vbid,
                 rollbackSeqno);
    ExTask task = std::make_shared<RollbackTask>(
            engine_, opaque, vbid, rollbackSeqno, shared_from_this());
    ExecutorPool::get()->schedule(task);
    return true;
}

bool DcpConsumer::doRollback(uint32_t opaque,
                             Vbid vbid,
                             uint64_t rollbackSeqno) {
    TaskStatus status = engine_.getKVBucket()->rollback(vbid, rollbackSeqno);

    switch (status) {
    case TaskStatus::Reschedule:
        return true; // Reschedule the rollback.
    case TaskStatus::Abort:
        logger->warn("{} Rollback failed on the vbucket", vbid);
        break;
    case TaskStatus::Complete: {
        VBucketPtr vb = engine_.getVBucket(vbid);
        if (!vb) {
            logger->warn(
                    "{} Aborting rollback task as the vbucket was"
                    " deleted after rollback",
                    vbid);
            break;
        }
        auto stream = findStream(vbid);
        if (stream) {
            stream->reconnectStream(vb, opaque, vb->getHighSeqno());
        }
        break;
    }
    }
    return false; // Do not reschedule the rollback
}

void DcpConsumer::seqnoAckStream(Vbid vbid, int64_t seqno) {
    auto stream = findStream(vbid);
    if (!stream) {
        logger->warn("{} Could not ack seqno {} because stream was not found",
                     vbid,
                     seqno);
        return;
    }
    stream->seqnoAck(seqno);
}

void DcpConsumer::addStats(const AddStatFn& add_stat, CookieIface& c) {
    ConnHandler::addStats(add_stat, c);

    addStat("total_backoffs", backoffs, add_stat, c);
    addStat("processor_task_state", getProcessorTaskStatusStr(), add_stat, c);
    flowControl.addStats(add_stat, c);

    bufferedVBQueue.addStats(
            getName() + ":dcp_buffered_ready_queue_", add_stat, c);
    addStat("processor_notification",
            processorNotification.load(),
            add_stat,
            c);

    addStat("synchronous_replication", isSyncReplicationEnabled(), add_stat, c);
}

void DcpConsumer::addStreamStats(const AddStatFn& add_stat,
                                 CookieIface& c,
                                 StreamStatsFormat format) {
    // Make a copy of all valid streams (under lock), and then call addStats
    // for each one. (Done in two stages to minmise how long we have the
    // streams map locked for).
    std::vector<std::shared_ptr<Stream>> valid_streams;

    streams.for_each(
            [&valid_streams](const PassiveStreamMap::value_type& element) {
                if (element.second->isActive()) {
                    valid_streams.push_back(element.second);
                }
            });

    if (format == StreamStatsFormat::Json) {
        doStreamStatsJson(valid_streams, add_stat, c);
    } else {
        doStreamStatsLegacy(valid_streams, add_stat, c);
    }
}

void DcpConsumer::aggregateQueueStats(ConnCounter& aggregator) const {
    aggregator.conn_passiveStreams += streams.size();
    aggregator.conn_queueBackoff += backoffs;
}

ProcessUnackedBytesResult DcpConsumer::processUnackedBytes() {
    std::shared_ptr<PassiveStream> stream;
    Vbid vbid;
    // Note: The order of conditions here matters. In the opposite order, when
    // a non-null stream is found we would exit the loop *only after popping an
    // extra vbucket entry from bufferedVBQueue*. That vbucket entry would be
    // lost and its unacked-bytes never processed.
    while (!stream && bufferedVBQueue.popFront(vbid)) {
        stream = findStream(vbid);
    }
    if (!stream) {
        return all_processed;
    }

    switch (engine_.getKVBucket()->getReplicationThrottleStatus()) {
    case KVBucket::ReplicationThrottleStatus::Pause:
        backoffs++;
        bufferedVBQueue.pushUnique(stream->getVBucket());
        return more_to_process;
    case KVBucket::ReplicationThrottleStatus::Disconnect:
        backoffs++;
        bufferedVBQueue.pushUnique(stream->getVBucket());
        logger->warn(
                "{} Processor task indicating disconnection "
                "as there is no memory to complete replication",
                stream->getVBucket());
        setDisconnect();
        return stop_processing;
    case KVBucket::ReplicationThrottleStatus::Process:
        uint32_t bytesProcessed = 0;
        const auto res = stream->processUnackedBytes(bytesProcessed);
        if (res == more_to_process) {
            backoffs++;
            bufferedVBQueue.pushUnique(stream->getVBucket());
        }
        incrFlowControlFreedBytes(bytesProcessed);
        return bufferedVBQueue.empty() ? all_processed : more_to_process;
    }

    folly::assume_unreachable();
}

void DcpConsumer::notifyVbucketReady(Vbid vbucket) {
    if (bufferedVBQueue.pushUnique(vbucket) && notifiedProcessor(true)) {
        ExecutorPool::get()->wake(processorTaskId);
    }
}

bool DcpConsumer::notifiedProcessor(bool to) {
    bool inverse = !to;
    return processorNotification.compare_exchange_strong(inverse, to);
}

void DcpConsumer::setProcessorTaskState(enum ProcessUnackedBytesResult to) {
    processorTaskState = to;
}

std::string DcpConsumer::getProcessorTaskStatusStr() const {
    switch (processorTaskState.load()) {
        case all_processed:
            return "ALL_PROCESSED";
        case more_to_process:
            return "MORE_TO_PROCESS";
        case stop_processing:
            return "STOP_PROCESSING";
    }
    folly::assume_unreachable();
}

std::unique_ptr<DcpResponse> DcpConsumer::getNextItem() {
    auto locked = readyStreamsVBQueue.lock();

    unPause();
    while (!locked->empty()) {
        Vbid vbucket = locked->front();
        locked->pop_front();

        auto stream = findStream(vbucket);
        if (!stream) {
            continue;
        }

        auto response = stream->next();
        if (!response) {
            continue;
        }
        switch (response->getEvent()) {
        case DcpResponse::Event::StreamReq:
        case DcpResponse::Event::AddStream:
        case DcpResponse::Event::SetVbucket:
        case DcpResponse::Event::SnapshotMarker:
        case DcpResponse::Event::SeqnoAcknowledgement:
            break;
        default:
            throw std::logic_error(
                    std::string("DcpConsumer::getNextItem: ") + logHeader() +
                    " is attempting to write an unexpected event: " +
                    response->to_string());
        }

        locked->push_back(vbucket);
        return response;
    }
    pause(PausedReason::ReadyListEmpty);

    return nullptr;
}

void DcpConsumer::notifyStreamReady(Vbid vbucket) {
    {
        auto locked = readyStreamsVBQueue.lock();
        auto iter = std::find(locked->begin(), locked->end(), vbucket);
        if (iter != locked->end()) {
            return;
        }

        locked->push_back(vbucket);
    }

    scheduleNotify();
}

void DcpConsumer::streamAccepted(uint32_t opaque,
                                 cb::mcbp::Status status,
                                 const uint8_t* body,
                                 uint32_t bodylen) {
    auto oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        Vbid vbucket = oitr->second.second;

        auto stream = findStream(vbucket);
        if (stream && stream->getOpaque() == opaque && stream->isPending()) {
            if (status == cb::mcbp::Status::Success) {
                VBucketPtr vb = engine_.getVBucket(vbucket);
                vb->failovers->replaceFailoverLog(body, bodylen);
                KVBucketIface* kvBucket = engine_.getKVBucket();
                kvBucket->scheduleVBStatePersist(vbucket);
            }
            logger->debug("({}) Add stream for opaque {} with error code {}",
                          vbucket,
                          opaque,
                          status == cb::mcbp::Status::Success ? "succeeded"
                                                              : "failed");
            stream->acceptStream(status, add_opaque);
        } else {
            logger->warn(
                    "({}) Trying to add stream, but "
                    "none exists (opaque: {}, add_opaque: {})",
                    vbucket,
                    opaque,
                    add_opaque);
        }
        opaqueMap_.erase(opaque);
    } else {
        logger->warn(
                "No opaque found for add stream response "
                "with opaque {}",
                opaque);
    }
}

bool DcpConsumer::isValidOpaque(uint32_t opaque, Vbid vbucket) {
    auto stream = findStream(vbucket);
    return stream && stream->getOpaque() == opaque;
}

void DcpConsumer::closeAllStreams() {
    std::vector<Vbid> vbvector;

    {
        // Need to synchronise the disconnect and clear, therefore use
        // external locking here.
        std::lock_guard<PassiveStreamMap> guard(streams);

        streams.for_each(
                [&vbvector](PassiveStreamMap::value_type& iter) {
                    auto* stream = iter.second.get();
                    stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);
                    vbvector.push_back(stream->getVBucket());
                },
                guard);
        streams.clear(guard);
    }

    // We put the ConnHandler in the vbConns "map" for seqno acking so we need
    // to remove them when we close streams.
    for (auto vbid : vbvector) {
        engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbid);
    }
}

void DcpConsumer::closeStreamDueToVbStateChange(Vbid vbucket,
                                                vbucket_state_t state) {
    auto stream = removeStream(vbucket);
    if (stream) {
        logger->debug("({}) State changed to {}, closing passive stream!",
                      vbucket,
                      VBucket::toString(state));
        stream->setDead(cb::mcbp::DcpStreamEndStatus::StateChanged);
    }
}

cb::engine_errc DcpConsumer::handleNoop(DcpMessageProducersIface& producers) {
    if (pendingEnableNoop) {
        cb::engine_errc ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        ret = producers.control(opaque, noopCtrlMsg, val);
        pendingEnableNoop = false;
        return ret;
    }

    using State = NoopIntervalNegotiation::State;
    using namespace std::chrono;

    logger->debug("handleNoop(): state:{}", noopIntervalNegotiation.state);

    switch (noopIntervalNegotiation.state) {
    case State::PendingMillisecondsRequest: {
        // MB-56973: For v7.6+ producers, support sub-second noop-interval
        // encoded as fractional seconds.
        uint32_t opaque = ++opaqueCounter;
        duration<float> interval = dcpNoopTxInterval;
        noopIntervalNegotiation = {State::PendingMillisecondsResponse, opaque};
        return producers.control(
                opaque, noopIntervalCtrlMsg, std::to_string(interval.count()));
    }
    case State::PendingMillisecondsResponse:
        // We have to wait for the response before proceeding.
        return cb::engine_errc::would_block;

    case State::PendingSecondsRequest: {
        // MB-56973: pre 7.6.0 producer, send integer seconds count, at least
        // 1 second - rounding dcpNoopTxInterval to seconds, so we can correctly
        // query the configured value after.
        uint32_t opaque = ++opaqueCounter;
        dcpNoopTxInterval = round<seconds>(dcpNoopTxInterval);
        dcpNoopTxInterval =
                std::max(std::chrono::duration<float>(1), dcpNoopTxInterval);
        seconds interval = duration_cast<seconds>(dcpNoopTxInterval);

        noopIntervalNegotiation = {State::PendingSecondsResponse, opaque};
        return producers.control(
                opaque, noopIntervalCtrlMsg, std::to_string(interval.count()));
    }
    case State::PendingSecondsResponse:
        // We have to wait for the response before proceeding.
        return cb::engine_errc::would_block;

    case State::Completed:
        break;
    }

    const auto now = ep_uptime_now();
    auto dcpIdleTimeout = getIdleTimeout();
    if ((now - lastMessageTime) > dcpIdleTimeout) {
        logger->warn(
                "Disconnecting because a message has not been received for "
                "the DCP idle timeout of {}s. "
                "Received last message (e.g. mutation/noop/StreamEnd) {}s ago. "
                "DCP noop interval is {}s.",
                dcpIdleTimeout.count(),
                (now - lastMessageTime),
                dcpNoopTxInterval.count());
        return cb::engine_errc::disconnect;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::handleGetErrorMap(
        DcpMessageProducersIface& producers) {
    if (getErrorMapState == GetErrorMapState::PendingRequest) {
        cb::engine_errc ret;
        uint32_t opaque = ++opaqueCounter;
        // Note: just send 0 as version to get the default error map loaded
        //     from file at startup. The error map returned is not used, we
        //     just want to issue a valid request.
        ret = producers.get_error_map(opaque, 0 /*version*/);
        getErrorMapState = GetErrorMapState::PendingResponse;
        return ret;
    }

    // We have to wait for the GetErrorMap response before proceeding
    if (getErrorMapState == GetErrorMapState::PendingResponse) {
        return cb::engine_errc::would_block;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::handlePriority(
        DcpMessageProducersIface& producers) {
    if (pendingSetPriority) {
        cb::engine_errc ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("high");
        ret = producers.control(opaque, priorityCtrlMsg, val);
        pendingSetPriority = false;
        return ret;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::supportCursorDropping(
        DcpMessageProducersIface& producers) {
    if (pendingSupportCursorDropping) {
        cb::engine_errc ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        ret = producers.control(opaque, cursorDroppingCtrlMsg, val);
        pendingSupportCursorDropping = false;
        return ret;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::supportHifiMFU(
        DcpMessageProducersIface& producers) {
    if (pendingSupportHifiMFU) {
        cb::engine_errc ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        ret = producers.control(opaque, hifiMFUCtrlMsg, val);
        pendingSupportHifiMFU = false;
        return ret;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::sendStreamEndOnClientStreamClose(
        DcpMessageProducersIface& producers) {
    /* Sending this ctrl message tells the DCP producer that the consumer is
       expecting a "STREAM_END" message when it initiates a stream close */
    if (pendingSendStreamEndOnClientStreamClose) {
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        cb::engine_errc ret = producers.control(
                opaque, sendStreamEndOnClientStreamCloseCtrlMsg, val);
        pendingSendStreamEndOnClientStreamClose = false;
        return ret;
    }
    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::enableExpiryOpcode(
        DcpMessageProducersIface& producers) {
    if (pendingEnableExpiryOpcode) {
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        cb::engine_errc ret =
                producers.control(opaque, enableOpcodeExpiryCtrlMsg, val);
        pendingEnableExpiryOpcode = false;
        return ret;
    }
    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::enableSynchronousReplication(
        DcpMessageProducersIface& producers) {
    // enable_sync_writes and consumer_name are separated into two
    // different variables as in the future non-replication consumers may wish
    // to stream prepares and commits.
    switch (syncReplNegotiation.state) {
    case BlockingDcpControlNegotiation::State::PendingRequest: {
        uint32_t opaque = ++opaqueCounter;
        cb::engine_errc ret =
                producers.control(opaque, "enable_sync_writes", "true");
        syncReplNegotiation.state =
                BlockingDcpControlNegotiation::State::PendingResponse;
        syncReplNegotiation.opaque = opaque;
        return ret;
    }
    case BlockingDcpControlNegotiation::State::PendingResponse:
        // We have to wait for the response before proceeding
        return cb::engine_errc::would_block;
    case BlockingDcpControlNegotiation::State::Completed:
        break;
    }

    if (pendingSendConsumerName && isSyncReplicationEnabled()) {
        uint32_t opaque = ++opaqueCounter;
        NonBucketAllocationGuard guard;
        cb::engine_errc ret =
                producers.control(opaque, "consumer_name", consumerName);
        pendingSendConsumerName = false;
        return ret;
    }

    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::enableV7DcpStatus(
        DcpMessageProducersIface& producers) {
    switch (v7DcpStatusCodesNegotiation.state) {
    case BlockingDcpControlNegotiation::State::PendingRequest: {
        uint32_t opaque = ++opaqueCounter;
        auto ret = producers.control(opaque, "v7_dcp_status_codes", "true");
        v7DcpStatusCodesNegotiation.state =
                BlockingDcpControlNegotiation::State::PendingResponse;
        v7DcpStatusCodesNegotiation.opaque = opaque;
        return ret;
    }
    case BlockingDcpControlNegotiation::State::PendingResponse:
        return cb::engine_errc::would_block;
    case BlockingDcpControlNegotiation::State::Completed:
        break;
    }
    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::enableFlatBuffersSystemEvents(
        DcpMessageProducersIface& producers) {
    switch (flatBuffersNegotiation.state) {
    case BlockingDcpControlNegotiation::State::PendingRequest: {
        uint32_t opaque = ++opaqueCounter;
        auto ret = producers.control(
                opaque, DcpControlKeys::FlatBuffersSystemEvents, "true");
        flatBuffersNegotiation.state =
                BlockingDcpControlNegotiation::State::PendingResponse;
        flatBuffersNegotiation.opaque = opaque;
        return ret;
    }
    case BlockingDcpControlNegotiation::State::PendingResponse:
        return cb::engine_errc::would_block;
    case BlockingDcpControlNegotiation::State::Completed:
        break;
    }
    return cb::engine_errc::failed;
}

cb::engine_errc DcpConsumer::handleDeletedUserXattrs(
        DcpMessageProducersIface& producers) {
    switch (deletedUserXattrsNegotiation.state) {
    case BlockingDcpControlNegotiation::State::PendingRequest: {
        uint32_t opaque = ++opaqueCounter;
        NonBucketAllocationGuard guard;
        // Note: the protocol requires a value in the payload, make it happy
        cb::engine_errc ret = producers.control(
                opaque, "include_deleted_user_xattrs", "true");
        deletedUserXattrsNegotiation.state =
                BlockingDcpControlNegotiation::State::PendingResponse;
        deletedUserXattrsNegotiation.opaque = opaque;
        return ret;
    }
    case BlockingDcpControlNegotiation::State::PendingResponse:
        return cb::engine_errc::would_block;
    case BlockingDcpControlNegotiation::State::Completed:
        return cb::engine_errc::failed;
    }
    folly::assume_unreachable();
}

cb::engine_errc DcpConsumer::handleChangeStreams(
        DcpMessageProducersIface& producers) {
    switch (changeStreamsNegotiation.state) {
    case BlockingDcpControlNegotiation::State::PendingRequest: {
        uint32_t opaque = ++opaqueCounter;
        NonBucketAllocationGuard guard;
        const auto ret = producers.control(
                opaque, DcpControlKeys::ChangeStreams, "true");
        changeStreamsNegotiation.state =
                BlockingDcpControlNegotiation::State::PendingResponse;
        changeStreamsNegotiation.opaque = opaque;
        return ret;
    }
    case BlockingDcpControlNegotiation::State::PendingResponse:
        return cb::engine_errc::would_block;
    case BlockingDcpControlNegotiation::State::Completed:
        return cb::engine_errc::failed;
    }
    folly::assume_unreachable();
}

uint64_t DcpConsumer::incrOpaqueCounter()
{
    return (++opaqueCounter);
}

uint32_t DcpConsumer::getFlowControlBufSize() const {
    return flowControl.getBufferSize();
}

void DcpConsumer::setFlowControlBufSize(uint32_t newSize) {
    flowControl.setBufferSize(newSize);
}

const std::string& DcpConsumer::getControlMsgKey()
{
    return connBufferCtrlMsg;
}

bool DcpConsumer::isStreamPresent(Vbid vbucket) {
    auto stream = findStream(vbucket);
    return stream && stream->isActive();
}

void DcpConsumer::scheduleNotifyIfNecessary() {
    if (flowControl.isBufferSufficientlyDrained()) {
        /**
         * Notify memcached to get flow control buffer ack out.
         * We cannot wait till the ConnManager daemon task notifies
         * the memcached as it would cause delay in buffer ack being
         * sent out to the producer.
         */
        scheduleNotify();
    }
}

std::shared_ptr<PassiveStream> DcpConsumer::findStream(Vbid vbid) {
    auto it = streams.find(vbid);
    if (it.second) {
        return it.first;
    }
    return {};
}

cb::engine_errc DcpConsumer::systemEvent(uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) {
    lastMessageTime = ep_uptime_now();
    UpdateFlowControl ufc(
            *this,
            SystemEventMessage::baseMsgBytes + key.size() + eventData.size());

    auto msg = std::make_unique<SystemEventConsumerMessage>(
            opaque, event, bySeqno, vbucket, version, key, eventData);
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

cb::engine_errc DcpConsumer::prepare(uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint32_t flags,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t expiration,
                                     uint32_t lock_time,
                                     uint8_t nru,
                                     DocumentState document_state,
                                     cb::durability::Level level) {
    lastMessageTime = ep_uptime_now();

    if (by_seqno == 0) {
        logger->warn("({}) Invalid sequence number(0) for prepare!", vbucket);
        return cb::engine_errc::invalid_arguments;
    }

    queued_item item(new Item(key,
                              flags,
                              expiration,
                              value.data(),
                              value.size(),
                              datatype,
                              cas,
                              by_seqno,
                              vbucket,
                              rev_seqno,
                              nru /*freqCounter */));
    using cb::durability::Requirements;
    using cb::durability::Timeout;
    item->setPendingSyncWrite(Requirements{level, Timeout::Infinity()});
    // Any incoming Prepares could have already been make visible by the Active
    // node by the time the replica receives / processes it (assuming the
    // SyncWrite was committed without this node / consumer having to ACK it).
    // As such, always mark as MaybeVisible; so *if* we are later promoted to
    // active this node must (re)commit the Prepare before exposing any
    // value for it.
    item->setPreparedMaybeVisible();
    if (document_state == DocumentState::Deleted) {
        item->setDeleted();

        // MB-37374: From 6.6 a SyncDelete may contain user-xattrs but still no
        // body.
        if (cb::xattr::get_body_size(
                    datatype,
                    {reinterpret_cast<const char*>(value.data()),
                     value.size()}) > 0) {
            if (!allowSanitizeValueInDeletion) {
                logger->error(
                        "DcpConsumer::prepare: ({}) Value cannot contain a "
                        "body "
                        "for SyncDelete",
                        vbucket);
                return cb::engine_errc::invalid_arguments;
            }

            item->removeBody();
        }
    }

    const auto msgBytes =
            MutationResponse::prepareBaseMsgBytes + key.size() + value.size();
    return processMutationOrPrepare(
            vbucket, opaque, key, std::move(item), {}, msgBytes);
}

cb::engine_errc DcpConsumer::lookupStreamAndDispatchMessage(
        UpdateFlowControl& ufc,
        Vbid vbucket,
        uint32_t opaque,
        std::unique_ptr<DcpResponse> msg) {
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    auto stream = findStream(vbucket);
    if (!stream) {
        return getNoStreamFoundErrorCode();
    }
    if (stream->getOpaque() != opaque) {
        return getOpaqueMissMatchErrorCode();
    }

    // Pass the message to the associated stream.
    cb::engine_errc err;
    try {
        err = stream->messageReceived(std::move(msg), ufc);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    if (err == cb::engine_errc::temporary_failure) {
        // The item was forced into the Checkpoint but bytes aren't being acked
        // back to the producer yet as the node is OOM.
        // Here we schedule the DcpConsumerTask. There we verify the OOM state
        // of the node and we resume with acking unacked bytes when we recover
        // from OOM.
        notifyVbucketReady(vbucket);
        return cb::engine_errc::success;
    }

    return err;
}

cb::engine_errc DcpConsumer::commit(uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKeyView& key,
                                    uint64_t prepare_seqno,
                                    uint64_t commit_seqno) {
    lastMessageTime = ep_uptime_now();
    const size_t msgBytes = CommitSyncWrite::commitBaseMsgBytes + key.size();
    UpdateFlowControl ufc(*this, msgBytes);

    if (commit_seqno == 0) {
        logger->warn("({}) Invalid sequence number(0) for commit!", vbucket);
        return cb::engine_errc::invalid_arguments;
    }

    auto msg = std::make_unique<CommitSyncWriteConsumer>(
            opaque, vbucket, prepare_seqno, commit_seqno, key);
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

cb::engine_errc DcpConsumer::abort(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKeyView& key,
                                   uint64_t prepareSeqno,
                                   uint64_t abortSeqno) {
    lastMessageTime = ep_uptime_now();
    UpdateFlowControl ufc(*this,
                          AbortSyncWrite::abortBaseMsgBytes + key.size());

    if (!abortSeqno) {
        logger->warn("({}) Invalid abort-seqno (0)", vbucket);
        return cb::engine_errc::invalid_arguments;
    }

    auto msg = std::make_unique<AbortSyncWriteConsumer>(
            opaque, vbucket, key, prepareSeqno, abortSeqno);
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}

void DcpConsumer::setDisconnect() {
    ConnHandler::setDisconnect();

    closeAllStreams();

    scheduleNotify();
}

void DcpConsumer::registerStream(std::shared_ptr<PassiveStream> stream) {
    auto vbid = stream->getVBucket();
    streams.insert({vbid, stream});
    auto& connMap = engine_.getDcpConnMap();

    Expects(!connMap.vbConnectionExists(this, vbid));

    connMap.addVBConnByVBId(*this, vbid);
}

std::shared_ptr<PassiveStream> DcpConsumer::removeStream(Vbid vbid) {
    auto eraseResult = streams.erase(vbid).first;
    engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbid);
    return eraseResult;
}
cb::engine_errc DcpConsumer::getNoStreamFoundErrorCode() const {
    // No stream for this vBucket / opaque - return ENOENT to indicate this.
    // Or use V7 dcp code cb::engine_errc::stream_not_found if enabled
    return isV7DcpStatusEnabled ? cb::engine_errc::stream_not_found
                                : cb::engine_errc::no_such_key;
}

cb::engine_errc DcpConsumer::getOpaqueMissMatchErrorCode() const {
    // No such stream with the given opaque - return KEY_EEXISTS to indicate
    // that a stream exists but not for this opaque (similar to InvalidCas).
    // Or use V7 dcp code cb::engine_errc::opaque_no_match if enabled
    return isV7DcpStatusEnabled ? cb::engine_errc::opaque_no_match
                                : cb::engine_errc::key_already_exists;
}

bool DcpConsumer::isFlowControlEnabled() const {
    return flowControl.isEnabled();
}

void DcpConsumer::incrFlowControlFreedBytes(uint32_t bytes) {
    flowControl.incrFreedBytes(bytes);
    scheduleNotifyIfNecessary();
}

std::string format_as(DcpConsumer::NoopIntervalNegotiation::State state) {
    using State = DcpConsumer::NoopIntervalNegotiation::State;
    switch (state) {
    case State::PendingMillisecondsRequest:
        return "PendingMillisecondsRequest";
    case State::PendingMillisecondsResponse:
        return "PendingMillisecondsResponse";
    case State::PendingSecondsRequest:
        return "PendingSecondsRequest";
    case State::PendingSecondsResponse:
        return "PendingSecondsResponse";
    case State::Completed:
        return "Completed";
    }
    return fmt::format("Invalid NoopIntervalNegotiation::State: {}",
                       uint8_t(state));
}

std::ostream& operator<<(std::ostream& os,
                         DcpConsumer::NoopIntervalNegotiation::State state) {
    return os << format_as(state);
}
