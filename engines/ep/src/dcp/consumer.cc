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
#include <phosphor/phosphor.h>
#include <platform/json_log_conversions.h>
#include <xattr/utils.h>

#include <charconv>
#include <utility>

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
      consumerName(std::move(consumerName_)),
      processorTaskRunning(false),
      flowControl(engine, *this) {
    Configuration& config = engine.getConfiguration();
    setSupportAck(false);
    setLogContext("DCP (Consumer) " + getName() + " -",
                  {{"dcp", "consumer"}, {"dcp_name", getName()}});

    allowSanitizeValueInDeletion.store(config.isAllowSanitizeValueInDeletion());

    // Contstructor adds DCP controls we will negoiate with the producer.
    auto controls = pendingControls.lock();
    if (config.isDcpEnableNoop()) {
        controls->emplace_back(DcpControlKeys::EnableNoop, "true");
        // First setup for millisecond negotiation.
        // MB-56973: For v7.6+ producers, support sub-second noop-interval
        // encoded as fractional seconds.
        std::chrono::duration<float> interval = dcpNoopTxInterval;
        // The control has an empty success callback, but failure injects the
        // pre 7.6 seconds negotiation.
        controls->emplace_back(
                DcpControlKeys::SetNoopInterval,
                std::to_string(interval.count()),
                []() { /*nothing on success*/ },
                [this](Controls& controls) {
                    /* inject a new control and don't disconnect */
                    addNoopSecondsPendingControl(controls);
                    return false;
                });
        // When noop is enabled, GetErrorMap is used to determine if the
        // producer is the correct version.
        getErrorMapState = GetErrorMapState::PendingRequest;
    }
    controls->emplace_back(DcpControlKeys::SetPriority, "high");
    controls->emplace_back(DcpControlKeys::SupportsCursorDroppingVulcan,
                           "true");
    controls->emplace_back(DcpControlKeys::SupportsHifiMfu, "true");
    controls->emplace_back(DcpControlKeys::SendStreamEndOnClientCloseStream,
                           "true");
    controls->emplace_back(DcpControlKeys::EnableExpiryOpcode, "true");
    if (!consumerName.empty()) {
        // If a consumer_name was provided then tell the producer about it.
        // Having a consumer name determines if we should support
        // SyncReplication. If we have not yet received a consumer name then the
        // cluster is in a mixed mode state and ns_server will not have set the
        // topology on any producer nodes. We should NOT attempt to enable
        // SyncReplication if this is the case. When the cluster is fully
        // upgraded to MadHatter+, ns_server will tear down DCP connections and
        // recreate them with the consumer name.
        controls->emplace_back(
                DcpControlKeys::EnableSyncWrites, "true", [this]() {
                    supportsSyncReplication.store(
                            SyncReplication::SyncReplication);
                });
        controls->emplace_back(DcpControlKeys::ConsumerName,
                                       consumerName);
    }
    controls->emplace_back(
            DcpControlKeys::IncludeDeletedUserXattrs, "true", [this]() {
                includeDeletedUserXattrs = IncludeDeletedUserXattrs::Yes;
            });
    controls->emplace_back(DcpControlKeys::V7DcpStatusCodes,
                                   "true",
                                   [this]() { useDcpV7StatusCodes = true; });

    controls->emplace_back(
            DcpControlKeys::FlatBuffersSystemEvents, "true", [this]() {
                flatBuffersSystemEventsEnabled = true;
            });
    controls->emplace_back(DcpControlKeys::ChangeStreams,
                                   "true",
                                   [this]() { changeStreams = true; });

    if (engine.getDcpConsumerMaxMarkerVersion() >= 2.2) {
        controls->emplace_back(DcpControlKeys::SnapshotMaxMarkerVersion, "2.2");
    }

    controls->emplace_back(DcpControlKeys::CacheTransfer, "true", [this]() {
        cacheTransfer = true;
    });
}

DcpConsumer::~DcpConsumer() {
    // Log runtime / pause information when we destruct.
    const auto now = ep_uptime_now();
    OBJ_LOG_INFO_CTX(*logger,
                     "Destroying connection",
                     {"since_created", (now - created)},
                     {"since_last_message", (now - lastMessageTime)},
                     {"paused_details", getPausedDetailsDescription()});

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
        OBJ_LOG_WARN_CTX(*logger,
                         "Add stream failed because this vbucket doesn't exist",
                         {"vb", vbucket});
        return cb::engine_errc::not_my_vbucket;
    }

    if (vb->getState() == vbucket_state_active) {
        OBJ_LOG_WARN_CTX(
                *logger,
                "Add stream failed because this vbucket happens to be in "
                "active state",
                {"vb", vbucket});
        return cb::engine_errc::not_my_vbucket;
    }

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.getEnd() == info.start) {
        info.range.setStart(info.start);
    }

    uint32_t new_opaque = ++opaqueCounter;
    failover_entry_t entry = vb->failovers->getLatestEntry();
    uint64_t start_seqno = info.start;
    uint64_t vbucket_uuid = entry.vb_uuid;
    uint64_t snap_start_seqno = info.range.getStart();
    uint64_t snap_end_seqno = info.range.getEnd();
    uint64_t high_seqno = vb->getHighSeqno();
    const Collections::ManifestUid vb_manifest_uid =
            vb->lockCollections().getManifestUid();

    auto stream = findStream(vbucket);
    if (stream) {
        if(stream->isActive()) {
            OBJ_LOG_WARN_CTX(*logger,
                             "Cannot add stream because one already exists",
                             {"vb", vbucket});
            return cb::engine_errc::key_already_exists;
        }
        removeStream(vbucket);
    }

    /* We need 'Processor' task only when we have a stream. Hence create it
     only once when the first stream is added */
    bool exp = false;
    if (processorTaskRunning.compare_exchange_strong(exp, true)) {
        ExTask task = std::make_shared<DcpConsumerTask>(
                engine_, shared_from_base<DcpConsumer>(), 1);
        processorTaskId = ExecutorPool::get()->schedule(task);
    }

    if (cacheTransfer && vb->shouldUseDcpCacheTransfer()) {
        OBJ_LOG_INFO_CTX(*logger, "Requesting cache transfer", {"vb", vbucket});
        flags |= cb::mcbp::DcpAddStreamFlag::CacheTransfer;
    }

    stream = makePassiveStream(engine_,
                               shared_from_base<DcpConsumer>(),
                               getName(),
                               flags,
                               new_opaque,
                               vbucket,
                               start_seqno,
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
        OBJ_LOG_WARN_CTX(
                *logger,
                "Cannot close stream because no stream exists for this vbucket",
                {"vb", vbucket});
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
        OBJ_LOG_WARN_CTX(
                *logger,
                "End stream received but no such stream for this vBucket",
                {"vb", vbucket},
                {"opaque", opaque});
        return getNoStreamFoundErrorCode();
    }

    if (stream->getOpaque() != opaque) {
        // MB-34951: By the time the DcpConsumer receives the StreamEnd from
        // the DcpProducer it is possible that ns_server has already started
        // a new Stream (with updated opaque) for this vbucket.
        // In which case just ignore this StreamEnd message, returning SUCCESS.
        OBJ_LOG_INFO_CTX(
                *logger,
                "End stream received but current opaque for that vb is "
                "different - ignoring",
                {"vb", vbucket},
                {"opaque", opaque},
                {"stream_opaque", stream->getOpaque()});
        return cb::engine_errc::success;
    }

    OBJ_LOG_INFO_CTX(*logger,
                     "End stream received",
                     {"vb", vbucket},
                     {"status", cb::mcbp::to_string(status)});

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
        OBJ_LOG_WARN_CTX(*logger,
                         "Invalid sequence number (0) for mutation!",
                         {"vb", vbucket});
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
        OBJ_LOG_WARN_CTX(*logger,
                         "Invalid sequence number (0) for deletion!",
                         {"vb", vbucket});
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
                OBJ_LOG_ERROR_CTX(
                        *logger,
                        "DcpConsumer::deletion: Value cannot contain a body",
                        {"vb", vbucket});
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
    size_t bytes = 0;
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
        std::optional<uint64_t> high_prepared_seqno,
        std::optional<uint64_t> max_visible_seqno,
        std::optional<uint64_t> purge_seqno) {
    lastMessageTime = ep_uptime_now();
    auto msg = std::make_unique<SnapshotMarker>(opaque,
                                                vbucket,
                                                start_seqno,
                                                end_seqno,
                                                flags,
                                                high_completed_seqno,
                                                high_prepared_seqno,
                                                max_visible_seqno,
                                                purge_seqno,
                                                cb::mcbp::DcpStreamId{});
    UpdateFlowControl ufc(*this, msg->getMessageSize());

    if (start_seqno > end_seqno) {
        OBJ_LOG_WARN_CTX(
                *logger,
                "Invalid snapshot marker received, snap_start <= snap_end",
                {"vb", vbucket},
                {"snapshot", {start_seqno, end_seqno}});
        return cb::engine_errc::invalid_arguments;
    }

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
    // Note: calling handleFlowCtl may update pendingControls
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

    // Process all controls before anything else.
    if (auto controls = pendingControls.lock(); !controls->empty()) {
        return stepControlNegotiation(producers, controls->front());
    }

    if ((ret = handleNoop(producers)) != cb::engine_errc::failed) {
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
            OBJ_LOG_WARN_CTX(*logger,
                             "Unknown consumer event, disconnecting",
                             {"event", int(resp->getEvent())});
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
        // we want to reschedule the operation as we failed to acquire the
        // necessary locks - sleep for 500ms to avoid busy looping
        snooze(0.5);
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

    OBJ_LOG_DEBUG_CTX(*logger,
                      "handleResponse()",
                      {"opcode", opcode},
                      {"opaque", opaque},
                      {"status", response.getStatus()});

    if (opcode == cb::mcbp::ClientOpcode::DcpStreamReq) {
        auto oitr = opaqueMap_.find(opaque);
        if (oitr == opaqueMap_.end()) {
            EP_LOG_WARN_CTX(
                    "Received response with opaque that does not exist in "
                    "opaqueMap",
                    {"opaque", opaque});
            return false;
        }
        if (!isValidOpaque(opaque, oitr->second.second)) {
            EP_LOG_WARN_CTX(
                    "Received response with opaque that does not have a stream",
                    {"opaque", opaque},
                    {"vb", Vbid(oitr->second.second)});
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
                OBJ_LOG_WARN_CTX(
                        *logger,
                        "Received rollback request with incorrect bodylen, "
                        "disconnecting",
                        {"vb", vbid},
                        {"size", value.size()});
                return false;
            }
            uint64_t rollbackSeqno = 0;
            memcpy(&rollbackSeqno, value.data(), sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);
            return handleRollbackResponse(vbid, opaque, rollbackSeqno);
        }

        if (((value.size() % 16) != 0 || value.empty()) &&
            status == cb::mcbp::Status::Success) {
            OBJ_LOG_WARN_CTX(*logger,
                             "Got a stream response with a bad failover log, "
                             "disconnecting",
                             {"vb", vbid},
                             {"size", value.size()});
            return false;
        }

        streamAccepted(opaque, status, value);
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::DcpBufferAcknowledgement) {
        return true;
    }
    if (opcode == cb::mcbp::ClientOpcode::DcpControl) {
        auto controls = pendingControls.lock();
        if (controls->empty() || !controls->front().opaque) {
            // This case exists for DcpControl responses which are not initiated
            // from "pendingControl" e.g. FlowControl can send DcpControl.
            // There is nothing todo other than swallow the response and stay
            // connected (return true).
            return true;
        }

        // pendingControls is processed front to back. If we have a response
        // to DcpControl we expect the front to be the control which sent the
        // DcpControl request, thus we expect the opaque to match.
        const auto& control = controls->front();
        if (response.getOpaque() != control.opaque.value()) {
            OBJ_LOG_ERROR_CTX(
                    *logger,
                    "Got non-matching opaque for DcpControl - disconnecting",
                    {"opaque", response.getOpaque()},
                    {"control_key", control.key},
                    {"control_value", control.value},
                    {"expected", control.opaque.value()});
            return false;
        }

        // Opaque matches, is this success or failure from the producer?
        bool stayConnected = true;
        if (response.getStatus() == cb::mcbp::Status::Success) {
            control.success();
        } else if (control.failure(*controls)) {
            OBJ_LOG_ERROR_CTX(
                    *logger,
                    "Got non-success status for DcpControl - disconnecting",
                    {"status", response.getStatus()},
                    {"control_key", control.key},
                    {"control_value", control.value});
            // failure returned true, we must disconnect
            stayConnected = false;
        } else {
            // Log info about what didn't get enabled
            OBJ_LOG_INFO_CTX(*logger,
                             "Got non-success status for DcpControl",
                             {"status", response.getStatus()},
                             {"control_key", control.key},
                             {"control_value", control.value});
        }

        // We have completed processing this control, discard it ready for the
        // next.
        controls->pop_front();
        return stayConnected;
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
            OBJ_LOG_ERROR_CTX(
                    *logger,
                    "Incompatible Producer node version detected - this "
                    "version of CB Server requires version 6 or higher - "
                    "disconnecting. (Producer responded to GetErrorMap "
                    "request indicating version <5.0.0)",
                    {"status", status});
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

    OBJ_LOG_WARN_CTX(*logger,
                     "Trying to handle an unknown response , disconnecting",
                     {"opcode", opcode});

    return false;
}

bool DcpConsumer::handleRollbackResponse(Vbid vbid,
                                         uint32_t opaque,
                                         uint64_t rollbackSeqno) {
    auto vb = engine_.getVBucket(vbid);
    auto stream = findStream(vbid);

    if (!(vb && stream)) {
        OBJ_LOG_WARN_CTX(*logger,
                         "handleRollbackResponse",
                         {"vb", vbid},
                         {"vb_ok", bool(vb.get())},
                         {"stream_ok", bool(stream.get())});
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
            OBJ_LOG_INFO_CTX(
                    *logger,
                    "Received rollback request. Rollback to 0, have entries "
                    "remaining. Retrying with previous failover vb_uuid",
                    {"vb", vbid},
                    {"num_entries", vb->failovers->getNumEntries()},
                    {"vb_uuid", entry.vb_uuid});

            stream->streamRequest(entry.vb_uuid);
            return true;
        }
        OBJ_LOG_INFO_CTX(
                *logger,
                "Cannot avoid rollback to 0, vb_uuid cannot be used as "
                "entry.by_seqno does not match stream start_seqno",
                {"vb", vbid},
                {"vb_uuid", entry.vb_uuid},
                {"entry_by_seqno", entry.by_seqno},
                {"start_seqno", stream->getStartSeqno()});
    }

    OBJ_LOG_INFO_CTX(*logger,
                     "Received rollback request. Rolling back",
                     {"vb", vbid},
                     {"rollback_seqno", rollbackSeqno});
    ExTask task =
            std::make_shared<RollbackTask>(engine_,
                                           opaque,
                                           vbid,
                                           rollbackSeqno,
                                           shared_from_base<DcpConsumer>());
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
        OBJ_LOG_WARN_CTX(
                *logger, "Rollback failed on the vbucket", {"vb", vbid});
        break;
    case TaskStatus::Complete: {
        VBucketPtr vb = engine_.getVBucket(vbid);
        if (!vb) {
            OBJ_LOG_WARN_CTX(
                    *logger,
                    "Aborting rollback task as the vbucket was deleted after "
                    "rollback",
                    {"vb", vbid});
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
        OBJ_LOG_WARN_CTX(*logger,
                         "Could not ack seqno because stream was not found",
                         {"vb", vbid},
                         {"seqno", seqno});
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

    auto controls = pendingControls.lock();
    if (!controls->empty()) {
        auto sz = controls->size();
        // copy front and dump it
        auto control = controls->front();
        controls.unlock();

        addStat("pending_controls_size", sz, add_stat, c);
        addStat("pending_control", control.key, add_stat, c);
        addStat("pending_control_value", control.value, add_stat, c);
        if (control.opaque) {
            addStat("pending_control_opaque",
                    control.opaque.value(),
                    add_stat,
                    c);
        }
    }
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
        OBJ_LOG_WARN_CTX(
                *logger,
                "Processor task indicating disconnection as there is no memory "
                "to complete replication",
                {"vb", stream->getVBucket()});
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
                                 cb::const_byte_buffer newFailoverLog) {
    auto oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        Vbid vbucket = oitr->second.second;

        auto stream = findStream(vbucket);
        if (stream && stream->getOpaque() == opaque && stream->isPending()) {
            if (status == cb::mcbp::Status::Success) {
                VBucketPtr vb = engine_.getVBucket(vbucket);
                vb->failovers->replaceFailoverLog(newFailoverLog);
                engine_.getKVBucket()->persistVBState(vbucket);
            }
            OBJ_LOG_DEBUG_CTX(*logger,
                              "Add stream for opaque with error code",
                              {"vb", vbucket},
                              {"opaque", opaque},
                              {"status", status});
            stream->acceptStream(status, add_opaque);
        } else {
            OBJ_LOG_WARN_CTX(*logger,
                             "Trying to add stream, but none exists",
                             {"vb", vbucket},
                             {"opaque", opaque},
                             {"add_opaque", add_opaque});
        }
        opaqueMap_.erase(opaque);
    } else {
        OBJ_LOG_WARN_CTX(*logger,
                         "No opaque found for add stream response",
                         {"opaque", opaque});
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
        OBJ_LOG_DEBUG_CTX(*logger,
                          "State changed, closing passive stream!",
                          {"vb", vbucket},
                          {"state", VBucket::toString(state)});
        stream->setDead(cb::mcbp::DcpStreamEndStatus::StateChanged);
    }
}

cb::engine_errc DcpConsumer::handleNoop(DcpMessageProducersIface& producers) {
    const auto now = ep_uptime_now();
    auto dcpIdleTimeout = getIdleTimeout();
    if ((now - lastMessageTime) > dcpIdleTimeout) {
        OBJ_LOG_WARN_CTX(
                *logger,
                "Disconnecting because a message has not been received for the "
                "DCP idle timeout",
                {"dcp_idle_timeout", dcpIdleTimeout},
                {"since_last_message", (now - lastMessageTime)},
                {"dcp_noop_tx_interval", dcpNoopTxInterval});
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

cb::engine_errc DcpConsumer::stepControlNegotiation(
        DcpMessageProducersIface& producers,
        BlockingDcpControlNegotiation& control) {
    if (!control.opaque) {
        // Assign the opaque and send the DcpControl request
        control.opaque = incrOpaqueCounter();
        NonBucketAllocationGuard guard;
        return producers.control(
                control.opaque.value(), control.key, control.value);
    }
    // opaque is assigned, wait
    return cb::engine_errc::would_block;
}

uint32_t DcpConsumer::incrOpaqueCounter() {
    if (opaqueCounter == std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
                "DcpConsumer::incrOpaqueCounter: opaqueCounter "
                "has reached the maximum value");
    }
    return (++opaqueCounter);
}

size_t DcpConsumer::getFlowControlBufSize() const {
    return flowControl.getBufferSize();
}

void DcpConsumer::setFlowControlBufSize(size_t newSize) {
    flowControl.setBufferSize(newSize);
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
        OBJ_LOG_WARN_CTX(*logger,
                         "Invalid sequence number(0) for prepare!",
                         {"vb", vbucket});
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
                OBJ_LOG_ERROR_CTX(
                        *logger,
                        "DcpConsumer::prepare: Value cannot contain a body for "
                        "SyncDelete",
                        {"vb", vbucket});
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
        OBJ_LOG_WARN_CTX(*logger,
                         "Invalid sequence number (0) for commit!",
                         {"vb", vbucket});
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
        OBJ_LOG_WARN_CTX(*logger, "Invalid abort-seqno (0)", {"vb", vbucket});
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
    return useDcpV7StatusCodes ? cb::engine_errc::stream_not_found
                               : cb::engine_errc::no_such_key;
}

cb::engine_errc DcpConsumer::getOpaqueMissMatchErrorCode() const {
    // No such stream with the given opaque - return KEY_EEXISTS to indicate
    // that a stream exists but not for this opaque (similar to InvalidCas).
    // Or use V7 dcp code cb::engine_errc::opaque_no_match if enabled
    return useDcpV7StatusCodes ? cb::engine_errc::opaque_no_match
                               : cb::engine_errc::key_already_exists;
}

bool DcpConsumer::isFlowControlEnabled() const {
    return flowControl.isEnabled();
}

void DcpConsumer::incrFlowControlFreedBytes(size_t bytes) {
    flowControl.incrFreedBytes(bytes);
    scheduleNotifyIfNecessary();
}

void DcpConsumer::addNoopSecondsPendingControl(Controls& controls) {
    using namespace std::chrono;

    // Compute a integer seconds interval, which must be 1 or greater.
    dcpNoopTxInterval = round<seconds>(dcpNoopTxInterval);
    dcpNoopTxInterval =
            std::max(std::chrono::duration<float>(1), dcpNoopTxInterval);
    seconds interval = duration_cast<seconds>(dcpNoopTxInterval);

    // Always put new controls to the back of the queue
    controls.emplace_back(
            DcpControlKeys::SetNoopInterval,
            std::to_string(interval.count()),
            []() { /*nothing on success*/ },
            [this](Controls&) {
                /* we really want noop, so on failure force disconnect */;
                return true;
            });
}

void DcpConsumer::addBufferSizeControl(size_t bufferSize) {
    pendingControls.lock()->emplace_back(DcpControlKeys::ConnectionBufferSize,
                                         std::to_string(bufferSize));
}

cb::engine_errc DcpConsumer::cached_value(uint32_t opaque,
                                          const DocKeyView& key,
                                          cb::const_byte_buffer value,
                                          uint8_t datatype,
                                          uint64_t cas,
                                          Vbid vbucket,
                                          uint32_t flags,
                                          uint64_t bySeqno,
                                          uint64_t revSeqno,
                                          uint32_t expiration,
                                          uint32_t lockTime,
                                          cb::const_byte_buffer meta,
                                          uint8_t nru) {
    lastMessageTime = ep_uptime_now();

    if (bySeqno == 0) {
        logger->warnWithContext("Invalid sequence number (0) for cached_value!",
                                {{"vb", vbucket}});
        return cb::engine_errc::invalid_arguments;
    }

    queued_item item(new Item(key,
                              flags,
                              expiration,
                              value.data(),
                              value.size(),
                              datatype,
                              cas,
                              bySeqno,
                              vbucket,
                              revSeqno,
                              nru /*freqCounter */));
    const auto msgBytes = MutationResponse::mutationBaseMsgBytes + key.size() +
                          meta.size() + value.size();
    UpdateFlowControl ufc(*this, msgBytes);
    auto msg = std::make_unique<MutationConsumerMessage>(
            std::move(item),
            opaque,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeleteTime::No,
            IncludeDeletedUserXattrs::Yes,
            key.getEncoding(),
            nullptr,
            cb::mcbp::DcpStreamId{},
            DcpResponse::Event::CachedValue);
    return lookupStreamAndDispatchMessage(ufc, vbucket, opaque, std::move(msg));
}
