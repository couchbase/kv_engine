/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "dcp/consumer.h"

#include "ep_engine.h"
#include "failover-table.h"
#include "replicationthrottle.h"
#include "dcp/dcpconnmap.h"
#include "dcp/stream.h"
#include "dcp/response.h"
#include <platform/make_unique.h>

#include <climits>
#include <phosphor/phosphor.h>

const std::string DcpConsumer::noopCtrlMsg = "enable_noop";
const std::string DcpConsumer::noopIntervalCtrlMsg = "set_noop_interval";
const std::string DcpConsumer::connBufferCtrlMsg = "connection_buffer_size";
const std::string DcpConsumer::priorityCtrlMsg = "set_priority";
const std::string DcpConsumer::extMetadataCtrlMsg = "enable_ext_metadata";
const std::string DcpConsumer::valueCompressionCtrlMsg = "enable_value_compression";
const std::string DcpConsumer::cursorDroppingCtrlMsg = "supports_cursor_dropping";

class Processor : public GlobalTask {
public:
    Processor(EventuallyPersistentEngine* e,
              connection_t c,
              double sleeptime = 1,
              bool completeBeforeShutdown = true)
        : GlobalTask(e, TaskId::Processor, sleeptime, completeBeforeShutdown),
          conn(c) {}

    ~Processor() {
        DcpConsumer* consumer = static_cast<DcpConsumer*>(conn.get());
        consumer->taskCancelled();
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "Processor");
        DcpConsumer* consumer = static_cast<DcpConsumer*>(conn.get());
        if (consumer->doDisconnect()) {
            return false;
        }

        double sleepFor = 0.0;
        enum process_items_error_t state = consumer->processBufferedItems();
        switch (state) {
            case all_processed:
                sleepFor = INT_MAX;
                break;
            case more_to_process:
                sleepFor = 0.0;
                break;
            case cannot_process:
                sleepFor = 5.0;
                break;
        }

        if (consumer->notifiedProcessor(false)) {
            snooze(0.0);
            state = more_to_process;
        } else {
            snooze(sleepFor);
            // Check if the processor was notified again,
            // in which case the task should wake immediately.
            if (consumer->notifiedProcessor(false)) {
                snooze(0.0);
                state = more_to_process;
            }
        }

        consumer->setProcessorTaskState(state);

        return true;
    }

    std::string getDescription() {
        std::stringstream ss;
        ss << "Processing buffered items for " << conn->getName();
        return ss.str();
    }

private:
    connection_t conn;
};

DcpConsumer::DcpConsumer(EventuallyPersistentEngine &engine, const void *cookie,
                         const std::string &name)
    : Consumer(engine, cookie, name),
      lastMessageTime(ep_current_time()),
      opaqueCounter(0),
      processorTaskId(0),
      processorTaskState(all_processed),
      processorNotification(false),
      backoffs(0),
      dcpIdleTimeout(engine.getConfiguration().getDcpIdleTimeout()),
      dcpNoopTxInterval(engine.getConfiguration().getDcpNoopTxInterval()),
      taskAlreadyCancelled(false),
      flowControl(engine, this),
      processBufferedMessagesYieldThreshold(engine.getConfiguration().
                                                getDcpConsumerProcessBufferedMessagesYieldLimit()),
      processBufferedMessagesBatchSize(engine.getConfiguration().
                                            getDcpConsumerProcessBufferedMessagesBatchSize()) {
    Configuration& config = engine.getConfiguration();
    setSupportAck(false);
    setLogHeader("DCP (Consumer) " + getName() + " -");
    setReserved(true);

    pendingEnableNoop = config.isDcpEnableNoop();
    pendingSendNoopInterval = config.isDcpEnableNoop();
    pendingSetPriority = true;
    pendingEnableExtMetaData = true;
    pendingEnableValueCompression = config.isDcpValueCompressionEnabled();
    pendingSupportCursorDropping = true;

    ExTask task = new Processor(&engine, this, 1);
    processorTaskId = ExecutorPool::get()->schedule(task, NONIO_TASK_IDX);
}

DcpConsumer::~DcpConsumer() {
    cancelTask();
}


void DcpConsumer::cancelTask() {
    bool inverse = false;
    if (taskAlreadyCancelled.compare_exchange_strong(inverse, true)) {
        ExecutorPool::get()->cancel(processorTaskId);
    }
}

void DcpConsumer::taskCancelled() {
    bool inverse = false;
    taskAlreadyCancelled.compare_exchange_strong(inverse, true);
}

ENGINE_ERROR_CODE DcpConsumer::addStream(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    lastMessageTime = ep_current_time();
    LockHolder lh(readyMutex);
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger.log(EXTENSION_LOG_WARNING,
            "(vb %d) Add stream failed because this vbucket doesn't exist",
            vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (vb->getState() == vbucket_state_active) {
        logger.log(EXTENSION_LOG_WARNING,
            "(vb %d) Add stream failed because this vbucket happens to be in "
            "active state", vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    snapshot_info_t info = vb->checkpointManager.getSnapshotInfo();
    if (info.range.end == info.start) {
        info.range.start = info.start;
    }

    uint32_t new_opaque = ++opaqueCounter;
    failover_entry_t entry = vb->failovers->getLatestEntry();
    uint64_t start_seqno = info.start;
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = entry.vb_uuid;
    uint64_t snap_start_seqno = info.range.start;
    uint64_t snap_end_seqno = info.range.end;
    uint64_t high_seqno = vb->getHighSeqno();

    auto stream = findStream(vbucket);
    if (stream) {
        if(stream->isActive()) {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot add stream because "
                "one already exists", logHeader(), vbucket);
            return ENGINE_KEY_EEXISTS;
        } else {
            streams.erase(vbucket);
        }
    }

    streams.insert({vbucket,
                    SingleThreadedRCPtr<PassiveStream>(
                           new PassiveStream(&engine_, this, getName(), flags,
                                             new_opaque, vbucket, start_seqno,
                                             end_seqno, vbucket_uuid,
                                             snap_start_seqno, snap_end_seqno,
                                             high_seqno))});
    ready.push_back(vbucket);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::closeStream(uint32_t opaque, uint16_t vbucket) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        streams.erase(vbucket);
        return ENGINE_DISCONNECT;
    }

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        opaqueMap_.erase(oitr);
    }

    auto stream = findStream(vbucket);
    if (!stream) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because no "
            "stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    }

    uint32_t bytesCleared = stream->setDead(END_STREAM_CLOSED);
    flowControl.incrFreedBytes(bytesCleared);
    streams.erase(vbucket);
    notifyConsumerIfNecessary(true/*schedule*/);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    auto stream = findStream(vbucket);
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        LOG(EXTENSION_LOG_INFO, "%s (vb %d) End stream received with reason %d",
            logHeader(), vbucket, flags);

        try {
            err = stream->messageReceived(make_unique<StreamEndResponse>(opaque,
                                                                         flags,
                                                                         vbucket));
        } catch (const std::bad_alloc&) {
            return ENGINE_ENOMEM;
        }

        if (err == ENGINE_TMPFAIL) {
            notifyVbucketReady(vbucket);
        }
    }

    // The item was buffered and will be processed later
    if (err == ENGINE_TMPFAIL) {
        return ENGINE_SUCCESS;
    }

    if (err != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) End stream received with opaque "
            "%d but does not exist", logHeader(), vbucket, opaque);
    }

    flowControl.incrFreedBytes(StreamEndResponse::baseMsgBytes);
    notifyConsumerIfNecessary(true/*schedule*/);

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::mutation(uint32_t opaque, const void* key,
                                        uint16_t nkey, const void* value,
                                        uint32_t nvalue, uint64_t cas,
                                        uint16_t vbucket, uint32_t flags,
                                        uint8_t datatype, uint32_t locktime,
                                        uint64_t bySeqno, uint64_t revSeqno,
                                        uint32_t exptime, uint8_t nru,
                                        const void* meta, uint16_t nmeta) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (bySeqno == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid sequence number(0) "
            "for mutation!", logHeader(), vbucket);
        return ENGINE_EINVAL;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    auto stream = findStream(vbucket);
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        queued_item item(new Item(key, nkey, flags, exptime, value, nvalue,
                                  &datatype, EXT_META_LEN, cas, bySeqno,
                                  vbucket, revSeqno));

        ExtendedMetaData *emd = NULL;
        if (nmeta > 0) {
            emd = new ExtendedMetaData(meta, nmeta);

            if (emd == NULL) {
                return ENGINE_ENOMEM;
            }
            if (emd->getStatus() == ENGINE_EINVAL) {
                delete emd;
                return ENGINE_EINVAL;
            }
        }

        try {
            err = stream->messageReceived(make_unique<MutationResponse>(item,
                                                                        opaque,
                                                                        emd));
        } catch (const std::bad_alloc&) {
            delete emd;
            return ENGINE_ENOMEM;
        }


        if (err == ENGINE_TMPFAIL) {
            notifyVbucketReady(vbucket);
        }
    }

    // The item was buffered and will be processed later
    if (err == ENGINE_TMPFAIL) {
        return ENGINE_SUCCESS;
    }

    uint32_t bytes =
        MutationResponse::mutationBaseMsgBytes + nkey + nmeta + nvalue;
    flowControl.incrFreedBytes(bytes);
    notifyConsumerIfNecessary(true/*schedule*/);

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::deletion(uint32_t opaque, const void* key,
                                        uint16_t nkey, uint64_t cas,
                                        uint16_t vbucket, uint64_t bySeqno,
                                        uint64_t revSeqno, const void* meta,
                                        uint16_t nmeta) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (bySeqno == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid sequence number(0)"
            "for deletion!", logHeader(), vbucket);
        return ENGINE_EINVAL;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    auto stream = findStream(vbucket);
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        queued_item item(new Item(key, nkey, 0, 0, NULL, 0, NULL, 0, cas, bySeqno,
                                  vbucket, revSeqno));
        item->setDeleted();

        ExtendedMetaData *emd = NULL;
        if (nmeta > 0) {
            emd = new ExtendedMetaData(meta, nmeta);

            if (emd == NULL) {
                return ENGINE_ENOMEM;
            }
            if (emd->getStatus() == ENGINE_EINVAL) {
                delete emd;
                return ENGINE_EINVAL;
            }
        }

        try {
            err = stream->messageReceived(make_unique<MutationResponse>(item,
                                                                        opaque,
                                                                        emd));
        } catch (const std::bad_alloc&) {
            delete emd;
            return ENGINE_ENOMEM;
        }

        if (err == ENGINE_TMPFAIL) {
            notifyVbucketReady(vbucket);
        }
    }

    // The item was buffered and will be processed later
    if (err == ENGINE_TMPFAIL) {
        return ENGINE_SUCCESS;
    }

    uint32_t bytes = MutationResponse::deletionBaseMsgBytes + nkey + nmeta;
    flowControl.incrFreedBytes(bytes);
    notifyConsumerIfNecessary(true/*schedule*/);

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::expiration(uint32_t opaque, const void* key,
                                          uint16_t nkey, uint64_t cas,
                                          uint16_t vbucket, uint64_t bySeqno,
                                          uint64_t revSeqno, const void* meta,
                                          uint16_t nmeta) {
    // lastMessageTime is set in deletion function
    return deletion(opaque, key, nkey, cas, vbucket, bySeqno, revSeqno, meta,
                    nmeta);
}

ENGINE_ERROR_CODE DcpConsumer::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint32_t flags) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid snapshot marker "
            "received, snap_start (%" PRIu64 ") <= snap_end (%" PRIu64 ")",
            logHeader(), vbucket, start_seqno, end_seqno);
        return ENGINE_EINVAL;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    auto stream = findStream(vbucket);
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        try {
            err = stream->messageReceived(make_unique<SnapshotMarker>(opaque,
                                                                      vbucket,
                                                                      start_seqno,
                                                                      end_seqno,
                                                                      flags));

        } catch (const std::bad_alloc&) {
            return ENGINE_ENOMEM;
        }


        if (err == ENGINE_TMPFAIL) {
            notifyVbucketReady(vbucket);
        }
    }

    // The item was buffered and will be processed later
    if (err == ENGINE_TMPFAIL) {
        return ENGINE_SUCCESS;
    }

    flowControl.incrFreedBytes(SnapshotMarker::baseMsgBytes);
    notifyConsumerIfNecessary(true/*schedule*/);

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::noop(uint32_t opaque) {
    lastMessageTime = ep_current_time();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::flush(uint32_t opaque, uint16_t vbucket) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE DcpConsumer::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    lastMessageTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    auto stream = findStream(vbucket);
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        try {
            err = stream->messageReceived(make_unique<SetVBucketState>(opaque,
                                                                       vbucket,
                                                                       state));
        } catch (const std::bad_alloc&) {
            return ENGINE_ENOMEM;
        }

        if (err == ENGINE_TMPFAIL) {
            notifyVbucketReady(vbucket);
        }
    }

    // The item was buffered and will be processed later
    if (err == ENGINE_TMPFAIL) {
        return ENGINE_SUCCESS;
    }

    flowControl.incrFreedBytes(SetVBucketState::baseMsgBytes);
    notifyConsumerIfNecessary(true/*schedule*/);

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::step(struct dcp_message_producers* producers) {
    setLastWalkTime();

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE ret;
    if ((ret = flowControl.handleFlowCtl(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = handleNoop(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = handlePriority(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = handleExtMetaData(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = handleValueCompression(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = supportCursorDropping(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    DcpResponse *resp = getNextItem();
    if (resp == NULL) {
        return ENGINE_SUCCESS;
    }

    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    switch (resp->getEvent()) {
        case DCP_ADD_STREAM:
        {
            AddStreamResponse *as = static_cast<AddStreamResponse*>(resp);
            ret = producers->add_stream_rsp(getCookie(), as->getOpaque(),
                                            as->getStreamOpaque(),
                                            as->getStatus());
            break;
        }
        case DCP_STREAM_REQ:
        {
            StreamRequest *sr = static_cast<StreamRequest*> (resp);
            ret = producers->stream_req(getCookie(), sr->getOpaque(),
                                        sr->getVBucket(), sr->getFlags(),
                                        sr->getStartSeqno(), sr->getEndSeqno(),
                                        sr->getVBucketUUID(),
                                        sr->getSnapStartSeqno(),
                                        sr->getSnapEndSeqno());
            break;
        }
        case DCP_SET_VBUCKET:
        {
            SetVBucketStateResponse* vs;
            vs = static_cast<SetVBucketStateResponse*>(resp);
            ret = producers->set_vbucket_state_rsp(getCookie(), vs->getOpaque(),
                                                   vs->getStatus());
            break;
        }
        case DCP_SNAPSHOT_MARKER:
        {
            SnapshotMarkerResponse* mr;
            mr = static_cast<SnapshotMarkerResponse*>(resp);
            ret = producers->marker_rsp(getCookie(), mr->getOpaque(),
                                        mr->getStatus());
            break;
        }
        default:
            LOG(EXTENSION_LOG_WARNING, "%s Unknown consumer event (%d), "
                "disconnecting", logHeader(), resp->getEvent());
            ret = ENGINE_DISCONNECT;
    }
    ObjectRegistry::onSwitchThread(epe);
    delete resp;

    if (ret == ENGINE_SUCCESS) {
        return ENGINE_WANT_MORE;
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

ENGINE_ERROR_CODE DcpConsumer::handleResponse(
                                        protocol_binary_response_header *resp) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    uint8_t opcode = resp->response.opcode;
    uint32_t opaque = resp->response.opaque;

    opaque_map::iterator oitr = opaqueMap_.find(opaque);

    bool validOpaque = false;
    if (oitr != opaqueMap_.end()) {
        validOpaque = isValidOpaque(opaque, oitr->second.second);
    }

    if (!validOpaque) {
        logger.log(EXTENSION_LOG_WARNING,
            "Received response with opaque %" PRIu32 " and that stream no "
                    "longer exists", opaque);
        return ENGINE_KEY_ENOENT;
    }

    if (opcode == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ) {
        protocol_binary_response_dcp_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_dcp_stream_req*>(resp);

        uint16_t vbid = oitr->second.second;
        uint16_t status = ntohs(pkt->message.header.response.status);
        uint64_t bodylen = ntohl(pkt->message.header.response.bodylen);
        uint8_t* body = pkt->bytes + sizeof(protocol_binary_response_header);

        if (status == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
            if (bodylen != sizeof(uint64_t)) {
                LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Received rollback "
                    "request with incorrect bodylen of %" PRIu64 ", disconnecting",
                    logHeader(), vbid, bodylen);
                return ENGINE_DISCONNECT;
            }
            uint64_t rollbackSeqno = 0;
            memcpy(&rollbackSeqno, body, sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);

            LOG(EXTENSION_LOG_NOTICE, "%s (vb %d) Received rollback request "
                "to rollback seq no. %" PRIu64, logHeader(), vbid, rollbackSeqno);

            ExTask task = new RollbackTask(&engine_, opaque, vbid,
                                           rollbackSeqno, this);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            return ENGINE_SUCCESS;
        }

        if (((bodylen % 16) != 0 || bodylen == 0) && status == ENGINE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d)Got a stream response with a "
                "bad failover log (length %" PRIu64 "), disconnecting",
                logHeader(), vbid, bodylen);
            return ENGINE_DISCONNECT;
        }

        streamAccepted(opaque, status, body, bodylen);
        return ENGINE_SUCCESS;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT ||
               opcode == PROTOCOL_BINARY_CMD_DCP_CONTROL) {
        return ENGINE_SUCCESS;
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
}

bool DcpConsumer::doRollback(uint32_t opaque, uint16_t vbid,
                             uint64_t rollbackSeqno) {
    ENGINE_ERROR_CODE err = engine_.getKVBucket()->rollback(vbid,
                                                                 rollbackSeqno);

    switch (err) {
    case ENGINE_NOT_MY_VBUCKET:
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Rollback failed because the "
                "vbucket was not found", logHeader(), vbid);
        return false;

    case ENGINE_TMPFAIL:
        return true; // Reschedule the rollback.

    case ENGINE_SUCCESS:
        // expected
        break;

    default:
        throw std::logic_error("DcpConsumer::doRollback: Unexpected error "
                "code from EpStore::rollback: " + std::to_string(err));
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbid);
    auto stream = findStream(vbid);
    if (stream) {
        stream->reconnectStream(vb, opaque, vb->getHighSeqno());
    }

    return false;
}

void DcpConsumer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    // Make a copy of all valid streams (under lock), and then call addStats
    // for each one. (Done in two stages to minmise how long we have the
    // streams map locked for).
    std::vector<PassiveStreamMap::mapped_type> valid_streams;

    streams.for_each(
        [&valid_streams](const PassiveStreamMap::value_type& element) {
            valid_streams.push_back(element.second);
        }
    );
    for (const auto& stream : valid_streams) {
        stream->addStats(add_stat, c);
    }

    addStat("total_backoffs", backoffs, add_stat, c);
    addStat("processor_task_state", getProcessorTaskStatusStr(), add_stat, c);
    flowControl.addStats(add_stat, c);
}

void DcpConsumer::aggregateQueueStats(ConnCounter& aggregator) {
    aggregator.conn_queueBackoff += backoffs;
}


process_items_error_t DcpConsumer::drainStreamsBufferedItems(SingleThreadedRCPtr<PassiveStream>& stream,
                                                             size_t yieldThreshold) {
    process_items_error_t rval = all_processed;
    uint32_t bytesProcessed = 0;
    size_t iterations = 0;
    do {
        if (!engine_.getReplicationThrottle().shouldProcess()) {
            backoffs++;
            vbReady.pushUnique(stream->getVBucket());
            return cannot_process;
        }

        bytesProcessed = 0;
        rval = stream->processBufferedMessages(bytesProcessed,
                                               processBufferedMessagesBatchSize);
        flowControl.incrFreedBytes(bytesProcessed);

        // Notifying memcached on clearing items for flow control
        notifyConsumerIfNecessary(false/*schedule*/);

        iterations++;
    } while (bytesProcessed > 0 &&
             rval == all_processed &&
             iterations <= yieldThreshold);

    // The stream may not be done yet so must go back in the ready queue
    if (bytesProcessed > 0) {
        vbReady.pushUnique(stream->getVBucket());
        rval = more_to_process; // Return more_to_process to force a snooze(0.0)
    }

    return rval;
}

process_items_error_t DcpConsumer::processBufferedItems() {
    process_items_error_t process_ret = all_processed;
    uint16_t vbucket = 0;
    while (vbReady.popFront(vbucket)) {
        auto stream = findStream(vbucket);

        if (!stream) {
            continue;
        }

        process_ret = drainStreamsBufferedItems(stream,
                                                processBufferedMessagesYieldThreshold);

        if (process_ret == all_processed) {
            return more_to_process;
        }

        if (process_ret == cannot_process) {
            // If items for current vbucket weren't processed,
            // re-add current vbucket
            if (vbReady.size() > 0) {
                // If there are more vbuckets in queue, sleep(0).
                process_ret = more_to_process;
            }
            vbReady.pushUnique(vbucket);
        }

        return process_ret;
    }

    return process_ret;
}

void DcpConsumer::notifyVbucketReady(uint16_t vbucket) {
    if (vbReady.pushUnique(vbucket) &&
        notifiedProcessor(true)) {
        ExecutorPool::get()->wake(processorTaskId);
    }
}

bool DcpConsumer::notifiedProcessor(bool to) {
    bool inverse = !to;
    return processorNotification.compare_exchange_strong(inverse, to);
}

void DcpConsumer::setProcessorTaskState(enum process_items_error_t to) {
    processorTaskState = to;
}

std::string DcpConsumer::getProcessorTaskStatusStr() {
    switch (processorTaskState.load()) {
        case all_processed:
            return "ALL_PROCESSED";
        case more_to_process:
            return "MORE_TO_PROCESS";
        case cannot_process:
            return "CANNOT_PROCESS";
    }

    return "UNKNOWN";
}

DcpResponse* DcpConsumer::getNextItem() {
    LockHolder lh(readyMutex);

    setPaused(false);
    while (!ready.empty()) {
        uint16_t vbucket = ready.front();
        ready.pop_front();

        auto stream = findStream(vbucket);
        if (!stream) {
            continue;
        }

        DcpResponse* op = stream->next();
        if (!op) {
            continue;
        }
        switch (op->getEvent()) {
            case DCP_STREAM_REQ:
            case DCP_ADD_STREAM:
            case DCP_SET_VBUCKET:
            case DCP_SNAPSHOT_MARKER:
                break;
            default:
                throw std::logic_error(
                        std::string("DcpConsumer::getNextItem: ") + logHeader() +
                        " is attempting to write an unexpected event: " +
                        std::to_string(op->getEvent()));
        }

        ready.push_back(vbucket);
        return op;
    }
    setPaused(true);

    return NULL;
}

void DcpConsumer::notifyStreamReady(uint16_t vbucket) {
    LockHolder lh(readyMutex);
    std::list<uint16_t>::iterator iter =
        std::find(ready.begin(), ready.end(), vbucket);
    if (iter != ready.end()) {
        return;
    }

    ready.push_back(vbucket);
    lh.unlock();

    notifyPaused(/*schedule*/true);
}

void DcpConsumer::streamAccepted(uint32_t opaque, uint16_t status, uint8_t* body,
                                 uint32_t bodylen) {

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;

        auto stream = findStream(vbucket);
        if (stream && stream->getOpaque() == opaque &&
            stream->getState() == STREAM_PENDING) {
            if (status == ENGINE_SUCCESS) {
                RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
                vb->failovers->replaceFailoverLog(body, bodylen);
                KVBucket* kvBucket = engine_.getKVBucket();
                kvBucket->scheduleVBSnapshot(
                    VBSnapshotTask::Priority::HIGH,
                    kvBucket->getVBuckets().getShardByVbId(vbucket)->getId());
            }
            LOG(EXTENSION_LOG_INFO, "%s (vb %d) Add stream for opaque %" PRIu32
                " %s with error code %d", logHeader(), vbucket, opaque,
                status == ENGINE_SUCCESS ? "succeeded" : "failed", status);
            stream->acceptStream(status, add_opaque);
        } else {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Trying to add stream, but "
                "none exists (opaque: %" PRIu32 ", add_opaque: %" PRIu32 ")",
                logHeader(), vbucket, opaque, add_opaque);
        }
        opaqueMap_.erase(opaque);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s No opaque found for add stream response "
            "with opaque %" PRIu32, logHeader(), opaque);
    }
}

bool DcpConsumer::isValidOpaque(uint32_t opaque, uint16_t vbucket) {
    auto stream = findStream(vbucket);
    return stream && stream->getOpaque() == opaque;
}

void DcpConsumer::closeAllStreams() {

    // Need to synchronise the disconnect and clear, therefore use
    // external locking here.
    std::lock_guard<PassiveStreamMap> guard(streams);

    streams.for_each(
        [](PassiveStreamMap::value_type& iter) {
            iter.second->setDead(END_STREAM_DISCONNECTED);
        },
        guard);
    streams.clear(guard);
}

void DcpConsumer::vbucketStateChanged(uint16_t vbucket, vbucket_state_t state) {
    auto it = streams.erase(vbucket);
    if (it.second) {
        LOG(EXTENSION_LOG_INFO, "%s (vb %" PRIu16 ") State changed to "
            "%s, closing passive stream!",
            logHeader(), vbucket, VBucket::toString(state));
        auto& stream = it.first;
        uint32_t bytesCleared = stream->setDead(END_STREAM_STATE);
        flowControl.incrFreedBytes(bytesCleared);
        notifyConsumerIfNecessary(true/*schedule*/);
    }
}

ENGINE_ERROR_CODE DcpConsumer::handleNoop(struct dcp_message_producers* producers) {
    if (pendingEnableNoop) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 noopCtrlMsg.c_str(), noopCtrlMsg.size(),
                                 val.c_str(), val.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingEnableNoop = false;
        return ret;
    }

    if (pendingSendNoopInterval) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string interval = std::to_string(dcpNoopTxInterval.count());
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 noopIntervalCtrlMsg.c_str(),
                                 noopIntervalCtrlMsg.size(),
                                 interval.c_str(), interval.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingSendNoopInterval = false;
        return ret;
    }

    if ((ep_current_time() - lastMessageTime) > dcpIdleTimeout.count()) {
        LOG(EXTENSION_LOG_NOTICE, "%s Disconnecting because a message has not been "
            "received for %" PRIu64 " seconds. lastMessageTime was %u seconds ago.",
            logHeader(), uint64_t(dcpIdleTimeout.count()),
            (ep_current_time() - lastMessageTime));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpConsumer::handlePriority(struct dcp_message_producers* producers) {
    if (pendingSetPriority) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("high");
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 priorityCtrlMsg.c_str(), priorityCtrlMsg.size(),
                                 val.c_str(), val.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingSetPriority = false;
        return ret;
    }

    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpConsumer::handleExtMetaData(struct dcp_message_producers* producers) {
    if (pendingEnableExtMetaData) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 extMetadataCtrlMsg.c_str(),
                                 extMetadataCtrlMsg.size(),
                                 val.c_str(), val.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingEnableExtMetaData = false;
        return ret;
    }

    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpConsumer::handleValueCompression(struct dcp_message_producers* producers) {
    if (pendingEnableValueCompression) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 valueCompressionCtrlMsg.c_str(),
                                 valueCompressionCtrlMsg.size(),
                                 val.c_str(), val.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingEnableValueCompression = false;
        return ret;
    }

    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpConsumer::supportCursorDropping(struct dcp_message_producers* producers) {
    if (pendingSupportCursorDropping) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        std::string val("true");
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque,
                                 cursorDroppingCtrlMsg.c_str(),
                                 cursorDroppingCtrlMsg.size(),
                                 val.c_str(), val.size());
        ObjectRegistry::onSwitchThread(epe);
        pendingSupportCursorDropping = false;
        return ret;
    }

    return ENGINE_FAILED;
}

uint64_t DcpConsumer::incrOpaqueCounter()
{
    return (++opaqueCounter);
}

uint32_t DcpConsumer::getFlowControlBufSize()
{
    return flowControl.getFlowControlBufSize();
}

void DcpConsumer::setFlowControlBufSize(uint32_t newSize)
{
    flowControl.setFlowControlBufSize(newSize);
}

const std::string& DcpConsumer::getControlMsgKey(void)
{
    return connBufferCtrlMsg;
}

bool DcpConsumer::isStreamPresent(uint16_t vbucket)
{
    auto stream = findStream(vbucket);
    return stream && stream->isActive();
}

void DcpConsumer::notifyConsumerIfNecessary(bool schedule) {
    if (flowControl.isBufferSufficientlyDrained()) {
        /**
         * Notify memcached to get flow control buffer ack out.
         * We cannot wait till the ConnManager daemon task notifies
         * the memcached as it would cause delay in buffer ack being
         * sent out to the producer.
         */
        notifyPaused(schedule);
    }
}

SingleThreadedRCPtr<PassiveStream> DcpConsumer::findStream(uint16_t vbid) {
    auto it = streams.find(vbid);
    if (it.second) {
        return it.first;
    } else {
        return SingleThreadedRCPtr<PassiveStream>();
    }
}

void DcpConsumer::notifyPaused(bool schedule) {
    engine_.getDcpConnMap().notifyPausedConnection(this, schedule);
}
