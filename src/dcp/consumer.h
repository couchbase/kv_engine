/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_DCP_CONSUMER_H_
#define SRC_DCP_CONSUMER_H_ 1

#include "config.h"

#include <relaxed_atomic.h>

#include "connmap.h"
#include "dcp/dcp-types.h"
#include "dcp/flow-control.h"
#include "dcp/stream.h"
#include "tapconnection.h"

class DcpResponse;
class StreamEndResponse;

class DcpConsumer : public Consumer, public Notifiable {
typedef std::map<uint32_t, std::pair<uint32_t, uint16_t> > opaque_map;
public:

    DcpConsumer(EventuallyPersistentEngine &e, const void *cookie,
                const std::string &n);

    ~DcpConsumer();

    ENGINE_ERROR_CODE addStream(uint32_t opaque, uint16_t vbucket,
                                uint32_t flags);

    ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    ENGINE_ERROR_CODE streamEnd(uint32_t opaque, uint16_t vbucket,
                                uint32_t flags);

    ENGINE_ERROR_CODE mutation(uint32_t opaque, const void* key, uint16_t nkey,
                               const void* value, uint32_t nvalue, uint64_t cas,
                               uint16_t vbucket, uint32_t flags,
                               uint8_t datatype, uint32_t locktime,
                               uint64_t bySeqno, uint64_t revSeqno,
                               uint32_t exptime, uint8_t nru, const void* meta,
                               uint16_t nmeta);

    ENGINE_ERROR_CODE deletion(uint32_t opaque, const void* key, uint16_t nkey,
                               uint64_t cas, uint16_t vbucket, uint64_t bySeqno,
                               uint64_t revSeqno, const void* meta,
                               uint16_t nmeta);

    ENGINE_ERROR_CODE expiration(uint32_t opaque, const void* key,
                                 uint16_t nkey, uint64_t cas, uint16_t vbucket,
                                 uint64_t bySeqno, uint64_t revSeqno,
                                 const void* meta, uint16_t nmeta);

    ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque,
                                     uint16_t vbucket,
                                     uint64_t start_seqno,
                                     uint64_t end_seqno,
                                     uint32_t flags);

    ENGINE_ERROR_CODE noop(uint32_t opaque);

    ENGINE_ERROR_CODE flush(uint32_t opaque, uint16_t vbucket);

    ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                      vbucket_state_t state);

    ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE handleResponse(protocol_binary_response_header *resp);

    bool doRollback(uint32_t opaque, uint16_t vbid, uint64_t rollbackSeqno);

    void addStats(ADD_STAT add_stat, const void *c);

    void aggregateQueueStats(ConnCounter& aggregator);

    void notifyStreamReady(uint16_t vbucket);

    void closeAllStreams();

    void vbucketStateChanged(uint16_t vbucket, vbucket_state_t state);

    process_items_error_t processBufferedItems();

    uint64_t incrOpaqueCounter();

    uint32_t getFlowControlBufSize();

    void setFlowControlBufSize(uint32_t newSize);

    static const std::string& getControlMsgKey(void);

    bool isStreamPresent(uint16_t vbucket);

    void cancelTask();

    void taskCancelled();

    bool notifiedProcesser(bool to);

    void setProcesserTaskState(enum process_items_error_t to);

    std::string getProcesserTaskStatusStr();

    /**
     * Check if the enough bytes have been removed from the
     * flow control buffer, for the consumer to send an ACK
     * back to the producer.
     *
     * @param schedule true if the notification is to be
     *                 scheduled
     */
    void notifyConsumerIfNecessary(bool schedule);

protected:
    /**
     * Records when the consumer last received a message from producer.
     * It is used to detect dead connections. The connection is closed
     * if a message, including a No-Op message, is not seen in a period
     * equal to twice the "noop interval".
     * It is protected so we can access from MockDcpConsumer, for
     * for testing purposes.
     */
    rel_time_t lastMessageTime;

    // Searches the streams map for a stream for vbucket ID. Returns the found
    // stream, or an empty pointer if none found.
    SingleThreadedRCPtr<PassiveStream> findStream(uint16_t vbid);

    DcpResponse* getNextItem();

    /**
     * Check if the provided opaque id is one of the
     * current open "session" id's
     *
     * @param opaque the provided opaque
     * @param vbucket the provided vbucket
     * @return true if the session is open, false otherwise
     */
    bool isValidOpaque(uint32_t opaque, uint16_t vbucket);

    void streamAccepted(uint32_t opaque, uint16_t status, uint8_t* body,
                        uint32_t bodylen);

    ENGINE_ERROR_CODE handleNoop(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE handlePriority(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE handleExtMetaData(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE handleValueCompression(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE supportCursorDropping(struct dcp_message_producers* producers);

    void notifyVbucketReady(uint16_t vbucket);

    uint64_t opaqueCounter;
    size_t processerTaskId;
    std::atomic<enum process_items_error_t> processerTaskState;

    DcpReadyQueue vbReady;
    std::atomic<bool> processerNotification;

    std::mutex readyMutex;
    std::list<uint16_t> ready;

    // Map of vbid -> passive stream. Map itself is atomic (thread-safe).
    typedef AtomicUnorderedMap<uint16_t,
                               SingleThreadedRCPtr<PassiveStream>> PassiveStreamMap;
    PassiveStreamMap streams;

    opaque_map opaqueMap_;

    Couchbase::RelaxedAtomic<uint32_t> backoffs;
    uint32_t noopInterval;

    bool pendingEnableNoop;
    bool pendingSendNoopInterval;
    bool pendingSetPriority;
    bool pendingEnableExtMetaData;
    bool pendingEnableValueCompression;
    bool pendingSupportCursorDropping;
    std::atomic<bool> taskAlreadyCancelled;

    FlowControl flowControl;

    static const std::string noopCtrlMsg;
    static const std::string noopIntervalCtrlMsg;
    static const std::string connBufferCtrlMsg;
    static const std::string priorityCtrlMsg;
    static const std::string extMetadataCtrlMsg;
    static const std::string valueCompressionCtrlMsg;
    static const std::string cursorDroppingCtrlMsg;
};

/*
 * Task that orchestrates rollback on Consumer,
 * runs in background.
 */
class RollbackTask : public GlobalTask {
public:
    RollbackTask(EventuallyPersistentEngine* e,
                 uint32_t opaque_, uint16_t vbid_,
                 uint64_t rollbackSeqno_, dcp_consumer_t conn,
                 const Priority &p):
        GlobalTask(e, p, 0, false), engine(e),
        opaque(opaque_), vbid(vbid_), rollbackSeqno(rollbackSeqno_),
        cons(conn) { }

    std::string getDescription() {
        return std::string("Running rollback task for vbucket %d", vbid);
    }

    bool run();

private:
    EventuallyPersistentEngine *engine;
    uint32_t opaque;
    uint16_t vbid;
    uint64_t rollbackSeqno;
    dcp_consumer_t cons;
};

#endif  // SRC_DCP_CONSUMER_H_
