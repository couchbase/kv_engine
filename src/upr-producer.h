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

#ifndef SRC_UPR_PRODUCER_H_
#define SRC_UPR_PRODUCER_H_ 1

#include "config.h"

#include "tapconnection.h"
#include "upr-response.h"
#include "upr-stream.h"

class UprLogElement : public LogElement {

public:

    UprLogElement(uint32_t seqno, const VBucketEvent &event) :
        LogElement(seqno, event)
    {
    }

    UprLogElement(uint32_t seqno, const queued_item &qi)
    {
        seqno_ = seqno;
        event_ = TAP_MUTATION;
        vbucket_ = qi->getVBucketId();
        state_ = vbucket_state_active;
        item_ = qi;

        switch(item_->getOperation()) {
        case queue_op_set:
            event_ = UPR_MUTATION;
            break;
        case queue_op_del:
            event_ = UPR_DELETION;
            break;
        case queue_op_flush:
            event_ = UPR_FLUSH;
            break;
        case queue_op_checkpoint_end:
            event_ = UPR_SNAPSHOT_MARKER;
            break;
        default:
            break;
        }
    }
};

class UprProducer : public Producer {

public:

    UprProducer(EventuallyPersistentEngine &e, const void *cookie,
                const std::string &n);

    ~UprProducer();

    ENGINE_ERROR_CODE streamRequest(uint32_t flags, uint32_t opaque,
                                    uint16_t vbucket, uint64_t start_seqno,
                                    uint64_t end_seqno, uint64_t vbucket_uuid,
                                    uint64_t high_seqno,
                                    uint64_t *rollback_seqno,
                                    upr_add_failover_log callback);

    ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                     upr_add_failover_log callback);

    ENGINE_ERROR_CODE step(struct upr_message_producers* producers);

    ENGINE_ERROR_CODE handleResponse(protocol_binary_response_header *resp);

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void* c, uint16_t vbid);

    void aggregateQueueStats(ConnCounter* aggregator);

    void setDisconnect(bool disconnect);

    bool isTimeForNoop();

    void setTimeForNoop();

    void clearQueues();

    void appendQueue(std::list<queued_item> *q);

    size_t getBackfillQueueSize();

    bool windowIsFull();

    void flush();

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

private:

    UprResponse* getNextItem();

    bool isValidStream(uint32_t opaque, uint16_t vbucket);

    std::queue<UprResponse*> readyQ;
    std::map<uint16_t, active_stream_t> streams;
};

#endif  // SRC_UPR_PRODUCER_H_
