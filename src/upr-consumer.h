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

#ifndef SRC_UPR_CONSUMER_H_
#define SRC_UPR_CONSUMER_H_ 1

#include "config.h"

#include "tapconnection.h"
#include "upr-response.h"

class PassiveStream;

class UprConsumer : public Consumer {
typedef std::map<uint32_t, std::pair<uint32_t, uint16_t> > opaque_map;
public:

    UprConsumer(EventuallyPersistentEngine &e, const void *cookie,
                const std::string &n);

    ~UprConsumer() {}

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

    ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque, uint16_t vbucket);

    ENGINE_ERROR_CODE flush(uint32_t opaque, uint16_t vbucket);

    ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                      vbucket_state_t state);

    ENGINE_ERROR_CODE step(struct upr_message_producers* producers);

    ENGINE_ERROR_CODE handleResponse(protocol_binary_response_header *resp);

    void addStats(ADD_STAT add_stat, const void *c);

private:

    UprResponse* getNextItem();

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

    uint64_t opaqueCounter;
    Mutex streamMutex;
    std::map<uint16_t, PassiveStream*> streams_;
    opaque_map opaqueMap_;
};

#endif  // SRC_UPR_CONSUMER_H_
