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

class UprConsumer : public Consumer {

public:

    UprConsumer(EventuallyPersistentEngine &e,
                const void *cookie,
                const std::string &n) :
        Consumer(e, cookie, n), opaqueCounter(0) {
        setReserved(false);
    }

    ~UprConsumer() {}
    virtual bool processCheckpointCommand(uint8_t event, uint16_t vbucket,
                                          uint64_t checkpointId = -1);

    UprResponse* peekNextItem();

    void popNextItem();

    /**
     * Check if the provided opaque id is one of the
     * current open "session" id's
     *
     * @param opaque the provided opaque
     * @param vbucket the provided vbucket
     * @return true if the session is open, false otherwise
     */
    bool isValidOpaque(uint32_t opaque, uint16_t vbucket);

    ENGINE_ERROR_CODE addPendingStream(uint16_t vbucket,
                                       uint32_t opaque,
                                       uint32_t flags);

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint16_t vbucket);

    void streamAccepted(uint32_t opaque, uint16_t status);

private:
    uint64_t opaqueCounter;
    Mutex streamMutex;
    std::queue<UprResponse*> readyQ;
    std::map<uint16_t, Stream*> streams_;
    std::map<uint32_t, std::pair<uint32_t, uint16_t> > opaqueMap_;
};

#endif  // SRC_UPR_CONSUMER_H_
