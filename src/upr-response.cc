/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "config.h"

#include "upr-response.h"

/*
 * These constants are calculated from the size of the packets that are
 * created by each message when it gets sent over the wire. The packet
 * structures are located in the protocol_binary.h file in the memcached
 * project.
 */

const uint32_t StreamRequest::messageSize = 72;
const uint32_t AddStreamResponse::messageSize = 28;
const uint32_t SetVBucketStateResponse::messageSize = 24;
const uint32_t StreamEndResponse::messageSize = 28;
const uint32_t SetVBucketState::messageSize = 25;
const uint32_t SnapshotMarker::messageSize = 44;
const uint32_t MutationResponse::mutationMessageSize = 55;
const uint32_t MutationResponse::deletionMessageSize = 42;
