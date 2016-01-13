/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Forward declarations of various DCP types.
 * To utilise the types the correct header must also be included in
 * the users compilation unit.
 */

#pragma once

#include "atomic.h"

// Implementation defined in dcp/consumer.h
class DcpConsumer;
typedef SingleThreadedRCPtr<DcpConsumer> dcp_consumer_t;

// Implementation defined in dcp/producer.h
class DcpProducer;
typedef SingleThreadedRCPtr<DcpProducer> dcp_producer_t;

// Implementation defined in dcp/stream.h
class Stream;
typedef SingleThreadedRCPtr<Stream> stream_t;

// Implementation defined in dcp/stream.h
class PassiveStream;
typedef RCPtr<PassiveStream> passive_stream_t;
