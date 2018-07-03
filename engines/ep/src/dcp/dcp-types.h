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

template <class S, class Pointer, class Deleter> class SingleThreadedRCPtr;
template <class C> class RCPtr;

// Implementation defined in dcp/consumer.h
class DcpConsumer;

// Implementation defined in dcp/producer.h
class DcpProducer;

// Implementation defined in dcp/stream.h
class Stream;

// Implementation defined in dcp/stream.h
class ActiveStream;

// Implementation defined in dcp/stream.h
class PassiveStream;

class ConnHandler;

enum process_items_error_t {
    all_processed,
    more_to_process,
    cannot_process,
    stop_processing
};

/*
 * IncludeValue is used to state whether an active stream needs to send the
 * value in the response.
 */
enum class IncludeValue {
    Yes,
    No,
};

/*
 * IncludeXattrs is used to state whether an active stream needs to send the
 * xattrs, (if any exist), in the response.
 */
enum class IncludeXattrs {
    Yes,
    No,
};

/*
 * IncludeDeleteTime is used to state whether an active stream needs to send the
 * tombstone creation time (only applies to backfilled items)
 */
enum class IncludeDeleteTime {
    Yes,
    No,
};

/**
 * SnappyEnabled is used to state whether an active stream supports snappy
 * compressed documents.
 */
enum class SnappyEnabled {
    Yes,
    No,
};

/**
 * ForceValueCompression is used to state whether an active stream
 * should forcefully compress all items.
 */
enum class ForceValueCompression {
    Yes,
    No,
};
