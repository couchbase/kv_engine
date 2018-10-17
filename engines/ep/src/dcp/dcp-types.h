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

#include <cstdint>

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

enum end_stream_status_t {
    //! The stream ended due to all items being streamed
    END_STREAM_OK,
    //! The stream closed early due to a close stream message
    END_STREAM_CLOSED,
    //! The stream closed early because the vbucket state changed
    END_STREAM_STATE,
    //! The stream closed early because the connection was disconnected
    END_STREAM_DISCONNECTED,
    //! The stream was closed early because it was too slow (currently unused,
    //! but not deleted because it is part of the externally-visible API)
    END_STREAM_SLOW,
    //! The stream closed early due to backfill failure
    END_STREAM_BACKFILL_FAIL,
    //! The stream closed early because the vbucket is rolling back and
    //! downstream needs to reopen the stream and rollback too
    END_STREAM_ROLLBACK,

    //! All filtered collections have been removed so no more data can be sent.
    END_STREAM_FILTER_EMPTY
};

enum dcp_marker_flag_t {
    MARKER_FLAG_MEMORY = 0x01,
    MARKER_FLAG_DISK = 0x02,
    MARKER_FLAG_CHK = 0x04,
    MARKER_FLAG_ACK = 0x08
};

/*
 * IncludeValue is used to state whether an active stream needs to send the
 * value in the response.
 */
enum class IncludeValue : char {
    /** Include value in the response. */
    Yes,
    /**
     * Don't include value in the response. response.datatype will reflect the
     * sent payload.
     */
    No,
    /**
     * Don't include value in the response. Response.datatype will reflect the
     * underlying document's datatype.
     */
    NoWithUnderlyingDatatype,
};

/*
 * IncludeXattrs is used to state whether an active stream needs to send the
 * xattrs, (if any exist), in the response.
 */
enum class IncludeXattrs : bool {
    Yes,
    No,
};

/*
 * IncludeDeleteTime is used to state whether an active stream needs to send the
 * tombstone creation time (only applies to backfilled items)
 */
enum class IncludeDeleteTime : bool {
    Yes,
    No,
};

/**
 * SnappyEnabled is used to state whether an active stream supports snappy
 * compressed documents.
 */
enum class SnappyEnabled : bool {
    Yes,
    No,
};

/**
 * ForceValueCompression is used to state whether an active stream
 * should forcefully compress all items.
 */
enum class ForceValueCompression : bool {
    Yes,
    No,
};

/*
 * EnableExpiryOutput is used to state whether an active stream should
 * support outputting expiry messages.
 */
enum class EnableExpiryOutput : bool {
    Yes,
    No,
};

/**
 * MultipleStreamRequests determines if a Producer is allowed to create more
 * than one active stream for a vbucket, this is used in conjunction with
 * collection streaming.
 */
enum class MultipleStreamRequests : bool {
    Yes,
    No
};
