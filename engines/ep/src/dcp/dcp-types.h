/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Forward declarations of various DCP types.
 * To utilise the types the correct header must also be included in
 * the users compilation unit.
 */

#pragma once

#include <cstdint>

template <class S, class Pointer, class Deleter> class SingleThreadedRCPtr;

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

/*
 * IncludeDeletedUserXattrs is used to state whether an active stream needs to
 * send the UserXattrs (if any) in the message for normal and sync DCP delete.
 */
enum class IncludeDeletedUserXattrs : bool {
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

/// PointInTimeEnabled is used to state whether an active stream wants to
/// stream the data including historical snapshots (and not just the latest
/// version of the data)
enum class PointInTimeEnabled : bool { Yes, No };

/** Does the stream support synchronous replication (i.e. acking Prepares)?
 * A Stream may also support just SyncWrites (receiving Prepares and Commits
 * without acking).
 */
enum class SyncReplication : char { No, SyncWrites, SyncReplication };

/**
 * OutOfOrderSnapshots determines if the streams created on the producer can
 * (when possible) return a snapshot in a different order from sequence order.
 *
 * Yes - enable OSO backfill
 * YesWithSeqnoAdvanced - before the OSO end marker, KV may send a seqno
 *       advanced that tells the client the maximum seqno of the snapshot.
 * No - feature disabled
 */
enum class OutOfOrderSnapshots : char { Yes, YesWithSeqnoAdvanced, No };
