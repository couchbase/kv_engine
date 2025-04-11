/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <mcbp/protocol/dcp_add_stream_flags.h>

/**
 * Struct for storing all information for a stream-request.
 * Use of non-const for many members is intentional as a stream-request can
 * adjust the values depending on thr request flags and system state.
 */
class StreamRequestInfo {
public:
    StreamRequestInfo(cb::mcbp::DcpAddStreamFlag flags,
                      uint64_t vbucket_uuid,
                      uint64_t high_seqno,
                      uint64_t start_seqno,
                      uint64_t end_seqno,
                      uint64_t snap_start_seqno,
                      uint64_t snap_end_seqno)
        : flags{flags},
          vbucket_uuid{vbucket_uuid},
          high_seqno{high_seqno},
          start_seqno{start_seqno},
          end_seqno{end_seqno},
          snap_start_seqno{snap_start_seqno},
          snap_end_seqno{snap_end_seqno} {
    }

    const cb::mcbp::DcpAddStreamFlag flags;
    uint64_t vbucket_uuid;
    uint64_t high_seqno;
    uint64_t start_seqno;
    uint64_t end_seqno;
    uint64_t snap_start_seqno;
    uint64_t snap_end_seqno;
};