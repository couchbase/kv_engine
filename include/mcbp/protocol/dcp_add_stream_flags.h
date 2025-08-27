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

#include <platform/define_enum_class_bitmask_functions.h>
#include <cstdint>
#include <string>

namespace cb::mcbp {
/// DcpAddStreamFlag is a bitmask where the following values are used:
enum class DcpAddStreamFlag : uint32_t {
    None = 0,
    TakeOver = 1,
    DiskOnly = 2,
    /**
     * Request that the server sets the end-seqno (ignoring any client input)
     * The end-seqno is set to the current high-seqno of the requested vbucket.
     */
    ToLatest = 4,
    /**
     * This flag is not used anymore, and should NOT be
     * set. It is replaced by DCP_OPEN_NO_VALUE.
     */
    NoValue = 8,
    /**
     * Indicate the server to add stream only if the vbucket
     * is active.
     * If the vbucket is not active, the stream request fails with
     * error cb::engine_errc::not_my_vbucket
     */
    ActiveVbOnly = 16,
    /**
     * Indicate the server to check for vb_uuid match even at start_seqno 0
     * before adding the stream successfully. If the flag is set and there is a
     * vb_uuid mismatch at start_seqno 0, then the server returns
     * cb::engine_errc::rollback error.
     */
    StrictVbUuid = 32,
    /**
     * Request that the server sets the start-seqno to the vbucket high-seqno.
     * The client is stating they have no DCP history and are not resuming, thus
     * the input snapshot start/end and UUID are ignored. Only supported for
     * stream_request (produce from latest)
     */
    FromLatest = 64,
    /**
     * Request that the server skips rolling back if the client is behind the
     * purge seqno, but the request is otherwise valid and satifiable (i.e. no
     * other rollback checks such as UUID mismatch fail). The client could end
     * up missing purged tombstones (and hence could end up never being told
     * about a document deletion). The intent of this flag is to allow clients
     * who ignore deletes to avoid rollbacks to zero which are sorely due to
     * them being behind the purge seqno.
     */
    IgnorePurgedTombstones = 128,

    /**
     * Request that the server transfers the cache before "regular" streaming.
     * The stream's start seqno is the limit used for cache transfer, resident
     * values with a seqno below or equal the start will be considered eligible
     * for the transfer (other configuration options may apply in the final
     * decision to transfer a resident value).
     * This flags has very limited use-cases, e.g. combining with
     * ToLatest/FromLatest will fail.
     *
     * stream-request documentation is where this flag will be fully described.
     */
    CacheTransfer = 256
};
std::string format_as(DcpAddStreamFlag flag);
DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(DcpAddStreamFlag);
} // namespace cb::mcbp
