/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/vbucket.h>

#include <chrono>
#include <string>

namespace cb::rangescan {

// client can request a Key or Key/Value scan.
enum class KeyOnly : char { No, Yes };

// Client can request for xattrs to be included for each document
enum class IncludeXattrs : char { No, Yes };

// How the key is to be interpreted in a range start/end
enum class KeyType : char { Inclusive, Exclusive };

// KeyView wraps a std::string_view and is the type passed through from
// executor to engine.
class KeyView {
public:
    /**
     * Construct a KeyView onto key/len
     */
    KeyView(const char* key, size_t len) : key{key, len} {
    }

    /**
     * Construct a key onto a view
     */
    KeyView(std::string_view key) : key{key} {
    }

    /**
     * Construct a key onto a view and set the type
     */
    KeyView(std::string_view key, KeyType type) : key{key}, type{type} {
    }

    std::string_view getKeyView() const {
        return key;
    }

    bool isInclusive() const {
        return type == KeyType::Inclusive;
    }

private:
    std::string_view key;
    KeyType type{KeyType::Inclusive};
};

struct CreateParameters {
    CreateParameters(Vbid vbid,
                     CollectionID cid,
                     KeyView start,
                     KeyView end,
                     KeyOnly keyOnly,
                     IncludeXattrs includeXattrs,
                     std::optional<SnapshotRequirements> snapshotReqs,
                     std::optional<SamplingConfiguration> samplingConfig,
                     std::string_view name = {})
        : vbid(vbid),
          cid(cid),
          start(start),
          end(end),
          keyOnly(keyOnly),
          includeXattrs(includeXattrs),
          snapshotReqs(snapshotReqs),
          samplingConfig(samplingConfig),
          name(name) {
    }

    /// The vbucket the scan is associated with
    Vbid vbid{0};

    /// The collection to scan
    CollectionID cid;

    /// scan start
    KeyView start;

    /// scan end
    KeyView end;

    /// key or value configuration
    KeyOnly keyOnly{KeyOnly::Yes};

    /// include xattrs with document
    IncludeXattrs includeXattrs{IncludeXattrs::No};

    /// optional snapshot requirements
    std::optional<SnapshotRequirements> snapshotReqs;

    /// optional sampling configuration
    std::optional<SamplingConfiguration> samplingConfig;

    /// a name (can be empty) that the client can provide
    std::string_view name;
};

/// All of the parameters required to continue a RangeScan included any I/O
/// complete phase of a request.
struct ContinueParameters {
    ContinueParameters(Vbid vbid,
                       Id uuid,
                       size_t itemLimit,
                       std::chrono::milliseconds timeLimit,
                       size_t byteLimit,
                       cb::engine_errc currentStatus)
        : vbid(vbid),
          uuid(uuid),
          itemLimit(itemLimit),
          timeLimit(timeLimit),
          byteLimit(byteLimit),
          currentStatus(currentStatus) {
    }

    /// The vbucket the scan is associated with
    Vbid vbid{0};

    /// The identifier of the scan to continue
    Id uuid;

    /// The maximum number of items the continue can return, 0 means no limit
    /// enforced
    size_t itemLimit{0};

    /// The maximum duration the continue can return, 0 means no limit enforced
    std::chrono::milliseconds timeLimit;

    /// When the number of bytes included in the scan exceeds this value, the
    /// continue is complete. This is not an absolute limit, but a trigger.
    /// A value of 0 disables this trigger.
    size_t byteLimit{0};

    /// The current status of the continue, required for driving the command
    /// via the async IO complete pattern
    cb::engine_errc currentStatus{cb::engine_errc::success};
};

const size_t MaximumNameSize = 50;

} // namespace cb::rangescan
