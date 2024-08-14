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

#include "ep_task.h"
#include <memcached/engine_error.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/storeddockey.h>
#include <memcached/vbucket.h>

class CookieIface;
class EPBucket;
struct RangeScanCreateToken;
class RangeScanDataHandlerIFace;

/**
 * RangeScanCreateTask performs the I/O required as part of creating a range
 * scan
 */
class RangeScanCreateTask : public EpTask {
public:
    RangeScanCreateTask(EPBucket& bucket,
                        CookieIface& cookie,
                        std::unique_ptr<RangeScanDataHandlerIFace> handler,
                        const cb::rangescan::CreateParameters& params,
                        std::unique_ptr<RangeScanCreateToken> scanData);

    bool run() override;

    std::string getDescription() const override {
        return "RangeScanCreateTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo: adjust this based on real data. This value is a placeholder
        return std::chrono::seconds(1);
    }

    static StoredDocKey makeStartStoredDocKey(CollectionID cid,
                                              cb::rangescan::KeyView key);
    static StoredDocKey makeEndStoredDocKey(CollectionID cid,
                                            cb::rangescan::KeyView key);

protected:
    /// @return status and uuid. The uuid is only valid is status is success
    std::pair<cb::engine_errc, cb::rangescan::Id> create();

    EPBucket& bucket;
    Vbid vbid;
    StoredDocKey start;
    StoredDocKey end;
    std::unique_ptr<RangeScanDataHandlerIFace> handler;
    CookieIface& cookie;
    cb::rangescan::KeyOnly keyOnly;
    cb::rangescan::IncludeXattrs includeXattrs;
    std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs;
    std::optional<cb::rangescan::SamplingConfiguration> samplingConfig;
    std::unique_ptr<RangeScanCreateToken> scanData;
    const std::string name;
};
