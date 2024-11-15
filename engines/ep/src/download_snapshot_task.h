/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_task.h"
#include <memcached/vbucket.h>

namespace cb {
enum class engine_errc;
}
class CookieIface;
namespace cb::snapshot {
class Cache;
}

/**
 * DownloadSnapshotTask is used to download a full snapshot from another
 * server.
 */
class DownloadSnapshotTask : public EpTask {
public:
    /**
     * Factory method to create an instance of the DownloadSnapshotTask
     *
     * @param cookie The cookie executing the DownloadSnapshot command
     * @param ep The ep-engine instance owning the task
     * @param manager The snapshot manager to utilize
     * @param vbid The vbucket to download the snapshot for
     * @param manifest The manifest containing the properties to perform
     *                 the download and the actual snapshot description
     * @return the newly created instance
     */
    static std::shared_ptr<DownloadSnapshotTask> create(
            CookieIface& cookie,
            EventuallyPersistentEngine& ep,
            cb::snapshot::Cache& manager,
            Vbid vbid,
            std::string_view manifest);

    /**
     * Pick up the result from the operation
     *
     * @return pair where the first element is the status code for the
     *              operation, and the second element a payload to return
     *              to the client (to insert in the error context)
     */
    virtual std::pair<cb::engine_errc, std::string> getResult() const = 0;

protected:
    explicit DownloadSnapshotTask(EventuallyPersistentEngine& ep);
};
