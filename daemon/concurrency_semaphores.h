/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/awaitable_semaphore.h>

/**
 * A singleton holding the various semaphores used to control the
 * concurrency of various components in the core
 */
class ConcurrencySemaphores {
public:
    ConcurrencySemaphores(const ConcurrencySemaphores&) = delete;
    static ConcurrencySemaphores& instance();

    /// authentication should be used to limit the parallellism of
    /// authentication requests (SASL START / STEP)
    cb::AwaitableSemaphore authentication{6};
    /// bucket_management should be used to serialize bucket create/pause/delete
    cb::AwaitableSemaphore bucket_management{4};
    /// cccp_notification is used to ensure that we only run one task at a
    /// time pushing the CCCP notifications
    cb::AwaitableSemaphore cccp_notification{1};
    /// ifconfig controls that only one interface gets updated at a time
    cb::AwaitableSemaphore ifconfig{1};
    /// rbac_reload makes sure that only one task is trying to read and
    /// update the RBAC database
    cb::AwaitableSemaphore rbac_reload{1};
    /// sasl_reload makes sure that only one client can force us to read
    /// the password database at the same time
    cb::AwaitableSemaphore sasl_reload{1};
    /// settings makes sure that only one client can run the settings reload
    /// at the same time
    cb::AwaitableSemaphore settings{1};
    /// Compress Cluster Config should be serialized.
    cb::AwaitableSemaphore compress_cluster_config{1};
    /// Encryption key and snapshot management must be serialized as:
    /// 1. we don't want key rotation to happen while generating snapshots
    ///    (which could result in keys being removed)
    /// 2. we don't want multiple snapshots to be created in parallel
    cb::AwaitableSemaphore encryption_and_snapshot_management{1};
    /// We don't want to be able to run too many tasks to read chunks
    /// off disk in parallel (each task may read up to 50MB of data)
    cb::AwaitableSemaphore read_vbucket_chunk{4};
    /// Used for limiting the number of IO tasks that can run for Fusion
    /// management (eg Mount, Unmount, Stat).
    cb::AwaitableSemaphore fusion_management{4};

protected:
    ConcurrencySemaphores();
};
