/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp_packet_filter.h"

#include <memory>
#include <vector>

namespace cb::test {

class Cluster;
class Bucket;

/**
 * A configuration object for setting up the desired DCP connection with the
 * DcpReplicator.
 */
struct ReplicationConfig {
    ReplicationConfig(size_t producer,
                      size_t consumer,
                      bool syncRepl = true,
                      uint32_t dcpOpenFlags = 0)
        : producer(producer),
          consumer(consumer),
          syncRepl(syncRepl),
          dcpOpenFlags(dcpOpenFlags) {
    }

    // Index in the Cluster::nodes vector of the producer
    size_t producer;

    // Index in the Cluster::nodes vector of the consumer
    size_t consumer;

    // Should the connection support SyncReplication? Setting this to false can
    // be used to mimic legacy connections.
    bool syncRepl;

    uint32_t dcpOpenFlags;
};

/**
 * The DCP replicator class is a holder class for full DCP replication
 * for a single Bucket. The replication runs in its own thread.
 */
class DcpReplicator {
public:
    virtual ~DcpReplicator();

    /**
     * Create a new instance of a DCP replication
     *
     * @param cluster The cluster the bucket belongs to
     * @param bucket The bucket to create the replication streams for
     * @param packet_filter An optional packet filter which is called for
     *                      with all of the packets going over the replication
     *                      streams for the bucket _before_ it is passed to
     *                      the other side. It is the content of the vector
     *                      which is put on the stream to the other end,
     *                      so the callback is free to inspect, modify or drop
     *                      the entire packet.
     * @param configs A vector of SpecificReplicationSetup to set up only the
     *                  desired connections between the given nodes with the
     *                  given features.
     * @return A new DCP replication object (which manages its own thread)
     */
    static std::unique_ptr<DcpReplicator> create(
            const Cluster& cluster,
            Bucket& bucket,
            DcpPacketFilter& packet_filter,
            const std::vector<ReplicationConfig>& configs = {});
};

} // namespace cb::test
