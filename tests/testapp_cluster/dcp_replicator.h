/*
 *     Copyright 2019 Couchbase, Inc
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

#pragma once

#include "dcp_packet_filter.h"

#include <memory>
#include <vector>

namespace cb {
namespace test {

class Cluster;
class Bucket;

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
     * @return A new DCP replication object (which manages its own thread)
     */
    static std::unique_ptr<DcpReplicator> create(
            const Cluster& cluster,
            Bucket& bucket,
            DcpPacketFilter& packet_filter);
};

} // namespace test
} // namespace cb
