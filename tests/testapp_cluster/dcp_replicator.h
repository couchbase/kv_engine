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

#include <memory>

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
     * @return A new DCP replication object (which manages its own thread)
     */
    static std::unique_ptr<DcpReplicator> create(const Cluster& cluster,
                                                 Bucket& bucket);
};

} // namespace test
} // namespace cb
