/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_replicator.h"

#include "bucket.h"
#include "cluster.h"
#include "dcppipe.h"

#include <event2/event.h>
#include <mcbp/mcbp.h>
#include <memcached/vbucket.h>
#include <platform/strerror.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <atomic>
#include <iostream>
#include <thread>

#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

namespace cb::test {

DcpReplicator::~DcpReplicator() = default;

class DcpReplicatorImpl : public DcpReplicator {
public:
    explicit DcpReplicatorImpl(DcpPacketFilter& packet_filter)
        : base(event_base_new()), packet_filter(packet_filter) {
    }

    ~DcpReplicatorImpl() override;

    /**
     * Set up replication connections between all nodes
     */
    void createPipes(const Cluster& cluster, Bucket& bucket);

    /**
     * Set up one replication connection using the given configuration
     */
    void createPipeForNodes(const Cluster& cluster,
                            Bucket& bucket,
                            ReplicationConfig specific);

    void start();

protected:
    static void thread_main(DcpReplicatorImpl& instance);

    void run();

    /**
     * Set up all replication connections for the node with the given ID
     */
    void createPipesForNode(const Cluster& cluster, Bucket& bucket, size_t me);

    /**
     * Set up a replication connection between the given nodes with the given
     * features
     */
    void createDcpPipe(const Cluster& cluster,
                       Bucket& bucket,
                       std::vector<cb::mcbp::Feature> features,
                       ReplicationConfig specific);

    struct BaseDeleter {
        void operator()(event_base* base) {
            event_base_free(base);
        }
    };
    std::atomic<size_t> num_ready{0};
    std::atomic_bool shutdown{false};
    std::unique_ptr<event_base, BaseDeleter> base;
    std::vector<std::unique_ptr<DcpPipe>> pipelines;
    std::unique_ptr<std::thread> thread;
    DcpPacketFilter& packet_filter;
};

DcpReplicatorImpl::~DcpReplicatorImpl() {
    shutdown = true;
    event_base_loopbreak(base.get());
    for (auto& pipe : pipelines) {
        pipe->close();
    }
    if (thread) {
        thread->join();
    }
}

void DcpReplicatorImpl::createPipes(const Cluster& cluster, Bucket& bucket) {
    for (size_t id = 0; id < cluster.size(); ++id) {
        createPipesForNode(cluster, bucket, id);
    }
}

void DcpReplicatorImpl::start() {
    thread = std::make_unique<std::thread>(
            [this] { return thread_main(*this); });
    // And wait for all of the streams to be set up
    while (num_ready < pipelines.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void DcpReplicatorImpl::thread_main(DcpReplicatorImpl& instance) {
    try {
        instance.run();
    } catch (const std::exception& e) {
        if (!instance.shutdown) {
            // Wait 5 secs before throwing the exception because the
            // error is probably caused by one of the memcached process
            // terminating and we want the monitor to be able to catch
            // the death and dump the logic before this thread goes
            // ahead and crash the process by no one catching the
            // exception.
            std::cerr << "Received exception: " << e.what()
                      << ". wait 5 sec before throwing the exception"
                      << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds{5});
            throw;
        }
    }
}

void DcpReplicatorImpl::run() {
    event_base_loop(base.get(), 0);
}

std::array<SOCKET, 2> createNotificationPipe() {
    std::array<SOCKET, 2> ret{};
    if (cb::net::socketpair(SOCKETPAIR_AF, SOCK_STREAM, 0, ret.data()) ==
        SOCKET_ERROR) {
        throw std::runtime_error("Can't create notify pipe: " +
                                 cb_strerror(cb::net::get_socket_error()));
    }

    for (auto sock : ret) {
        int flags = 1;
        const auto* flag_ptr = reinterpret_cast<const void*>(&flags);
        cb::net::setsockopt(
                sock, IPPROTO_TCP, TCP_NODELAY, flag_ptr, sizeof(flags));
        cb::net::setsockopt(
                sock, SOL_SOCKET, SO_REUSEADDR, flag_ptr, sizeof(flags));

        if (evutil_make_socket_nonblocking(sock) == -1) {
            throw std::runtime_error("Failed to enable non-blocking: " +
                                     cb_strerror(cb::net::get_socket_error()));
        }
    }

    return ret;
}

void DcpReplicatorImpl::createPipesForNode(const Cluster& cluster,
                                           Bucket& bucket,
                                           size_t me) {
    const auto& map = bucket.getVbucketMap();

    // Locate all of the vbuckets I'm supposed to be contain a replica for
    std::vector<std::vector<size_t>> vbids(cluster.size());

    for (size_t vb = 0; vb < map.size(); ++vb) {
        for (size_t node = 1; node < map[vb].size(); ++node) {
            if (map[vb][node] == int(me)) {
                vbids[map[vb][0]].push_back(vb);
            }
        }
    }

    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SELECT_BUCKET,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON}};

    for (size_t node = 0; node < vbids.size(); ++node) {
        if (vbids[node].empty()) {
            // I don't have any connections towards this node
            continue;
        }
        createDcpPipe(cluster, bucket, features, ReplicationConfig(node, me));
        pipelines.back()->addStreams(vbids[node]);
    }
}

void DcpReplicatorImpl::createPipeForNodes(const Cluster& cluster,
                                           Bucket& bucket,
                                           ReplicationConfig specific) {
    const auto& map = bucket.getVbucketMap();

    // Locate all of the vbuckets I'm supposed to be contain a replica for
    std::vector<std::vector<size_t>> vbids(cluster.size());

    for (size_t vb = 0; vb < map.size(); ++vb) {
        for (size_t node = 1; node < map[vb].size(); ++node) {
            if (map[vb][node] == int(specific.consumer)) {
                vbids[specific.consumer].push_back(vb);
            }
        }
    }

    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SELECT_BUCKET,
             cb::mcbp::Feature::SNAPPY,
             cb::mcbp::Feature::JSON}};

    if (vbids[specific.consumer].empty()) {
        // I don't have any connections towards this node
        return;
    }
    createDcpPipe(cluster, bucket, features, specific);
    pipelines.back()->addStreams(vbids[specific.consumer]);
}

void DcpReplicatorImpl::createDcpPipe(const Cluster& cluster,
                                      Bucket& bucket,
                                      std::vector<cb::mcbp::Feature> features,
                                      ReplicationConfig specific) {
    auto connection = cluster.getConnection(specific.producer);
    connection->authenticate("@admin", "password", "PLAIN");
    connection->selectBucket(bucket.getName());
    std::string name("n_" + std::to_string(specific.producer) + "->n_" +
                     std::to_string(specific.consumer));
    connection->setAgentName(name);
    connection->setFeatures(features);

    // Create and send a DCP open
    auto rsp = connection->execute(BinprotDcpOpenCommand{
            name, cb::mcbp::request::DcpOpenPayload::Producer});
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                "DcpReplicatorImpl::start: Failed to set up "
                "producer: " +
                name);
    }

    // DCP is set up from the Consumer
    auto mine = cluster.getConnection(specific.consumer);
    mine->authenticate("@admin", "password", "PLAIN");
    mine->selectBucket(bucket.getName());
    mine->setAgentName(name);
    mine->setFeatures(features);
    BinprotDcpOpenCommand consumerOpenCommand{name};

    consumerOpenCommand.setFlags(specific.dcpOpenFlags);

    if (specific.syncRepl) {
        consumerOpenCommand.setConsumerName("n_" +
                                            std::to_string(specific.consumer));
    }
    rsp = mine->execute(consumerOpenCommand);
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                "DcpReplicatorImpl::start: Failed to set up "
                "consumer: " +
                name);
    }

    pipelines.emplace_back(
            std::make_unique<DcpPipe>(base.get(),
                                      packet_filter,
                                      "n_" + std::to_string(specific.producer),
                                      "n_" + std::to_string(specific.consumer),
                                      connection->releaseSocket(),
                                      mine->releaseSocket(),
                                      createNotificationPipe(),
                                      [this]() { this->num_ready++; }));
    mine->close();
}

std::unique_ptr<DcpReplicator> DcpReplicator::create(
        const Cluster& cluster,
        Bucket& bucket,
        DcpPacketFilter& packet_filter,
        const std::vector<ReplicationConfig>& configs) {
    auto ret = std::make_unique<DcpReplicatorImpl>(packet_filter);
    if (configs.empty()) {
        ret->createPipes(cluster, bucket);
    } else {
        for (const auto& config : configs) {
            ret->createPipeForNodes(cluster, bucket, config);
        }
    }
    ret->start();
    return ret;
}

} // namespace cb::test
