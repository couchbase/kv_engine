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

#include <event2/bufferevent.h>
#include <platform/socket.h>
#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <vector>

namespace cb {
namespace mcbp {
class Header;
}

namespace test {

class DcpPipe {
public:
    DcpPipe(event_base* base,
            DcpPacketFilter& packet_filter,
            std::string producer_name,
            std::string consumer_name,

            SOCKET psd,
            SOCKET csd,
            std::array<SOCKET, 2> notification_pipe,
            std::function<void()> replication_running_callback);
    ~DcpPipe();

    void addStreams(const std::vector<size_t>& vbuckets);

    void close();

protected:
    std::vector<uint8_t> getFrame(bufferevent* bev);

    void read_callback(bufferevent* bev);
    void read_callback_passthrough(bufferevent* bev);
    void event_callback(bufferevent* bev, short event);

    static void read_callback(bufferevent* bev, void* ctx);
    static void read_callback_passthrough(bufferevent* bev, void* ctx);
    static void event_callback(bufferevent* bev, short event, void* ctx);

    struct EventDeleter {
        void operator()(bufferevent* ev);
    };

    SOCKET psd;
    SOCKET csd;
    std::array<SOCKET, 2> notification_pipe;
    std::size_t awaiting;
    std::atomic_bool shutdown{false};
    std::unique_ptr<bufferevent, EventDeleter> producer;
    std::unique_ptr<bufferevent, EventDeleter> consumer;
    std::unique_ptr<bufferevent, EventDeleter> notification;
    std::function<void()> replication_running_callback;
    DcpPacketFilter& packet_filter;
    std::string producer_name;
    std::string consumer_name;
};

} // namespace test
} // namespace cb