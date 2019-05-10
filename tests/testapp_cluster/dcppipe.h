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

#include <event2/bufferevent.h>
#include <platform/socket.h>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace cb {
namespace mcbp {
class Header;
}

namespace test {

class DcpPipe {
public:
    DcpPipe(event_base* base,
            SOCKET psd,
            SOCKET csd,
            std::function<void()> replication_running_callback);
    ~DcpPipe();

    void addStreams(const std::vector<size_t>& vbuckets);

    void close();

protected:
    const cb::mcbp::Header* getFrame(bufferevent* bev);

    void read_callback(bufferevent* bev);
    void event_callback(bufferevent* bev, short event);

    static void read_callback(bufferevent* bev, void* ctx);

    static void event_callback(bufferevent* bev, short event, void* ctx);

    struct EventDeleter {
        void operator()(bufferevent* ev);
    };

    SOCKET psd;
    SOCKET csd;
    std::size_t awaiting;
    std::unique_ptr<bufferevent, EventDeleter> producer;
    std::unique_ptr<bufferevent, EventDeleter> consumer;
    std::function<void()> replication_running_callback;
    std::mutex mutex;
};

} // namespace test
} // namespace cb