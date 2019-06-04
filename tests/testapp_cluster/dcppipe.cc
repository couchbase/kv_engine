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

#include "dcppipe.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <memcached/vbucket.h>
#include <protocol/connection/client_mcbp_commands.h>

namespace cb {
namespace test {

DcpPipe::DcpPipe(event_base* base,
                 SOCKET psd,
                 SOCKET csd,
                 std::array<SOCKET, 2> notification_pipe,
                 std::function<void()> replication_running_callback)
    : psd(psd),
      csd(csd),
      notification_pipe(notification_pipe),
      replication_running_callback(std::move(replication_running_callback)) {
    evutil_make_socket_nonblocking(psd);
    evutil_make_socket_nonblocking(csd);

    producer.reset(bufferevent_socket_new(base, psd, 0));

    bufferevent_setcb(producer.get(),
                      DcpPipe::read_callback,
                      nullptr,
                      DcpPipe::event_callback,
                      static_cast<void*>(this));

    consumer.reset(bufferevent_socket_new(base, csd, 0));
    bufferevent_setcb(consumer.get(),
                      DcpPipe::read_callback,
                      nullptr,
                      DcpPipe::event_callback,
                      static_cast<void*>(this));

    notification.reset(bufferevent_socket_new(base, notification_pipe[1], 0));
    bufferevent_setcb(consumer.get(),
                      DcpPipe::read_callback,
                      nullptr,
                      DcpPipe::event_callback,
                      static_cast<void*>(this));

    bufferevent_enable(producer.get(), EV_READ);
    bufferevent_enable(consumer.get(), EV_READ);
}

void DcpPipe::addStreams(const std::vector<size_t>& vbuckets) {
    for (const auto& vb : vbuckets) {
        BinprotDcpAddStreamCommand cmd(0);
        cmd.setVBucket(Vbid(vb));
        std::vector<uint8_t> buf;
        cmd.encode(buf);
        bufferevent_write(consumer.get(), buf.data(), buf.size());
    }
    awaiting = vbuckets.size();
}

const cb::mcbp::Header* DcpPipe::getFrame(bufferevent* bev) {
    const cb::mcbp::Header* header;

    auto* in = bufferevent_get_input(bev);
    const auto size = evbuffer_get_length(in);
    if (size < sizeof(*header)) {
        return nullptr;
    }

    auto* ptr = evbuffer_pullup(in, sizeof(cb::mcbp::Header));
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }

    header = reinterpret_cast<const cb::mcbp::Header*>(ptr);
    if (!header->isValid()) {
        throw std::runtime_error(
                "DcpPipe::isPacketAvailable(): Invalid packet header "
                "detected");
    }

    const auto framesize = sizeof(*header) + header->getBodylen();
    if (size >= framesize) {
        ptr = evbuffer_pullup(in, framesize);
        if (ptr == nullptr) {
            throw std::bad_alloc();
        }

        return reinterpret_cast<const cb::mcbp::Header*>(ptr);
    }

    return nullptr;
}

void DcpPipe::read_callback(bufferevent* bev) {
    if (shutdown) {
        event_base_loopbreak(bufferevent_get_base(bev));
        return;
    }

    const cb::mcbp::Header* next;
    while ((next = getFrame(bev)) != nullptr) {
        // @todo I could call a filter
        const size_t size = sizeof(*next) + next->getBodylen();

        // cb::mcbp::dumpStream({(uint8_t*)(next), size}, std::cout);

        if (next->getOpcode() ==
            uint8_t(cb::mcbp::ClientOpcode::DcpAddStream)) {
            if (!cb::mcbp::is_response(cb::mcbp::Magic(next->getMagic()))) {
                throw std::runtime_error("Invalid magic for dcp add stream");
            }
            --awaiting;
            if (awaiting == 0 && replication_running_callback) {
                replication_running_callback();
            }
        } else {
            if (bev == producer.get()) {
                // From producer to consumer
                bufferevent_write(consumer.get(), next, size);
            } else {
                // From consumer to producer
                bufferevent_write(producer.get(), next, size);
            }
        }

        // Consume the data from the input pipe
        if (evbuffer_drain(bufferevent_get_input(bev), size) == -1) {
            throw std::runtime_error("Failed to drain buffer");
        }
    }
}

void DcpPipe::event_callback(bufferevent* bev, short event) {
    if (shutdown) {
        event_base_loopbreak(bufferevent_get_base(bev));
        return;
    }
    // @todo fixme
    throw std::runtime_error(" DcpPipe::event_callback: got event: " +
                             std::to_string(event));
}

void DcpPipe::read_callback(bufferevent* bev, void* ctx) {
    auto* instance = reinterpret_cast<DcpPipe*>(ctx);
    instance->read_callback(bev);
}

void DcpPipe::event_callback(bufferevent* bev, short event, void* ctx) {
    auto* instance = reinterpret_cast<DcpPipe*>(ctx);
    instance->event_callback(bev, event);
}
void DcpPipe::close() {
    shutdown = true;
    // write to the notification pipe so that there is something to
    // read in the other end to trigger that it does something
    cb::net::send(notification_pipe[0], this, sizeof(this), 0);
}

DcpPipe::~DcpPipe() {
    cb::net::closesocket(psd);
    cb::net::closesocket(csd);
    for (auto& sock : notification_pipe) {
        cb::net::closesocket(sock);
    }
}

void DcpPipe::EventDeleter::operator()(bufferevent* ev) {
    bufferevent_free(ev);
}

} // namespace test
} // namespace cb
