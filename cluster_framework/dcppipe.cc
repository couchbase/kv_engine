/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

namespace cb::test {

DcpPipe::DcpPipe(event_base* base,
                 DcpPacketFilter& packet_filter,
                 std::string producer_name,
                 std::string consumer_name,
                 SOCKET psd,
                 SOCKET csd,
                 std::array<SOCKET, 2> notification_pipe,
                 std::function<void()> replication_running_callback)
    : psd(psd),
      csd(csd),
      notification_pipe(notification_pipe),
      replication_running_callback(std::move(replication_running_callback)),
      packet_filter(packet_filter),
      producer_name(std::move(producer_name)),
      consumer_name(std::move(consumer_name)) {
    evutil_make_socket_nonblocking(psd);
    evutil_make_socket_nonblocking(csd);

    producer.reset(bufferevent_socket_new(base, psd, BEV_OPT_CLOSE_ON_FREE));

    bufferevent_setcb(producer.get(),
                      DcpPipe::read_callback,
                      nullptr,
                      DcpPipe::event_callback,
                      static_cast<void*>(this));

    consumer.reset(bufferevent_socket_new(base, csd, BEV_OPT_CLOSE_ON_FREE));
    bufferevent_setcb(consumer.get(),
                      DcpPipe::read_callback,
                      nullptr,
                      DcpPipe::event_callback,
                      static_cast<void*>(this));

    notification.reset(bufferevent_socket_new(
            base, notification_pipe[1], BEV_OPT_CLOSE_ON_FREE));
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
        BinprotDcpAddStreamCommand cmd(cb::mcbp::DcpAddStreamFlag::None);
        cmd.setVBucket(Vbid(vb));
        std::vector<uint8_t> buf;
        cmd.encode(buf);
        bufferevent_write(consumer.get(), buf.data(), buf.size());
    }
    awaiting = vbuckets.size();
}

std::vector<uint8_t> DcpPipe::getFrame(bufferevent* bev) {
    const cb::mcbp::Header* header;

    auto* in = bufferevent_get_input(bev);
    const auto size = evbuffer_get_length(in);
    if (size < sizeof(*header)) {
        return {};
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

        std::vector<uint8_t> ret;
        std::copy(ptr, ptr + framesize, std::back_inserter(ret));
        // Consume the data from the input pipe
        if (evbuffer_drain(in, framesize) == -1) {
            throw std::runtime_error("Failed to drain buffer");
        }
        return ret;
    }

    return {};
}

void DcpPipe::read_callback(bufferevent* bev) {
    if (shutdown) {
        event_base_loopbreak(bufferevent_get_base(bev));
        return;
    }

    std::vector<uint8_t> frame;
    while (!(frame = getFrame(bev)).empty()) {
        if (packet_filter) {
            if (bev == producer.get()) {
                packet_filter(producer_name, consumer_name, frame);
            } else {
                packet_filter(consumer_name, producer_name, frame);
            }

            if (frame.empty()) {
                // frame dropped.. look at the next one
                continue;
            }
        }

        const auto* header =
                reinterpret_cast<const cb::mcbp::Header*>(frame.data());
        if (header->getOpcode() ==
            uint8_t(cb::mcbp::ClientOpcode::DcpAddStream)) {
            if (!cb::mcbp::is_response(cb::mcbp::Magic(header->getMagic()))) {
                throw std::runtime_error("Invalid magic for dcp add stream");
            }
            --awaiting;
            if (awaiting == 0 && replication_running_callback) {
                replication_running_callback();

                // Setup / AddStream phase done, switch to an alternative
                // callback that just moves the stream from one socket to
                // another. That minimizes the runtime overhead of the proxy.
                // Note that by doing that we will skip any packet-filtering,
                // so we enable the alternative callback only if there is no
                // filtering set.
                if (!packet_filter) {
                    bufferevent_setcb(producer.get(),
                                      DcpPipe::read_callback_passthrough,
                                      nullptr,
                                      DcpPipe::event_callback,
                                      static_cast<void*>(this));
                    bufferevent_setcb(consumer.get(),
                                      DcpPipe::read_callback_passthrough,
                                      nullptr,
                                      DcpPipe::event_callback,
                                      static_cast<void*>(this));
                    // Switch to the new callback immediately
                    read_callback_passthrough(bev);
                    return;
                }
            }
        } else {
            if (bev == producer.get()) {
                // From producer to consumer
                bufferevent_write(consumer.get(), frame.data(), frame.size());
            } else {
                // From consumer to producer
                bufferevent_write(producer.get(), frame.data(), frame.size());
            }
        }
    }
}

void DcpPipe::read_callback_passthrough(bufferevent* bev) {
    auto* in = bufferevent_get_input(bev);
    if (bev == producer.get()) {
        // From producer to consumer
        auto* out = bufferevent_get_output(consumer.get());
        evbuffer_add_buffer(out, in);
    } else {
        // From consumer to producer
        auto* out = bufferevent_get_output(producer.get());
        evbuffer_add_buffer(out, in);
    }
}

void DcpPipe::event_callback(bufferevent* bev, short event) {
    if (shutdown) {
        event_base_loopbreak(bufferevent_get_base(bev));
        return;
    }
    // @todo fixme
    std::string decoded;
    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        decoded.append("eof,");
    }
    if ((event & BEV_EVENT_READING) == BEV_EVENT_READING) {
        decoded.append("reading,");
    }
    if ((event & BEV_EVENT_WRITING) == BEV_EVENT_WRITING) {
        decoded.append("writing,");
    }
    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        decoded.append("error,");
    }
    if ((event & BEV_EVENT_TIMEOUT) == BEV_EVENT_TIMEOUT) {
        decoded.append("timeout,");
    }
    if (!decoded.empty()) {
        decoded.pop_back();
        decoded = " (" + decoded + ")";
    }

    throw std::runtime_error(" DcpPipe::event_callback: got event: " +
                             std::to_string(event) + decoded);
}

void DcpPipe::read_callback(bufferevent* bev, void* ctx) {
    auto* instance = reinterpret_cast<DcpPipe*>(ctx);
    instance->read_callback(bev);
}

void DcpPipe::read_callback_passthrough(bufferevent* bev, void* ctx) {
    auto* instance = reinterpret_cast<DcpPipe*>(ctx);
    instance->read_callback_passthrough(bev);
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
    // The bufferevent_free close the socket bound to the bufferevent
    cb::net::closesocket(notification_pipe[0]);
}

void DcpPipe::EventDeleter::operator()(bufferevent* ev) {
    bufferevent_free(ev);
}

} // namespace cb::test
