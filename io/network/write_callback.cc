/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "write_callback.h"

namespace cb::io::network {

AsyncWriteCallback::AsyncWriteCallback(
        OutputStreamListener listener,
        std::function<void(AsyncWriteCallback&, std::unique_ptr<folly::IOBuf>)>
                sink)
    : listener(std::move(listener)), sink(std::move(sink)) {
}

AsyncWriteCallback::~AsyncWriteCallback() = default;

void AsyncWriteCallback::writeSuccess() noexcept {
    const auto nbytes = sendq.front().nbytes;
    sendq.pop_front();
    listener.transferred(nbytes);
}

void AsyncWriteCallback::writeErr(
        size_t bytesWritten, const folly::AsyncSocketException& ex) noexcept {
    sendq.pop_front();
    listener.error(fmt::format("Write error: {}", ex.what()));
}

std::size_t AsyncWriteCallback::getSendQueueSize() {
    size_t ret = 0;
    for (const auto& v : sendq) {
        ret += v.nbytes;
    }
    return ret;
}

void AsyncWriteCallback::send(std::unique_ptr<folly::IOBuf> iob) {
    if (iob) {
        sendq.emplace_back(iob->computeChainDataLength());
        sink(*this, std::move(iob));
    }
}

void AsyncWriteCallback::send(gsl::span<std::string_view> data) {
    std::size_t total = 0;

    for (const auto& d : data) {
        total += d.size();
    }

    if (!total) {
        return;
    }

    auto iob = folly::IOBuf::createCombined(total);
    for (const auto& d : data) {
        if (!d.empty()) {
            std::memcpy(iob->writableTail(), d.data(), d.size());
            iob->append(d.size());
        }
    }

    sendq.emplace_back(total);
    sink(*this, std::move(iob));
}

void AsyncWriteCallback::send(std::unique_ptr<SendBuffer> buffer) {
    auto payload = buffer->getPayload();
    auto iob = folly::IOBuf::wrapBuffer(payload.data(), payload.size());
    sendq.emplace_back(std::move(buffer));
    sink(*this, std::move(iob));
}

} // namespace cb::io::network
