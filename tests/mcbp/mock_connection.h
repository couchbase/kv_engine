/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <daemon/connection.h>
#include <stdexcept>

struct FrontEndThread;

/// A mock connection which doesn't own a socket and isn't bound to libevent
class MockConnection : public Connection {
public:
    explicit MockConnection(struct FrontEndThread& thr) : Connection(thr) {
    }

    void copyToOutputStream(std::string_view data) override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void copyToOutputStream(gsl::span<std::string_view> data) override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    bool isPacketAvailable() const override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    cb::const_byte_buffer getAvailableBytes(size_t max) const override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void triggerCallback() override {
        throw std::runtime_error("MockConnection: Not implemented");
    }

protected:
    const cb::mcbp::Header& getPacket() const override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void drainInputPipe(size_t bytes) override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void disableReadEvent() override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    void enableReadEvent() override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
    size_t getSendQueueSize() const override {
        throw std::runtime_error("MockConnection: Not implemented");
    }
};
