/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "connection.h"

#include <libevent/utilities.h>

struct bufferevent;

/// Implementation of the Connection class using libevents bufferevent for IO.
class LibeventConnection : public Connection {
public:
    LibeventConnection(SOCKET sfd,
                       FrontEndThread& thr,
                       std::shared_ptr<ListeningPort> descr,
                       uniqueSslPtr sslStructure);
    ~LibeventConnection() override;
    void copyToOutputStream(std::string_view data) override;
    void copyToOutputStream(gsl::span<std::string_view> data) override;
    void chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) override;
    bool isPacketAvailable() const override;
    const cb::mcbp::Header& getPacket() const override;
    void drainInputPipe(size_t bytes) override;
    cb::const_byte_buffer getAvailableBytes(size_t max = 1024) const override;
    size_t getSendQueueSize() const override;
    void triggerCallback() override;
    void disableReadEvent() override;
    void enableReadEvent() override;

protected:
    LibeventConnection(FrontEndThread& thr) : Connection(thr) {
    }
    /// The bufferevent structure for the object
    cb::libevent::unique_bufferevent_ptr bev;

    std::string getOpenSSLErrors();

    /**
     * The callback method called from bufferevent when there is new data
     * available on the socket
     *
     * @param bev the bufferevent structure the event belongs to
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void read_callback(bufferevent* bev, void* ctx);
    void read_callback();

    /**
     * The callback method called from bufferevent when we're below the write
     * threshold (or all data is sent)
     *
     * @param bev the bufferevent structure the event belongs to
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void write_callback(bufferevent* bev, void* ctx);
    void write_callback();

    /**
     * The callback method called from bufferevent for "other" callbacks
     *
     * @param bev the bufferevent structure the event belongs to
     * @param event the event type
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void event_callback(bufferevent* bev, short event, void* ctx);

    /**
     * The initial read callback for SSL connections and perform
     * client certificate verification, authentication and authorization
     * if configured. When the action is performed we'll switch over to
     * the standard read callback.
     */
    static void ssl_read_callback(bufferevent*, void* ctx);
};
