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
#include "ssl_utils.h"
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
#ifndef WIN32
    bool isSendfileSupported() const override {
        return !isTlsEnabled();
    }
#endif
    cb::engine_errc sendFile(int fd, off_t offset, off_t length) override;
    bool isPacketAvailable() const override;
    const cb::mcbp::Header& getPacket() const override;
    void nextPacket() override;
    cb::const_byte_buffer getAvailableBytes() const override;
    size_t getSendQueueSize() const override;
    void triggerCallback(bool force) override;
    void disableReadEvent() override;
    void enableReadEvent() override;

protected:
    LibeventConnection(FrontEndThread& thr) : Connection(thr) {
    }

    /// Non-virtual method as it is called from the destructor and
    /// that emits "warnings" in the IDE (as you shouldn't be calling
    /// virtual methods from the destructor)
    size_t getSendQueueSizeImpl() const;

    /// The bufferevent structure for the object
    cb::libevent::unique_bufferevent_ptr bev;

    /// The max value we would like to use for the watermark (typically
    /// 2x the socket send buffer size
    std::size_t max_send_watermark_size;

    /// Get a formatted string of all OpenSSL Errors
    std::string getOpenSSLErrors();
    /// Format all the OpenSSL Errors in the provided vector
    std::string formatOpenSslErrorCodes(
            const std::vector<unsigned long>& codes);
    /// Get all of the OpenSSL Errors currently set
    std::vector<unsigned long> getOpenSslErrorCodes();

    /// Shared callback for Read and Write events as there is
    /// no difference in the actions performed for the events. No need
    /// to duplicate the implementation.
    void rw_callback();
    static void rw_callback(bufferevent* bev, void* ctx);

    /// The callback method called from bufferevent for "other" callbacks.
    void event_callback(short event);

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
