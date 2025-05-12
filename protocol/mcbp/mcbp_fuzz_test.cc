/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "auditd/src/audit_descriptor_manager.h"
#include <auditd/couchbase_audit_events.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/mcaudit.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <cstdint>
#include <filesystem>
#include <iostream>

/**
 * A small test driver which allows libFuzzer to test the validator and
 * report buffer overflow etc.
 *
 * @todo Add a mode where we can use the input to send over the network
 *       (needs to detect server disconnect and reconnect)
 * @todo Add a mode where we can use the input to send the packets
 *       over the network _AFTER_ we've selected the bucket so that we
 *       will execute the full command on the server.
 */

/**
 * We need a connection object as the validator tries to use some of the
 * members in its connection, but we don't want a connection which owns a
 * socket.
 */
class FuzzConnection : public Connection {
public:
    explicit FuzzConnection(struct FrontEndThread& thr) : Connection(thr) {
    }

    void copyToOutputStream(std::string_view data) override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void copyToOutputStream(gsl::span<std::string_view> data) override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    bool isPacketAvailable() const override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    cb::const_byte_buffer getAvailableBytes() const override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void triggerCallback(bool) override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }

protected:
    const cb::mcbp::Header& getPacket() const override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void nextPacket() override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void disableReadEvent() override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    void enableReadEvent() override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
    size_t getSendQueueSize() const override {
        throw std::runtime_error("FuzzConnection: Not implemented");
    }
};

/**
 * The FuzzValidator is a singleton class which allows us to do the
 * initialization of the Connection; Settings and Thread object.
 */
class FuzzValidator {
public:
    /**
     * Test the validators with the following data and size and make
     * sure that we don't crash.
     *
     * We bypass the validator for some input values as that's already
     * been validated by the Connection object before creating the
     * cookie:
     *    * That there is at least the size of a header available
     *    * That the "isValid" method reports the header as valid
     *    * That there is at least the size of the header AND the rest
     *      of the frame is available
     *
     * @param data The random generated data to test
     * @param size The size of the generated data
     */
    void fuzz(const uint8_t* data, size_t size) {
        const auto* header = reinterpret_cast<const cb::mcbp::Header*>(data);
        if (size < sizeof(*header) || !header->isValid() ||
            (size < (sizeof(*header) + header->getBodylen()))) {
            return;
        }

        Cookie cookie(connection);
        cookie.setPacket(*header);
        cookie.validate();
    }

    FuzzValidator() : connection(thread) {
        connection.setAuthenticated({"@admin", cb::rbac::Domain::Local});
        connection.setCollectionsSupported(true);
        connection.enableDatatype(cb::mcbp::Feature::JSON);
        connection.enableDatatype(cb::mcbp::Feature::XATTR);
        connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
        connection.setAllowUnorderedExecution(true);
    }

protected:
    FrontEndThread thread;
    FuzzConnection connection;
};

/**
 * The callback libFuzzer will call to test a single input
 *
 * @param data Data to test
 * @param size Size of data to test
 * @return 0
 */
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    static FuzzValidator framework;
    framework.fuzz(data, size);
    return 0;
}

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv) {
    cb::logger::createBlackholeLogger();
    Settings::instance().setXattrEnabled(true);
    cb::rbac::initialize();
    initialize_audit();
    const auto path = std::filesystem::path(SOURCE_ROOT) / "protocol" / "mcbp" /
                      "mcbp_fuzz_test_rbac.json";
    cb::rbac::loadPrivilegeDatabase(path.generic_string());
    // We need to make sure that the AuditDescriptorManager gets initialized
    // before the Fuzz instance otherwise the AuditDescriptorManager gets
    // initialized as part of checking for an event, but the connection object
    // will use it in its destructor to submit a "session terminated" event
    AuditDescriptorManager::lookup(MEMCACHED_AUDIT_INVALID_PACKET);
    return 0;
}

#ifndef HAVE_LIBFUZZER
int main() {
    LLVMFuzzerInitialize(nullptr, nullptr);
    LLVMFuzzerTestOneInput(nullptr, 0);
    shutdown_audit();
    return 0;
}
#endif
