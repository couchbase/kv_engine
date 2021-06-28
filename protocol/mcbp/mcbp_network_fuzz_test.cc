/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <mcbp/protocol/header.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>

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
            (size < (sizeof(*header) + header->getBodylen())) ||
            header->isResponse() ||
            cb::mcbp::is_server_magic(cb::mcbp::Magic(header->getMagic()))) {
            return;
        }

        if (!connection) {
            connection = std::make_unique<MemcachedConnection>(
                    "localhost", 11210, AF_UNSPEC, false);
            connection->connect();
            connection->setXerrorSupport(true);
        }

        const auto opcode = cb::mcbp::ClientOpcode(header->getOpcode());
        if (opcode == cb::mcbp::ClientOpcode::Quit ||
            opcode == cb::mcbp::ClientOpcode::Quitq ||
            opcode == cb::mcbp::ClientOpcode::Hello) {
            /// we don't want to loose xerror or close the connection
            return;
        }

        try {
            Frame frame;
            frame.payload.resize(sizeof(*header) + header->getBodylen());
            memcpy(frame.payload.data(), data, frame.payload.size());
            connection->sendFrame(frame);
        } catch (const std::exception& e) {
            std::cerr << "Failed to send data to the server: " << e.what()
                      << std::endl;
            std::exit(1);
        }

        try {
            BinprotResponse rsp;
            connection->recvResponse(rsp);
        } catch (const std::exception& e) {
            std::cerr << "Failed to receive data to the server: " << e.what()
                      << std::endl;
            std::exit(1);
        }
    }

protected:
    std::array<char, 512> blob;
    std::unique_ptr<MemcachedConnection> connection;
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
