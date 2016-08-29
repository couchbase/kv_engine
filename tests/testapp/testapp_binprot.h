/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

// Utility functions used to build up packets in the memcached binary protocol
#include <protocol/connection/client_connection.h>

#include <algorithm>
#include <cstdlib>
#include <sys/types.h>

#include <memcached/protocol_binary.h>

off_t mcbp_raw_command(Frame& frame,
                       uint8_t cmd,
                       const void* key,
                       size_t keylen,
                       const void* data,
                       size_t datalen);

/* Populate buf with a binary command with the given parameters. */
off_t mcbp_raw_command(char* buf, size_t bufsz,
                       uint8_t cmd,
                       const void* key, size_t keylen,
                       const void* dta, size_t dtalen);

off_t mcbp_flush_command(char* buf, size_t bufsz, uint8_t cmd, uint32_t exptime,
                         bool use_extra);

off_t mcbp_arithmetic_command(char* buf,
                              size_t bufsz,
                              uint8_t cmd,
                              const void* key,
                              size_t keylen,
                              uint64_t delta,
                              uint64_t initial,
                              uint32_t exp);

/**
 * Constructs a storage command using the give arguments into buf.
 *
 * @param buf the buffer to write the command into
 * @param bufsz the size of the buffer
 * @param cmd the command opcode to use
 * @param key the key to use
 * @param keylen the number of bytes in key
 * @param dta the value for the key
 * @param dtalen the number of bytes in the value
 * @param flags the value to use for the flags
 * @param exp the expiry time
 * @return the number of bytes in the storage command
 */
size_t mcbp_storage_command(char* buf,
                            size_t bufsz,
                            uint8_t cmd,
                            const void* key,
                            size_t keylen,
                            const void* dta,
                            size_t dtalen,
                            uint32_t flags,
                            uint32_t exp);

size_t mcbp_storage_command(Frame &frame,
                            uint8_t cmd,
                            const std::string &id,
                            const std::vector<uint8_t> &value,
                            uint32_t flags,
                            uint32_t exp);



/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   uint8_t cmd, uint16_t status);

void mcbp_validate_arithmetic(const protocol_binary_response_incr* incr,
                              uint64_t expected);


/**
 * This class may be used in the future for other commands. It's currently
 * restricted to subdoc.
 */
class TestCmd {
public:
    /**
     * Encode the command to a buffer.
     * @param buf The buffer
     * @note the buffer's contents are _not_ reset, and the encoded command
     *       is simply appended to it.
     */
    virtual void encode(std::vector<char>& buf) const {
        protocol_binary_request_header header;
        fillHeader(header);
        buf.insert(buf.end(), header.bytes,
                   &header.bytes[0] + sizeof(header.bytes));
    }

    virtual ~TestCmd(){}

    uint8_t getOp() const {
        return cmd;
    }

protected:
    TestCmd& setKeyPriv(const std::string& key_) {
        key = key_;
        return *this;
    }

    TestCmd& setCasPriv(uint64_t cas_) {
        cas = cas_;
        return *this;
    }

    TestCmd& setExpiryPriv(uint32_t expiry_) {
        has_expiry = true;
        expiry = expiry_;
        return *this;
    }

    TestCmd& clearExpiryPriv() {
        has_expiry = false;
        return *this;
    }

    TestCmd& setOpPriv(protocol_binary_command cmd_) {
        cmd = cmd_;
        return *this;
    }

    void fillHeader(protocol_binary_request_header& header,
                    size_t payload_len = 0, size_t extlen = 0) const {
        header.request.magic = PROTOCOL_BINARY_REQ;
        header.request.opcode = cmd;
        header.request.keylen = htons(key.size());
        header.request.extlen = extlen;
        header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        header.request.vbucket = 0;
        header.request.bodylen = htonl(key.size() + extlen + payload_len);
        header.request.opaque = 0xdeadbeef;
        header.request.cas = cas;
    }

public:
    protocol_binary_command cmd = PROTOCOL_BINARY_CMD_INVALID;
    std::string key;
    uint64_t cas = 0;
    uint32_t expiry = 0;
    bool has_expiry = false;


};

/**
 * For use with subclasses of @class TestCmd. This installs setter methods which
 * return the actual class rather than TestCmd.
 *
 * @code{.c++}
 * class SomeCmd : public TestCmdT<SomeCmd> {
 *   // ...
 * };
 * @endcode
 */
template <typename T>
class TestCmdT: public TestCmd {
public:
    T& setKey(const std::string& key_) {
        return static_cast<T&>(setKeyPriv(key_));
    }
    T& setCas(uint64_t cas_) {
        return static_cast<T&>(setCasPriv(cas_));
    }
    T& setOp(protocol_binary_command cmd_) {
        return static_cast<T&>(setOpPriv(cmd_));
    }
    T& setExpiry(uint32_t expiry_) {
        return static_cast<T&>(setExpiryPriv(expiry_));
    }
    T& clearExpiry() {
        return static_cast<T&>(clearExpiryPriv());
    }
};


class TestResponse {
public:
    bool isSuccess() const {
        return getStatus() == PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }
    uint16_t getStatus() const {
        return header.response.status;
    }
    size_t getExtlen() const {
        return header.response.extlen;
    }
    size_t getBodylen() const {
        return header.response.bodylen;
    }
    uint64_t getCas() const {
        return header.response.cas;
    }
    const char* getPayload() const {
        return raw_packet.data() + sizeof header.bytes;
    }
    const std::vector<char>& getRawPacket() const {
        return raw_packet;
    }
    bool isValid() const {
        return !raw_packet.empty() && header.response.magic == PROTOCOL_BINARY_RES;
    }

    const protocol_binary_response_header& getHeader() const {
        return header;
    }
    virtual void clear() {
        raw_packet.clear();
        std::memset(&header, 0, sizeof header);
    }
    virtual void assign(std::vector<char>&& srcbuf) {
        raw_packet = std::move(srcbuf);
        std::memcpy(header.bytes, raw_packet.data(),
                    std::min(sizeof header.bytes, raw_packet.size()));
    }
    TestResponse() {
        std::memset(&header, 0, sizeof header);
    }
    virtual ~TestResponse() {}

protected:
    protocol_binary_response_header header;
    std::vector<char> raw_packet;
};
