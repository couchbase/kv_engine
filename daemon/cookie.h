/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <platform/uuid.h>
#include <stdexcept>

// Forward decl
namespace cb {
namespace mcbp {
struct Request;
struct Response;
} // namespace mcbp
} // namespace cb

class McbpConnection;

/**
 * The Cookie class represents the cookie passed from the memcached core
 * down through the engine interface to the engine.
 *
 * A cookie represents a single command context, and contains the packet
 * it is about to execute.
 *
 * By passing a common class as the cookie our notification model may
 * know what the argument is and provide it's own logic depending on
 * which field is set
 */
class Cookie {
public:
    explicit Cookie(McbpConnection& conn) : connection(conn) {
    }

    void validate() const {
        if (magic != 0xdeadcafe) {
            throw std::runtime_error(
                "Cookie::validate: Invalid magic detected");
        }
    }

    /**
     * Reset the Cookie object to allow it to be reused in the same
     * context as the last time.
     */
    void reset() {
        event_id.clear();
        error_context.clear();
        json_message.clear();
        packet = {};
    }

    /**
     * Get the unique event identifier created for this command. It should
     * be included in all log messages related to a given request, and
     * returned in the response sent back to the client.
     *
     * @return A "random" UUID
     */
    const std::string& getEventId() const {
        if (event_id.empty()) {
            event_id = to_string(cb::uuid::random());
        }

        return event_id;
    }

    void setEventId(std::string& uuid) {
        event_id = std::move(uuid);
    }

    /**
     * Does this cookie contain a UUID to be inserted into the error
     * message to be sent back to the client.
     */
    bool hasEventId() const {
        return !event_id.empty();
    }

    /**
     * Add a more descriptive error context to response sent back for
     * this command.
     */
    void setErrorContext(std::string message) {
        error_context = std::move(message);
    }

    /**
     * Get the context to send back for this command.
     */
    const std::string& getErrorContext() const {
        return error_context;
    }

    /**
     * Return the error "object" to return to the client.
     *
     * @return An empty string if no extended error information is being set
     */
    const std::string& getErrorJson();

    /**
     * Get the connection object the cookie is bound to.
     *
     * A cookie is bound to the conneciton at create time, and will never
     * switch connections
     */
    McbpConnection& getConnection() const {
        return connection;
    }

    /**
     * The cookie is created for every command we want to execute, but in
     * some cases we don't want to (or can't) get the entire packet content
     * in memory (for instance if a client tries to send us a 2GB packet we
     * want to just keep the header and disconnect the client instead).
     */
    enum class PacketContent { Header, Full };

    /**
     * Set the packet used by this command context.
     *
     * Note that the cookie does not _own_ the actual packet content,
     * as we don't want to perform an extra memory copy (the actual data
     * may belong to the network IO buffer code).
     */
    void setPacket(PacketContent content, cb::const_byte_buffer buffer);

    /**
     * Get the packet for this command / response packet
     *
     * @param content do you want the entire packet or not
     * @return the byte buffer containing the packet
     * @throws std::logic_error if the packet isn't available
     */
    cb::const_byte_buffer getPacket(
            PacketContent content = PacketContent::Full) const;

    /**
     * All of the (current) packet validators expects a void* and I don't
     * want to refactor all of them at this time.. Create a convenience
     * methods for now
     *
     * @return the current packet as a void pointer..
     */
    void* getPacketAsVoidPtr() const {
        return const_cast<void*>(static_cast<const void*>(getPacket().data()));
    }

    /**
     * Get the packet as a request packet
     *
     * @param content if we want just the header or the entire request
     *                available. In some cases we want to inspect the packet
     *                header before requiring the entire packet to be read
     *                off disk (ex: someone ship a 2GB packet and we don't want
     *                to read all of that into an in-memory buffer)
     * @return the packet if it is a request
     * @throws std::invalid_argument if the packet is of an invalid type
     * @throws std::logic_error if the packet is a response
     */
    const cb::mcbp::Request& getRequest(
            PacketContent content = PacketContent::Header) const;

    /**
     * Get the packet as a response packet
     *
     * @param content if we want just the header or the entire response
     *                available. In some cases we want to inspect the packet
     *                header before requiring the entire packet to be read
     *                off disk (ex: someone ship a 2GB packet and we don't want
     *                to read all of that into an in-memory buffer)
     * @return the packet if it is a response, or nullptr if it is a requests
     * @throws std::invalid_argument if the packet is of an invalid type
     */
    const cb::mcbp::Response& getResponse(
            PacketContent content = PacketContent::Header) const;

protected:
    /**
     * The connection object this cookie is bound to
     */
    McbpConnection& connection;

    /**
     * The magic byte is used for development only and will be removed when
     * we've successfully verified that we don't have any calls through the
     * engine API where we are passing invalid cookies (connection objects).
     *
     * We've always had the cookie meaning the connection object, so it could
     * be that I've missed some places where we pass something else than
     * a new cookie. We want to track those errors as soon as possible.
     */
    const uint64_t magic = 0xdeadcafe;

    mutable std::string event_id;
    std::string error_context;
    /**
     * A member variable to keep the data around until it's been safely
     * transferred to the client.
     */
    std::string json_message;

    /**
     * The input packet used in this command context
     */
    cb::const_byte_buffer packet;
};
