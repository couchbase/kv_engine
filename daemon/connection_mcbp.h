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

// @todo make this file "standalone" with includes and forward decl.

#include "config.h"

#include "datatype.h"
#include "dynamic_buffer.h"
#include "log_macros.h"
#include "settings.h"
#include "sslcert.h"
#include "statemachine_mcbp.h"

#include <cJSON.h>
#include <cbsasl/cbsasl.h>
#include <daemon/protocol/mcbp/command_context.h>
#include <daemon/protocol/mcbp/steppable_command_context.h>
#include <memcached/openssl.h>
#include <platform/cb_malloc.h>
#include <platform/make_unique.h>
#include <platform/pipe.h>
#include <platform/sized_buffer.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "connection.h"
#include "cookie.h"
#include "ssl_context.h"
#include "task.h"

/**
 * Adjust a message header structure by "consuming" nbytes of data.
 *
 * The msghdr structure contains an io-vector of data to send, and
 * by consuming data, we "rebuild" the io-vector by moving the
 * base pointer to the iovector past all of the fully transferred
 * elements, and move the last iov_base pointer the resulting bytes
 * forward (and reduce the last iov_len the same number of bytes)
 *
 * @param pipe The pipe structure where we may have stored data pointed
 *             to in the IO vector. We need to mark those as consumed
 *             when we skip them in the IO vector.
 * @param m The message header structure to update
 * @param nbytes The number of bytes to skip
 * @return The number of bytes left in the first element in the io-vector
 */
size_t adjust_msghdr(cb::Pipe& pipe, struct msghdr* m, ssize_t nbytes);

class McbpConnection : public Connection {
public:
    McbpConnection() = delete;

    McbpConnection(SOCKET sfd, event_base* b, const ListeningPort& ifc);

    virtual ~McbpConnection();


    virtual void initiateShutdown() override;

    virtual void signalIfIdle(bool logbusy, int workerthread) override;


    virtual void setPriority(const Priority& priority) override;

    void setState(TaskFunction next_state);

    TaskFunction getState() const {
        return stateMachine->getCurrentTask();
    }

    const char* getStateName() const {
        return stateMachine->getCurrentTaskName();
    }

    virtual bool isDCP() const override {
        return dcp;
    }

    void setDCP(bool dcp) {
        McbpConnection::dcp = dcp;
    }

    bool isDcpXattrAware() const {
        return dcpXattrAware;
    }

    void setDcpXattrAware(bool dcpXattrAware) {
        McbpConnection::dcpXattrAware = dcpXattrAware;
    }

    bool isDcpCollectionAware() const {
        return dcpCollectionAware;
    }

    void setDcpCollectionAware(bool dcpCollectionAware) {
        McbpConnection::dcpCollectionAware = dcpCollectionAware;
    }

    /**
     * Get the DocNamespace for a DcpMessage (mutation/deletion/expiration)
     * If the connection is dcp aware and the passed length is not zero, then
     * the document belongs to a collection.
     * @param collectionLength the length sent by the producer
     * @return the DocNamespace (DefaultCollection or Collections)
     */
    DocNamespace getDocNamespaceForDcpMessage(uint8_t collectionLength) const {
        DocNamespace ns = DocNamespace::DefaultCollection;
        if (isDcpCollectionAware() && collectionLength != 0) {
            // Collection aware DCP sends non-zero collectionLength for
            // documents that belong to a collection.
            ns = DocNamespace::Collections;
        }
        return ns;
    }

    bool isDcpNoValue() const {
        return dcpNoValue;
    }

    void setDcpNoValue(bool dcpNoValue) {
        McbpConnection::dcpNoValue = dcpNoValue;
    }

    unique_cJSON_ptr toJSON() const override;


    virtual const Protocol getProtocol() const override;

    /**
     * Log the current connection if its execution time exceeds the
     * threshold for the command
     *
     * @param elapsed the number of ms elapsed while executing the command
     *
     * @todo refactor this into the command object when we introduce them
     */
    void maybeLogSlowCommand(const std::chrono::milliseconds& elapsed) const;

    /**
     * Return the opaque value for the command being processed
     */
    uint32_t getOpaque() const {
        return binary_header.request.opaque;
    }

    uint8_t getCmd() const {
        return cmd;
    }

    void setCmd(uint8_t cmd) {
        McbpConnection::cmd = cmd;
    }

    /**
     * Return the key of the currently processing command.
     * @return the buffer to the key.
     */
    cb::const_char_buffer getKey() const {
        auto *pkt = reinterpret_cast<const char *>(getPacket(cookie));
        cb::const_char_buffer ret;
        ret.len = binary_header.request.keylen;
        ret.buf = pkt + sizeof binary_header.bytes + binary_header.request.extlen;
        return ret;
    }

    /**
     * Get a printable key from the header. Replace all non-printable
     * charachters with '.'
     */
    std::string getPrintableKey() const;

    const protocol_binary_request_header& getBinaryHeader() const {
        return binary_header;
    }

    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;

    /**
     * Decrement the number of events to process and return the new value
     */
    int decrementNumEvents() {
        return --numEvents;
    }

    /**
     * Set the number of events to process per timeslice of the worker
     * thread before yielding.
     */
    void setNumEvents(int nevents) {
        McbpConnection::numEvents = nevents;
    }

    /**
     * Get the maximum number of events we should process per invocation
     * for a connection object (to avoid starvation of other connections)
     */
    int getMaxReqsPerEvent() const {
        return max_reqs_per_event;
    }

    /**
     * Update the settings in libevent for this connection
     *
     * @param mask the new event mask to get notified about
     */
    bool updateEvent(const short new_flags);

    /**
     * Reapply the event mask (in case of a timeout we might want to do
     * that)
     */
    bool reapplyEventmask();

    /**
     * Unregister the event structure from libevent
     * @return true if success, false otherwise
     */
    bool unregisterEvent();

    /**
     * Register the event structure in libevent
     * @return true if success, false otherwise
     */
    bool registerEvent();

    bool isRegisteredInLibevent() const {
        return registered_in_libevent;
    }

    short getEventFlags() const {
        return ev_flags;
    }

    short getCurrentEvent() const {
        return currentEvent;
    }

    void setCurrentEvent(short ev) {
        currentEvent = ev;
    }

    /** Is the current event a readevent? */
    bool isReadEvent() const {
        return currentEvent & EV_READ;
    }

    /** Is the current event a writeevent? */
    bool isWriteEvent() const {
        return currentEvent & EV_WRITE;
    }

    /**
     * Shrinks a connection's buffers if they're too big.  This prevents
     * periodic large "get" requests from permanently chewing lots of server
     * memory.
     *
     * This should only be called in between requests since it can wipe output
     * buffers!
     */
    void shrinkBuffers();

    /**
     * Receive data from the socket
     *
     * @param where to store the result
     * @param nbytes the size of the buffer
     *
     * @return the number of bytes read, or -1 for an error
     */
    int recv(char* dest, size_t nbytes);

    /**
     * Send data over the socket
     *
     * @param m the message header to send
     * @return the number of bytes sent, or -1 for an error
     */
    int sendmsg(struct msghdr* m);

    enum class TransmitResult {
        /** All done writing. */
            Complete,
        /** More data remaining to write. */
            Incomplete,
        /** Can't write any more right now. */
            SoftError,
        /** Can't write (c->state is set to conn_closing) */
            HardError
    };

    /**
     * Transmit the next chunk of data from our list of msgbuf structures.
     *
     * Returns:
     *   Complete   All done writing.
     *   Incomplete More data remaining to write.
     *   SoftError Can't write any more right now.
     *   HardError Can't write (c->state is set to conn_closing)
     */
    TransmitResult transmit();

    enum class TryReadResult {
        /** Data received on the socket and ready to parse */
            DataReceived,
        /** No data received on the socket */
            NoDataReceived,
        /** The client closed the connection */
            SocketClosed,
        /** An error occurred on the socket */
            SocketError,
        /** Failed to allocate more memory for the input buffer */
            MemoryError
    };

    /**
     * read from network as much as we can, handle buffer overflow and
     * connection close. Before reading, move the remaining incomplete fragment
     * of a command (if any) to the beginning of the buffer.
     *
     * @return enum try_read_result
     */
    TryReadResult tryReadNetwork();

    const TaskFunction getWriteAndGo() const {
        return write_and_go;
    }

    void setWriteAndGo(TaskFunction write_and_go) {
        McbpConnection::write_and_go = write_and_go;
    }

    /**
     * Get the number of entries in use in the IO Vector
     */
    int getIovUsed() const {
        return iovused;
    }

    /**
     * Adds a message header to a connection.
     *
     * @param reset set to true to reset all message headers
     * @throws std::bad_alloc
     */
    void addMsgHdr(bool reset);

    /**
     * Add a chunk of memory to the the IO vector to send
     *
     * @param buf pointer to the data to send
     * @param len number of bytes to send
     * @throws std::bad_alloc
     */
    void addIov(const void* buf, size_t len);

    /**
     * Release all of the items we've saved a reference to
     */
    void releaseReservedItems() {
        ENGINE_HANDLE* handle = reinterpret_cast<ENGINE_HANDLE*>(bucketEngine);
        for (auto* it : reservedItems) {
            bucketEngine->release(handle, this, it);
        }
        reservedItems.clear();
    }

    /**
     * Put an item on our list of reserved items (which we should release
     * at a later time through releaseReservedItems).
     *
     * @return true if success, false otherwise
     */
    bool reserveItem(void* item) {
        try {
            reservedItems.push_back(item);
            return true;
        } catch (std::bad_alloc) {
            return false;
        }
    }

    void releaseTempAlloc() {
        for (auto* ptr : temp_alloc) {
            cb_free(ptr);
        }
        temp_alloc.resize(0);
    }

    bool pushTempAlloc(char* ptr) {
        try {
            temp_alloc.push_back(ptr);
            return true;
        } catch (std::bad_alloc) {
            LOG_WARNING(this,
                        "%u: FATAL: failed to allocate space to keep temporary buffer",
                        getId());
            return false;
        }
    }

    bool isNoReply() const {
        return noreply;
    }

    void setNoReply(bool noreply) {
        McbpConnection::noreply = noreply;
    }


    /**
     * Enable the datatype which corresponds to the feature
     *
     * @param feature mcbp::Feature::JSON|XATTR|SNAPPY
     * @throws if feature does not correspond to a datatype
     */
    void enableDatatype(cb::mcbp::Feature feature) {
        datatype.enable(feature);
    }

    /**
     * Disable all the datatypes
     */
    void disableAllDatatypes() {
        datatype.disableAll();
    }

    /**
     * Given the input datatype, return only those which are enabled for the
     * connection.
     *
     * @param dtype the set to intersect against the enabled set
     * @returns the intersection of the enabled bits and dtype
     */
    protocol_binary_datatype_t getEnabledDatatypes(
            protocol_binary_datatype_t dtype) const {
        return datatype.getIntersection(dtype);
    }

    /**
     * @return true if the all of the dtype datatypes are all enabled
     */
    bool isDatatypeEnabled(protocol_binary_datatype_t dtype) const {
        bool rv = datatype.isEnabled(dtype);

        // If the bucket has disabled xattr, then we must reflect that in the
        // returned value
        if (rv && mcbp::datatype::is_xattr(dtype) &&
            !selectedBucketIsXattrEnabled()) {
            rv = false;
        }
        return rv;
    }

    /**
     * @return true if JSON datatype is enabled
     */
    bool isJsonEnabled() const {
        return datatype.isJsonEnabled();
    }

    /**
     * @return true if compression datatype is enabled
     */
    bool isSnappyEnabled() const {
        return datatype.isSnappyEnabled();
    }

    /**
     * @return true if the XATTR datatype is enabled
     */
    bool isXattrEnabled() const {
        return datatype.isXattrEnabled();
    }

    virtual bool isSupportsMutationExtras() const override {
        return supports_mutation_extras;
    }

    void setSupportsMutationExtras(bool supports_mutation_extras) {
        McbpConnection::supports_mutation_extras = supports_mutation_extras;
    }

    /**
     * Clear the dynamic buffer
     */
    void clearDynamicBuffer() {
        dynamicBuffer.clear();
    }

    /**
     * Grow the dynamic buffer to
     */
    bool growDynamicBuffer(size_t needed) {
        return dynamicBuffer.grow(needed);
    }

    DynamicBuffer& getDynamicBuffer() {
        return dynamicBuffer;
    }

    hrtime_t getStart() const {
        return start;
    }

    void setStart(hrtime_t start) {
        McbpConnection::start = start;
    }

    uint64_t getCAS() const {
        return cas;
    }

    void setCAS(uint64_t cas) {
        McbpConnection::cas = cas;
    }


    const ENGINE_ERROR_CODE& getAiostat() const {
        return aiostat;
    }

    void setAiostat(const ENGINE_ERROR_CODE& aiostat) {
        McbpConnection::aiostat = aiostat;
    }

    bool isEwouldblock() const {
        return ewouldblock;
    }

    void setEwouldblock(bool ewouldblock) {
        McbpConnection::ewouldblock = ewouldblock;
    }

    /**
     *  Get the command context stored for this command
     */
    CommandContext* getCommandContext() const {
        return commandContext.get();
    }

    /**
     * Get the command context stored for this command as
     * the given type or make it if it doesn't exist
     *
     * @tparam ContextType CommandContext type to create
     * @return the context object
     * @throws std::logic_error if the object is the wrong type
     */
    template <typename ContextType, typename... Args>
    ContextType& obtainContext(Args&&... args) {
        auto* context = commandContext.get();
        if (context == nullptr) {
            auto* ret = new ContextType(std::forward<Args>(args)...);
            commandContext.reset(ret);
            return *ret;
        }
        auto* ret = dynamic_cast<ContextType*>(context);
        if (ret == nullptr) {
            throw std::logic_error(
                    std::string("McbpConnection::obtainContext<") +
                    typeid(ContextType).name() +
                    ">(): context is not the requested type");
        }
        return *ret;
    }

    /**
     * Get the command context which SHOULD be a steppable command context
     *
     * @return the context object
     * @throws std::logic_error if the object is non-existent or the wrong type
     */
    SteppableCommandContext& getSteppableCommandContext() const {
        auto* context = commandContext.get();
        if (context == nullptr) {
            throw std::logic_error(
                "McbpConnection::getSteppableCommandContext(): context should not be nullptr");
        }
        auto* ret = dynamic_cast<SteppableCommandContext*>(context);
        if (ret == nullptr) {
            throw std::logic_error(
                "McbpConnection::getSteppableCommandContext(): context is not steppable");
        }
        return *ret;
    }

    /**
     *  Set the command context stored for this command
     */
    void setCommandContext(CommandContext* cmd_context) {
        McbpConnection::commandContext.reset(cmd_context);
    }

    /**
     * Reset the command context
     *
     * Release the allocated resources and set the command context to nullptr
     */
    void resetCommandContext() {
        commandContext.reset();
    }

    /**
     * Log the start of processing a command received from the client in the
     * generic form which (may change over time, but currently it) looks like:
     *
     *     id> COMMAND KEY
     */
    void logCommand() const;

    /**
     * Log the end of processing a command and the result of the command:
     *
     *     id< COMMAND KEY - STATUS
     *
     * @param code The execution result
     */
    void logResponse(ENGINE_ERROR_CODE code) const;

    /**
     * Try to enable SSL for this connection
     *
     * @param cert the SSL certificate to use
     * @param pkey the SSL private key to use
     * @return true if successful, false otherwise
     */
    bool enableSSL(const std::string& cert, const std::string& pkey) {
        if (ssl.enable(cert, pkey)) {
            if (settings.getVerbose() > 1) {
                ssl.dumpCipherList(getId());
            }

            return true;
        }

        return false;
    }

    /**
     * Disable SSL for this connection
     */
    void disableSSL() {
        ssl.disable();
    }

    /**
     * Is SSL enabled for this connection or not?
     *
     * @return true if the connection is running over SSL, false otherwise
     */
    bool isSslEnabled() const {
        return ssl.isEnabled();
    }

    /**
     * Do we have any pending input data on this connection?
     */
    bool havePendingInputData() {
        return (!read->empty() || ssl.havePendingInputData());
    }

    /**
     * Try to find RBAC user from the client ssl cert
     *
     * @return true if username has been linked to RBAC or ssl cert was not
     * presented by the client.
     */
    bool tryAuthFromSslCert(const std::string& userName);

    virtual bool shouldDelete() override;

    virtual void runEventLoop(short which) override;

    /**
     * Input buffer containing the data we've read of the socket. It is
     * assigned to the connection when the connection is to be served, and
     * returned to the thread context if the pipe is empty when we're done
     * serving this connection.
     */
    std::unique_ptr<cb::Pipe> read;

    /** Write buffer */
    std::unique_ptr<cb::Pipe> write;

    const void* getCookie() const {
        return &cookie;
    }

    Cookie& getCookieObject() {
        return cookie;
    }

    /**
     * Obtain a pointer to the packet for the Cookie's connection
     */
    static void* getPacket(const Cookie& cookie) {
        auto avail = cookie.connection.read->rdata();
        return const_cast<void*>(static_cast<const void*>(avail.data()));
    }

    /**
      * Check to see if the next packet to process is completely received
      * and available in the input pipe.
      *
      * @return true if we've got the entire packet, false otherwise
      */
    bool isPacketAvailable() const {
        auto buffer = read->rdata();

        if (buffer.size() < sizeof(cb::mcbp::Request)) {
            // we don't have the header, so we can't even look at the body
            // length
            return false;
        }

        const auto* req =
                reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
        return buffer.size() >= sizeof(cb::mcbp::Request) + req->getBodylen();
    }

    /**
     *  Invoke the validator function(s) for the command
     */
    protocol_binary_response_status validateCommand(protocol_binary_command command);

    /**
     * Is SASL disabled for this connection or not? (connection authenticated
     * with SSL certificates will disable the possibility re-authenticate over
     * SASL)
     */
    bool isSaslAuthEnabled() const {
        return saslAuthEnabled;
    }

    bool selectedBucketIsXattrEnabled() const;

    /**
     * Try to process some of the server events. This may _ONLY_ be performed
     * after we've completely transferred the response for one command, and
     * before we start executing the next one.
     *
     * @return true if processing server events set changed the path in the
     *              state machine (and the current task should be
     *              terminated immediately)
     */
    bool processServerEvents();

protected:
    void runStateMachinery();

    /**
     * Initialize the event structure and add it to libevent
     *
     * @return true upon success, false otherwise
     */
    bool initializeEvent();

    void logResponse(const char* reason) const;

    /**
     * The state machine we're currently using
     */
    std::unique_ptr<McbpStateMachine> stateMachine;

    /** Is this connection used by a DCP connection? */
    bool dcp;

    /** Is this DCP channel XAttrAware */
    bool dcpXattrAware;

    /** Shuld values be stripped off? */
    bool dcpNoValue;

    /** Is this DCP channel collection aware? */
    bool dcpCollectionAware;

    int max_reqs_per_event; /** The maximum requests we can process in a worker
                                thread timeslice */
    /**
     * number of events this connection can process in a single worker
     * thread timeslice
     */
    int numEvents;

    /** current command being processed */
    uint8_t cmd;

    // Members related to libevent

    /** Is the connection currently registered in libevent? */
    bool registered_in_libevent;
    /** The libevent object */
    struct event event;
    /** The current flags we've registered in libevent */
    short ev_flags;
    /** which events were just triggered */
    short currentEvent;
    /** When we inserted the object in libevent */
    rel_time_t ev_insert_time;
    /** Do we have an event timeout or not */
    bool ev_timeout_enabled;
    /** If ev_timeout_enabled is true, the current timeout in libevent */
    rel_time_t ev_timeout;

    /** which state to go into after finishing current write */
    TaskFunction write_and_go;

    /* data for the mwrite state */
    std::vector<iovec> iov;
    /** number of elements used in iov[] */
    size_t iovused;

    /** The message list being used for transfer */
    std::vector<struct msghdr> msglist;
    /** element in msglist[] being transmitted now */
    size_t msgcurr;
    /** number of bytes in current msg */
    int msgbytes;

    /**
     * List of items we've reserved during the command (should call
     * item_release when transmit is complete)
     */
    std::vector<void*> reservedItems;

    /**
     * A vector of temporary allocations that should be freed when the
     * the connection is done sending all of the data. Use pushTempAlloc to
     * push a pointer to this list (must be allocated with malloc/calloc/strdup
     * etc.. will be freed by calling "free")
     */
    std::vector<char*> temp_alloc;

    /** True if the reply should not be sent (unless there is an error) */
    bool noreply;

    /**
     * If the client enabled the datatype support the response packet
     * will contain the datatype as set for the object
     */
    bool supports_datatype;

    /**
     * If the client enabled the mutation seqno feature each mutation
     * command will return the vbucket UUID and sequence number for the
     * mutation.
     */
    bool supports_mutation_extras;

    /**
     * The dynamic buffer is used to format output packets to be sent on
     * the wire.
     */
    DynamicBuffer dynamicBuffer;

    /**
     * The high resolution timer value for when we started executing the
     * current command.
     */
    hrtime_t start;

    /** the cas to return */
    uint64_t cas;

    /**
     * The status for the async io operation
     */
    ENGINE_ERROR_CODE aiostat;

    /**
     * Is this connection currently in an "ewouldblock" state?
     */
    bool ewouldblock;

    /**
     *  command-specific context - for use by command executors to maintain
     *  additional state while executing a command. For example
     *  a command may want to maintain some temporary state between retries
     *  due to engine returning EWOULDBLOCK.
     *
     *  Between each command this is deleted and reset to nullptr.
     */
    std::unique_ptr<CommandContext> commandContext;

    /**
     * The SSL context used by this connection (if enabled)
     */
    SslContext ssl;

    /**
     * Ensures that there is room for another struct iovec in a connection's
     * iov list.
     *
     * @throws std::bad_alloc
     */
    void ensureIovSpace();

    /**
     * Read data over the SSL connection
     *
     * @param dest where to store the data
     * @param nbytes the size of the destination buffer
     * @return the number of bytes read
     */
    int sslRead(char* dest, size_t nbytes);

    /**
     * Write data over the SSL stream
     *
     * @param src the source of the data
     * @param nbytes the number of bytes to send
     * @return the number of bytes written
     */
    int sslWrite(const char* src, size_t nbytes);

    /**
     * Handle the state for the ssl connection before the ssl connection
     * is fully established
     */
    int sslPreConnection();

    // Total number of bytes received on the network
    size_t totalRecv;
    // Total number of bytes sent to the network
    size_t totalSend;

    Cookie cookie;

    Datatype datatype;

    /**
     * It is possible to disable the SASL authentication for some
     * connections after they've been established.
     */
    bool saslAuthEnabled = true;
};
