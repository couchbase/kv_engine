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

typedef bool (* STATE_FUNC)(Connection*);

/**
 * The SslContext class is a holder class for all of the ssl-related
 * information used by the connection object.
 */
class SslContext {
public:
    SslContext()
        : enabled(false),
          connected(false),
          error(false),
          application(nullptr),
          network(nullptr),
          ctx(nullptr),
          client(nullptr)
    {
        in.total = 0;
        in.current = 0;
        out.total = 0;
        out.current = 0;
    }

    ~SslContext();

    /**
     * Is ssl enabled for this connection or not?
     */
    bool isEnabled() const {
        return enabled;
    }

    /**
     * Is the client fully connected over ssl or not?
     */
    bool isConnected() const {
        return connected;
    }

    /**
     * Set the status of the connected flag
     */
    void setConnected() {
        connected = true;
    }

    /**
     * Is there an error on the SSL stream?
     */
    bool hasError() const {
        return error;
    }

    /**
     * Enable SSL for this connection.
     *
     * @param cert the certificate file to use
     * @param pkey the private key file to use
     * @return true if success, false if we failed to enable SSL
     */
    bool enable(const std::string& cert, const std::string& pkey);

    /**
     * Disable SSL for this connection
     */
    void disable();

    /**
     * Try to fill the SSL stream with as much data from the network
     * as possible.
     *
     * @param sfd the socket to read data from
     */
    void drainBioRecvPipe(SOCKET sfd);

    /**
     * Try to drain the SSL stream with as much data as possible and
     * send it over the network.
     *
     * @param sfd the socket to write data to
     */
    void drainBioSendPipe(SOCKET sfd);

    bool moreInputAvailable() const {
        return (in.current < in.total);
    }

    bool morePendingOutput() const {
        return (out.total > 0);
    }

    /**
     * Dump the list of available ciphers to the log
     * @param id the connection id. Its only used in the
     *            log messages.
     */
    void dumpCipherList(uint32_t id) const;

    int accept() {
        return SSL_accept(client);
    }

    int getError(int errormask) const {
        return SSL_get_error(client, errormask);
    }

    int read(void* buf, int num) {
        return SSL_read(client, buf, num);
    }

    int write(const void* buf, int num) {
        return SSL_write(client, buf, num);
    }

    int peek(void* buf, int num) {
        return SSL_peek(client, buf, num);
    }

protected:
    bool enabled;
    bool connected;
    bool error;
    BIO* application;
    BIO* network;
    SSL_CTX* ctx;
    SSL* client;
    struct {
        // The data located in the buffer
        std::vector<char> buffer;
        // The number of bytes currently stored in the buffer
        size_t total;
        // The current offset of the buffer
        size_t current;
    } in, out;
};

/**
 *  A command may need to store command specific context during the duration
 *  of a command (you might for instance want to keep state between multiple
 *  calls that returns EWOULDBLOCK).
 *
 *  The implementation of such commands should subclass this class and
 *  allocate an instance and store in the commands commandContext member (which
 *  will be deleted and set to nullptr between each command being processed).
 */
class CommandContext {
public:
    virtual ~CommandContext() { };
};

/**
 * The structure representing a connection in memcached.
 */
class Connection {
public:
    Connection();

    ~Connection();

    Connection(const Connection&) = delete;

    /**
     * Return an identifier for this connection. To be backwards compatible
     * this is the socket filedescriptor (or the socket handle casted to an
     * unsigned integer on windows).
     */
    uint32_t getId() const {
        return uint32_t(socketDescriptor);
    }

    /**
     *  Get the socket descriptor used by this connection.
     */
    SOCKET getSocketDescriptor() const {
        return socketDescriptor;
    }

    /**
     * Set the socket descriptor used by this connection
     */
    void setSocketDescriptor(SOCKET sfd) {
        Connection::socketDescriptor = sfd;
    }

    bool isSocketClosed() const {
        return socketDescriptor == INVALID_SOCKET;
    }

    /**
     * Set the connection state in the state machine. Any special
     * processing that needs to happen on certain state transitions can
     * happen here.
     */
    void setState(STATE_FUNC next_state);

    /**
     * Get the current state
     */
    STATE_FUNC getState() const {
        return state;
    }

    /**
     * Run the state machinery
     */
    void runStateMachinery();

    /**
     * Resolve the name of the local socket and the peer for the connected
     * socket
     */
    void resolveConnectionName();

    const std::string& getPeername() const {
        return peername;
    }

    const std::string& getSockname() const {
        return sockname;
    }

    /**
     * Update the settings in libevent for this connection
     *
     * @param mask the new event mask to get notified about
     */
    bool updateEvent(const short new_flags);

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

    /**
     * Initialize the event structure and add it to libevent
     *
     * @param base the event base to bind to
     * @return true upon success, false otherwise
     */
    bool initializeEvent(struct event_base* base);

    /**
     * Terminate the eventloop for the current event base. This method doesn't
     * really fit as a member for the class, but I don't want clients to access
     * the libevent details from outside the class (so I didn't want to make
     * a "getEventBase()" method.
     */
    void eventBaseLoopbreak() {
        event_base_loopbreak(event.ev_base);
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
        return currentEvent & EV_READ;
    }

    /** Is the connection authorized with admin privileges? */
    bool isAdmin() const {
        return admin;
    }

    void setAdmin(bool admin) {
        Connection::admin = admin;
    }

    bool isAuthenticated() const {
        return authenticated;
    }

    void setAuthenticated(bool authenticated) {
        Connection::authenticated = authenticated;

        static const char unknown[] = "unknown";
        const void *unm = unknown;

        if (authenticated) {
            if (cbsasl_getprop(sasl_conn, CBSASL_USERNAME, &unm) != CBSASL_OK) {
                unm = unknown;
            }
        }

        username.assign(reinterpret_cast<const char*>(unm));
    }

    /**
     * Get the maximum number of events we should process per invocation
     * for a connection object (to avoid starvation of other connections)
     */
    int getMaxReqsPerEvent() const {
        return max_reqs_per_event;
    }

    /**
     * Set the maximum number of events we should process per invocation
     * for a connection object (to avoid starvation of other connections)
     */
    void setMaxReqsPerEvent(int max_reqs_per_event) {
        Connection::max_reqs_per_event = max_reqs_per_event;
    }

    const Protocol& getProtocol() const {
        return protocol;
    }

    void setProtocol(const Protocol& protocol) {
        Connection::protocol = protocol;
    }

    /**
     * Create a cJSON representation of the members of the connection
     * Caller is responsible for freeing the result with cJSON_Delete().
     */
    cJSON* toJSON() const;

    /**
     * Enable or disable TCP NoDelay on the underlying socket
     *
     * @return true on success, false otherwise
     */
    bool setTcpNoDelay(bool enable);

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
        /** An error occurred on the socket (or the client closed the connection) */
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
        Connection::numEvents = nevents;
    }

    /**
     * Get the username this connection is authenticated as
     *
     * NOTE: the return value should not be returned by the client
     */
    const char* getUsername() const {
        return username.c_str();
    }

    cbsasl_conn_t* getSaslConn() const {
        return sasl_conn;
    }

    void setSaslConn(cbsasl_conn_t* sasl_conn) {
        Connection::sasl_conn = sasl_conn;
    }

    const bin_substates& getSubstate() const {
        return substate;
    }

    void setSubstate(const bin_substates& substate) {
        Connection::substate = substate;
    }

    const net_buf& getRead() const {
        return read;
    }

    void setRead(const net_buf& read) {
        Connection::read = read;
    }

    const net_buf& getWrite() const {
        return write;
    }

    void setWrite(const net_buf& write) {
        Connection::write = write;
    }

    const STATE_FUNC getWriteAndGo() const {
        return write_and_go;
    }

    void setWriteAndGo(STATE_FUNC write_and_go) {
        Connection::write_and_go = write_and_go;
    }

    char* getRitem() const {
        return ritem;
    }

    void setRitem(char* ritem) {
        Connection::ritem = ritem;
    }

    uint32_t getRlbytes() const {
        return rlbytes;
    }

    void setRlbytes(uint32_t rlbytes) {
        Connection::rlbytes = rlbytes;
    }

    void* getItem() const {
        return item;
    }

    void setItem(void* item) {
        Connection::item = item;
    }

    /**
     * Get the pointer to the IO Vector
     */
    iovec* getIov() {
        return iov.data();
    }

    /**
     * Reset the number of elements used in the IO Vector
     */
    void resetIovUsed() {
        iovused = 0;
    }

    /**
     * Increment the number of elements used in the IO Vector
     */
    void incIovUsed() {
        ++iovused;
    }

    /**
     * Get the number of entries in use in the IO Vector
     */
    int getIovUsed() const {
        return iovused;
    }

    /**
     * Grow the message list if it is full
     */
    bool growMsglist() {
        if (msglist.size() == msgused) {
            cb_assert(msglist.size() > 0);
            try {
                msglist.resize(msglist.size() * 2);
            } catch (std::bad_alloc) {
                return false;
            }
        }
        return true;
    }

    struct msghdr* getMsglist() {
        return msglist.data();
    }

    size_t getMsgused() const {
        return msgused;
    }

    void setMsgused(int msgused) {
        Connection::msgused = msgused;
    }

    size_t getMsgcurr() const {
        return msgcurr;
    }

    void setMsgcurr(size_t msgcurr) {
        Connection::msgcurr = msgcurr;
    }

    int getMsgbytes() const {
        return msgbytes;
    }

    void setMsgbytes(int msgbytes) {
        Connection::msgbytes = msgbytes;
    }

    /**
     * Release all of the items we've saved a reference to
     */
    void releaseReservedItems() {
        ENGINE_HANDLE* handle = reinterpret_cast<ENGINE_HANDLE*>(bucketEngine);
        for (auto *it : reservedItems) {
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
    bool reserveItem(void *item) {
        try {
            reservedItems.push_back(item);
            return true;
        } catch (std::bad_alloc) {
            return false;
        }
    }

    void releaseTempAlloc() {
        for (auto* ptr : temp_alloc) {
            free(ptr);
        }
        temp_alloc.resize(0);
    }

    bool pushTempAlloc(char* ptr) {
        try {
            temp_alloc.push_back(ptr);
            return true;
        } catch (std::bad_alloc) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, this,
                                            "%u: FATAL: failed to allocate space to keep temporary buffer",
                                            getId());
            return false;
        }
    }

    bool isNoReply() const {
        return noreply;
    }

    void setNoReply(bool noreply) {
        Connection::noreply = noreply;
    }

    /**
     * Get the current reference count
     */
    uint8_t getRefcount() const {
        return refcount;
    }

    void incrementRefcount() {
        ++refcount;
    }

    void decrementRefcount() {
        --refcount;
    }

    bool isSupportsDatatype() const {
        return supports_datatype;
    }

    void setSupportsDatatype(bool supports_datatype) {
        Connection::supports_datatype = supports_datatype;
    }

    bool isSupportsMutationExtras() const {
        return supports_mutation_extras;
    }

    void setSupportsMutationExtras(bool supports_mutation_extras) {
        Connection::supports_mutation_extras = supports_mutation_extras;
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

    void* getEngineStorage() const {
        return engine_storage;
    }

    void setEngineStorage(void* engine_storage) {
        Connection::engine_storage = engine_storage;
    }

    hrtime_t getStart() const {
        return start;
    }

    void setStart(hrtime_t start) {
        Connection::start = start;
    }

    const protocol_binary_request_header& getBinaryHeader() const {
        return binary_header;
    }

    void setBinaryHeader(const protocol_binary_request_header& binary_header) {
        Connection::binary_header = binary_header;
    }

    uint64_t getCAS() const {
        return cas;
    }

    void setCAS(uint64_t cas) {
        Connection::cas = cas;
    }

    uint8_t getCmd() const {
        return cmd;
    }

    void setCmd(uint8_t cmd) {
        Connection::cmd = cmd;
    }

    /**
     * Return the opaque value for the command being processed
     */
    uint32_t getOpaque() const {
        return binary_header.request.opaque;
    }

    Connection* getNext() const {
        return next;
    }

    void setNext(Connection* next) {
        Connection::next = next;
    }

    LIBEVENT_THREAD* getThread() const {
        return thread;
    }

    void setThread(LIBEVENT_THREAD* thread) {
        Connection::thread = thread;
    }

    const ENGINE_ERROR_CODE& getAiostat() const {
        return aiostat;
    }

    void setAiostat(const ENGINE_ERROR_CODE& aiostat) {
        Connection::aiostat = aiostat;
    }

    bool isEwouldblock() const {
        return ewouldblock;
    }

    void setEwouldblock(bool ewouldblock) {
        Connection::ewouldblock = ewouldblock;
    }

    const TAP_ITERATOR getTapIterator() const {
        return tap_iterator;
    }

    void setTapIterator(TAP_ITERATOR tap_iterator) {
        Connection::tap_iterator = tap_iterator;
    }

    in_port_t getParentPort() const {
        return parent_port;
    }

    void setParentPort(in_port_t parent_port) {
        Connection::parent_port = parent_port;
    }

    bool isTAP() const {
        return tap_iterator != nullptr;
    }

    bool isDCP() const {
        return dcp;
    }

    void setDCP(bool dcp) {
        Connection::dcp = dcp;
    }

    /**
     *  Get the command context stored for this command
     */
    CommandContext* getCommandContext() const {
        return commandContext;
    }

    /**
     *  Set the command context stored for this command
     */
    void setCommandContext(CommandContext* cmd_context) {
        Connection::commandContext = cmd_context;
    }

    /**
     * Reset the command context
     *
     * Release the allocated resources and set the command context to nullptr
     */
    void resetCommandContext() {
        if (commandContext != nullptr) {
            delete commandContext;
            commandContext = nullptr;
        }
    }

    /**
     * Set the authentication context to be used by this connection.
     *
     * The connection object takes the ownership of the pointer and is
     * responsible for releasing the memory.
     */
    void setAuthContext(AuthContext* auth_context) {
        if (Connection::auth_context != nullptr) {
            // Delete the previously allocated auth object
            auth_destroy(Connection::auth_context);
        }
        Connection::auth_context = auth_context;
    }

    /**
     * Check if the client is allowed to execute the specified opcode
     *
     * @param opcode the opcode in the memcached binary protocol
     */
    AuthResult checkAccess(uint8_t opcode) const {
        return auth_check_access(auth_context, opcode);
    }

    /**
     * Update the authentication context to operate on behalf of a given
     * role
     */
    AuthResult assumeRole(const std::string &role) {
        return auth_assume_role(auth_context, role.c_str());
    }

    /**
     * Update the authentication context to operate as the authenticated
     * user rather than the current role
     */
    AuthResult dropRole() {
        return auth_drop_role(auth_context);
    }

    /**
     * Try to enable SSL for this connection
     *
     * @param cert the SSL certificate to use
     * @param pkey the SSL private key to use
     * @return true if successful, false otherwise
     */
    bool enableSSL(const std::string& cert, const std::string& pkey) {
        if (ssl.enable(cert, pkey)) {
            if (settings.verbose > 1) {
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
     * Do we have any pending input data on this connection?
     */
    bool havePendingInputData() {
        int block = (read.bytes > 0);

        if (!block && ssl.isEnabled()) {
            char dummy;
            block |= ssl.peek(&dummy, 1);
        }

        return block != 0;
    }

    /**
     * Ensures that there is room for another struct iovec in a connection's
     * iov list.
     */
    bool ensureIovSpace() {
        if (iovused < iov.size()) {
            // There is still size in the list
            return true;
        }

        // Try to double the size of the array
        try {
            iov.resize(iov.size() * 2);
        } catch (std::bad_alloc) {
            return false;
        }

        /* Point all the msghdr structures at the new list. */
        size_t ii;
        int iovnum;
        for (ii = 0, iovnum = 0; ii < msgused; ii++) {
            msglist[ii].msg_iov = &iov[iovnum];
            iovnum += msglist[ii].msg_iovlen;
        }

        return true;
    }

    int getBucketIndex() const {
        return bucketIndex;
    }

    void setBucketIndex(int bucketIndex) {
        Connection::bucketIndex = bucketIndex;
    }

    ENGINE_HANDLE_V1* getBucketEngine() const {
        return bucketEngine;
    };

    void setBucketEngine(ENGINE_HANDLE_V1* bucketEngine) {
        Connection::bucketEngine = bucketEngine;
    };

protected:
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

private:
    /**
     * The actual socket descriptor used by this connection
     */
    SOCKET socketDescriptor;

    int max_reqs_per_event; /** The maximum requests we can process in a worker
                                thread timeslice */
    /**
     * number of events this connection can process in a single worker
     * thread timeslice
     */
    int numEvents;

    /**
     * The SASL object used to do sasl authentication
     */
    cbsasl_conn_t* sasl_conn;

    /** The current state we're in */
    STATE_FUNC state;

    /** The current substate we're in */
    bin_substates substate;

    /** The protocol used by the connection */
    Protocol protocol;

    /** Is the connection set up with admin privileges */
    bool admin;

    /** Is the connection authenticated or not */
    bool authenticated;

    /** The username authenticated as */
    std::string username;

    // Members related to libevent

    /** Is the connection currently registered in libevent? */
    bool registered_in_libevent;
    /** The libevent object */
    struct event event;
    /** The current flags we've registered in libevent */
    short ev_flags;
    /** which events were just triggered */
    short currentEvent;

public:
    /** Read buffer */
    struct net_buf read;
    /** Write buffer */
    struct net_buf write;

private:
    /** which state to go into after finishing current write */
    STATE_FUNC write_and_go;

public:
    /** when we read in an item's value, it goes here */
    char* ritem;

    /* data for the nread state */
    uint32_t rlbytes;

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data.
     */
    void* item;

private:
    /* data for the mwrite state */
    std::vector<iovec> iov;
    /** number of elements used in iov[] */
    size_t iovused;

    /** The message list being used for transfer */
    std::vector<struct msghdr> msglist;
    /** number of elements used in msglist[] */
    size_t msgused;
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

    /** Is tcp nodelay enabled or not? */
    bool nodelay;

    /** number of references to the object */
    uint8_t refcount;

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
     * Pointer to engine-specific data which the engine has requested the server
     * to persist for the life of the connection.
     * See SERVER_COOKIE_API::{get,store}_engine_specific()
     */
    void* engine_storage;

    /**
     * The high resolution timer value for when we started executing the
     * current command.
     */
    hrtime_t start;

public:
    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;

private:
    /** the cas to return */
    uint64_t cas;

    /** current command being processed */
    uint8_t cmd;

    /* Used for generating a list of Connection structures */
    Connection* next;

    /** Pointer to the thread object serving this connection */
    LIBEVENT_THREAD* thread;

    /**
     * The status for the async io operation
     */
    ENGINE_ERROR_CODE aiostat;

    /**
     * Is this connection currently in an "ewouldblock" state?
     */
    bool ewouldblock;

    /** Listening port that creates this connection instance */
    in_port_t parent_port;
    /** The iterator function to use to traverse the TAP stream */
    TAP_ITERATOR tap_iterator;

    /** Is this connection used by a DCP connection? */
    bool dcp;

    /**
     *  command-specific context - for use by command executors to maintain
     *  additional state while executing a command. For example
     *  a command may want to maintain some temporary state between retries
     *  due to engine returning EWOULDBLOCK.
     *
     *  Between each command this is deleted and reset to nullptr.
     */
    CommandContext* commandContext;

    /**
     * The SSL context used by this connection (if enabled)
     */
    SslContext ssl;

    /** Name of the peer if known */
    std::string peername;

    /** Name of the local socket if known */
    std::string sockname;

    /**
     * The authentication context in use by this connection
     */
    AuthContext* auth_context;

    /**
     * The index of the connected bucket
     */
    int bucketIndex;

    /**
     * The engine interface for the connected bucket
     */
    ENGINE_HANDLE_V1* bucketEngine;
};
