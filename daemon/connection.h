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


    Connection* getAllNext() const {
        return all_next;
    }

    void setAllNext(Connection* all_next) {
        Connection::all_next = all_next;
    }

    Connection* getAllPrev() const {
        return all_prev;
    }

    void setAllPrev(Connection* all_prev) {
        Connection::all_prev = all_prev;
    }

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

    const struct event& getEvent() const {
        return event;
    }

    void setEvent(const struct event& event) {
        Connection::event = event;
    }

    short getEvFlags() const {
        return ev_flags;
    }

    void setEvFlags(short ev_flags) {
        Connection::ev_flags = ev_flags;
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

    void* getWriteAndFree() const {
        return write_and_free;
    }

    void setWriteAndFree(void* write_and_free) {
        Connection::write_and_free = write_and_free;
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

    iovec* getIov() const {
        return iov;
    }

    void setIov(iovec* iov) {
        Connection::iov = iov;
    }

    int getIovsize() const {
        return iovsize;
    }

    void setIovsize(int iovsize) {
        Connection::iovsize = iovsize;
    }

    int getIovused() const {
        return iovused;
    }

    void setIovused(int iovused) {
        Connection::iovused = iovused;
    }

    msghdr* getMsglist() const {
        return msglist;
    }

    void setMsglist(msghdr* msglist) {
        Connection::msglist = msglist;
    }

    int getMsgsize() const {
        return msgsize;
    }

    void setMsgsize(int msgsize) {
        Connection::msgsize = msgsize;
    }

    int getMsgused() const {
        return msgused;
    }

    void setMsgused(int msgused) {
        Connection::msgused = msgused;
    }

    int getMsgcurr() const {
        return msgcurr;
    }

    void setMsgcurr(int msgcurr) {
        Connection::msgcurr = msgcurr;
    }

    int getMsgbytes() const {
        return msgbytes;
    }

    void setMsgbytes(int msgbytes) {
        Connection::msgbytes = msgbytes;
    }

    const std::vector<void*>& getReservedItems() const {
        return reservedItems;
    }

    void setReservedItems(const std::vector<void*>& reservedItems) {
        Connection::reservedItems = reservedItems;
    }

    const std::vector<char*>& getTempAlloc() const {
        return temp_alloc;
    }

    void setTempAlloc(const std::vector<char*>& temp_alloc) {
        Connection::temp_alloc = temp_alloc;
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

    const struct dynamic_buffer& getDynamicBuffer() const {
        return dynamic_buffer;
    }

    void setDynamicBuffer(const struct dynamic_buffer& dynamic_buffer) {
        Connection::dynamic_buffer = dynamic_buffer;
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

    uint32_t getOpaque() const {
        return opaque;
    }

    void setOpaque(uint32_t opaque) {
        Connection::opaque = opaque;
    }

    int getKeylen() const {
        return keylen;
    }

    void setKeylen(int keylen) {
        Connection::keylen = keylen;
    }

    int getListState() const {
        return list_state;
    }

    void setListState(int list_state) {
        Connection::list_state = list_state;
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
    Connection* all_next;
    /** Intrusive list to track all connections */
    Connection* all_prev;

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

public:
    cbsasl_conn_t* sasl_conn;
private:
    STATE_FUNC state;
    /**
     * The current substate we're in
     */
    bin_substates substate;

    /* The protocol used by the connection */
    Protocol protocol;

    /** Is the connection set up with admin privileges */
    bool admin;

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
    struct net_buf read;
    /** Read buffer */
    struct net_buf write; /* Write buffer */

    /** which state to go into after finishing current write */
    STATE_FUNC write_and_go;
    void* write_and_free;
    /** free this memory after finishing writing. Note:
                                   used only by write_and_free(); shouldn't be
                                   directly used by any commands.*/

    char* ritem;
    /** when we read in an item's value, it goes here */
    uint32_t rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data.
     */
    void* item;

    /* data for the mwrite state */
    struct iovec* iov;
    int iovsize;
    /* number of elements allocated in iov[] */
    int iovused;
    /* number of elements used in iov[] */

    struct msghdr* msglist;
    int msgsize;
    /* number of elements allocated in msglist[] */
    int msgused;
    /* number of elements used in msglist[] */
    int msgcurr;
    /* element in msglist[] being transmitted now */
    int msgbytes;  /* number of bytes in current msg */

    // List of items we've reserved during the command (should call item_release)
    std::vector<void*> reservedItems;
    std::vector<char*> temp_alloc;

private:
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

public:
    struct dynamic_buffer dynamic_buffer;

    // Pointer to engine-specific data which the engine has requested the server
    // to persist for the life of the connection.
    // See SERVER_COOKIE_API::{get,store}_engine_specific()
    void* engine_storage;

    hrtime_t start;

    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;
    uint64_t cas;
    /* the cas to return */
    uint8_t cmd;
    /* current command being processed */
    uint32_t opaque;
    int keylen;

    int list_state;
    /* bitmask of list state data for this connection */
    Connection* next;
    /* Used for generating a list of Connection structures */
    LIBEVENT_THREAD* thread;
    /* Pointer to the thread object serving this connection */

    ENGINE_ERROR_CODE aiostat;
    bool ewouldblock;

private:
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

    /**
     * The authentication context in use by this connection
     */
    AuthContext* auth_context;
public:
    struct {
        int idx;
        /* The internal index for the connected bucket */
        ENGINE_HANDLE_V1* engine;
    } bucket;

private:

    /** Name of the peer if known */
    std::string peername;

    /** Name of the local socket if known */
    std::string sockname;

    void resetBufferSize();
};
