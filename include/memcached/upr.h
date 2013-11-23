/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_UPR_H
#define MEMCACHED_UPR_H

#ifndef MEMCACHED_ENGINE_H
#error "Please include memcached/engine.h instead"
#endif

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * The message producers is used by the engine by the UPR producers
     * to add messages into the UPR stream. Please look at the full
     * UPR documentation to figure out the real meaning for all of the
     * messages.
     *
     * The UPR client is free to call this functions multiple times
     * to add more messages into the pipeline as long as the producer
     * returns ENGINE_WANT_MORE.
     */
    struct upr_message_producers {
        /**
         *
         */
        ENGINE_ERROR_CODE (*get_failover_log)(const void *cookie,
                                              uint32_t opaque,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*stream_req)(const void *cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags,
                                        uint64_t start_seqno,
                                        uint64_t end_seqno,
                                        uint64_t vbucket_uuid,
                                        uint64_t high_seqno);

        ENGINE_ERROR_CODE (*add_stream_rsp)(const void *cookie,
                                            uint32_t opaque,
                                            uint32_t stream_opaque,
                                            uint8_t status);

        /**
         * Send a Stream End message
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param vbucket the vbucket id the message belong to
         * @param flags the reason for the stream end.
         *              0 = success
         *              1 = Something happened on the vbucket causing
         *                  us to abort it.
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*stream_end)(const void *cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags);

        /**
         * Send a marker
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param vbucket the vbucket id the message belong to
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*marker)(const void *cookie,
                                    uint32_t opaque,
                                    uint16_t vbucket);

        /**
         * Send a Mutation
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param itm the item to send. The core will call item_release on
         *            the item when it is sent so remember to keep it around
         * @param vbucket the vbucket id the message belong to
         * @param by_seqno
         * @param rev_seqno
         * @param lock_time
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*mutation)(const void* cookie,
                                      uint32_t opaque,
                                      item *itm,
                                      uint16_t vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t lock_time);

        /**
         * Send a deletion
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param key pointer to the items key
         * @param nkey the number of bytes in the key
         * @param cas the cas value representing the deleted item
         * @param vbucket the vbucket id the message belong to
         * @param by_seqno
         * @param rev_seqno
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*deletion)(const void* cookie,
                                      uint32_t opaque,
                                      const void *key,
                                      uint16_t nkey,
                                      uint64_t cas,
                                      uint16_t vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno);
        /**
         * Send an expiration
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param key pointer to the items key
         * @param nkey the number of bytes in the key
         * @param cas the cas value representing the expired item
         * @param vbucket the vbucket id the message belong to
         * @param by_seqno
         * @param rev_seqno
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*expiration)(const void* cookie,
                                        uint32_t opaque,
                                        const void *key,
                                        uint16_t nkey,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno);

        /**
         * Send a flush for a single vbucket
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param vbucket the vbucket id the message belong to
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*flush)(const void* cookie,
                                   uint32_t opaque,
                                   uint16_t vbucket);

        /**
         * Send a state transition for a vbucket
         *
         * @param cookie passed on the cookie provided by step
         * @param opaque this is the opaque requested by the consumer
         *               in the Stream Request message
         * @param vbucket the vbucket id the message belong to
         * @param state the new state
         *
         * @return ENGINE_WANT_MORE or ENGINE_SUCCESS upon success
         */
        ENGINE_ERROR_CODE (*set_vbucket_state)(const void* cookie,
                                               uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state);
    };

    typedef ENGINE_ERROR_CODE (*upr_add_failover_log)(vbucket_failover_t*,
                                                      size_t nentries,
                                                      const void *cookie);

    struct upr_interface {
        /**
         * Called from the memcached core for a UPR connection to allow it to
         * inject new messages on the stream.
         *
         * @param handle reference to the engine itself
         * @param cookie a unique handle the engine should pass on to the
         *               message producers
         * @param producers functions the client may use to add messages to
         *                  the UPR stream
         *
         * @return the return code from the message producers
         */
        ENGINE_ERROR_CODE (*step)(ENGINE_HANDLE* handle, const void* cookie,
                                  struct upr_message_producers *producers);


        ENGINE_ERROR_CODE (*open)(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  uint32_t opaque,
                                  uint32_t seqno,
                                  uint32_t flags,
                                  void *name,
                                  uint16_t nname);

        ENGINE_ERROR_CODE (*add_stream)(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags);

        ENGINE_ERROR_CODE (*close_stream)(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          uint16_t vbucket);

        /**
         * Callback to the engine that a Stream Request message was received
         */
        ENGINE_ERROR_CODE (*stream_req)(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t flags,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint64_t start_seqno,
                                        uint64_t end_seqno,
                                        uint64_t vbucket_uuid,
                                        uint64_t high_seqno,
                                        uint64_t *rollback_seqno,
                                        upr_add_failover_log callback);

        /**
         * Callback to the engine that a get failover log message was received
         */
        ENGINE_ERROR_CODE (*get_failover_log)(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              uint32_t opaque,
                                              uint16_t vbucket,
                                              upr_add_failover_log callback);

        /**
         * Callback to the engine that a stream end message was received
         */
        ENGINE_ERROR_CODE (*stream_end)(ENGINE_HANDLE* handle, const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags);

        /**
         * Callback to the engine that a snapshot marker message was received
         */
        ENGINE_ERROR_CODE (*snapshot_marker)(ENGINE_HANDLE* handle, const void* cookie,
                                            uint32_t opaque,
                                            uint16_t vbucket);

        /**
         * Callback to the engine that a mutation message was received
         */
        ENGINE_ERROR_CODE (*mutation)(ENGINE_HANDLE* handle, const void* cookie,
                                      uint32_t opaque,
                                      const void *key,
                                      uint16_t nkey,
                                      const void *value,
                                      uint32_t nvalue,
                                      uint64_t cas,
                                      uint16_t vbucket,
                                      uint32_t flags,
                                      uint8_t datatype,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t expiration,
                                      uint32_t lock_time);

        /**
         * Callback to the engine that a deletion message was received
         */
        ENGINE_ERROR_CODE (*deletion)(ENGINE_HANDLE* handle, const void* cookie,
                                      uint32_t opaque,
                                      const void *key,
                                      uint16_t nkey,
                                      uint64_t cas,
                                      uint16_t vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno);

        /**
         * Callback to the engine that an expiration message was received
         */
        ENGINE_ERROR_CODE (*expiration)(ENGINE_HANDLE* handle, const void* cookie,
                                        uint32_t opaque,
                                        const void *key,
                                        uint16_t nkey,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno);

        /**
         * Callback to the engine that a flush message was received
         */
        ENGINE_ERROR_CODE (*flush)(ENGINE_HANDLE* handle, const void* cookie,
                                   uint32_t opaque,
                                   uint16_t vbucket);

        /**
         * Callback to the engine that a set vbucket state message was received
         */
        ENGINE_ERROR_CODE (*set_vbucket_state)(ENGINE_HANDLE* handle, const void* cookie,
                                               uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state);


        ENGINE_ERROR_CODE (*response_handler)(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_response_header *response);
    };

#ifdef __cplusplus
}
#endif

#endif
