#ifndef MEMCACHED_TYPES_H
#define MEMCACHED_TYPES_H 1

#include <sys/types.h>
#include <stdint.h>

#ifdef WIN32
#include <platform/platform.h>
#else
#include <sys/uio.h>
#endif

#include "engine_error.h"

/**
 * Time relative to server start. Smaller than time_t on 64-bit systems.
 */
typedef uint32_t rel_time_t;

/**
 * Engine storage operations.
 */
typedef enum {
    OPERATION_ADD = 1, /**< Store with add semantics */
    OPERATION_SET = 2, /**< Store with set semantics */
    OPERATION_REPLACE = 3, /**< Store with replace semantics */
    OPERATION_CAS = 6 /**< Store with set semantics. */
} ENGINE_STORE_OPERATION;

typedef enum {
    CONN_PRIORITY_HIGH,
    CONN_PRIORITY_MED,
    CONN_PRIORITY_LOW
} CONN_PRIORITY;

/**
 * Data common to any item stored in memcached.
 */
typedef void item;

/**
 * The legal state a document may be in (from the cores perspective)
 */
enum class DocumentState : uint8_t {
    /**
     * The document is deleted from the users perspective, and trying
     * to fetch the document will return KEY_NOT_FOUND unless one
     * asks specifically for deleted documents. The Deleted documents
     * will not hang around forever and may be reaped by the purger
     * at any time (from the core's perspective. That's an internal
     * detail within the underlying engine).
     */
    Deleted = 0x0F,

    /**
     * The document is alive and all operations should work as expected.
     */
    Alive = 0xF0,
};

typedef struct {
    uint64_t cas;
    uint64_t vbucket_uuid;
    uint64_t seqno;
    rel_time_t exptime; /**< When the item will expire (relative to process
                         * startup) */
    uint32_t nbytes; /**< The total size of the data (in bytes) */
    uint32_t flags; /**< Flags associated with the item (in network byte order)*/
    uint8_t datatype;

    /**
     * The current state of the document (Deleted or Alive).
     */
    DocumentState document_state;
    uint16_t nkey; /**< The total length of the key (in bytes) */
    const void *key;
    /**
     * If the xattr bit is set in datatype the first uint32_t contains
     * the size of the extended attributes which follows next, and
     * finally the actual document payload.
     */
    struct iovec value[1];
} item_info;

typedef struct {
    const char *username;
} auth_data_t;

/* Forward declaration of the server handle -- to be filled in later */
typedef struct server_handle_v1_t SERVER_HANDLE_V1;

/* Information to uniquely identify (and order) a mutation. */
typedef struct {
    uint64_t vbucket_uuid; /** vBucket UUID for this mutation. */
    uint64_t seqno; /** sequence number of the mutation. */
} mutation_descr_t;

/* Value used to distinguish one bucket from another */
typedef uint32_t bucket_id_t;

#endif /* MEMCACHED_TYPES_H */
