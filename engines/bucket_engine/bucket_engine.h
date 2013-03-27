#ifndef BUCKET_ENGINE_H
#define BUCKET_ENGINE_H 1

#include <memcached/protocol_binary.h>

/* Membase up to 1.7 use command ids from the reserved
 * set. Let's deprecate them (but still accept them)
 * and drop support for them when we move to 2.0
 */
#define CREATE_BUCKET_DEPRECATED 0x25
#define DELETE_BUCKET_DEPRECATED 0x26
#define LIST_BUCKETS_DEPRECATED  0x27
#define SELECT_BUCKET_DEPRECATED 0x29

#define CREATE_BUCKET 0x85
#define DELETE_BUCKET 0x86
#define LIST_BUCKETS  0x87
#define SELECT_BUCKET 0x89

/*
 * The following bits are copied from ep-engine/commands_ids.h to
 * allow us to compile without depending on that module
 */
#define CMD_GET_REPLICA 0x83
#define CMD_EVICT_KEY 0x93
#define CMD_GET_LOCKED 0x94
#define CMD_UNLOCK_KEY 0x95
#define CMD_GET_META 0xa0
#define CMD_GETQ_META 0xa1
#define CMD_SET_WITH_META 0xa2
#define CMD_SETQ_WITH_META 0xa3
#define CMD_DEL_WITH_META 0xa8
#define CMD_DELQ_WITH_META 0xa9

typedef protocol_binary_request_no_extras protocol_binary_request_create_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_delete_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_list_buckets;
typedef protocol_binary_request_no_extras protocol_binary_request_select_bucket;

#endif /* BUCKET_ENGINE_H */
