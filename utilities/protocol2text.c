/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <memcached/protocol_binary.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include "protocol2text.h"

const char *memcached_opcode_2_text(uint8_t opcode) {
    switch ((unsigned int)opcode) {
    case PROTOCOL_BINARY_CMD_GET:
        return "GET";
    case PROTOCOL_BINARY_CMD_SET:
        return "SET";
    case PROTOCOL_BINARY_CMD_ADD:
        return "ADD";
    case PROTOCOL_BINARY_CMD_REPLACE:
        return "REPLACE";
    case PROTOCOL_BINARY_CMD_DELETE:
        return "DELETE";
    case PROTOCOL_BINARY_CMD_INCREMENT:
        return "INCREMENT";
    case PROTOCOL_BINARY_CMD_DECREMENT:
        return "DECREMENT";
    case PROTOCOL_BINARY_CMD_QUIT:
        return "QUIT";
    case PROTOCOL_BINARY_CMD_FLUSH:
        return "FLUSH";
    case PROTOCOL_BINARY_CMD_GETQ:
        return "GETQ";
    case PROTOCOL_BINARY_CMD_NOOP:
        return "NOOP";
    case PROTOCOL_BINARY_CMD_VERSION:
        return "VERSION";
    case PROTOCOL_BINARY_CMD_GETK:
        return "GETK";
    case PROTOCOL_BINARY_CMD_GETKQ:
        return "GETKQ";
    case PROTOCOL_BINARY_CMD_APPEND:
        return "APPEND";
    case PROTOCOL_BINARY_CMD_PREPEND:
        return "PREPEND";
    case PROTOCOL_BINARY_CMD_STAT:
        return "STAT";
    case PROTOCOL_BINARY_CMD_SETQ:
        return "SETQ";
    case PROTOCOL_BINARY_CMD_ADDQ:
        return "ADDQ";
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        return "REPLACEQ";
    case PROTOCOL_BINARY_CMD_DELETEQ:
        return "DELETEQ";
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        return "INCREMENTQ";
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        return "DECREMENTQ";
    case PROTOCOL_BINARY_CMD_QUITQ:
        return "QUITQ";
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        return "FLUSHQ";
    case PROTOCOL_BINARY_CMD_APPENDQ:
        return "APPENDQ";
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        return "PREPENDQ";
    case PROTOCOL_BINARY_CMD_VERBOSITY:
        return "VERBOSITY";
    case PROTOCOL_BINARY_CMD_TOUCH:
        return "TOUCH";
    case PROTOCOL_BINARY_CMD_GAT:
        return "GAT";
    case PROTOCOL_BINARY_CMD_GATQ:
        return "GATQ";
    case PROTOCOL_BINARY_CMD_HELLO:
        return "HELLO";
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
        return "SASL_LIST_MECHS";
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        return "SASL_AUTH";
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        return "SASL_STEP";
    case PROTOCOL_BINARY_CMD_IOCTL_GET:
        return "IOCTL_GET";
    case PROTOCOL_BINARY_CMD_IOCTL_SET:
        return "IOCTL_SET";
    case PROTOCOL_BINARY_CMD_CONFIG_VALIDATE:
        return "CONFIG_VALIDATE";
    case PROTOCOL_BINARY_CMD_CONFIG_RELOAD:
        return "CONFIG_RELOAD";
    case PROTOCOL_BINARY_CMD_AUDIT_PUT:
        return "AUDIT_PUT";
    case PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD:
        return "AUDIT_CONFIG_RELOAD";
    case PROTOCOL_BINARY_CMD_RGET:
        return "RGET";
    case PROTOCOL_BINARY_CMD_RSET:
        return "RSET";
    case PROTOCOL_BINARY_CMD_RSETQ:
        return "RSETQ";
    case PROTOCOL_BINARY_CMD_RAPPEND:
        return "RAPPEND";
    case PROTOCOL_BINARY_CMD_RAPPENDQ:
        return "RAPPENDQ";
    case PROTOCOL_BINARY_CMD_RPREPEND:
        return "RPREPEND";
    case PROTOCOL_BINARY_CMD_RPREPENDQ:
        return "RPREPENDQ";
    case PROTOCOL_BINARY_CMD_RDELETE:
        return "RDELETE";
    case PROTOCOL_BINARY_CMD_RDELETEQ:
        return "RDELETEQ";
    case PROTOCOL_BINARY_CMD_RINCR:
        return "RINCR";
    case PROTOCOL_BINARY_CMD_RINCRQ:
        return "RINCRQ";
    case PROTOCOL_BINARY_CMD_RDECR:
        return "RDECR";
    case PROTOCOL_BINARY_CMD_RDECRQ:
        return "RDECRQ";
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
        return "SET_VBUCKET";
    case PROTOCOL_BINARY_CMD_GET_VBUCKET:
        return "GET_VBUCKET";
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        return "DEL_VBUCKET";
    case PROTOCOL_BINARY_CMD_TAP_CONNECT:
        return "TAP_CONNECT";
    case PROTOCOL_BINARY_CMD_TAP_MUTATION:
        return "TAP_MUTATION";
    case PROTOCOL_BINARY_CMD_TAP_DELETE:
        return "TAP_DELETE";
    case PROTOCOL_BINARY_CMD_TAP_FLUSH:
        return "TAP_FLUSH";
    case PROTOCOL_BINARY_CMD_TAP_OPAQUE:
        return "TAP_OPAQUE";
    case PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET:
        return "TAP_VBUCKET_SET";
    case PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START:
        return "TAP_CHECKPOINT_START";
    case PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END:
        return "TAP_CHECKPOINT_END";
    case PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS:
        return "GET_ALL_VB_SEQNOS";
    case PROTOCOL_BINARY_CMD_DCP_OPEN:
        return "DCP_OPEN";
    case PROTOCOL_BINARY_CMD_DCP_ADD_STREAM:
        return "DCP_ADD_STREAM";
    case PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM:
        return "DCP_CLOSE_STREAM";
    case PROTOCOL_BINARY_CMD_DCP_STREAM_REQ:
        return "DCP_STREAM_REQ";
    case PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG:
        return "DCP_GET_FAILOVER_LOG";
    case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
        return "DCP_STREAM_END";
    case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
        return "DCP_SNAPSHOT_MARKER";
    case PROTOCOL_BINARY_CMD_DCP_MUTATION:
        return "DCP_MUTATION";
    case PROTOCOL_BINARY_CMD_DCP_DELETION:
        return "DCP_DELETION";
    case PROTOCOL_BINARY_CMD_DCP_EXPIRATION:
        return "DCP_EXPIRATION";
    case PROTOCOL_BINARY_CMD_DCP_FLUSH:
        return "DCP_FLUSH";
    case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
        return "DCP_SET_VBUCKET_STATE";
    case PROTOCOL_BINARY_CMD_DCP_NOOP:
        return "DCP_NOOP";
    case PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT:
        return "DCP_BUFFER_ACKNOWLEDGEMENT";
    case PROTOCOL_BINARY_CMD_DCP_CONTROL:
        return "DCP_CONTROL";
    case PROTOCOL_BINARY_CMD_DCP_RESERVED4:
        return "DCP_RESERVED4";
    case PROTOCOL_BINARY_CMD_STOP_PERSISTENCE:
        return "STOP_PERSISTENCE";
    case PROTOCOL_BINARY_CMD_START_PERSISTENCE:
        return "START_PERSISTENCE";
    case PROTOCOL_BINARY_CMD_SET_PARAM:
        return "SET_PARAM";
    case PROTOCOL_BINARY_CMD_GET_REPLICA:
        return "GET_REPLICA";
    case PROTOCOL_BINARY_CMD_CREATE_BUCKET:
        return "CREATE_BUCKET";
    case PROTOCOL_BINARY_CMD_DELETE_BUCKET:
        return "DELETE_BUCKET";
    case PROTOCOL_BINARY_CMD_LIST_BUCKETS:
        return "LIST_BUCKET";
    case PROTOCOL_BINARY_CMD_SELECT_BUCKET:
        return "SELECT_BUCKET";
    case PROTOCOL_BINARY_CMD_ASSUME_ROLE:
        return "ASSUME_ROLE";
    case PROTOCOL_BINARY_CMD_OBSERVE:
        return "OBSERVE";
    case PROTOCOL_BINARY_CMD_OBSERVE_SEQNO:
        return "OBSERVE_SEQNO";
    case PROTOCOL_BINARY_CMD_EVICT_KEY:
        return "EVICT_KEY";
    case PROTOCOL_BINARY_CMD_GET_LOCKED:
        return "GET_LOCKED";
    case PROTOCOL_BINARY_CMD_UNLOCK_KEY:
        return "UNLOCK_KEY";
    case PROTOCOL_BINARY_CMD_LAST_CLOSED_CHECKPOINT:
        return "LAST_CLOSED_CHECKPOINT";
    case PROTOCOL_BINARY_CMD_DEREGISTER_TAP_CLIENT:
        return "DEREGISTER_TAP_CLIENT";
    case PROTOCOL_BINARY_CMD_RESET_REPLICATION_CHAIN:
        return "RESET_REPLICATION_CHAIN";
    case PROTOCOL_BINARY_CMD_GET_META:
        return "GET_META";
    case PROTOCOL_BINARY_CMD_GETQ_META:
        return "GETQ_META";
    case PROTOCOL_BINARY_CMD_SET_WITH_META:
        return "SET_WITH_META";
    case PROTOCOL_BINARY_CMD_SETQ_WITH_META:
        return "SETQ_WITH_META";
    case PROTOCOL_BINARY_CMD_ADD_WITH_META:
        return "ADD_WITH_META";
    case PROTOCOL_BINARY_CMD_ADDQ_WITH_META:
        return "ADDQ_WITH_META";
    case PROTOCOL_BINARY_CMD_SNAPSHOT_VB_STATES:
        return "SNAPSHOT_VB_STATES";
    case PROTOCOL_BINARY_CMD_VBUCKET_BATCH_COUNT:
        return "VBUCKET_BATCH_COUNT";
    case PROTOCOL_BINARY_CMD_DEL_WITH_META:
        return "DEL_WITH_META";
    case PROTOCOL_BINARY_CMD_DELQ_WITH_META:
        return "DELQ_WITH_META";
    case PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT:
        return "CREATE_CHECKPOINT";
    case PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE:
        return "NOTIFY_VBUCKET_UPDATE";
    case PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC:
        return "ENABLE_TRAFFIC";
    case PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC:
        return "DISABLE_TRAFFIC";
    case PROTOCOL_BINARY_CMD_CHANGE_VB_FILTER:
        return "CHANGE_VB_FILTER";
    case PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE:
        return "CHECKPOINT_PERSISTENCE";
    case PROTOCOL_BINARY_CMD_RETURN_META:
        return "RETURN_META";
    case PROTOCOL_BINARY_CMD_COMPACT_DB:
        return "COMPACT_DB";
    case PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG:
        return "SET_CLUSTER_CONFIG";
    case PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG:
        return "GET_CLUSTER_CONFIG";
    case PROTOCOL_BINARY_CMD_GET_RANDOM_KEY:
        return "GET_RANDOM_KEY";
    case PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE:
        return "SEQNO_PERSISTENCE";
    case PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME:
        return "GET_ADJUSTED_TIME";
    case PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE:
        return "SET_DRIFT_COUNTER_STATE";
    case PROTOCOL_BINARY_CMD_SUBDOC_GET:
        return "SUBDOC_GET";
    case PROTOCOL_BINARY_CMD_SUBDOC_EXISTS:
        return "SUBDOC_EXISTS";
    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
        return "SUBDOC_DICT_ADD";
    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
        return "SUBDOC_DICT_UPSERT";
    case PROTOCOL_BINARY_CMD_SUBDOC_DELETE:
        return "SUBDOC_DELETE";
    case PROTOCOL_BINARY_CMD_SUBDOC_REPLACE:
        return "SUBDOC_REPLACE";
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
        return "SUBDOC_ARRAY_PUSH_LAST";
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
        return "SUBDOC_ARRAY_PUSH_FIRST";
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
        return "SUBDOC_ARRAY_INSERT";
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
        return "SUBDOC_ARRAY_ADD_UNIQUE";
    case PROTOCOL_BINARY_CMD_SUBDOC_INCREMENT:
        return "SUBDOC_INCREMENT";
    case PROTOCOL_BINARY_CMD_SUBDOC_DECREMENT:
        return "SUBDOC_DECREMENT";
    case PROTOCOL_BINARY_CMD_SCRUB:
        return "SCRUB";
    case PROTOCOL_BINARY_CMD_ISASL_REFRESH:
        return "ISASL_REFRESH";
    case PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH:
        return "SSL_CERTS_REFRESH";
    case PROTOCOL_BINARY_CMD_GET_CMD_TIMER:
        return "GET_CMD_TIMER";
    case PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN:
        return "SET_CTRL_TOKEN";
    case PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN:
        return "GET_CTRL_TOKEN";
    case PROTOCOL_BINARY_CMD_INIT_COMPLETE:
        return "INIT_COMPLETE";
    default:
        return NULL;
    }
}


uint8_t memcached_text_2_opcode(const char *cmd) {
    /* Check if this is a number */
    size_t len = strlen(cmd);
    int ii;
    for (ii = 0; ii < len; ++ii) {
        if (!isdigit(cmd[ii])) {
            break;
        }
    }

    if (ii == len) {
        return (uint8_t)atoi(cmd);
    }

    if (strcasecmp("GET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET;
    }
    if (strcasecmp("SET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET;
    }
    if (strcasecmp("ADD", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ADD;
    }
    if (strcasecmp("REPLACE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_REPLACE;
    }
    if (strcasecmp("DELETE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DELETE;
    }
    if (strcasecmp("INCREMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_INCREMENT;
    }
    if (strcasecmp("DECREMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DECREMENT;
    }
    if (strcasecmp("QUIT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_QUIT;
    }
    if (strcasecmp("FLUSH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_FLUSH;
    }
    if (strcasecmp("GETQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GETQ;
    }
    if (strcasecmp("NOOP", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_NOOP;
    }
    if (strcasecmp("VERSION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_VERSION;
    }
    if (strcasecmp("GETK", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GETK;
    }
    if (strcasecmp("GETKQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GETKQ;
    }
    if (strcasecmp("APPEND", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_APPEND;
    }
    if (strcasecmp("PREPEND", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_PREPEND;
    }
    if (strcasecmp("STAT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_STAT;
    }
    if (strcasecmp("SETQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SETQ;
    }
    if (strcasecmp("ADDQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ADDQ;
    }
    if (strcasecmp("REPLACEQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_REPLACEQ;
    }
    if (strcasecmp("DELETEQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DELETEQ;
    }
    if (strcasecmp("INCREMENTQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_INCREMENTQ;
    }
    if (strcasecmp("DECREMENTQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DECREMENTQ;
    }
    if (strcasecmp("QUITQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_QUITQ;
    }
    if (strcasecmp("FLUSHQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_FLUSHQ;
    }
    if (strcasecmp("APPENDQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_APPENDQ;
    }
    if (strcasecmp("PREPENDQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_PREPENDQ;
    }
    if (strcasecmp("VERBOSITY", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_VERBOSITY;
    }
    if (strcasecmp("TOUCH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TOUCH;
    }
    if (strcasecmp("GAT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GAT;
    }
    if (strcasecmp("GATQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GATQ;
    }
    if (strcasecmp("HELLO", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_HELLO;
    }
    if (strcasecmp("SASL_LIST_MECHS", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SASL_LIST_MECHS;
    }
    if (strcasecmp("SASL_AUTH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SASL_AUTH;
    }
    if (strcasecmp("SASL_STEP", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SASL_STEP;
    }
    if (strcasecmp("IOCTL_GET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_IOCTL_GET;
    }
    if (strcasecmp("IOCTL_SET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_IOCTL_SET;
    }
    if (strcasecmp("CONFIG_VALIDATE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CONFIG_VALIDATE;
    }
    if (strcasecmp("CONFIG_RELOAD", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CONFIG_RELOAD;
    }
    if (strcasecmp("AUDIT_PUT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_AUDIT_PUT;
    }
    if (strcasecmp("AUDIT_CONFIG_RELOAD", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD;
    }
    if (strcasecmp("RGET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RGET;
    }
    if (strcasecmp("RSET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RSET;
    }
    if (strcasecmp("RSETQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RSETQ;
    }
    if (strcasecmp("RAPPEND", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RAPPEND;
    }
    if (strcasecmp("RAPPENDQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RAPPENDQ;
    }
    if (strcasecmp("RPREPEND", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RPREPEND;
    }
    if (strcasecmp("RPREPENDQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RPREPENDQ;
    }
    if (strcasecmp("RDELETE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RDELETE;
    }
    if (strcasecmp("RDELETEQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RDELETEQ;
    }
    if (strcasecmp("RINCR", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RINCR;
    }
    if (strcasecmp("RINCRQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RINCRQ;
    }
    if (strcasecmp("RDECR", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RDECR;
    }
    if (strcasecmp("RDECRQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RDECRQ;
    }
    if (strcasecmp("SET_VBUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_VBUCKET;
    }
    if (strcasecmp("GET_VBUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_VBUCKET;
    }
    if (strcasecmp("DEL_VBUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DEL_VBUCKET;
    }
    if (strcasecmp("TAP_CONNECT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_CONNECT;
    }
    if (strcasecmp("TAP_MUTATION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_MUTATION;
    }
    if (strcasecmp("TAP_DELETE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_DELETE;
    }
    if (strcasecmp("TAP_FLUSH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_FLUSH;
    }
    if (strcasecmp("TAP_OPAQUE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_OPAQUE;
    }
    if (strcasecmp("TAP_VBUCKET_SET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET;
    }
    if (strcasecmp("TAP_CHECKPOINT_START", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START;
    }
    if (strcasecmp("TAP_CHECKPOINT_END", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END;
    }
    if (strcasecmp("GET_ALL_VB_SEQNOS", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS;
    }
    if (strcasecmp("DCP_OPEN", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_OPEN;
    }
    if (strcasecmp("DCP_ADD_STREAM", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_ADD_STREAM;
    }
    if (strcasecmp("DCP_CLOSE_STREAM", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM;
    }
    if (strcasecmp("DCP_STREAM_REQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    }
    if (strcasecmp("DCP_GET_FAILOVER_LOG", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG;
    }
    if (strcasecmp("DCP_STREAM_END", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_END;
    }
    if (strcasecmp("DCP_SNAPSHOT_MARKER", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    }
    if (strcasecmp("DCP_MUTATION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    }
    if (strcasecmp("DCP_DELETION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
    }
    if (strcasecmp("DCP_EXPIRATION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    }
    if (strcasecmp("DCP_FLUSH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_FLUSH;
    }
    if (strcasecmp("DCP_SET_VBUCKET_STATE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    }
    if (strcasecmp("DCP_NOOP", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_NOOP;
    }
    if (strcasecmp("DCP_BUFFER_ACKNOWLEDGEMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT;
    }
    if (strcasecmp("DCP_CONTROL", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL;
    }
    if (strcasecmp("DCP_RESERVED4", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DCP_RESERVED4;
    }
    if (strcasecmp("STOP_PERSISTENCE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_STOP_PERSISTENCE;
    }
    if (strcasecmp("START_PERSISTENCE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_START_PERSISTENCE;
    }
    if (strcasecmp("SET_PARAM", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_PARAM;
    }
    if (strcasecmp("GET_REPLICA", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_REPLICA;
    }
    if (strcasecmp("CREATE_BUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CREATE_BUCKET;
    }
    if (strcasecmp("DELETE_BUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DELETE_BUCKET;
    }
    if (strcasecmp("LIST_BUCKETS", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_LIST_BUCKETS;
    }
    if (strcasecmp("SELECT_BUCKET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SELECT_BUCKET;
    }
    if (strcasecmp("ASSUME_ROLE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ASSUME_ROLE;
    }
    if (strcasecmp("OBSERVE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_OBSERVE;
    }
    if (strcasecmp("OBSERVE_SEQNO", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_OBSERVE_SEQNO;
    }
    if (strcasecmp("EVICT_KEY", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_EVICT_KEY;
    }
    if (strcasecmp("GET_LOCKED", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_LOCKED;
    }
    if (strcasecmp("UNLOCK_KEY", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UNLOCK_KEY;
    }
    if (strcasecmp("LAST_CLOSED_CHECKPOINT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_LAST_CLOSED_CHECKPOINT;
    }
    if (strcasecmp("DEREGISTER_TAP_CLIENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DEREGISTER_TAP_CLIENT;
    }
    if (strcasecmp("RESET_REPLICATION_CHAIN", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RESET_REPLICATION_CHAIN;
    }
    if (strcasecmp("GET_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_META;
    }
    if (strcasecmp("UNLOCK_KEY", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UNLOCK_KEY;
    }
    if (strcasecmp("GET_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_META;
    }
    if (strcasecmp("GETQ_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GETQ_META;
    }
    if (strcasecmp("SET_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_WITH_META;
    }
    if (strcasecmp("SETQ_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SETQ_WITH_META;
    }
    if (strcasecmp("ADD_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ADD_WITH_META;
    }
    if (strcasecmp("ADDQ_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ADDQ_WITH_META;
    }
    if (strcasecmp("SNAPSHOT_VB_STATES", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SNAPSHOT_VB_STATES;
    }
    if (strcasecmp("VBUCKET_BATCH_COUNT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_VBUCKET_BATCH_COUNT;
    }
    if (strcasecmp("DEL_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DEL_WITH_META;
    }
    if (strcasecmp("DELQ_WITH_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DELQ_WITH_META;
    }
    if (strcasecmp("CREATE_CHECKPOINT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT;
    }
    if (strcasecmp("NOTIFY_VBUCKET_UPDATE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE;
    }
    if (strcasecmp("ENABLE_TRAFFIC", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC;
    }
    if (strcasecmp("DISABLE_TRAFFIC", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC;
    }
    if (strcasecmp("CHANGE_VB_FILTER", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CHANGE_VB_FILTER;
    }
    if (strcasecmp("CHECKPOINT_PERSISTENCE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE;
    }
    if (strcasecmp("RETURN_META", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_RETURN_META;
    }
    if (strcasecmp("COMPACT_DB", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_COMPACT_DB;
    }
    if (strcasecmp("SET_CLUSTER_CONFIG", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG;
    }
    if (strcasecmp("GET_CLUSTER_CONFIG", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG;
    }
    if (strcasecmp("GET_RANDOM_KEY", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_RANDOM_KEY;
    }
    if (strcasecmp("SEQNO_PERSISTENCE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE;
    }
    if (strcasecmp("GET_ADJUSTED_TIME", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME;
    }
    if (strcasecmp("SET_DRIFT_COUNTER_STATE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE;
    }
    if (strcasecmp("SUBDOC_GET", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_GET;
    }
    if (strcasecmp("SUBDOC_EXISTS", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_EXISTS;
    }
    if (strcasecmp("SUBDOC_DICT_ADD", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD;
    }
    if (strcasecmp("SUBDOC_DICT_UPSERT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT;
    }
    if (strcasecmp("SUBDOC_DELETE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_DELETE;
    }
    if (strcasecmp("SUBDOC_REPLACE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_REPLACE;
    }
    if (strcasecmp("SUBDOC_ARRAY_PUSH_LAST", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST;
    }
    if (strcasecmp("SUBDOC_ARRAY_PUSH_FIRST", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST;
    }
    if (strcasecmp("SUBDOC_ARRAY_INSERT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT;
    }
    if (strcasecmp("SUBDOC_ARRAY_ADD_UNIQUE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE;
    }
    if (strcasecmp("SUBDOC_INCREMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_INCREMENT;
    }
    if (strcasecmp("SUBDOC_DECREMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SUBDOC_DECREMENT;
    }
    if (strcasecmp("SCRUB", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SCRUB;
    }
    if (strcasecmp("ISASL_REFRESH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_ISASL_REFRESH;
    }
    if (strcasecmp("SSL_CERTS_REFRESH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH;
    }
    if (strcasecmp("GET_CMD_TIMER", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_CMD_TIMER;
    }
    if (strcasecmp("SET_CTRL_TOKEN", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN;
    }
    if (strcasecmp("GET_CTRL_TOKEN", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN;
    }
    if (strcasecmp("INIT_COMPLETE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_INIT_COMPLETE;
    }

    return 0xff;
}
