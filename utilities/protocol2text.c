/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <memcached/protocol_binary.h>
#include <string.h>
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
    case PROTOCOL_BINARY_CMD_UPR_OPEN:
        return "UPR_OPEN";
    case PROTOCOL_BINARY_CMD_UPR_ADD_STREAM:
        return "UPR_ADD_STREAM";
    case PROTOCOL_BINARY_CMD_UPR_CLOSE_STREAM:
        return "UPR_CLOSE_STREAM";
    case PROTOCOL_BINARY_CMD_UPR_STREAM_REQ:
        return "UPR_STREAM_REQ";
    case PROTOCOL_BINARY_CMD_UPR_GET_FAILOVER_LOG:
        return "UPR_GET_FAILOVER_LOG";
    case PROTOCOL_BINARY_CMD_UPR_STREAM_END:
        return "UPR_STREAM_END";
    case PROTOCOL_BINARY_CMD_UPR_SNAPSHOT_MARKER:
        return "UPR_SNAPSHOT_MARKER";
    case PROTOCOL_BINARY_CMD_UPR_MUTATION:
        return "UPR_MUTATION";
    case PROTOCOL_BINARY_CMD_UPR_DELETION:
        return "UPR_DELETION";
    case PROTOCOL_BINARY_CMD_UPR_EXPIRATION:
        return "UPR_EXPIRATION";
    case PROTOCOL_BINARY_CMD_UPR_FLUSH:
        return "UPR_FLUSH";
    case PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE:
        return "UPR_SET_VBUCKET_STATE";
    case PROTOCOL_BINARY_CMD_UPR_NOOP:
        return "UPR_NOOP";
    case PROTOCOL_BINARY_CMD_UPR_BUFFER_ACKNOWLEDGEMENT:
        return "UPR_BUFFER_ACKNOWLEDGEMENT";
    case PROTOCOL_BINARY_CMD_UPR_CONTROL:
        return "UPR_CONTROL";
    case PROTOCOL_BINARY_CMD_UPR_RESERVED4:
        return "UPR_RESERVED4";
    case PROTOCOL_BINARY_CMD_SCRUB:
        return "SCRUB";
    case PROTOCOL_BINARY_CMD_ISASL_REFRESH:
        return "ISASL_REFRESH";
    case PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH:
        return "SSL_CERTS_REFRESH";
    case PROTOCOL_BINARY_CMD_GET_CMD_TIMER:
        return "GET_CMD_TIMER";
    default:
        return NULL;
    }
}


uint8_t memcached_text_2_opcode(const char *cmd) {
    /* Check if this is a number */
    int len = strlen(cmd);
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
    if (strcasecmp("UPR_OPEN", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_OPEN;
    }
    if (strcasecmp("UPR_ADD_STREAM", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_ADD_STREAM;
    }
    if (strcasecmp("UPR_CLOSE_STREAM", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_CLOSE_STREAM;
    }
    if (strcasecmp("UPR_STREAM_REQ", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_REQ;
    }
    if (strcasecmp("UPR_GET_FAILOVER_LOG", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_GET_FAILOVER_LOG;
    }
    if (strcasecmp("UPR_STREAM_END", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_STREAM_END;
    }
    if (strcasecmp("UPR_SNAPSHOT_MARKER", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_SNAPSHOT_MARKER;
    }
    if (strcasecmp("UPR_MUTATION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_MUTATION;
    }
    if (strcasecmp("UPR_DELETION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_DELETION;
    }
    if (strcasecmp("UPR_EXPIRATION", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_EXPIRATION;
    }
    if (strcasecmp("UPR_FLUSH", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_FLUSH;
    }
    if (strcasecmp("UPR_SET_VBUCKET_STATE", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE;
    }
    if (strcasecmp("UPR_NOOP", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_NOOP;
    }
    if (strcasecmp("UPR_BUFFER_ACKNOWLEDGEMENT", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_BUFFER_ACKNOWLEDGEMENT;
    }
    if (strcasecmp("UPR_CONTROL", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_CONTROL;
    }
    if (strcasecmp("UPR_RESERVED4", cmd) == 0) {
        return (uint8_t)PROTOCOL_BINARY_CMD_UPR_RESERVED4;
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

    return 0xff;
}
