/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef SYSLOG_H
#define    SYSLOG_H

/*
 *   Defines the structures required to hold syslog compliant event.
 *   As stated in the Syslog Protocol, (standardized by the IETF in RFC 5424).
 */

#include <memcached/events.h>


typedef enum {

    SYSLOG_EMERGENCY=0,
    SYSLOG_ALERT=1,
    SYSLOG_CRITICAL=2,
    SYSLOG_ERROR=3,
    SYSLOG_WARNING=4,
    SYSLOG_NOTICE=5,
    SYSLOG_INFORMATIONAL=6,
    SYSLOG_DEBUG=7,
    SYSLOG_UNKNOWN=8

} SyslogSeverity;



typedef struct {

    uint8_t prival; /* range 0...191 */
    uint8_t version; /* 0...99 */
    char hostname[256]; /* max 255 char */
    char app_name[49]; /* max 48 char */
    uint64_t procid;
    EventID msgid;
    time_t time;
    uint32_t time_secfrac; /* 0..999999 */
    char msg[2048];
    char sd[2048];

}SyslogEvent;

#endif    /*SYSLOG_EVENTS_H */
