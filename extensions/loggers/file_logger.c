/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @todo "chain" the loggers - I should use the next logger instead of stderr
 * @todo don't format into a temporary buffer, but directly into the
 *       destination buffer
 */
#include "config.h"
#include <stdarg.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <time.h>

#ifdef WIN32
#include <io.h>
#include <process.h>
#define getpid() _getpid()
#endif

#include <platform/platform.h>

#ifdef WIN32_H
#undef close
#endif

#include <memcached/extension.h>
#include <memcached/engine.h>
#include <memcached/syslog.h>

#include "extensions/protocol_extension.h"

/* Pointer to the server API */
static SERVER_HANDLE_V1 *sapi;

/* The current log level set by the user. We should ignore all log requests
 * with a finer log level than this. We've registered a listener to update
 * the log level when the user change it
 */
static EXTENSION_LOG_LEVEL current_log_level = EXTENSION_LOG_WARNING;

/* All messages above the current level shall be sent to stderr immediately */
static EXTENSION_LOG_LEVEL output_level = EXTENSION_LOG_WARNING;

/* To avoid the logfile to grow forever, we'll start logging to another
 * file when we've added a certain amount of data to the logfile. You may
 * tune this size by using the "cyclesize" configuration parameter. Use 100MB
 * as the default (makes it a reasonable size to work with in your favorite
 * editor ;-))
 */
static size_t cyclesz = 100 * 1024 * 1024;

/*
 * We're using two buffers for logging. We'll be inserting data into one,
 * while we're working on writing the other one to disk. Given that the disk
 * is way slower than our CPU, we might end up in a situation that we'll be
 * blocking the frontend threads if you're logging too much.
 */
static struct logbuffer {
    /* Pointer to beginning of the datasegment of this buffer */
    char *data;
    /* The current offset of the buffer */
    size_t offset;
} buffers[2];

/* The index in the buffers where we're currently inserting more data */
static int currbuffer;

/* If we should try to pretty-print the severity or not */
static bool prettyprint = false;

/* Are we running in a unit test (don't print warnings to stderr) */
static bool unit_test = false;

/* The size of the buffers (this may be tuned by the buffersize configuration
 * parameter */
static size_t buffersz = 2048 * 1024;

/* The sleeptime between each forced flush of the buffer */
static size_t sleeptime = 60;

/* To avoid race condition we're protecting our shared resources with a
 * single mutex. */
static cb_mutex_t mutex;

/* The thread performing the disk IO will be waiting for the input buffers
 * to be filled by sleeping on the following condition variable. The
 * frontend threads will notify the condition variable when the buffer is
 * > 75% full
 */
static cb_cond_t cond;

/* In the "worst case scenarios" we're logging so much that the disk thread
 * can't keep up with the the frontend threads. In these rare situations
 * the frontend threads will block and wait for the flusher to free up log
 * space
 */
static cb_cond_t space_cond;

static char hostname[256];
static pid_t pid;

/* To avoid the logs beeing flooded by the same log messages we try to
 * de-duplicate the messages and instead print out:
 *   "message repeated xxx times"
 */
static struct {
    /* The last message being added to the log */
    char buffer[512];
    /* The number of times we've seen this message */
    int count;
    /* The offset into the buffer for where the text start (after the
     * timestamp)
     */
    int offset;
} lastlog;

typedef void * HANDLE;

static HANDLE stdio_open(const char *path, const char *mode) {
    HANDLE ret = fopen(path, mode);
    if (ret) {
        setbuf(ret, NULL);
    }
    return ret;
}

static void stdio_close(HANDLE handle) {
    (void)fclose(handle);
}

static void stdio_flush(HANDLE handle) {
    fflush(handle);
}

static ssize_t stdio_write(HANDLE handle, const void *ptr, size_t nbytes) {
    return (ssize_t)fwrite(ptr, 1, nbytes, handle);
}

struct io_ops {
    HANDLE (*open)(const char *path, const char *mode);
    void (*close)(HANDLE handle);
    void (*flush)(HANDLE handle);
    ssize_t (*write)(HANDLE handle, const void *ptr, size_t nbytes);
} iops;

static const char *extension = "txt";

static void do_add_log_entry(const char *msg, size_t size) {
    /* wait until there is room in the current buffer */
    while ((buffers[currbuffer].offset + size) >= buffersz) {
        if (!unit_test) {
            fprintf(stderr, "WARNING: waiting for log space to be available\n");
        }
        cb_cond_wait(&space_cond, &mutex);
    }

    /* We could have performed the memcpy outside the locked region,
     * but then we would need to handle the situation where we're
     * flipping the ownership of the buffer (otherwise we could be
     * writing rubbish to the file) */
    memcpy(buffers[currbuffer].data + buffers[currbuffer].offset,
           msg, size);
    buffers[currbuffer].offset += size;
    if (buffers[currbuffer].offset > (buffersz * 0.75)) {
        /* we're getting full.. time get the logger to start doing stuff! */
        cb_cond_signal(&cond);
    }
}

static void flush_last_log(void) {
    if (lastlog.count > 1) {
        char buffer[80];
        size_t len = snprintf(buffer, sizeof(buffer),
                              "message repeated %u times\n",
                              lastlog.count);
        do_add_log_entry(buffer, len);
    }
}

static void add_log_entry(const char *msg, int prefixlen, size_t size)
{
    cb_mutex_enter(&mutex);

    if (size < sizeof(lastlog.buffer)) {
        if (memcmp(lastlog.buffer + lastlog.offset, msg + prefixlen, size-prefixlen) == 0) {
            ++lastlog.count;
        } else {
            flush_last_log();
            do_add_log_entry(msg, size);
            memcpy(lastlog.buffer, msg, size);
            lastlog.offset = prefixlen;
            lastlog.count = 0;
        }
    } else {
        flush_last_log();
        lastlog.buffer[0] = '\0';
        lastlog.count = 0;
        lastlog.offset = 0;
        do_add_log_entry(msg, size);
    }

    cb_mutex_exit(&mutex);
}

static const char *severity2string(EXTENSION_LOG_LEVEL sev) {
    switch (sev) {
    case EXTENSION_LOG_WARNING:
        return "WARNING";
    case EXTENSION_LOG_INFO:
        return "INFO";
    case EXTENSION_LOG_DEBUG:
        return "DEBUG";
    case EXTENSION_LOG_DETAIL:
        return "DETAIL";
    default:
        return "????";
    }
}

/* Takes the syslog compliant event and calls the native logging functionality */
static void syslog_event_receiver(SyslogEvent *event) {
    char buffer[2048];
    size_t avail_char_in_buffer = sizeof(buffer) - 1; /*space excluding terminating char */
    int prefixlen = 0;
    char str[40];
    int error;
    struct tm tval;
    time_t nsec;
    uint8_t syslog_severity = event->prival & 7; /* Mask out all but 3 least-significant bits */
    EXTENSION_LOG_LEVEL severity = EXTENSION_LOG_WARNING;

    switch (syslog_severity) {
    case SYSLOG_WARNING:
        severity = EXTENSION_LOG_WARNING;
        break;
    case SYSLOG_NOTICE:
        severity = EXTENSION_LOG_INFO;
        break;
    case SYSLOG_INFORMATIONAL:
        severity = EXTENSION_LOG_DEBUG;
        break;
    case SYSLOG_DEBUG:
        severity = EXTENSION_LOG_DETAIL;
        break;
    default:
        fprintf(stderr, "ERROR: Unknown syslog_severity\n");
    }

    tval.tm_sec = event->time_second;
    tval.tm_min = event->time_minute + event->offset_minute;
    tval.tm_hour = event->time_hour + event->offset_hour;
    tval.tm_mday = event->date_mday;
    tval.tm_mon = event->date_month - 1;
    tval.tm_year = event->date_fullyear - 1900;
    tval.tm_isdst = -1;
    tval.tm_wday = -1;
    tval.tm_yday = -1;

    nsec = mktime(&tval);

#ifdef WIN32
    error = (asctime_s(str, sizeof(str), &tval) != 0);
#else
    error = (asctime_r(&tval, str) == NULL);
#endif

    if (error) {
        prefixlen = snprintf(buffer, avail_char_in_buffer, "%u.%06u",
                             (unsigned int)nsec,
                             (unsigned int)event->time_secfrac);
    } else {
        const char *tz;
#ifdef HAVE_TM_ZONE
        tz = tval.tm_zone;
#else
        tz = tzname[tval.tm_isdst ? 1 : 0];
#endif
        /* trim off ' YYYY\n' */
        str[strlen(str) - 6] = '\0';
        prefixlen = snprintf(buffer, avail_char_in_buffer, "%s.%06u %s",
                             str, (unsigned int)event->time_secfrac,
                             tz);
    }

    if (prettyprint) {
        prefixlen += snprintf(buffer+prefixlen, avail_char_in_buffer-prefixlen,
                              " %s: ", severity2string(severity));
    } else {
        prefixlen += snprintf(buffer+prefixlen, avail_char_in_buffer-prefixlen,
                              " %u: ", (unsigned int)severity);
    }

    avail_char_in_buffer -= prefixlen;

    /* now copy the syslog msg into the buffer */
    strncat(buffer, event->msg, avail_char_in_buffer);

    if (strlen(event->msg) > avail_char_in_buffer) {
        fprintf(stderr, "Event message too big... cropped. Full msg: %s \n", event->msg);
    }

    if (severity >= current_log_level || severity >= output_level) {
        if (severity >= output_level) {
            fputs(buffer, stderr);
            fflush(stderr);
        }

        if (severity >= current_log_level) {
            add_log_entry(buffer, prefixlen, strlen(buffer));
        }
    }
}


/* Takes the current logging format and produces syslogd compliant event */
static void logger_log_wrapper(EXTENSION_LOG_LEVEL severity,
                               const void* client_cookie,
                               const char *fmt, ...) {
    (void)client_cookie;
    SyslogEvent event;
    size_t avail_char_in_msg = sizeof(event.msg) - 1; /*space excluding terminating char */
    struct timeval now;
    va_list ap;
    int len;
    uint8_t facility = 16;  /* Facility - defaulting to local0 */
    uint8_t syslog_severity;

    /* RFC5424 uses version 1 of syslog protocol */
    event.version = 1;
    event.msgid = GENERIC_EVENT;
    strcpy(event.app_name, "memcached");
    strcpy(event.hostname, hostname);
    event.procid = pid;

    va_start(ap, fmt);
    len = vsnprintf(event.msg, avail_char_in_msg, fmt, ap);
    va_end(ap);

    /* If an encoding error occurs with vsnprintf a -ive number is returned */
    if ((len <= avail_char_in_msg) && (len >= 0)) {
        /* add a new line to the message if not already there */
        if (event.msg[len - 1] != '\n') {
            event.msg[len++] = '\n';
            event.msg[len] ='\0';
        }
    } else {
        fprintf(stderr, "Syslog message dropped... too big \n");
    }

    switch (severity) {
    case EXTENSION_LOG_WARNING:
        syslog_severity = SYSLOG_WARNING;
        break;
    case EXTENSION_LOG_INFO:
        syslog_severity = SYSLOG_NOTICE;
        break;
    case EXTENSION_LOG_DEBUG:
        syslog_severity = SYSLOG_INFORMATIONAL;
        break;
    case EXTENSION_LOG_DETAIL:
        syslog_severity = SYSLOG_DEBUG;
        break;
    default:
        fprintf(stderr, "Unknown severity\n");
        syslog_severity = SYSLOG_UNKNOWN;
    }

    /*
       To produce the priority_value multiply facility by 8
       i.e. shift to left 3 places. Then add the syslog_severity
     */
    event.prival = (facility << 3) + syslog_severity;

    /* Fill-in date structure */
    if (cb_get_timeofday(&now) == 0) {
        struct tm localval, utcval;
        time_t nsec = (time_t)now.tv_sec;

        cb_gmtime_r(&nsec, &utcval);
        cb_localtime_r(&nsec, &localval);

        event.date_fullyear = 1900 + utcval.tm_year;
        event.date_month = 1 + utcval.tm_mon;
        event.date_mday = utcval.tm_mday;
        event.time_hour = utcval.tm_hour;
        event.time_minute = utcval.tm_min;
        event.time_second = utcval.tm_sec;
        event.time_secfrac = now.tv_usec;
        /* Calculate the offset from UTC to local-time */
        event.offset_hour = localval.tm_hour - utcval.tm_hour;
        event.offset_minute = localval.tm_min - utcval.tm_min;

    } else {
        fprintf(stderr, "gettimeofday failed in file_logger.c: %s\n", strerror(errno));
        return;
    }
    /* Send the syslog event */
    syslog_event_receiver(&event);
}


static HANDLE open_logfile(const char *fnm) {
    static unsigned int next_id = 0;
    char fname[1024];
    HANDLE ret;
    do {
        sprintf(fname, "%s.%d.%s", fnm, next_id++, extension);
    } while (access(fname, F_OK) == 0);
    ret = iops.open(fname, "wb");
    if (!ret) {
        fprintf(stderr, "Failed to open memcached log file\n");
    }
    return ret;
}

static void close_logfile(HANDLE fp) {
    if (fp) {
        iops.close(fp);
    }
}

static HANDLE reopen_logfile(HANDLE old, const char *fnm) {
    close_logfile(old);
    return open_logfile(fnm);
}

static size_t flush_pending_io(HANDLE file, struct logbuffer *lb) {
    size_t ret = 0;
    if (lb->offset > 0) {
        char *ptr = lb->data;
        size_t towrite = ret = lb->offset;

        while (towrite > 0) {
            int nw = iops.write(file, ptr, towrite);
            if (nw > 0) {
                ptr += nw;
                towrite -= nw;
            }
        }
        lb->offset = 0;
        iops.flush(file);
    }

    return ret;
}

static void flush_all_buffers_to_file(HANDLE file) {
    while (buffers[currbuffer].offset) {
        int this  = currbuffer;
        currbuffer = (currbuffer == 0) ? 1 : 0;
        flush_pending_io(file, buffers + this);
    }
}

static volatile int run = 1;
static cb_thread_t tid;
static HANDLE fp;

static void logger_thead_main(void* arg)
{
    size_t currsize = 0;
    fp = open_logfile(arg);

    struct timeval tp;
    cb_get_timeofday(&tp);
    time_t next = (time_t)tp.tv_sec;

    cb_mutex_enter(&mutex);
    while (run) {
        cb_get_timeofday(&tp);

        while ((time_t)tp.tv_sec >= next  ||
               buffers[currbuffer].offset > (buffersz * 0.75)) {
            int this  = currbuffer;
            next = (time_t)tp.tv_sec + 1;
            currbuffer = (currbuffer == 0) ? 1 : 0;
            /* Let people who is blocked for space continue */
            cb_cond_broadcast(&space_cond);

            /* Perform file IO without the lock */
            cb_mutex_exit(&mutex);

            currsize += flush_pending_io(fp, buffers + this);
            if (currsize > cyclesz) {
                fp = reopen_logfile(fp, arg);
                currsize = 0;
            }
            cb_mutex_enter(&mutex);
        }

        cb_get_timeofday(&tp);
        next = (time_t)tp.tv_sec + (time_t)sleeptime;
        if (unit_test) {
            cb_cond_timedwait(&cond, &mutex, 100);
        } else {
            cb_cond_timedwait(&cond, &mutex, 1000 * sleeptime);
        }
    }

    if (fp) {
        flush_all_buffers_to_file(fp);
        close_logfile(fp);
    }

    cb_mutex_exit(&mutex);
    free(arg);
    free(buffers[0].data);
    free(buffers[1].data);
}

static void exit_handler(void) {
    /* Unfortunately it looks like the C runtime from MSVC "kills" the
     * threads before the "atexit" handler is run, causing the program
     * to halt in one of these steps depending on the state of the
     * variables. Just disable the code for now.
     */
#ifndef WIN32
    cb_mutex_enter(&mutex);
    run = 0;
    cb_cond_signal(&cond);
    cb_mutex_exit(&mutex);

    cb_join_thread(tid);
#endif
}

static const char *get_name(void) {
    return "file logger";
}

static EXTENSION_LOGGER_DESCRIPTOR descriptor;

static void on_log_level(const void *cookie, ENGINE_EVENT_TYPE type,
                         const void *event_data, const void *cb_data) {
    if (sapi != NULL) {
        current_log_level = sapi->log->get_level();
    }
}

static void logger_shutdown(bool force) {
    if (force) {
        // Don't bother attempting to take any mutexes - other threads may
        // never run again. Just flush the buffers asap.
        if (fp) {
            flush_all_buffers_to_file(fp);
            close_logfile(fp);
            fp = NULL;
        }
        return;
    }

    int running;
    cb_mutex_enter(&mutex);
    flush_last_log();
    running = run;
    run = 0;
    cb_cond_signal(&cond);
    cb_mutex_exit(&mutex);

    if (running) {
        cb_join_thread(tid);
    }
}

MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(const char *config,
                                                     GET_SERVER_API get_server_api)
{
    char *fname = NULL;

    cb_mutex_initialize(&mutex);
    cb_cond_initialize(&cond);
    cb_cond_initialize(&space_cond);

    iops.open = stdio_open;
    iops.close = stdio_close;
    iops.flush = stdio_flush;
    iops.write = stdio_write;
    descriptor.get_name = get_name;
    descriptor.log = logger_log_wrapper;
    descriptor.shutdown = logger_shutdown;

#ifdef HAVE_TM_ZONE
    tzset();
#endif

    sapi = get_server_api();
    if (sapi == NULL) {
        return EXTENSION_FATAL;
    }

    pid = getpid();

    if (gethostname(hostname, sizeof(hostname))) {
        fprintf(stderr,"Could not get the hostname");
        strcpy(hostname,"unknown");
    }

    if (config != NULL) {
        char *loglevel = NULL;
        struct config_item items[8];
        int ii = 0;
        memset(&items, 0, sizeof(items));

        items[ii].key = "filename";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &fname;
        ++ii;

        items[ii].key = "buffersize";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &buffersz;
        ++ii;

        items[ii].key = "cyclesize";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &cyclesz;
        ++ii;

        items[ii].key = "loglevel";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &loglevel;
        ++ii;

        items[ii].key = "prettyprint";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &prettyprint;
        ++ii;

        items[ii].key = "sleeptime";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &sleeptime;
        ++ii;

        items[ii].key = "unit_test";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &unit_test;
        ++ii;

        items[ii].key = NULL;
        ++ii;
        cb_assert(ii == 8);

        if (sapi->core->parse_config(config, items, stderr) != ENGINE_SUCCESS) {
            return EXTENSION_FATAL;
        }

        if (loglevel != NULL) {
            if (strcasecmp("warning", loglevel) == 0) {
                output_level = EXTENSION_LOG_WARNING;
            } else if (strcasecmp("info", loglevel) == 0) {
                output_level = EXTENSION_LOG_INFO;
            } else if (strcasecmp("debug", loglevel) == 0) {
                output_level = EXTENSION_LOG_DEBUG;
            } else if (strcasecmp("detail", loglevel) == 0) {
                output_level = EXTENSION_LOG_DETAIL;
            } else {
                fprintf(stderr, "Unknown loglevel: %s. Use warning/info/debug/detail\n",
                        loglevel);
                return EXTENSION_FATAL;
            }
        }
        free(loglevel);
    }

    if (fname == NULL) {
        fname = strdup("memcached");
    }

    buffers[0].data = malloc(buffersz);
    buffers[1].data = malloc(buffersz);

    if (buffers[0].data == NULL || buffers[1].data == NULL || fname == NULL) {
        fprintf(stderr, "Failed to allocate memory for the logger\n");
        free(fname);
        free(buffers[0].data);
        free(buffers[1].data);
        return EXTENSION_FATAL;
    }

    if (cb_create_thread(&tid, logger_thead_main, fname, 0) < 0) {
        fprintf(stderr, "Failed to initialize the logger\n");
        free(fname);
        free(buffers[0].data);
        free(buffers[1].data);
        return EXTENSION_FATAL;
    }
    atexit(exit_handler);

    current_log_level = sapi->log->get_level();
    if (!sapi->extension->register_extension(EXTENSION_LOGGER, &descriptor)) {
        return EXTENSION_FATAL;
    }
    sapi->callback->register_callback(NULL, ON_LOG_LEVEL, on_log_level, NULL);

    return EXTENSION_SUCCESS;
}
