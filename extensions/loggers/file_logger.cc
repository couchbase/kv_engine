/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @todo "chain" the loggers - I should use the next logger instead of stderr
 * @todo don't format into a temporary buffer, but directly into the
 *       destination buffer
 */
#include "config.h"
#include <stdarg.h>
#include <stdio.h>
#include <errno.h>
#include <mutex>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>

#ifdef WIN32
#include <io.h>
#endif

#include <platform/cb_malloc.h>
#include <platform/platform.h>

#ifdef WIN32_H
#undef close
#endif

#include <memcached/extension.h>
#include <memcached/engine.h>
#include <memcached/syslog.h>
#include <memcached/isotime.h>
#include <platform/strerror.h>

#include "extensions/protocol_extension.h"
#include "file_logger_utilities.h"

/* Pointer to the server API */
static SERVER_HANDLE_V1 *sapi;

/* The current log level set by the user. We should ignore all log requests
 * with a finer log level than this. We've registered a listener to update
 * the log level when the user change it
 */
static EXTENSION_LOG_LEVEL current_log_level = EXTENSION_LOG_NOTICE;

/* All messages above the current level shall be sent to stderr immediately */
static const EXTENSION_LOG_LEVEL stderr_output_level = EXTENSION_LOG_WARNING;

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
static int currbuffer = 0;

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

// mutex used to synchronize access to stderr
std::mutex stderr_mutex;

/* To avoid the logs beeing flooded by the same log messages we try to
 * de-duplicate the messages and instead print out:
 *   "message repeated xxx times"
 */
static struct {
    /* The last message being added to the log */
    char buffer[2048];
    /* The number of times we've seen this message */
    int count;
    /* The offset into the buffer for where the text start (after the
     * timestamp)
     */
    int offset;
    /* The sec when the entry was added (used for flushing of the
     * dedupe log)
     */
    time_t created;
} lastlog;

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

static void flush_last_log(bool timebased) {
    if (lastlog.count > 1) {
        ISOTime::ISO8601String timestamp;
        ISOTime::generatetimestamp(timestamp);

        char buffer[512];
        int offset = snprintf(buffer, sizeof(buffer),
                              "%s Message repeated %u times\n",
                              timestamp.data(), lastlog.count);

        if (offset < 0 || offset >= int(sizeof(buffer))) {
            // Failed to format... ignore for now..
            return;
        }

        // Only try to flush if there is enough free space, otherwise
        // we'll be causing a deadlock
        if (timebased && ((buffers[currbuffer].offset + offset) >= buffersz)) {
            return ;
        }

        do_add_log_entry(buffer, offset);
        lastlog.buffer[0] = '\0';
        lastlog.count = 0;
        lastlog.offset = 0;
        lastlog.created = 0;
    }
}

static void add_log_entry(time_t now, const char *msg, int prefixlen, size_t size)
{
    cb_mutex_enter(&mutex);

    if (size < sizeof(lastlog.buffer)) {
        if (memcmp(lastlog.buffer + lastlog.offset, msg + prefixlen, size-prefixlen) == 0) {
            ++lastlog.count;
        } else {
            flush_last_log(false);
            do_add_log_entry(msg, size);
            memcpy(lastlog.buffer, msg, size);
            lastlog.offset = prefixlen;
            lastlog.created = now;
        }
    } else {
        flush_last_log(false);
        do_add_log_entry(msg, size);
    }

    cb_mutex_exit(&mutex);
}

static const char *severity2string(EXTENSION_LOG_LEVEL sev) {
    switch (sev) {
    case EXTENSION_LOG_FATAL:
        return "FATAL";
    case EXTENSION_LOG_WARNING:
        return "WARNING";
    case EXTENSION_LOG_NOTICE:
        return "NOTICE";
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

/* Formats a log entry (consisting {time, frac_of_second, severity, message})
 * into the specified buffer.
 * Returns the number of characters for the prefix (timestamp / severity).
 */
static int format_log_entry(char* buffer, size_t buf_len, time_t time,
                            uint32_t frac_of_second,
                            EXTENSION_LOG_LEVEL severity, const char* message) {
    ISOTime::ISO8601String timestamp;
    ISOTime::generatetimestamp(timestamp, time, frac_of_second);
    const int prefix_len = snprintf(buffer, buf_len, "%s %s ", timestamp.data(),
                                   severity2string(severity));
    if (prefix_len < 0 || size_t(prefix_len) >= buf_len) {
        return 0;
    }

    const int msglen = snprintf(buffer + prefix_len, buf_len - prefix_len,
                                "%s", message);
    if (msglen < 0 || size_t(msglen) >= (buf_len - prefix_len)) {
        return 0;
    }

    if (size_t(prefix_len + msglen) > (buf_len - 1)) {
        {
            std::lock_guard<std::mutex> guard(stderr_mutex);
            // Message cropped.
            std::cerr << "Message too big to fit in event. Full message: "
                      << message;
            std::cerr.flush();
        }
        const char cropped[] = " [cut]";
        snprintf(buffer + (buf_len - sizeof(cropped)), sizeof(cropped),
                 "%s", cropped);
    }
    return prefix_len;
}

/* Takes the syslog compliant event and calls the native logging functionality */
static void syslog_event_receiver(SyslogEvent *event) {
    uint8_t syslog_severity = event->prival & 7; /* Mask out all but 3 least-significant bits */
    EXTENSION_LOG_LEVEL severity = EXTENSION_LOG_WARNING;

    switch (syslog_severity) {
    case SYSLOG_CRITICAL:
        severity = EXTENSION_LOG_FATAL;
        break;
    case SYSLOG_WARNING:
        severity = EXTENSION_LOG_WARNING;
        break;
    case SYSLOG_NOTICE:
        severity = EXTENSION_LOG_NOTICE;
        break;
    case SYSLOG_INFORMATIONAL:
        severity = EXTENSION_LOG_INFO;
        break;
    case SYSLOG_DEBUG:
        severity = EXTENSION_LOG_DEBUG;
        break;
    default:
        fprintf(stderr, "ERROR: Unknown syslog_severity\n");
    }

    char buffer[2048];
    const int prefixlen = format_log_entry(buffer, sizeof(buffer),
                                           event->time, event->time_secfrac,
                                           severity, event->msg);

    if (severity >= current_log_level || severity >= stderr_output_level) {
        if (severity >= stderr_output_level) {
            std::lock_guard<std::mutex> guard(stderr_mutex);
            std::cerr << buffer;
            std::cerr.flush();
        }

        if (severity >= current_log_level) {
            add_log_entry(event->time, buffer, prefixlen, strlen(buffer));
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
    event.procid = (uint64_t)pid;

    va_start(ap, fmt);
    len = vsnprintf(event.msg, avail_char_in_msg, fmt, ap);
    va_end(ap);

    /* array indices start from zero, so need to offset length by minus one */
    auto index = len - 1;
    /* If an encoding error occurs with vsnprintf a -ive number is returned */
    if (len >= 0) {
        if (len < static_cast<int>(avail_char_in_msg)) {
            /* add a new line to the message if not already there */
            if (event.msg[index] != '\n') {
                event.msg[index + 1] = '\n';
                event.msg[index + 2] = '\0';
            } else {
                event.msg[index + 1] = '\0';
            }
        } else {
            /* len is equal avail_char_in_msg */

            /* index to array element containing last character
             * (excluding terminating character)
             */
            index = avail_char_in_msg - 1;
            if (event.msg[index] != '\n') {
                std::lock_guard<std::mutex> guard(stderr_mutex);
                std::cerr << "Syslog message being truncated... too big"
                          << std::endl;
                event.msg[index] = '\n';
            }
            event.msg[index + 1] = '\0';
        }
    } else {
        std::lock_guard<std::mutex> guard(stderr_mutex);
        std::cerr << "Syslog message dropped... too big" << std::endl;
        return;
    }

    switch (severity) {
    case EXTENSION_LOG_FATAL:
        syslog_severity = SYSLOG_CRITICAL;
        break;
    case EXTENSION_LOG_WARNING:
        syslog_severity = SYSLOG_WARNING;
        break;
    case EXTENSION_LOG_NOTICE:
        syslog_severity = SYSLOG_NOTICE;
        break;
    case EXTENSION_LOG_INFO:
        syslog_severity = SYSLOG_INFORMATIONAL;
        break;
    case EXTENSION_LOG_DEBUG:
    case EXTENSION_LOG_DETAIL:
        syslog_severity = SYSLOG_DEBUG;
        break;
    default: {
            std::lock_guard<std::mutex> guard(stderr_mutex);
            std::cerr << "Unknown severity: " << severity
                      << " using SYSLOG_UNKNOWN" << std::endl;
        }
        syslog_severity = SYSLOG_UNKNOWN;
    }

    /*
       To produce the priority_value multiply facility by 8
       i.e. shift to left 3 places. Then add the syslog_severity
     */
    event.prival = (facility << 3) + syslog_severity;

    /* Fill-in date structure */
    if (cb_get_timeofday(&now) == 0) {
        event.time = (time_t)now.tv_sec;
        event.time_secfrac = (uint32_t)now.tv_usec;
    } else {
        std::lock_guard<std::mutex> guard(stderr_mutex);
        std::cerr << "gettimeofday failed in file_logger.cc: " << cb_strerror()
                  << std::endl;
        return;
    }
    /* Send the syslog event */
    syslog_event_receiver(&event);
}

static unsigned long next_file_id = 0;

static FILE *open_logfile(const char *fnm) {
    char fname[1024];
    FILE *ret;

    // Search for the next logfile number which doesn't yet exist.
    // There's a subtlety here - if we have run out of file descriptors (EMFILE) -
    // access() can fail with ENOENT, which means we need to ensure that we only
    // search from *after* the last successful open call - i.e. try_id always
    // starts from one after the last successful open, otherwise we can
    // re-open the existing log file and overwrite it!
    unsigned long try_id = next_file_id;
    do {
        sprintf(fname, "%s.%06lu.txt", fnm, try_id++);
    } while (access(fname, F_OK) == 0);

    ret = fopen(fname, "ab");
    if (ret != nullptr) {
        setbuf(ret, nullptr);
        next_file_id = try_id;
    }

    return ret;
}

static void close_logfile(FILE *fp) {
    if (fp) {
        fclose(fp);
    }
}

static FILE *rotate_logfile(FILE *old, const char *fnm) {
    FILE* new_log = open_logfile(fnm);
    if (new_log == NULL && old != NULL) {
        // Can't open the logfile. We already output messages of
        // sufficient level to stderr, let's hope that's enough.
        std::string msg("Failed to open next logfile: " +
                        std::string(strerror(errno)) +
                        " - disabling file logging. Messages at '" +
                        severity2string(stderr_output_level) +
                        "' or higher still output to babysitter log file\n");
        struct timeval now;
        cb_get_timeofday(&now);
        char log_entry[1024];
        format_log_entry(log_entry, sizeof(log_entry), now.tv_sec, now.tv_usec,
                         EXTENSION_LOG_WARNING, msg.c_str());

        fwrite(log_entry, 1, strlen(log_entry), old);
        fflush(old);
        // Send to stderr for good measure.
        std::lock_guard<std::mutex> guard(stderr_mutex);
        std::cerr << log_entry;
    }
    close_logfile(old);
    return new_log;
}

static size_t flush_pending_io(FILE *file, struct logbuffer *lb) {
    size_t ret = 0;
    if (lb->offset > 0) {
        if (file) {
            char *ptr = lb->data;
            size_t towrite = ret = lb->offset;
            while (towrite > 0) {
                auto nw = fwrite(ptr, 1, towrite, file);
                if (nw > 0) {
                    ptr += nw;
                    towrite -= nw;
                }
            }
            lb->offset = 0;
            fflush(file);
        } else {
            // Cannot write as have no FD, however we also don't want
            // to leave the logbuffer as-is as that would result in it
            // filling up and producers being deadlocked. Therefore
            // discard the logbuffer contents.
            // Note that any message at output_level is always logged
            // to stderr (for babysitter) so those messages will not
            // be lost.
            lb->offset = 0;
        }
    }

    return ret;
}

static void flush_all_buffers_to_file(FILE *file) {
    while (buffers[currbuffer].offset) {
        int curr  = currbuffer;
        currbuffer = (currbuffer == 0) ? 1 : 0;
        flush_pending_io(file, buffers + curr);
    }
}

static volatile int run = 1;
static cb_thread_t tid;
static FILE *fp;

static void logger_thread_main(void* arg)
{
    size_t currsize = 0;

    struct timeval tp;
    cb_get_timeofday(&tp);
    time_t next = (time_t)tp.tv_sec;

    cb_mutex_enter(&mutex);
    while (run) {
        cb_get_timeofday(&tp);

        while ((time_t)tp.tv_sec >= next  ||
               buffers[currbuffer].offset > (buffersz * 0.75)) {
            int curr  = currbuffer;
            next = (time_t)tp.tv_sec + 1;
            currbuffer = (currbuffer == 0) ? 1 : 0;
            /* Let people who is blocked for space continue */
            cb_cond_broadcast(&space_cond);

            /* Perform file IO without the lock */
            cb_mutex_exit(&mutex);

            /* In case we failed to open the log file last time (e.g. EMFILE),
               re-attempt now. */
            if (fp == NULL) {
                fp = open_logfile(reinterpret_cast<const char*>(arg));
                if (fp != NULL) {
                    // Record that the log is back online.
                    struct timeval now;
                    cb_get_timeofday(&now);
                    char log_entry[1024];
                    format_log_entry(log_entry, sizeof(log_entry),
                                     now.tv_sec, now.tv_usec,
                                     EXTENSION_LOG_WARNING,
                                     "Restarting file logging\n");

                    fwrite(log_entry, 1, strlen(log_entry), fp);
                    // Send to stderr for good measure.
                    std::lock_guard<std::mutex> guard(stderr_mutex);
                    std::cerr << log_entry;
                }
            }

            currsize += flush_pending_io(fp, buffers + curr);
            if (currsize > cyclesz) {
                fp = rotate_logfile(fp, reinterpret_cast<const char*>(arg));
                currsize = 0;
            }
            cb_mutex_enter(&mutex);
        }

        // Only run dedupe for ~5 seconds
        if (lastlog.count > 0 && (lastlog.created + 4 < tp.tv_sec)) {
            flush_last_log(true);
        }

        cb_get_timeofday(&tp);
        next = (time_t)tp.tv_sec + (time_t)sleeptime;

        if (unit_test) {
            cb_cond_timedwait(&cond, &mutex, 100);
        } else {
            cb_cond_timedwait(&cond, &mutex, (unsigned int)(1000 * sleeptime));
        }
    }

    /* The log file might not be open, however we may have
     * an event in the buffer that needs flushing to a file.
     */
    if (buffers[currbuffer].offset != 0 && !fp) {
        fp = open_logfile(reinterpret_cast<const char*>(arg));
    }
    if (fp) {
        flush_all_buffers_to_file(fp);
        close_logfile(fp);
    }

    cb_mutex_exit(&mutex);
    cb_free(arg);
    cb_free(buffers[0].data);
    cb_free(buffers[1].data);
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
    flush_last_log(false);
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
    /* memcached_logger_test invokes memcached_extensions_initialize
     * for each test.  Therefore it is necessary to ensure the following
     * state is reset.
     */
    run = 1;
    fp = nullptr;
    lastlog.buffer[0] = '\0';
    lastlog.count = 0;
    lastlog.offset = 0;
    lastlog.created = 0;

    char *fname = NULL;

    cb_mutex_initialize(&mutex);
    cb_cond_initialize(&cond);
    cb_cond_initialize(&space_cond);

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

    pid = (pid_t)cb_getpid();

    if (gethostname(hostname, sizeof(hostname))) {
        std::cerr << "Could not get the hostname: " << cb_strerror()
                  << std::endl;
        strcpy(hostname,"unknown");
    }

    if (config != NULL) {
        struct config_item items[6];
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
        cb_assert(ii == 6);

        if (sapi->core->parse_config(config, items, stderr) != ENGINE_SUCCESS) {
            return EXTENSION_FATAL;
        }
    }

    if (fname == NULL) {
        fname = cb_strdup("memcached");
    }

    buffers[0].data = reinterpret_cast<char*>(cb_malloc(buffersz));
    buffers[1].data = reinterpret_cast<char*>(cb_malloc(buffersz));

    if (buffers[0].data == NULL || buffers[1].data == NULL || fname == NULL) {
        std::cerr << "Failed to allocate memory for the logger" << std::endl;
        cb_free(fname);
        cb_free(buffers[0].data);
        cb_free(buffers[1].data);
        return EXTENSION_FATAL;
    }

    next_file_id = find_first_logfile_id(fname);

    if (cb_create_named_thread(&tid, logger_thread_main, fname, 0,
                               "mc:file_logger") < 0) {
        std::cerr << "Failed to create the logger backend thread: "
                  << cb_strerror() << std::endl;
        cb_free(fname);
        cb_free(buffers[0].data);
        cb_free(buffers[1].data);
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
