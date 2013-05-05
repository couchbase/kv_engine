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
#include <assert.h>

#include <platform/platform.h>

#ifdef WIN32_H
#undef close
#endif

#ifdef HAVE_ZLIB_H
#include <zlib.h>
#define supports_zlib true
#else
typedef FILE* gzFile;
#define gzopen(path, mode) fopen(path, mode)
#define gzsetparams(a, b, c)
#define gzflush(fp, b) fflush(fp);
#define gzclose(fp) fclose(fp)
#define gzwrite(fp, ptr, size) (int)fwrite(ptr, 1, size, fp);
#define supports_zlib false
#define Z_PARTIAL_FLUSH 0
#endif

#include <memcached/extension.h>
#include <memcached/engine.h>

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

/* If we should try to write the logs compressed or not */
static bool compress_files = false;

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

typedef void * HANDLE;

static HANDLE stdio_open(const char *path, const char *mode) {
    HANDLE ret = fopen(path, mode);
    if (ret) {
        setbuf(ret, NULL);
    }
    return ret;
}

static HANDLE zlib_file_open(const char *path, const char *mode) {
    gzFile ret = gzopen(path, mode);
    if (ret) {
        gzsetparams(ret, Z_DEFAULT_COMPRESSION, Z_DEFAULT_STRATEGY);
    }

    return ret;
}

static void stdio_close(HANDLE handle) {
    (void)fclose(handle);
}

static void zlib_file_close(HANDLE handle) {
    gzflush(handle, Z_FINISH);
    gzclose(handle);
}

static void stdio_flush(HANDLE handle, int type) {
    (void)type;
    fflush(handle);
}

static void zlib_file_flush(HANDLE handle, int type) {
    gzflush(handle, type);
}

static ssize_t stdio_write(HANDLE handle, const void *ptr, size_t nbytes) {
    return (ssize_t)fwrite(ptr, 1, nbytes, handle);
}

static ssize_t zlib_file_write(HANDLE handle, const void *ptr, size_t nbytes) {
    return (ssize_t)gzwrite(handle, ptr, nbytes);
}

struct io_ops {
    HANDLE (*open)(const char *path, const char *mode);
    void (*close)(HANDLE handle);
    void (*flush)(HANDLE handle, int type);
    ssize_t (*write)(HANDLE handle, const void *ptr, size_t nbytes);
} iops, zlib_ops;

static const char *extension = "txt";

static void add_log_entry(const char *msg, size_t size)
{
    cb_mutex_enter(&mutex);
    /* wait until there is room in the current buffer */
    while ((buffers[currbuffer].offset + size) >= buffersz) {
        fprintf(stderr, "WARNING: waiting for log space to be available\n");
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
    cb_mutex_exit(&mutex);
}

static const char *severity2string(EXTENSION_LOG_LEVEL sev) {
    switch (sev) {
    case EXTENSION_LOG_WARNING:
        return "WARNING";
    case EXTENSION_LOG_INFO:
        return "INFO   ";
    case EXTENSION_LOG_DEBUG:
        return "DEBUG  ";
    case EXTENSION_LOG_DETAIL:
        return "DETAIL ";
    default:
        return "????   ";
    }
}

static void logger_log(EXTENSION_LOG_LEVEL severity,
                       const void* client_cookie,
                       const char *fmt, ...)
{
    (void)client_cookie;

    if (severity >= current_log_level || severity >= output_level) {
        /* @fixme: We shouldn't have to go through this temporary
         *         buffer, but rather insert the data directly into
         *         the destination buffer
         */
        char buffer[2048];
        size_t avail = sizeof(buffer) - 1;
        int prefixlen = 0;
        va_list ap;
        int len;
        struct timeval now;

        if (gettimeofday(&now, NULL) == 0) {
            struct tm tval;
            time_t nsec = (time_t)now.tv_sec;
            char str[40];
            localtime_r(&nsec, &tval);
            if (asctime_r(&tval, str) == NULL) {
                prefixlen = snprintf(buffer, avail, "%u.%06u",
                                     (unsigned int)now.tv_sec,
                                     (unsigned int)now.tv_usec);
            } else {
                const char *tz;
#ifdef HAVE_TM_ZONE
                tz = tval.tm_zone;
#else
                tz = tzname[tval.tm_isdst ? 1 : 0];
#endif
                /* trim off ' YYYY\n' */
                str[strlen(str) - 6] = '\0';
                prefixlen = snprintf(buffer, avail, "%s.%06u %s",
                                     str, (unsigned int)now.tv_usec,
                                     tz);
            }
        } else {
            fprintf(stderr, "gettimeofday failed: %s\n", strerror(errno));
            return;
        }

        if (prettyprint) {
            prefixlen += snprintf(buffer+prefixlen, avail-prefixlen,
                                  " %s: ", severity2string(severity));
        } else {
            prefixlen += snprintf(buffer+prefixlen, avail-prefixlen,
                                  " %u: ", (unsigned int)severity);
        }

        avail -= prefixlen;
        va_start(ap, fmt);
        len = vsnprintf(buffer + prefixlen, avail, fmt, ap);
        va_end(ap);

        if (len < avail) {
            len += prefixlen;
            if (buffer[len - 1] != '\n') {
                buffer[len++] = '\n';
                buffer[len] ='\0';
            }

            if (severity >= output_level) {
                fputs(buffer, stderr);
                fflush(stderr);
            }

            if (severity >= current_log_level) {
                add_log_entry(buffer, len);
            }
        } else {
            fprintf(stderr, "Log message dropped... too big\n");
        }
    }
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
        iops.flush(file, Z_PARTIAL_FLUSH);
    }

    return ret;
}

static volatile int run = 1;
static cb_thread_t tid;

static void logger_thead_main(void* arg)
{
    size_t currsize = 0;
    HANDLE fp = open_logfile(arg);
    unsigned int next = time(NULL);

    cb_mutex_enter(&mutex);
    while (run) {
        struct timespec ts;
        struct timeval tp;
        gettimeofday(&tp, NULL);

        while (tp.tv_sec >= next  ||
               buffers[currbuffer].offset > (buffersz * 0.75)) {
            int this  = currbuffer;
            next = tp.tv_sec + 1;
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

        gettimeofday(&tp, NULL);
        next = tp.tv_sec + (unsigned int)sleeptime;
        memset(&ts, 0, sizeof(ts));
        ts.tv_sec = next;
        /* TROND FIXME! */
        cb_cond_timedwait(&cond, &mutex, 1000 * sleeptime);
    }

    if (fp) {
        while (buffers[currbuffer].offset) {
            int this  = currbuffer;
            currbuffer = (currbuffer == 0) ? 1 : 0;
            flush_pending_io(fp, buffers + this);
        }
        close_logfile(fp);
    }

    cb_mutex_exit(&mutex);
    free(arg);
    free(buffers[0].data);
    free(buffers[1].data);
}

static void exit_handler(void) {
    cb_mutex_enter(&mutex);
    run = 0;
    cb_cond_signal(&cond);
    cb_mutex_exit(&mutex);

    cb_join_thread(tid);
}

static const char *get_name(void) {
    return "compressed file logger";
}

static EXTENSION_LOGGER_DESCRIPTOR descriptor;

static void on_log_level(const void *cookie, ENGINE_EVENT_TYPE type,
                         const void *event_data, const void *cb_data) {
    if (sapi != NULL) {
        current_log_level = sapi->log->get_level();
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
    zlib_ops.open = zlib_file_open;
    zlib_ops.close = zlib_file_close;
    zlib_ops.flush = zlib_file_flush;
    zlib_ops.write = zlib_file_write;
    descriptor.get_name = get_name;
    descriptor.log = logger_log;

#ifdef HAVE_TM_ZONE
    tzset();
#endif

    sapi = get_server_api();
    if (sapi == NULL) {
        return EXTENSION_FATAL;
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

        items[ii].key = "compress";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &compress_files;
        ++ii;

        items[ii].key = NULL;
        ++ii;
        assert(ii == 8);

        if (sapi->core->parse_config(config, items, stderr) != ENGINE_SUCCESS) {
            return EXTENSION_FATAL;
        }

        if (compress_files && supports_zlib) {
            iops = zlib_ops;
            extension = "gz";
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
