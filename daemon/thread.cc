/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for memcached.
 */
#include "config.h"
#include "memcached.h"
#include "connections.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <fcntl.h>
#include <platform/platform.h>

#define ITEMS_PER_ALLOC 64

static char devnull[8192];
extern volatile sig_atomic_t memcached_shutdown;

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    SOCKET            sfd;
    int               parent_port;
    STATE_FUNC        init_state;
    int               event_flags;
    int               read_buffer_size;
    CQ_ITEM          *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    cb_mutex_t lock;
    cb_cond_t  cond;
};

/* Connection lock around accepting new connections */
cb_mutex_t conn_lock;

static LIBEVENT_THREAD dispatcher_thread;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static int nthreads;
static LIBEVENT_THREAD *threads;
static cb_thread_t *thread_ids;

/*
 * Number of worker threads that have finished setting themselves up.
 */
static int init_count = 0;
static cb_mutex_t init_lock;
static cb_cond_t init_cond;

static void thread_libevent_process(evutil_socket_t fd, short which, void *arg);

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    cb_mutex_initialize(&cq->lock);
    cb_cond_initialize(&cq->cond);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *item;

    cb_mutex_enter(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    cb_mutex_exit(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    cb_mutex_enter(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    cb_cond_signal(&cq->cond);
    cb_mutex_exit(&cq->lock);
}

/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new(void) {
    return reinterpret_cast<CQ_ITEM*>(malloc(sizeof(CQ_ITEM)));
}


/*
 * Frees a connection queue item.
 */
static void cqi_free(CQ_ITEM *item) {
    free(item);
}


/*
 * Creates a worker thread.
 */
static void create_worker(void (*func)(void *), void *arg, cb_thread_t *id,
                          const char* name) {
    int ret;

    if ((ret = cb_create_named_thread(id, func, arg, 0, name)) != 0) {
        log_system_error(EXTENSION_LOG_WARNING, NULL,
                         "Can't create thread: %s");
        exit(1);
    }
}

/****************************** LIBEVENT THREADS *****************************/

bool create_notification_pipe(LIBEVENT_THREAD *me)
{
    int j;

#ifdef WIN32
#define DATATYPE intptr_t
#else
#define DATATYPE int
#endif

    if (evutil_socketpair(SOCKETPAIR_AF, SOCK_STREAM, 0,
                          reinterpret_cast< DATATYPE *>(me->notify)) == SOCKET_ERROR) {
        log_socket_error(EXTENSION_LOG_WARNING, NULL,
                         "Can't create notify pipe: %s");
        return false;
    }

    for (j = 0; j < 2; ++j) {
        int flags = 1;
#if defined(WIN32)
        char* flag_ptr = reinterpret_cast<char*>(&flags);
#else
        void* flag_ptr = reinterpret_cast<void*>(&flags);
#endif
        setsockopt(me->notify[j], IPPROTO_TCP,
                   TCP_NODELAY, flag_ptr, sizeof(flags));
        setsockopt(me->notify[j], SOL_SOCKET,
                   SO_REUSEADDR, flag_ptr, sizeof(flags));


        if (evutil_make_socket_nonblocking(me->notify[j]) == -1) {
            log_socket_error(EXTENSION_LOG_WARNING, NULL,
                             "Failed to enable non-blocking: %s");
            return false;
        }
    }
    return true;
}

static void setup_dispatcher(struct event_base *main_base,
                             void (*dispatcher_callback)(evutil_socket_t, short, void *))
{
    memset(&dispatcher_thread, 0, sizeof(dispatcher_thread));
    dispatcher_thread.type = DISPATCHER;
    dispatcher_thread.base = main_base;
	dispatcher_thread.thread_id = cb_thread_self();
    if (!create_notification_pipe(&dispatcher_thread)) {
        exit(1);
    }
    /* Listen for notifications from other threads */
    event_set(&dispatcher_thread.notify_event, dispatcher_thread.notify[0],
              EV_READ | EV_PERSIST, dispatcher_callback, &dispatcher_callback);
    event_base_set(dispatcher_thread.base, &dispatcher_thread.notify_event);

    if (event_add(&dispatcher_thread.notify_event, 0) == -1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Can't monitor libevent notify pipe\n");
        exit(1);
    }
}

/*
 * Set up a thread's information.
 */
static void setup_thread(LIBEVENT_THREAD *me) {
    me->type = GENERAL;
    me->base = event_base_new();
    if (! me->base) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Can't allocate event base\n");
        exit(1);
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify[0],
              EV_READ | EV_PERSIST,
              thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    me->new_conn_queue = reinterpret_cast<struct conn_queue *>(malloc(sizeof(struct conn_queue)));
    if (me->new_conn_queue == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->new_conn_queue);

    cb_mutex_initialize(&me->mutex);

    // Initialize threads' sub-document parser / handler
    me->subdoc_op = subdoc_op_alloc();
}

/*
 * Worker thread: main event loop
 */
static void worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = reinterpret_cast<LIBEVENT_THREAD *>(arg);

    /* Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */

    cb_mutex_enter(&init_lock);
    init_count++;
    cb_cond_signal(&init_cond);
    cb_mutex_exit(&init_lock);

    event_base_loop(me->base, 0);
}

int number_of_pending(conn *c, conn *list) {
    int rv = 0;
    for (; list; list = list->next) {
        if (list == c) {
            rv ++;
        }
    }
    return rv;
}

static void drain_notification_channel(evutil_socket_t fd)
{
    int nread;
    while ((nread = recv(fd, devnull, sizeof(devnull), 0)) == (int)sizeof(devnull)) {
        /* empty */
    }

    if (nread == -1) {
        log_socket_error(EXTENSION_LOG_WARNING, NULL,
                         "Can't read from libevent pipe: %s");
    }
}

/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(evutil_socket_t fd, short which, void *arg) {
    LIBEVENT_THREAD *me = reinterpret_cast<LIBEVENT_THREAD*>(arg);
    CQ_ITEM *item;
    conn* pending;

    cb_assert(me->type == GENERAL);
    drain_notification_channel(fd);

    if (memcached_shutdown) {
         event_base_loopbreak(me->base);
         return ;
    }

    while ((item = cq_pop(me->new_conn_queue)) != NULL) {
        conn *c = conn_new(item->sfd, item->parent_port, item->init_state,
                           item->event_flags, item->read_buffer_size,
                           me->base);
        if (c == NULL) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Can't listen for events on fd %d",
                                            item->sfd);
            closesocket(item->sfd);
        } else {
            cb_assert(c->thread == NULL);
            c->thread = me;
        }
        cqi_free(item);
    }

    LOCK_THREAD(me);
    pending = me->pending_io;
    me->pending_io = NULL;
    while (pending != NULL) {
        conn *c = pending;
        cb_assert(me == c->thread);
        pending = pending->next;
        c->next = NULL;

        if (c->sfd != INVALID_SOCKET && !c->registered_in_libevent) {
            /* The socket may have been shut down while we're looping */
            /* in delayed shutdown */
            register_event(c, 0);
        }
        /*
         * We don't want the thread to keep on serving all of the data
         * from the context of the notification pipe, so just let it
         * run one time to set up the correct mask in libevent
         */
        c->nevents = 1;
        run_event_loop(c);
    }

    /*
     * I could look at all of the connection objects bound to dying buckets
     */
    if (me->deleting_buckets) {
        notify_thread_bucket_deletion(me);
    }
    UNLOCK_THREAD(me);
}

extern volatile rel_time_t current_time;

bool has_cycle(conn *c) {
    conn *slowNode, *fastNode1, *fastNode2;

    if (!c) {
        return false;
    }

    slowNode = fastNode1 = fastNode2 = c;
    while (slowNode && (fastNode1 = fastNode2->next) && (fastNode2 = fastNode1->next)) {
        if (slowNode == fastNode1 || slowNode == fastNode2) {
            return true;
        }
        slowNode = slowNode->next;
    }
    return false;
}

bool list_contains(conn *haystack, conn *needle) {
    for (; haystack; haystack = haystack -> next) {
        if (needle == haystack) {
            return true;
        }
    }
    return false;
}

conn* list_remove(conn *haystack, conn *needle) {
    if (!haystack) {
        return NULL;
    }

    if (haystack == needle) {
        conn *rv = needle->next;
        needle->next = NULL;
        return rv;
    }

    haystack->next = list_remove(haystack->next, needle);

    return haystack;
}

void enlist_conn(conn *c, conn **list) {
    LIBEVENT_THREAD *thr = c->thread;
    cb_assert(list == &thr->pending_io);
    if ((c->list_state & LIST_STATE_PROCESSING) == 0) {
        cb_assert(!list_contains(thr->pending_io, c));
        cb_assert(c->next == NULL);
        c->next = *list;
        *list = c;
        cb_assert(list_contains(*list, c));
        cb_assert(!has_cycle(*list));
    } else {
        c->list_state |= LIST_STATE_REQ_PENDING_IO;
    }
}

void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status)
{
    struct conn *conn = (struct conn *)cookie;
    LIBEVENT_THREAD *thr;
    int notify;

    cb_assert(conn);
    thr = conn->thread;
    cb_assert(thr);

    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                    "Got notify from %d, status %x\n",
                                    conn->sfd, status);

    LOCK_THREAD(thr);
    conn->aiostat = status;
    notify = add_conn_to_pending_io_list(conn);
    UNLOCK_THREAD(thr);

    /* kick the thread in the butt */
    if (notify) {
        notify_thread(thr);
    }
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = -1;

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, or because of an incoming connection.
 */
void dispatch_conn_new(SOCKET sfd, int parent_port,
                       STATE_FUNC init_state, int event_flags,
                       int read_buffer_size) {
    CQ_ITEM *item = cqi_new();
    int tid = (last_thread + 1) % settings.num_threads;

    LIBEVENT_THREAD *thread = threads + tid;

    last_thread = tid;

    item->sfd = sfd;
    item->parent_port = parent_port;
    item->init_state = init_state;
    item->event_flags = event_flags;
    item->read_buffer_size = read_buffer_size;

    cq_push(thread->new_conn_queue, item);

    MEMCACHED_CONN_DISPATCH(sfd, (uintptr_t)thread->thread_id);
    notify_thread(thread);
}

/*
 * Returns true if this is the thread that listens for new TCP connections.
 */
int is_listen_thread() {
    return dispatcher_thread.thread_id == cb_thread_self();
}

void notify_dispatcher(void) {
    notify_thread(&dispatcher_thread);
}

/******************************* GLOBAL STATS ******************************/

void threadlocal_stats_clear(struct thread_stats *stats) {
    stats->cmd_get = 0;
    stats->get_misses = 0;
    stats->delete_misses = 0;
    stats->incr_misses = 0;
    stats->decr_misses = 0;
    stats->incr_hits = 0;
    stats->decr_hits = 0;
    stats->cas_misses = 0;
    stats->bytes_written = 0;
    stats->bytes_read = 0;
    stats->cmd_flush = 0;
    stats->conn_yields = 0;
    stats->auth_cmds = 0;
    stats->auth_errors = 0;
    stats->rbufs_allocated = 0;
    stats->rbufs_loaned = 0;
    stats->rbufs_existing = 0;
    stats->wbufs_allocated = 0;
    stats->wbufs_loaned = 0;
    stats->iovused_high_watermark = 0;
    stats->msgused_high_watermark = 0;

    memset(stats->slab_stats, 0,
           sizeof(struct slab_stats) * MAX_NUMBER_OF_SLAB_CLASSES);
}

void threadlocal_stats_reset(struct thread_stats *thread_stats) {
    int ii;
    for (ii = 0; ii < settings.num_threads; ++ii) {
        cb_mutex_enter(&thread_stats[ii].mutex);
        threadlocal_stats_clear(&thread_stats[ii]);
        cb_mutex_exit(&thread_stats[ii].mutex);
    }
}

void threadlocal_stats_aggregate(struct thread_stats *thread_stats, struct thread_stats *stats) {
    int ii, sid;
    for (ii = 0; ii < settings.num_threads; ++ii) {
        cb_mutex_enter(&thread_stats[ii].mutex);

        stats->cmd_get += thread_stats[ii].cmd_get;
        stats->get_misses += thread_stats[ii].get_misses;
        stats->delete_misses += thread_stats[ii].delete_misses;
        stats->decr_misses += thread_stats[ii].decr_misses;
        stats->incr_misses += thread_stats[ii].incr_misses;
        stats->decr_hits += thread_stats[ii].decr_hits;
        stats->incr_hits += thread_stats[ii].incr_hits;
        stats->cas_misses += thread_stats[ii].cas_misses;
        stats->bytes_read += thread_stats[ii].bytes_read;
        stats->bytes_written += thread_stats[ii].bytes_written;
        stats->cmd_flush += thread_stats[ii].cmd_flush;
        stats->conn_yields += thread_stats[ii].conn_yields;
        stats->auth_cmds += thread_stats[ii].auth_cmds;
        stats->auth_errors += thread_stats[ii].auth_errors;
        stats->rbufs_allocated += thread_stats[ii].rbufs_allocated;
        stats->rbufs_loaned += thread_stats[ii].rbufs_loaned;
        stats->rbufs_existing += thread_stats[ii].rbufs_existing;
        stats->wbufs_allocated += thread_stats[ii].wbufs_allocated;
        stats->wbufs_loaned += thread_stats[ii].wbufs_loaned;

        if (thread_stats[ii].iovused_high_watermark > stats->iovused_high_watermark) {
            stats->iovused_high_watermark = thread_stats[ii].iovused_high_watermark;
        }
        if (thread_stats[ii].msgused_high_watermark > stats->msgused_high_watermark) {
            stats->msgused_high_watermark = thread_stats[ii].msgused_high_watermark;
        }

        for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
            stats->slab_stats[sid].cmd_set +=
                thread_stats[ii].slab_stats[sid].cmd_set;
            stats->slab_stats[sid].get_hits +=
                thread_stats[ii].slab_stats[sid].get_hits;
            stats->slab_stats[sid].delete_hits +=
                thread_stats[ii].slab_stats[sid].delete_hits;
            stats->slab_stats[sid].cas_hits +=
                thread_stats[ii].slab_stats[sid].cas_hits;
            stats->slab_stats[sid].cas_badval +=
                thread_stats[ii].slab_stats[sid].cas_badval;
        }

        cb_mutex_exit(&thread_stats[ii].mutex);
    }
}

void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out) {
    int sid;

    out->cmd_set = 0;
    out->get_hits = 0;
    out->delete_hits = 0;
    out->cas_hits = 0;
    out->cas_badval = 0;

    for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
        out->cmd_set += stats->slab_stats[sid].cmd_set;
        out->get_hits += stats->slab_stats[sid].get_hits;
        out->delete_hits += stats->slab_stats[sid].delete_hits;
        out->cas_hits += stats->slab_stats[sid].cas_hits;
        out->cas_badval += stats->slab_stats[sid].cas_badval;
    }
}

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of worker event handler threads to spawn
 * main_base Event base for main thread
 */
void thread_init(int nthr, struct event_base *main_base,
                 void (*dispatcher_callback)(evutil_socket_t, short, void *)) {
    int i;
    nthreads = nthr + 1;

    cb_mutex_initialize(&conn_lock);
    cb_mutex_initialize(&init_lock);
    cb_cond_initialize(&init_cond);

    threads = reinterpret_cast<LIBEVENT_THREAD*>(calloc(nthreads,
                                                        sizeof(LIBEVENT_THREAD)));
    if (! threads) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Can't allocate thread descriptors: %s",
                                        strerror(errno));
        exit(1);
    }
    thread_ids = reinterpret_cast<cb_thread_t*>(calloc(nthreads, sizeof(cb_thread_t)));
    if (! thread_ids) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Can't allocate thread descriptors: %s",
                                        strerror(errno));
        exit(1);
    }

    setup_dispatcher(main_base, dispatcher_callback);

    for (i = 0; i < nthreads; i++) {
        if (!create_notification_pipe(&threads[i])) {
            exit(1);
        }
        threads[i].index = i;

        setup_thread(&threads[i]);
    }

    /* Create threads after we've done all the libevent setup. */
    for (i = 0; i < nthreads; i++) {
        char name[16];
        snprintf(name, sizeof(name), "mc:worker %d", i);
        create_worker(worker_libevent, &threads[i], &thread_ids[i], name);
        threads[i].thread_id = thread_ids[i];
    }

    /* Wait for all the threads to set themselves up before returning. */
    cb_mutex_enter(&init_lock);
    while (init_count < nthreads) {
        cb_cond_wait(&init_cond, &init_lock);
    }
    cb_mutex_exit(&init_lock);
}

void threads_shutdown(void)
{
    int ii;
    for (ii = 0; ii < nthreads; ++ii) {
        notify_thread(&threads[ii]);
        cb_join_thread(thread_ids[ii]);
    }
}

void threads_cleanup(void)
{
    int ii;
    for (ii = 0; ii < nthreads; ++ii) {
        CQ_ITEM *it;

        safe_close(threads[ii].notify[0]);
        safe_close(threads[ii].notify[1]);
        event_base_free(threads[ii].base);

        while ((it = cq_pop(threads[ii].new_conn_queue)) != NULL) {
            cqi_free(it);
        }
        free(threads[ii].new_conn_queue);
        free(threads[ii].read.buf);
        free(threads[ii].write.buf);
        subdoc_op_free(threads[ii].subdoc_op);
    }

    free(thread_ids);
    free(threads);
}

void threads_notify_bucket_deletion(void)
{
    for (int ii = 0; ii < nthreads; ++ii) {
        LIBEVENT_THREAD *thr = threads + ii;
        notify_thread(thr);
    }
}

void threads_complete_bucket_deletion(void)
{
    for (int ii = 0; ii < nthreads; ++ii) {
        LIBEVENT_THREAD *thr = threads + ii;
        LOCK_THREAD(thr);
        threads[ii].deleting_buckets--;
        UNLOCK_THREAD(thr);
    }
}

void threads_initiate_bucket_deletion(void)
{
    for (int ii = 0; ii < nthreads; ++ii) {
        LIBEVENT_THREAD *thr = threads + ii;
        LOCK_THREAD(thr);
        threads[ii].deleting_buckets++;
        UNLOCK_THREAD(thr);
    }
}

void notify_thread(LIBEVENT_THREAD *thread) {
    if (send(thread->notify[1], "", 1, 0) != 1) {
        log_socket_error(EXTENSION_LOG_WARNING, NULL,
                         "Failed to notify thread: %s");
    }
}

int add_conn_to_pending_io_list(conn *c) {
    int notify = 0;
    if (number_of_pending(c, c->thread->pending_io) == 0) {
        if (c->thread->pending_io == NULL) {
            notify = 1;
        }
        enlist_conn(c, &c->thread->pending_io);
    }

    return notify;
}
