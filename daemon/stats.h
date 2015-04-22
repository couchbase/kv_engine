/* stats */

#ifndef STATS_H
#define STATS_H

struct thread_stats *get_thread_stats(conn *c);

/*
 *  Macros for managing statistics inside memcached
 */

/* The item must always be called "it" */
#define SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_stats[info.info.clsid].slab_op++;

#define THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->thread_op++;

#define THREAD_GUTS2(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_op++; \
    thread_stats->thread_op++;

#define SLAB_THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    THREAD_GUTS(conn, thread_stats, slab_op, thread_op)

#define STATS_INCR1(GUTS, conn, slab_op, thread_op, key, nkey) { \
    struct thread_stats *thread_stats = get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    GUTS(conn, thread_stats, slab_op, thread_op); \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_INCR(conn, op, key, nkey) \
    STATS_INCR1(THREAD_GUTS, conn, op, op, key, nkey)

#define SLAB_INCR(conn, op, key, nkey) \
    STATS_INCR1(SLAB_GUTS, conn, op, op, key, nkey)

#define STATS_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op, key, nkey)

#define SLAB_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op, key, nkey)

#define STATS_HIT(conn, op, key, nkey) \
    SLAB_TWO(conn, op##_hits, cmd_##op, key, nkey)

#define STATS_MISS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_misses, cmd_##op, key, nkey)

#define STATS_NOKEY(conn, op) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op++; \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_NOKEY2(conn, op1, op2) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op1++; \
    thread_stats->op2++; \
    cb_mutex_exit(&thread_stats->mutex); \
}

#define STATS_ADD(conn, op, amt) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    cb_mutex_enter(&thread_stats->mutex); \
    thread_stats->op += amt; \
    cb_mutex_exit(&thread_stats->mutex); \
}

/* Set the statistic to the maximum of the current value, and the specified
 * value.
 */
#define STATS_MAX(conn, op, value) { \
    struct thread_stats *thread_stats = get_thread_stats(conn); \
    if (value > thread_stats->op) { \
        cb_mutex_enter(&thread_stats->mutex); \
        thread_stats->op = value; \
        cb_mutex_exit(&thread_stats->mutex); \
    } \
}

#endif /* STATS_H */
