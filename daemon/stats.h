/* stats */

/* Default statistics, accessed from memcached.c */
extern struct thread_stats *default_independent_stats;

void stats_prefix_init(void);
void stats_prefix_clear(void);
void stats_prefix_record_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_delete(const char *key, const size_t nkey);
void stats_prefix_record_set(const char *key, const size_t nkey);
/*@null@*/
char *stats_prefix_dump(int *length);


void *new_independent_stats(void);
void release_independent_stats(void *stats);

struct thread_stats* get_independent_stats(conn *c);
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
