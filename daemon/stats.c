/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct thread_stats *default_independent_stats;

static int num_independent_stats(void) {
    return settings.num_threads + 1;
}

void *new_independent_stats(void) {
    int nrecords = num_independent_stats();
    struct thread_stats *ts = calloc(nrecords, sizeof(struct thread_stats));
    int ii;
    for (ii = 0; ii < nrecords; ii++) {
        cb_mutex_initialize(&ts[ii].mutex);
    }
    return ts;
}

void release_independent_stats(void *stats) {
    int nrecords = num_independent_stats();
    struct thread_stats *ts = stats;
    int ii;
    for (ii = 0; ii < nrecords; ii++) {
        cb_mutex_destroy(&ts[ii].mutex);
    }
    free(ts);
}

struct thread_stats* get_independent_stats(conn *c) {
    struct thread_stats *independent_stats;
    if (settings.engine.v1->get_stats_struct != NULL) {
        independent_stats = settings.engine.v1->get_stats_struct(settings.engine.v0, (const void *)c);
        if (independent_stats == NULL) {
            independent_stats = default_independent_stats;
        }
    } else {
        independent_stats = default_independent_stats;
    }
    return independent_stats;
}

struct thread_stats *get_thread_stats(conn *c) {
    struct thread_stats *independent_stats;
    cb_assert(c->thread->index < num_independent_stats());
    independent_stats = get_independent_stats(c);
    return &independent_stats[c->thread->index];
}
