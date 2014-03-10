#include "config.h"
#include <stdio.h>

#include "daemon/memcached.h"

static void display(const char *name, size_t size) {
    printf("%s\t%d\n", name, (int)size);
}

static long calc_conn_size(void) {
   long ret = sizeof(conn);
   ret += DATA_BUFFER_SIZE; /* read */
   ret += DATA_BUFFER_SIZE; /* write */
   ret += (sizeof(item *) * ITEM_LIST_INITIAL);
   ret += (sizeof(char *) * SUFFIX_LIST_INITIAL);
   ret += (sizeof(struct iovec) * IOV_LIST_INITIAL);
   ret += (sizeof(struct msghdr) * MSG_LIST_INITIAL);
   return ret;
}


int main(int argc, char **argv) {



    display("Slab Stats", sizeof(struct slab_stats));
    display("Thread stats",
            sizeof(struct thread_stats)
            - (200 * sizeof(struct slab_stats)));
    display("Global stats", sizeof(struct stats));
    display("Settings", sizeof(struct settings));
    display("Libevent thread",
            sizeof(LIBEVENT_THREAD));
    display("Connection", calc_conn_size());

    printf("----------------------------------------\n");

    display("libevent thread cumulative", sizeof(LIBEVENT_THREAD));
    display("Thread stats cumulative\t", sizeof(struct thread_stats));

    return 0;
}
