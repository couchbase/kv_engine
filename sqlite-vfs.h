#include "config.h"

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct vfs_callbacks {
    // size, stats
    void (*gotSectorSize)(int, void*);
    // rc, stats
    void (*gotOpen)(int, void*);
    // rc, stats
    void (*gotClose)(int, void*);
    // rc, stats
    void (*gotLock)(int, void*);
    // rc, stats
    void (*gotTrunc)(int, void*);

    // timers

    // rc, elapsed time, stats
    void (*gotDelete)(int, hrtime_t, void*);
    // rc, flags, elapsed time, stats
    void (*gotSync)(int, int, hrtime_t, void*);
    // rc, size, seek distance, elapsed time, stats
    void (*gotRead)(int, size_t, ssize_t, hrtime_t, void*);
    // rc, size, seek distance, elapsed time, stats
    void (*gotWrite)(int, size_t, ssize_t, hrtime_t, void*);
};

int vfsepstat_register(
                       const char *zEpstatName,          /* Name of the newly constructed VFS */
                       const char *zOldVfsName,          /* Name of the underlying VFS */
                       struct vfs_callbacks cb,          /* Callbacks (from above) */
                       void *cbarg                       /* Last argument callbacks */
                       );
#ifdef __cplusplus
}
#endif
