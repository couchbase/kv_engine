/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_SASL_CB_GETCONF
/* The locations we may search for a SASL config file if the user didn't
 * specify one in the environment variable SASL_CONF_PATH
 */
const char * const locations[] = {
    "/etc/sasl/memcached.conf",
    "/etc/sasl2/memcached.conf",
    NULL
};
#endif

#ifdef HAVE_SASL_CB_GETCONF
static int sasl_getconf(void *context, const char **path)
{
    *path = getenv("SASL_CONF_PATH");

    if (*path == NULL) {
        for (int i = 0; locations[i] != NULL; ++i) {
            if (access(locations[i], F_OK) == 0) {
                *path = locations[i];
                break;
            }
        }
    }

    if (settings.verbose) {
        if (*path != NULL) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                           "Reading configuration from: <%s>", *path);
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                             "Failed to locate a config path");
        }

    }

    return (*path != NULL) ? SASL_OK : SASL_FAIL;
}
#endif

#ifdef ENABLE_SASL
static int sasl_log(void *context, int level, const char *message)
{
    EXTENSION_LOG_LEVEL lvl = EXTENSION_LOG_DETAIL;

    switch (level) {
    case SASL_LOG_NONE:
        break;
    case SASL_LOG_PASS:
    case SASL_LOG_TRACE:
    case SASL_LOG_DEBUG:
    case SASL_LOG_NOTE:
        lvl = EXTENSION_LOG_DEBUG;
        break;
    case SASL_LOG_WARN:
    case SASL_LOG_FAIL:
        lvl = EXTENSION_LOG_INFO;
        break;
    default:
        /* This is an error */
        ;
    }

    settings.extensions.logger->log(lvl, NULL,
                                    "SASL (severity %d): %s", level, message);

    return SASL_OK;
}
#endif

typedef int (*sasl_callback_function)();


static sasl_callback_t sasl_callbacks[] = {
#ifdef ENABLE_SASL
   { .id = SASL_CB_LOG,
     .proc = (sasl_callback_function)sasl_log,
     .context = NULL },
#endif

#ifdef HAVE_SASL_CB_GETCONF
   { .id = SASL_CB_GETCONF,
     .proc = (sasl_callback_function)sasl_getconf,
     .context = NULL },
#endif

   { .id = SASL_CB_LIST_END,
     .proc = NULL,
     .context = NULL }
};

void init_sasl(void) {
    if (sasl_server_init(sasl_callbacks, "memcached") != SASL_OK) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Error initializing sasl.");
        exit(EXIT_FAILURE);
    } else {
        if (settings.verbose) {
            settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                                            "Initialized SASL.");
        }
    }
}
