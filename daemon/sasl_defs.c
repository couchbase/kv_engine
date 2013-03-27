/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
