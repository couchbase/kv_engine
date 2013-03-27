/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static sasl_callback_t sasl_callbacks[] = {
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
