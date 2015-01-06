/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <limits.h>
#include "config.h"
#include "memcached/extension.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "auditd.h"

int main (int argc, char *argv[])
{
    char audit_fname[PATH_MAX];
#ifdef WIN32
#define sleep(a) Sleep(a * 1000)
#endif
    sprintf(audit_fname, "%s%cetc%csecurity%caudit.json", DESTINATION_ROOT,
            DIRECTORY_SEPARATOR_CHARACTER, DIRECTORY_SEPARATOR_CHARACTER,
            DIRECTORY_SEPARATOR_CHARACTER);
    AUDIT_EXTENSION_DATA audit_extension_data;
    audit_extension_data.version = 1;
    audit_extension_data.log_extension = get_stderr_logger();
    if (initialize_auditdaemon(audit_fname, &audit_extension_data) != AUDIT_SUCCESS) {
        fprintf(stderr,"initialize audit daemon: FAILED\n");
    } else {
        fprintf(stderr,"initialize audit daemon: SUCCESS\n");
    }
    /* sleep is used to ensure get to cb_cond_wait(&events_arrived, &producer_consumer_lock); */
    sleep(1);
    if (shutdown_auditdaemon() != AUDIT_SUCCESS) {
        fprintf(stderr,"shutdown audit daemon: FAILED\n");
    } else {
        fprintf(stderr,"shutdown audit daemon: SUCCESS\n");
    }
    return 0;
}
