/*
 *     Copyright 2013 Couchbase, Inc.
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
#include "hash.h"
#include "pwfile.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

static cb_mutex_t uhash_lock;
static user_db_entry_t **user_ht;
static const unsigned int n_uht_buckets = 12289;

void pwfile_init(void)
{
    cb_mutex_initialize(&uhash_lock);
}

void pwfile_term(void)
{
    free_user_ht();
    cb_mutex_destroy(&uhash_lock);
}

static void kill_whitey(char *s)
{
    for (size_t i = strlen(s) - 1; i > 0 && isspace(s[i]); i--) {
        s[i] = '\0';
    }
}

static int u_hash_key(const char *u)
{
    uint32_t h = hash(u, strlen(u), 0) % n_uht_buckets;
    cb_assert(h < n_uht_buckets);
    return h;
}

static const char *get_isasl_filename(void)
{
    return getenv("ISASL_PWFILE");
}

void free_user_ht(void)
{
    if (user_ht) {
        for (unsigned int i = 0; i < n_uht_buckets; i++) {
            while (user_ht[i]) {
                user_db_entry_t *e = user_ht[i];
                user_db_entry_t *n = e->next;
                free(e->username);
                free(e->password);
                free(e->config);
                free(e);
                user_ht[i] = n;
            }
        }
        free(user_ht);
        user_ht = NULL;
    }
}

static void store_pw(user_db_entry_t **ht,
                     const char *u,
                     const char *p,
                     const char *cfg)
{
    user_db_entry_t *e;
    int h;

    cb_assert(ht);
    cb_assert(u);
    cb_assert(p);

    e = calloc(1, sizeof(user_db_entry_t));
    cb_assert(e);
    e->username = strdup(u);
    cb_assert(e->username);
    e->password = strdup(p);
    cb_assert(e->password);
    e->config = cfg ? strdup(cfg) : NULL;
    cb_assert(!cfg || e->config);

    h = u_hash_key(u);

    e->next = ht[h];
    ht[h] = e;
}

char *find_pw(const char *u, char **cfg)
{
    int h;
    user_db_entry_t *e;

    cb_assert(u);
    cb_assert(user_ht);

    cb_mutex_enter(&uhash_lock);
    h = u_hash_key(u);

    e = user_ht[h];
    while (e && strcmp(e->username, u) != 0) {
        e = e->next;
    }

    if (e != NULL) {
        *cfg = e->config;
        cb_mutex_exit(&uhash_lock);
        return e->password;
    } else {
        cb_mutex_exit(&uhash_lock);
        return NULL;
    }
}

cbsasl_error_t load_user_db(void)
{
    user_db_entry_t **new_ut;
    FILE *sfile;
    char up[128];
    const char *filename = get_isasl_filename();


    if (!filename) {
        return CBSASL_OK;
    }

    sfile = fopen(filename, "r");
    if (!sfile) {
        return CBSASL_FAIL;
    }

    new_ut = calloc(n_uht_buckets, sizeof(user_db_entry_t *));

    if (!new_ut) {
        fclose(sfile);
        return CBSASL_NOMEM;
    }

    /* File has lines that are newline terminated. */
    /* File may have comment lines that must being with '#'. */
    /* Lines should look like... */
    /*   <NAME><whitespace><PASSWORD><whitespace><CONFIG><optional_whitespace> */
    /* */
    while (fgets(up, sizeof(up), sfile)) {
        if (up[0] != '#') {
            char *uname = up, *p = up, *cfg = NULL;
            kill_whitey(up);
            while (*p && !isspace(p[0])) {
                p++;
            }
            /* If p is pointing at a NUL, there's nothing after the username. */
            if (p[0] != '\0') {
                p[0] = '\0';
                p++;
            }
            /* p now points to the first character after the (now) */
            /* null-terminated username. */
            while (*p && isspace(*p)) {
                p++;
            }
            /* p now points to the first non-whitespace character */
            /* after the above */
            cfg = p;
            if (cfg[0] != '\0') {
                /* move cfg past the password */
                while (*cfg && !isspace(cfg[0])) {
                    cfg++;
                }
                if (cfg[0] != '\0') {
                    cfg[0] = '\0';
                    cfg++;
                    /* Skip whitespace */
                    while (*cfg && isspace(cfg[0])) {
                        cfg++;
                    }
                }
            }
            store_pw(new_ut, uname, p, cfg);
        }
    }

    fclose(sfile);
    /*
     if (settings.verbose) {
     settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
     "Loaded isasl db from %s\n",
     filename);
     }
     */
    /* Replace the current configuration with the new one */
    cb_mutex_enter(&uhash_lock);
    free_user_ht();
    user_ht = new_ut;
    cb_mutex_exit(&uhash_lock);

    return CBSASL_OK;
}
