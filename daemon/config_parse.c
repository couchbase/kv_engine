/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"

#include <cJSON.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <ctype.h>

#include "config_util.h"
#include "runtime.h"

#ifdef WIN32
static int isDrive(const char *file) {
    if ((isupper(file[0]) || islower(file[0])) && file[1] == ':') {
        return 1;
    }
    return 0;
}
#endif

static const char *get_absolute_file(const char *file) {
    char buffer[1024];
    size_t len;
#ifdef WIN32
    const char sep = '\\';
#else
    const char sep = '/';
#endif

    if (file[0] == '/') {
        return strdup(file);
    }

#ifdef WIN32
    if (file[0] == '\\' || isDrive(file)) {
        return _strdup(file);
    }

    if (GetCurrentDirectory(sizeof(buffer), buffer) == 0) {
        fprintf(stderr, "Failed to determine current working directory");
        exit(EXIT_FAILURE);
    }
#else
    if (getcwd(buffer, sizeof(buffer)) == NULL) {
        fprintf(stderr, "Failed to determine current working directory: "
                "%s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
#endif

    len = strlen(buffer);
    snprintf(buffer + len, sizeof(buffer) - len, "%c%s", sep, file);

    fprintf(stderr, "WARNING: workaround for https://www.couchbase.com/"
            "issues/browse/MB-10305 to convert from \"%s\" to \"%s\"\n",
            file, buffer);

    return strdup(buffer);
}


static int get_int_value(cJSON *i, const char *key) {
    switch (i->type) {
    case cJSON_Number:
        if (i->valueint != i->valuedouble) {
            fprintf(stderr, "Invalid value specified for %s: %s\n", key,
                    cJSON_Print(i));
            exit(EXIT_FAILURE);
        } else {
            return i->valueint;
        }
    case cJSON_String:
        return atoi(i->valuestring);
    default:
        fprintf(stderr, "Invalid value specified for %s: %s\n", key,
                cJSON_Print(i));
        exit(EXIT_FAILURE);
    }
}

static in_port_t get_in_port_value(cJSON *i, const char *key) {
    int value = get_int_value(i, key);
    if (value < 0 || value > UINT16_MAX) {
        fprintf(stderr, "port must be in the range: [0,%u] for %s\n",
                UINT16_MAX, key);
        exit(EXIT_FAILURE);
    }

    return (in_port_t)value;
}

static bool get_bool_value(cJSON *i, const char *key) {
    switch (i->type) {
    case cJSON_False:
        return false;
    case cJSON_True:
        return true;
    default:
        fprintf(stderr, "Invalid value specified for %s: %s\n", key,
                cJSON_Print(i));
        exit(EXIT_FAILURE);
    }
}

static const char *get_string_value(cJSON *i, const char *key) {
    switch (i->type) {
    case cJSON_String:
        return i->valuestring;
    default:
        fprintf(stderr, "Invalid value specified for %s: %s\n", key,
                cJSON_Print(i));
        exit(EXIT_FAILURE);
    }
}

static const char *get_host_value(cJSON *i, const char *key) {
    const char *host = get_string_value(i, key);

    /* @todo add validation */
    return host;
}

static char *get_file_value(cJSON *i, const char *key) {
    struct stat st;
    if (i->type != cJSON_String) {
        fprintf(stderr, "Invalid value specified for %s (not a string): %s\n",
                key, cJSON_Print(i));
        exit(EXIT_FAILURE);
    }

    if (stat(i->valuestring, &st) == -1) {
        fprintf(stderr, "Cannot access \"%s\" specified for %s\n",
                i->valuestring, key);
        exit(EXIT_FAILURE);
    }

    return i->valuestring;
}

/**
 * The callback function for a single configuration attribute
 * @param obj the object containing the configuration value
 */
typedef void (*config_handler)(cJSON *obj);

/**************************************************************************
 **********************  configuration callback  **************************
 *************************************************************************/
static void get_admin(cJSON *o) {
    const char *ptr = get_string_value(o, o->string);
    if (strlen(ptr) == 0) {
        settings.disable_admin = true;
        settings.admin = NULL;
    } else {
        settings.admin = strdup(ptr);
    }
}

static bool get_ssl_cipher_list(cJSON *o) {
    const char *ptr = get_string_value(o, o->string);
    set_ssl_cipher_list(ptr);
}

static void get_threads(cJSON *o) {
    settings.num_threads = get_int_value(o, o->string);
}

static void get_verbosity(cJSON *o) {
    settings.verbose = get_int_value(o, o->string);
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
}

static void get_reqs_per_event(cJSON *o) {
    settings.default_reqs_per_event = get_int_value(o, o->string);
}

static void get_require_sasl(cJSON *o) {
    settings.require_sasl = get_bool_value(o, o->string);
}

static void get_allow_detailed(cJSON *o) {
    settings.allow_detailed = get_bool_value(o, o->string);
}

static void get_detailed_enabled(cJSON *o) {
    settings.detail_enabled = get_bool_value(o, o->string);
}

static void get_dedupe_nmvb_maps(cJSON *o) {
    settings.dedupe_nmvb_maps = get_bool_value(o, o->string);
}

static void get_prefix_delimiter(cJSON *o) {
    if (o->type != cJSON_String) {
        fprintf(stderr, "Invalid value specified for prefix_delimiter\n");
        exit(EXIT_FAILURE);
    }

    if (strlen(o->valuestring) > 1) {
        fprintf(stderr, "The prefix_delimiter may only be a single char\n");
        exit(EXIT_FAILURE);
    }

    settings.prefix_delimiter = o->valuestring[0];
}

static void handle_extension(cJSON *r) {
    const char *module = NULL;
    const char *config = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("module", p->string) == 0) {
                module = get_string_value(p, "extension module");
            } else if (strcasecmp("config", p->string) == 0) {
                config = get_string_value(p, "extension config");
            } else {
                fprintf(stderr, "Unknown attribute for extension: %s\n",
                        p->string);
            }
            p = p->next;
        }

        if (!load_extension(module, config)) {
            exit(EXIT_FAILURE);
        }
    } else {
        fprintf(stderr, "Invalid entry for extension\n");
        exit(EXIT_FAILURE);
    }
}

static void get_extensions(cJSON *o) {
    /* extensions is supposed to be a sub group */
    cJSON *e = o->child;
    while (e != NULL) {
        handle_extension(e);
        e = e->next;
    }
}

static void get_engine(cJSON *r) {
    const char *module = NULL;
    const char *config = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("module", p->string) == 0) {
                module = get_string_value(p, "engine module");
            } else if (strcasecmp("config", p->string) == 0) {
                config = get_string_value(p, "engine config");
            } else {
                fprintf(stderr, "Unknown attribute for engine: %s\n",
                        p->string);
            }
            p = p->next;
        }

        if (module == NULL) {
            fprintf(stderr,
                    "Mandatory attribute module not specified for engine\n");
            exit(EXIT_FAILURE);
        }

        settings.engine_module = strdup(module);
        settings.engine_config = config ? strdup(config) : NULL;
    } else {
        fprintf(stderr, "Invalid entry for engine\n");
    }
}

typedef void (*interface_handler)(int ii, cJSON *r);

static void get_interface_maxconn(int idx, cJSON *r) {
    settings.interfaces[idx].maxconn = get_int_value(r, "interface maxconn");
}

static void get_interface_port(int idx, cJSON *r) {
    settings.interfaces[idx].port = get_in_port_value(r, "interface port");
}

static void get_interface_backlog(int idx, cJSON *r) {
    settings.interfaces[idx].backlog = get_int_value(r, "interface backlog");
}

static void get_interface_tcp_nodelay(int idx, cJSON *o) {
    settings.interfaces[idx].tcp_nodelay = get_bool_value(o, o->string);
}

static void get_interface_ipv4(int idx, cJSON *r) {
    settings.interfaces[idx].ipv4 = get_bool_value(r, r->string);
}

static void get_interface_ipv6(int idx, cJSON *r) {
    settings.interfaces[idx].ipv6 = get_bool_value(r, r->string);
}

static void get_interface_host(int idx, cJSON *r) {
    settings.interfaces[idx].host = strdup(get_host_value(r, "interface host"));
}

static void get_interface_ssl(int idx, cJSON *r) {
    const char *cert = NULL;
    const char *key = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("key", p->string) == 0) {
                key = get_file_value(p, "interface key file");
            } else if (strcasecmp("cert", p->string) == 0) {
                cert = get_file_value(p, "interface ssl certificate");
            } else {
                fprintf(stderr, "Unknown attribute for ssl: %s\n",
                        p->string);
            }
            p = p->next;
        }

        if (key && cert) {
            settings.interfaces[idx].ssl.key = get_absolute_file(key);
            settings.interfaces[idx].ssl.cert = get_absolute_file(cert);
        } else if (key || cert) {
            fprintf(stderr, "You need to specify a value for cert and key\n");
            exit(EXIT_FAILURE);
        }
    } else if (r->type != cJSON_False) {
        fprintf(stderr, "Invalid entry for ssl\n");
        exit(EXIT_FAILURE);
    }
}

static void handle_interface(int idx, cJSON *r) {
    /* set default values */
    settings.interfaces[idx].backlog = 1024;
    settings.interfaces[idx].ipv4 = true;
    settings.interfaces[idx].ipv6 = true;
    settings.interfaces[idx].tcp_nodelay = true;

    if (r->type == cJSON_Object) {
        struct {
            const char *key;
            interface_handler handler;
        } handlers[] = {
            { "maxconn", get_interface_maxconn },
            { "port", get_interface_port },
            { "host", get_interface_host },
            { "backlog", get_interface_backlog },
            { "ipv4", get_interface_ipv4 },
            { "ipv6", get_interface_ipv6 },
            { "tcp_nodelay", get_interface_tcp_nodelay },
            { "ssl", get_interface_ssl }
        };
        cJSON *obj = r->child;
        while (obj != NULL) {
            int ii = 0;
            while (handlers[ii].key != NULL) {
                if (strcasecmp(handlers[ii].key, obj->string) == 0) {
                    break;
                }
                ++ii;
            }

            if (handlers[ii].key == NULL) {
                fprintf(stderr,
                        "Unknown token \"%s\"  for interface #%u ignored.\n",
                        obj->string, idx);
            } else {
                handlers[ii].handler(idx, obj);
            }

            obj = obj->next;
        }

        if (!settings.interfaces[idx].ipv4 && !settings.interfaces[idx].ipv6) {
            fprintf(stderr,
                    "IPv4 and IPv6 cannot be disabled at the same time\n");
            exit(EXIT_FAILURE);
        }
        /* validate !!! */

    } else {
        fprintf(stderr, "Invalid entry for interface #%u\n", idx);
        exit(EXIT_FAILURE);
    }
}

static void get_interfaces(cJSON *o) {
    int total = cJSON_GetArraySize(o);
    cJSON *c = o->child;
    int ii = 0;

    settings.interfaces = calloc(total, sizeof(struct interface));
    settings.num_interfaces = total;
    while (c != NULL) {
        handle_interface(ii, c);
        ++ii;
        c = c->next;
    }
}

static void handle_lock_memory(cJSON *o) {
    if (get_bool_value(o, "lock_memory")) {
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "mlockall() failed: %s. Proceeding without",
                                            strerror(errno));
        }
#else
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "mlockall() not supported on this platform. proceeding without");
#endif
    }
}

static void handle_large_memory_pages(cJSON *o) {
    if (get_bool_value(o, "large_memory_pages")) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
        size_t sizes[32];
        int avail = getpagesizes(sizes, 32);
        if (avail != -1) {
            size_t max = sizes[0];
            struct memcntl_mha arg = {0};
            int ii;

            for (ii = 1; ii < avail; ++ii) {
                if (max < sizes[ii]) {
                    max = sizes[ii];
                }
            }

            arg.mha_flags   = 0;
            arg.mha_pagesize = max;
            arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

            if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "Failed to set large pages: %s\nWill use default page size",
                                                strerror(errno));
            }
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "Failed to get supported pagesizes: %s\nWill use default page size\n",
                                            strerror(errno));
        }
#else
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Setting page size is not supported on this platform. Will use default page size");
#endif
    }
}

static void get_daemonize(cJSON *i) {
    settings.daemonize = get_bool_value(i, "daemonize");
}

static void get_pid_file(cJSON *i) {
    settings.pid_file = strdup(get_string_value(i, "pid_file"));
}

static void get_bio_drain_buffer_sz(cJSON *i) {
    settings.bio_drain_buffer_sz = get_int_value(i, "bio_drain_buffer_sz");
}

static void get_datatype(cJSON *o) {
    settings.datatype = get_bool_value(o, o->string);
}

void read_config_file(const char *file)
{
    struct {
        const char *key;
        config_handler handler;
    } handlers[] = {
        { "admin", get_admin },
        { "threads", get_threads },
        { "ssl_cipher_list", get_ssl_cipher_list },
        { "interfaces", get_interfaces },
        { "extensions", get_extensions },
        { "engine", get_engine },
        { "require_sasl", get_require_sasl },
        { "prefix_delimiter", get_prefix_delimiter },
        { "allow_detailed", get_allow_detailed },
        { "detail_enabled", get_detailed_enabled },
        { "reqs_per_event", get_reqs_per_event },
        { "verbosity", get_verbosity },
        { "lock_memory", handle_lock_memory },
        { "large_memory_pages", handle_large_memory_pages },
        { "daemonize", get_daemonize },
        { "pid_file", get_pid_file },
        { "bio_drain_buffer_sz", get_bio_drain_buffer_sz },
        { "datatype_support", get_datatype },
        { "dedupe_nmvb_maps", get_dedupe_nmvb_maps },
        { NULL, NULL}
    };
    cJSON *obj;
    cJSON *sys;
    config_error_t err = config_load_file(file, &sys);

    if (err != CONFIG_SUCCESS) {
        char *msg = config_strerror(file, err);
        fprintf(stderr, "%s\nTerminating\n", msg);
        free(msg);
        exit(EXIT_FAILURE);
    }
    settings.config = cJSON_PrintUnformatted(sys);

    obj = sys->child;
    while (obj) {
        int ii = 0;
        while (handlers[ii].key != NULL) {
            if (strcasecmp(handlers[ii].key, obj->string) == 0) {
                break;
            }
            ++ii;
        }

        if (handlers[ii].key == NULL) {
            fprintf(stderr, "Unknown token \"%s\" ignored.\n", obj->string);
        } else {
            handlers[ii].handler(obj);
        }

        obj = obj->next;
    }

    cJSON_Delete(sys);
}
