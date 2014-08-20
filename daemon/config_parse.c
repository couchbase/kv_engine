/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"

#include "config_parse.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <ctype.h>

#include "cmdline.h"
#include "config_util.h"
#include "config_parse.h"
#include "connections.h"

#ifdef WIN32
static int isDrive(const char *file) {
    if ((isupper(file[0]) || islower(file[0])) && file[1] == ':') {
        return 1;
    }
    return 0;
}
#endif

static bool get_absolute_file(const char *file, const char **value,
                              char **error_msg) {
    char buffer[1024];
    size_t len;
#ifdef WIN32
    const char sep = '\\';
#else
    const char sep = '/';
#endif

    if (file[0] == '/') {
        *value = strdup(file);
        return true;
    }

#ifdef WIN32
    if (file[0] == '\\' || isDrive(file)) {
        *value = _strdup(file);
        return true;
    }

    if (GetCurrentDirectory(sizeof(buffer), buffer) == 0) {
        asprintf(error_msg, "Failed to determine current working directory");
        return false;
    }
#else
    if (getcwd(buffer, sizeof(buffer)) == NULL) {
        asprintf(error_msg, "Failed to determine current working directory: "
                "%s\n", strerror(errno));
        return false;
    }
#endif

    len = strlen(buffer);
    snprintf(buffer + len, sizeof(buffer) - len, "%c%s", sep, file);

    fprintf(stderr, "WARNING: workaround for https://www.couchbase.com/"
            "issues/browse/MB-10305 to convert from \"%s\" to \"%s\"\n",
            file, buffer);

    *value = strdup(buffer);
    return true;
}


static bool get_int_value(cJSON *i, const char *key, int* value,
                          char **error_msg) {
    switch (i->type) {
    case cJSON_Number:
        if (i->valueint != i->valuedouble) {
            char *json = cJSON_Print(i);
            asprintf(error_msg, "Non-integer value specified for %s: %s\n", key,
                     json);
            cJSON_Free(json);
            return false;
        } else {
            *value = i->valueint;
            return true;
        }
    case cJSON_String:
        if (!safe_strtol(i->valuestring, value)) {
            char *json = cJSON_Print(i);
            asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                     json);
            cJSON_Free(json);
            return false;
        }
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                     json);
            cJSON_Free(json);
            return false;
        }
    }
}

static bool get_in_port_value(cJSON *i, const char *key, in_port_t* value,
                              char **error_msg) {
    int int_value;
    if (!get_int_value(i, key, &int_value, error_msg)) {
        return false;
    }
    if (int_value < 0 || int_value > UINT16_MAX) {
        asprintf(error_msg, "port must be in the range: [0,%u] for %s\n",
                UINT16_MAX, key);
        return false;
    }

    *value =  (in_port_t)int_value;
    return true;
}

static bool get_bool_value(cJSON *i, const char *key, bool *value,
                           char **error_msg) {
    switch (i->type) {
    case cJSON_False:
        *value = false;
        return true;
    case cJSON_True:
        *value = true;
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                     json);
            cJSON_Free(json);
            return false;
        }
    }
}

/* Gets a string value from the specified JSON object. Returns true, and sets
 * value to the string value on success; else returns false and sets
 * error_msg to a string describing the error.
 * Caller is responsible for free()ing *value.
 * @param i JSON object.
 * @param value the pointer to store the string value into if return value is
 *              true.
 * @param error_msg the pointer to store a string describing any error
 *                  encountered, if return value is false. Note: if non-null
 *                  this string should be free'd by the caller.
 * @return true if JSON object is a string, else false.
 */
static bool get_string_value(cJSON *i, const char* key, const char **value,
                             char **error_msg) {
    switch (i->type) {
    case cJSON_String:
        *value = strdup(i->valuestring);
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                    json);
            cJSON_Free(json);
            return false;
        }
    }
}

static bool get_host_value(cJSON *i, const char *key, const char **value,
                           char **error_msg) {
    /* @todo add validation */
    return get_string_value(i, key, value, error_msg);
}

static bool get_file_value(cJSON *i, const char *key, const char **value,
                           char **error_msg) {
    struct stat st;
    if (i->type != cJSON_String) {
        char *json = cJSON_Print(i);
        asprintf(error_msg, "Invalid value specified for %s (not a string): %s\n",
                 key, json);
        cJSON_Free(json);
        return false;
    }

    if (stat(i->valuestring, &st) == -1) {
        char *json = cJSON_Print(i);
        asprintf(error_msg, "Cannot access \"%s\" specified for %s\n",
                 i->valuestring, json);
        cJSON_Free(json);
        return false;
    }

    *value = i->valuestring;
    return true;
}

/**
 * The callback function for a single configuration attribute
 * @param obj the object containing the configuration value
 * @param settings The settings object to update.
 * @param error_msg If return false is false, message describing why the
 *                  attribute was incorrect. Note caller is responsible for
 *                  free()ing this.
 * @return true if attribute was successfully parsed, else false and error_msg
 *         is set to a string describing the error.
 */
typedef bool (*config_handler)(cJSON *obj, struct settings *settings,
                               char **error_msg);

/**************************************************************************
 **********************  configuration callback  **************************
 *************************************************************************/
static bool get_admin(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_string_value(o, o->string, &ptr, error_msg)) {
        return false;
    }
    if (strlen(ptr) == 0) {
        settings->disable_admin = true;
        settings->admin = NULL;
        free((char*)ptr);
    } else {
        settings->disable_admin = false;
        settings->admin = ptr;
    }
    settings->has.admin = true;
    return true;
}

static bool get_threads(cJSON *o, struct settings *settings,
                        char **error_msg) {
    if (get_int_value(o, o->string, &settings->num_threads, error_msg)) {
        settings->has.threads = true;
        return true;
    } else {
        return false;
    }
}

static bool get_verbosity(cJSON *o, struct settings *settings,
                          char **error_msg) {
    if (get_int_value(o, o->string, &settings->verbose, error_msg)) {
        settings->has.verbose = true;
        return true;
    } else {
        return false;
    }
}

static bool get_reqs_per_event(cJSON *o, struct settings *settings,
                               char **error_msg) {
    if (get_int_value(o, o->string, &settings->reqs_per_event, error_msg)) {
        settings->has.reqs_per_event = true;
        return true;
    } else {
        return false;
    }
}

static bool get_require_sasl(cJSON *o, struct settings *settings,
                             char **error_msg) {
    if (get_bool_value(o, o->string, &settings->require_sasl, error_msg)) {
        settings->has.require_sasl = true;
        return true;
    } else {
        return false;
    }
}

static bool get_extension(cJSON *r, struct extension_settings *ext_settings,
                          char **error_msg) {
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("module", p->string) == 0) {
                if (!get_string_value(p, "extension module",
                                      &ext_settings->soname, error_msg)) {
                    return false;
                }
            } else if (strcasecmp("config", p->string) == 0) {
                if (!get_string_value(p, "extension config",
                                      &ext_settings->config, error_msg)) {
                    return false;
                }
            } else {
                asprintf(error_msg, "Unknown attribute for extension: %s\n",
                         p->string);
                return false;
            }
            p = p->next;
        }
        return true;
    } else {
        asprintf(error_msg, "Invalid entry for extension\n");
        return false;
    }
}

static bool get_extensions(cJSON *o, struct settings *settings,
                           char **error_msg) {
    /* extensions is supposed to be a sub group */
    settings->num_pending_extensions = cJSON_GetArraySize(o);
    cJSON *e = o->child;
    int ii = 0;

    settings->pending_extensions = calloc(settings->num_pending_extensions,
                                          sizeof(struct extension_settings));

    while (e != NULL) {
        if (!get_extension(e, &settings->pending_extensions[ii], error_msg)) {
            return false;
        }
        ++ii;
        e = e->next;
    }
    settings->has.extensions = true;
    return true;
}

static bool get_engine(cJSON *r, struct settings *settings, char **error_msg) {
    const char *module = NULL;
    const char *config = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("module", p->string) == 0) {
                if (!get_string_value(p, "engine module", &module,
                                      error_msg)) {
                    return false;
                }
            } else if (strcasecmp("config", p->string) == 0) {
                if (!get_string_value(p, "engine config", &config,
                                      error_msg)) {
                    return false;
                }
            } else {
                asprintf(error_msg, "Unknown attribute for engine: %s\n",
                         p->string);
                return false;
            }
            p = p->next;
        }

        if (module == NULL) {
            asprintf(error_msg,
                     "Mandatory attribute module not specified for engine\n");
            return false;
        }

        settings->engine_module = module;
        settings->engine_config = config ? config : NULL;
        settings->has.engine = true;
        return true;
    } else {
        asprintf(error_msg, "Invalid entry for engine\n");
        return false;
    }
}

/**
 * The callback function for an interface configuration section.
 * @param ii the interface index.
 * @param r the object containing the configuration value
 * @param iface The interface settings object to update.
 * @error_msg if parsing failed, set to a string describing the error. Note: it
 *            is the caller's responsibility to free() this.
 * @return true if parsed successfully, else false.
 */
typedef bool (*interface_handler)(int ii, cJSON *r, struct interface* iface,
                                  char **erro_msg);

static bool get_interface_maxconn(int idx, cJSON *r, struct interface* iface,
                                  char **error_msg) {
    return get_int_value(r, "interface maxconn", &iface->maxconn, error_msg);
}

static bool get_interface_port(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_in_port_value(r, "interface port", &iface->port, error_msg);
}

static bool get_interface_backlog(int idx, cJSON *r, struct interface* iface,
                                  char **error_msg) {
    return get_int_value(r, "interface backlog", &iface->backlog, error_msg);
}

static bool get_interface_tcp_nodelay(int idx, cJSON *o,
                                      struct interface* iface,
                                      char **error_msg) {
    return get_bool_value(o, o->string, &iface->tcp_nodelay, error_msg);
}

static bool get_interface_ipv4(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_bool_value(r, r->string, &iface->ipv4, error_msg);
}

static bool get_interface_ipv6(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_bool_value(r, r->string, &iface->ipv6, error_msg);
}

static bool get_interface_host(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    const char* host;
    if (!get_host_value(r, "interface host", &host, error_msg)) {
        return false;
    }

    iface->host = host;
    return true;
}

static bool get_interface_ssl(int idx, cJSON *r, struct interface* iface,
                              char **error_msg) {
    const char *cert = NULL;
    const char *key = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("key", p->string) == 0) {
                if (!get_file_value(p, "interface key file", &key, error_msg)) {
                    return false;
                }
            } else if (strcasecmp("cert", p->string) == 0) {
                if (!get_file_value(p, "interface ssl certificate", &cert,
                                    error_msg)) {
                    return false;
                }
            } else {
                asprintf(error_msg, "Unknown attribute for ssl: %s\n",
                         p->string);
                return false;
            }
            p = p->next;
        }

        if (key && cert) {
            if (!get_absolute_file(key, &iface->ssl.key, error_msg)) {
                return false;
            }
            if (!get_absolute_file(cert, &iface->ssl.cert, error_msg)) {
                return false;
            }
        } else if (key || cert) {
            asprintf(error_msg, "You need to specify a value for cert and key\n");
            return false;
        }
    } else if (r->type != cJSON_False) {
        asprintf(error_msg, "Invalid entry for ssl\n");
        return false;
    }
    return true;
}

static bool handle_interface(int idx, cJSON *r, struct interface* iface,
                             char **error_msg) {
    /* set default values */
    iface->backlog = 1024;
    iface->ipv4 = true;
    iface->ipv6 = true;
    iface->tcp_nodelay = true;

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
            { "ssl", get_interface_ssl },
            { NULL, NULL }
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
                asprintf(error_msg,
                        "Unknown token \"%s\" for interface #%u ignored.\n",
                         obj->string, idx);
            } else {
                if (!handlers[ii].handler(idx, obj, iface, error_msg)) {
                    return false;
                }
            }

            obj = obj->next;
        }

        if (!iface->ipv4 && !iface->ipv6) {
            asprintf(error_msg,
                     "IPv4 and IPv6 cannot be disabled at the same time\n");
            return false;
        }
        /* validate !!! */
        return true;
    } else {
        asprintf(error_msg, "Invalid entry for interface #%u\n", idx);
        return false;
    }
}

static bool get_interfaces(cJSON *o, struct settings *settings,
                           char **error_msg) {
    int total = cJSON_GetArraySize(o);
    cJSON *c = o->child;
    int ii = 0;

    settings->interfaces = calloc(total, sizeof(struct interface));
    settings->num_interfaces = total;
    while (c != NULL) {
        if (!handle_interface(ii, c, &settings->interfaces[ii], error_msg)) {
            return false;
        }
        ++ii;
        c = c->next;
    }
    settings->has.interfaces = true;
    return true;
}

static bool get_bio_drain_sz(cJSON *i, struct settings *settings,
                                    char **error_msg) {
    int buffer_sz;
    if (!get_int_value(i, "bio_drain_buffer_sz", &buffer_sz, error_msg)) {
        return false;
    } else {
        settings->bio_drain_buffer_sz = (size_t)buffer_sz;
        settings->has.bio_drain_buffer_sz = true;
        return true;
    }
}

static bool get_datatype(cJSON *o, struct settings *settings,
                         char **error_msg) {
    if (get_bool_value(o, o->string, &settings->datatype, error_msg)) {
        settings->has.datatype = true;
        return true;
    } else {
        return false;
    }
}

/* reconfig (dynamic config update) handlers *********************************/

typedef bool (*dynamic_validate_handler)(const struct settings *new_settings,
                                         cJSON* errors);

typedef void (*dynamic_reconfig_handler)(const struct settings *new_settings);

static bool dyna_validate_admin(const struct settings *new_settings,
                                cJSON* errors) {
    if (!new_settings->has.admin) {
        return true;
    }
    if (settings.admin != NULL &&
        new_settings->admin != NULL &&
        strcmp(new_settings->admin, settings.admin) == 0) {
        return true;
    } else if (settings.admin == NULL && new_settings->admin == NULL) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'admin' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_threads(const struct settings *new_settings,
                                  cJSON* errors) {
    if (!new_settings->has.threads) {
        return true;
    }
    if (new_settings->num_threads == settings.num_threads) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'num_threads' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_interfaces(const struct settings *new_settings,
                                     cJSON* errors) {
    bool valid = false;
    if (!new_settings->has.interfaces) {
        return true;
    }

    /* parts of interface are dynamic, but not the overall number or name... */
    if (new_settings->num_interfaces == settings.num_interfaces) {
        char* tempstr = NULL;
        valid = true;
        int ii = 0;
        for (ii = 0; ii < settings.num_interfaces; ii++) {
            struct interface *cur_if = &settings.interfaces[ii];
            struct interface *new_if = &new_settings->interfaces[ii];

            /* These settings cannot change: */
            if (strcmp(new_if->host, cur_if->host) != 0) {
                asprintf(&tempstr,
                         "interface '%d' cannot change host dynamically.", ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->port != cur_if->port) {
                asprintf(&tempstr,
                         "interface '%d' cannot change port dynamically.", ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->ipv4 != cur_if->ipv4) {
                asprintf(&tempstr,
                         "interface '%d' cannot change IPv4 dynamically.", ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->ipv6 != cur_if->ipv6) {
                asprintf(&tempstr,
                         "interface '%d' cannot change IPv6 dynamically.", ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
        }
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("Number of interfaces cannot change dynamically."));
    }
    return valid;
}

static bool dyna_validate_extensions(const struct settings *new_settings,
                                     cJSON* errors)
{
    if (!new_settings->has.extensions) {
        return true;
    }

    /* extensions is not dynamic - validate it hasn't changed.*/
    bool valid = false;
    if (new_settings->num_pending_extensions ==
        settings.num_pending_extensions) {
        valid = true;
        int ii = 0;
        for (ii = 0; ii < settings.num_pending_extensions; ii++) {
            /* soname must be non-NULL and equal */
            valid &= new_settings->pending_extensions[ii].soname != NULL &&
                     strcmp(new_settings->pending_extensions[ii].soname,
                            settings.pending_extensions[ii].soname) == 0;

            /* new 'config' should either be NULL or equal to to the old one. */
            valid &= settings.pending_extensions[ii].config == NULL ||
                    (new_settings->pending_extensions[ii].config != NULL &&
                     strcmp(new_settings->pending_extensions[ii].config,
                            settings.pending_extensions[ii].config) == 0);
        }
    }

    if (valid) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'extensions' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_engine(const struct settings *new_settings,
                                 cJSON* errors)
{
    /* engine is not dynamic. */
    bool valid = true;

    if (!new_settings->has.engine) {
        return true;
    }

    /* module must be non-null, and the same as current.*/
    valid &= new_settings->engine_module != NULL &&
             strcmp(new_settings->engine_module, settings.engine_module) == 0;

    /* config may be null if current is, but must be equal */
    valid &= (settings.engine_config == NULL &&
              new_settings->engine_config == NULL) ||
             (settings.engine_config != NULL &&
              new_settings->engine_config != NULL &&
              strcmp(settings.engine_config, new_settings->engine_config) == 0);

    if (valid) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'engine' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_require_sasl(const struct settings *new_settings,
                                       cJSON* errors)
{
    if (!new_settings->has.require_sasl) {
        return true;
    }

    if (new_settings->require_sasl == settings.require_sasl) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'require_sasl' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_reqs_per_event(const struct settings *new_settings,
                                         cJSON* errors)
{
    /* reqs_per_event *is* dynamic */
    return true;
}

static bool dyna_validate_verbosity(const struct settings *new_settings,
                                    cJSON* errors)
{
    /* verbosity *is* dynamic */
    return true;
}

static bool dyna_validate_bio_drain_sz(const struct settings *new_settings,
                                              cJSON* errors)
{
    /* bio_drain_buffer_sz isn't dynamic */
    if (!new_settings->has.bio_drain_buffer_sz) {
        return true;
    }
    if (new_settings->bio_drain_buffer_sz == settings.bio_drain_buffer_sz) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'bio_drain_buffer_sz' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_datatype(const struct settings *new_settings,
                                   cJSON* errors)
{
    /* datatype isn't dynamic */
    if (!new_settings->has.datatype) {
        return true;
    }
    if (new_settings->datatype == settings.datatype) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'datatype_support' is not a dynamic setting."));
        return false;
    }
}

/* dynamic reconfiguration handlers ******************************************/

static void dyna_reconfig_iface_maxconns(const struct interface *new_if,
                                         struct interface *cur_if) {
    if (new_if->maxconn != cur_if->maxconn) {
        struct listening_port *port = get_listening_port_instance(cur_if->port);
        int old_maxconns = cur_if->maxconn;
        cur_if->maxconn = new_if->maxconn;
        port->maxconns = new_if->maxconn;
        calculate_maxconns();

        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Changed maxconns for interface %s:%hu from %d to %d",
            cur_if->host, cur_if->port, old_maxconns, cur_if->maxconn);
    }
}

static void dyna_reconfig_iface_backlog(const struct interface *new_if,
                                       struct interface *cur_if) {
    if (new_if->backlog != cur_if->backlog) {
        int old_backlog = cur_if->backlog;
        cur_if->backlog = new_if->backlog;

        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Changed backlog for interface %s:%hu from %d to %d",
                cur_if->host, cur_if->port, old_backlog, cur_if->backlog);
    }
}

static void dyna_reconfig_iface_nodelay(const struct interface *new_if,
                                       struct interface *cur_if) {
    if (new_if->tcp_nodelay != cur_if->tcp_nodelay) {
        conn* c = NULL;
        bool old_tcp_nodelay = cur_if->tcp_nodelay;
        cur_if->tcp_nodelay = new_if->tcp_nodelay;

        /* find all sockets for this connection, and update TCP_NODELAY sockopt */
        for (c = listen_conn; c != NULL; c = c->next) {
            if (c->parent_port == cur_if->port) {
                int nodelay_flag = cur_if->tcp_nodelay;
                int error = setsockopt(c->sfd, IPPROTO_TCP, TCP_NODELAY,
                                       (void*) &nodelay_flag,
                                       sizeof(nodelay_flag));
                if (error != 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to set TCP_NODELAY for FD %d, interface %s:%hu to %d: %s",
                         c->sfd, cur_if->host, cur_if->port, nodelay_flag,
                         strerror(errno));
                } else {
                    /* TODO: change to EXTENSION_LOG_INFO */
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Changed tcp_nodelay for FD %d, interface %s:%hu from %d to %d",
                        c->sfd, cur_if->host, cur_if->port, old_tcp_nodelay,
                        cur_if->tcp_nodelay);
                }
            }
        }
    }
}

static void dyna_reconfig_iface_ssl(const struct interface *new_if,
                                    struct interface *cur_if) {
    if (cur_if->ssl.cert != NULL && strcmp(new_if->ssl.cert,
                                           cur_if->ssl.cert) != 0) {
        const char *old_cert = cur_if->ssl.cert;
        cur_if->ssl.cert = strdup(new_if->ssl.cert);
        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Changed ssl.cert for interface %s:%hu from %s to %s",
            cur_if->host, cur_if->port, old_cert, cur_if->ssl.cert);
        free((char*)old_cert);
    }

    if (cur_if->ssl.key != NULL && strcmp(new_if->ssl.key,
                                           cur_if->ssl.key) != 0) {
        const char *old_key = cur_if->ssl.key;
        cur_if->ssl.key = strdup(new_if->ssl.key);
        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Changed ssl.key for interface %s:%hu from %s to %s",
            cur_if->host, cur_if->port, old_key, cur_if->ssl.key);
        free((char*)old_key);
    }
}

static void dyna_reconfig_interfaces(const struct settings *new_settings) {
    int ii = 0;
    for (ii = 0; ii < settings.num_interfaces; ii++) {
        struct interface *cur_if = &settings.interfaces[ii];
        struct interface *new_if = &new_settings->interfaces[ii];

        dyna_reconfig_iface_maxconns(new_if, cur_if);
        dyna_reconfig_iface_backlog(new_if, cur_if);
        dyna_reconfig_iface_nodelay(new_if, cur_if);
        dyna_reconfig_iface_ssl(new_if, cur_if);
    }
}

static void dyna_reconfig_reqs_per_event(const struct settings *new_settings) {
    if (new_settings->has.reqs_per_event &&
        new_settings->reqs_per_event != settings.reqs_per_event) {
        int old_reqs = settings.reqs_per_event;
        settings.reqs_per_event = new_settings->reqs_per_event;
        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Changed reqs_per_event from %d to %d", old_reqs,
            settings.reqs_per_event);
    }
}

static void dyna_reconfig_verbosity(const struct settings *new_settings) {
    if (new_settings->has.verbose &&
        new_settings->verbose != settings.verbose) {
        int old_verbose = settings.verbose;
        settings.verbose = new_settings->verbose;
        perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
        /* TODO: change to EXTENSION_LOG_INFO */
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Changed verbosity from %d to %d", old_verbose,
            settings.verbose);
    }
}

/* list of handlers for each setting */

struct {
    const char *key;
    config_handler handler;
    dynamic_validate_handler dynamic_validate;
    dynamic_reconfig_handler dyanamic_reconfig;
} handlers[] = {
    { "admin", get_admin, dyna_validate_admin, NULL},
    { "threads", get_threads, dyna_validate_threads, NULL },
    { "interfaces", get_interfaces, dyna_validate_interfaces, dyna_reconfig_interfaces },
    { "extensions", get_extensions, dyna_validate_extensions, NULL },
    { "engine", get_engine, dyna_validate_engine, NULL },
    { "require_sasl", get_require_sasl, dyna_validate_require_sasl, NULL },
    { "reqs_per_event", get_reqs_per_event, dyna_validate_reqs_per_event, dyna_reconfig_reqs_per_event },
    { "verbosity", get_verbosity, dyna_validate_verbosity, dyna_reconfig_verbosity },
    { "bio_drain_buffer_sz", get_bio_drain_sz, dyna_validate_bio_drain_sz, NULL },
    { "datatype_support", get_datatype, dyna_validate_datatype, NULL },
    { NULL, NULL, NULL, NULL }
};

/* Parses the specified JSON object, updating settings with all found
 * parameters.
 * @param sys JSON object containing config options.
 * @param settings Settings struct to fill in.
 * @param error_msg pointer to char* which will upon error will be set to a
 *                  string describing why parse failed. If non-null caller is
 *                  responsible for free()ing it.
 * @return true if JSON was successfully parsed, else false.
 */
static bool parse_JSON_config(cJSON* sys, struct settings *s,
                       char **error_msg) {
    s->config = cJSON_PrintUnformatted(sys);

    cJSON *obj = sys->child;
    while (obj) {
        int ii = 0;
        while (handlers[ii].key != NULL) {
            if (strcasecmp(handlers[ii].key, obj->string) == 0) {
                break;
            }
            ++ii;
        }

        if (handlers[ii].key == NULL) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Unknown token \"%s\" in config ignored.\n", obj->string);
        } else {
            if (!handlers[ii].handler(obj, s, error_msg)) {
                return false;
            }
        }

        obj = obj->next;
    }

    *error_msg = NULL;
    return true;
}

/* Reads the specified file and parses it, filling in settings with parsed
 * settings.
 * @param file path to config file to parse
 * @param settings Settings struct to fill in.
 * @param error_msg pointer to char* which will upon error will be set to a
 *                  string describing why parse failed. If non-null caller is
 *                  responsible for free()ing it.
 * @return true if file was successfully parsed, else false.
 */
static bool parse_config_file(const char* file, struct settings *settings,
                              char** error_msg) {
    cJSON *sys;
    bool result;
    config_error_t err = config_load_file(file, &sys);

    if (err != CONFIG_SUCCESS) {
        *error_msg = config_strerror(file, err);
        return false;
    }

    result = parse_JSON_config(sys, settings, error_msg);
    cJSON_Delete(sys);
    return result;
}

/******************************************************************************
 * Public functions
 *****************************************************************************/

void load_config_file(const char *file, struct settings *settings)
{
    char* error_msg = NULL;
    if (!parse_config_file(file, settings, &error_msg)) {
        fprintf(stderr, "%s\nTerminating\n", error_msg);
        free(error_msg);
        exit(EXIT_FAILURE);
    }
}

bool validate_proposed_config_changes(const char* new_cfg, cJSON* errors) {
    bool valid;
    struct settings new_settings = {0};
    char *error_msg = NULL;
    cJSON *config = cJSON_Parse(new_cfg);
    if (config == NULL) {
        cJSON_AddItemToArray(errors, cJSON_CreateString("JSON parse error"));
        return false;
    }

    if ((valid = parse_JSON_config(config, &new_settings, &error_msg))) {
        int i = 0;
        while (handlers[i].key != NULL) {
            valid &= handlers[i].dynamic_validate(&new_settings, errors);
            i++;
        }
    } else {
        cJSON_AddItemToArray(errors, cJSON_CreateString(error_msg));
        free(error_msg);
    }

    /* cleanup */
    free_settings(&new_settings);
    cJSON_Delete(config);
    return valid;
}

void reload_config_file(void) {
    struct settings new_settings = {0};
    char* error_msg;
    cJSON* errors = cJSON_CreateArray();
    int ii;
    bool valid = true;

    /* TODO: change to EXTENSION_LOG_INFO */
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Reloading config file %s", get_config_file());

    /* parse config into a new settings structure */
    if (!parse_config_file(get_config_file(), &new_settings, &error_msg)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to reload config file %s : %s\n", get_config_file(),
            error_msg);
        free(error_msg);
        return;
    }

    /* Validate */
    for (ii = 0; handlers[ii].key != NULL; ii++) {
        valid &= handlers[ii].dynamic_validate(&new_settings, errors);
    }

    if (valid) {
        /* for all dynamic options, apply any differences to the running config. */
        for (ii = 0; handlers[ii].key != NULL; ii++) {
            if (handlers[ii].dyanamic_reconfig != NULL) {
                handlers[ii].dyanamic_reconfig(&new_settings);
            }
        }
    } else {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Validation failed while reloading config file '%s'. Errors:",
            get_config_file());
        for (ii = 0; ii < cJSON_GetArraySize(errors); ii++) {
            char* json = cJSON_Print(cJSON_GetArrayItem(errors, ii));
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "\t%s", json);
            free(json);
        }
    }
    cJSON_Delete(errors);
}

/* Frees all dynamic memory associated with the given settings struct */
void free_settings(struct settings* s) {
    int ii;
    free((char*)s->admin);
    for (ii = 0; ii < s->num_interfaces; ii++) {
        free((char*)s->interfaces[ii].host);
        free((char*)s->interfaces[ii].ssl.key);
        free((char*)s->interfaces[ii].ssl.cert);
    }
    free(s->interfaces);
    for (ii = 0; ii < s->num_pending_extensions; ii++) {
        free((char*)s->pending_extensions[ii].soname);
        free((char*)s->pending_extensions[ii].config);
    }
    free(s->pending_extensions);
    free((char*)s->engine_module);
    free((char*)s->engine_config);
    free((char*)s->config);
}
