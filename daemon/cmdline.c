/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include "cmdline.h"
#include "config_parse.h"

#include <getopt.h>


/* path to our config file. */
static const char *config_file = NULL;

static void usage(void) {
    printf("memcached %s\n", get_server_version());
    printf("-C file       Read configuration from file\n");
    printf("-h            print this help and exit\n");
    printf("\nEnvironment variables:\n");
    printf("MEMCACHED_PORT_FILENAME   File to write port information to\n");
    printf("MEMCACHED_REQS_TAP_EVENT  Similar to \"reqs_per_event\" in\n"
           "   the configuration file, but but for tap_ship_log\n");
}

struct Option {
    int cmd;
    char *optarg;
    struct Option *next;
};

struct Option *options = NULL;
struct Option *tail = NULL;

static void add_option(int cmd, char *optarg) {
    struct Option *o = malloc(sizeof(*o));
    if (o == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }
    o->cmd = cmd;
    o->optarg = optarg;
    o->next = NULL;

    if (options == NULL) {
        options = tail = o;
    } else {
        tail->next = o;
        tail = o;
    }
}

static void handle_b(struct Option* o) {
    int backlog = atoi(o->optarg);
    int ii = 0;

    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        settings.interfaces[ii].backlog = backlog;
    }
}

static void handle_c(struct Option* o) {
    int maxconn = atoi(o->optarg);
    int ii = 0;

    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        settings.interfaces[ii].maxconn = maxconn;
    }
}

static void handle_X(struct Option* o) {
    char *module = o->optarg;
    char *config = strchr(o->optarg, ',');
    if (config == NULL) {
        *config = '\0';
        config++;
    }

    if (!load_extension(module, config)) {
        exit(EXIT_FAILURE);
    }
}

static void apply_compat_arguments(void) {
    struct Option *o;

    /* Handle all other options */
    for (o = options; o != NULL; o = o->next) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Option -%c passed to memcached is no longer supported. Consider using the configuration file instead\n", o->cmd);

        switch (o->cmd) {
        case 'b':
            handle_b(o);
            break;
        case 'R':
            settings.default_reqs_per_event = atoi(o->optarg);
            break;
        case 'c':
            handle_c(o);
            break;
        case 't':
            settings.num_threads = atoi(o->optarg);
            break;
        case 'v':
            ++settings.verbose;
            break;
        case 'E':
            settings.engine_module = strdup(o->optarg);
            break;
        case 'e':
            settings.engine_config = strdup(o->optarg);
            break;
        case 'X':
            handle_X(o);
            break;
        }
    }
}

void parse_arguments(int argc, char **argv) {
    int c;

    /* process arguments */
    while ((c = getopt(argc, argv, "I:b:C:R:D:Ln:f:P:ihkc:Mm:u:da:s:p:U:t:vrB:E:e:X:l:")) != -1) {
        switch (c) {
        case 'C':
            config_file = optarg;
            break;

        case 'b':
        case 'R':
        case 'c':
        case 't':
        case 'v':
        case 'E':
        case 'e':
        case 'X':
            add_option(c, optarg);
            break;

        case 'I':
        case 'm':
        case 'M':
        case 'f':
        case 'n':
            fprintf(stderr,
                    "-%c is no longer used. update the per-engine config\n",
                    c);
            exit(EXIT_FAILURE);
            break;

        case 'D':
            fprintf(stderr, "-D (delimiter) is no longer supported\n");
            exit(EXIT_FAILURE);
            break;

        case 'k':
            fprintf(stderr, "-k (mlockall) is no longer supported\n");
            exit(EXIT_FAILURE);
            break;

        case 'L':
            fprintf(stderr, "-L (Large memory pages) is no longer supported\n");
            exit(EXIT_FAILURE);
            break;

        case 'P':
            fprintf(stderr, "-P (pid file) is no longer supported\n");
            exit(EXIT_FAILURE);
            break;

        case 'd':
            fprintf(stderr, "-d is no longer used\n");
            exit(EXIT_FAILURE);
            break;

        case 'u':
            fprintf(stderr, "Changing user is no longer supported\n");
            exit(EXIT_FAILURE);
            break;

        case 'a': /* FALLTHROUGH */
        case 's':
            fprintf(stderr, "UNIX socket path support is removed\n");
            exit(EXIT_FAILURE);
            break;

        case 'r':
            fprintf(stderr, "-r is no longer supported. Increase core"
                    "file max size before starting memcached\n");
            exit(EXIT_FAILURE);
            break;

        case 'B':
            if (strcmp(optarg, "binary") != 0) {
                fprintf(stderr, "Only binary protocol is supported (-B)\n");
                exit(EXIT_FAILURE);
            }
            break;

        case 'U':
            fprintf(stderr, "WARNING: UDP support is removed\n");
            exit(EXIT_FAILURE);
            break;

        case 'l':
            fprintf(stderr, "-l is not supported you have to use -C\n");
            exit(EXIT_FAILURE);
            break;

        case 'p':
            fprintf(stderr, "-p is not supported you have to use -C\n");
            exit(EXIT_FAILURE);
            break;

        case 'i': /* license */
        case 'h': /* help */
        default:
            usage();
            exit(EXIT_FAILURE);
        }
    }

    if (config_file) {
        load_config_file(config_file, &settings);
    }

    /* Process other arguments */
    if (options) {
        apply_compat_arguments();
        while (options) {
            tail = options;
            options = options->next;
            free(tail);
        }
    }
}

const char* get_config_file(void)
{
    return config_file;
}
