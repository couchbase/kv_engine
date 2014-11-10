/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include "cmdline.h"

#include <getopt.h>

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

static void handle_L(struct Option* o) {
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

static void handle_k(struct Option* o) {
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
        case 'd':
            settings.daemonize = true;
            break;
        case 'R':
            settings.default_reqs_per_event = atoi(o->optarg);
            break;
        case 'D':
            settings.prefix_delimiter = o->optarg[0];
            break;
        case 'L':
            handle_L(o);
            break;
        case 'P':
            settings.pid_file = strdup(o->optarg);
            break;
        case 'k':
            handle_k(o);
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
    const char *config_file = NULL;
    int c;

    /* process arguments */
    while ((c = getopt(argc, argv, "I:b:C:R:D:Ln:f:P:ihkc:Mm:u:da:s:p:U:t:vrB:E:e:X:l:")) != -1) {
        switch (c) {
        case 'C':
            config_file = optarg;
            break;

        case 'b':
        case 'd':
        case 'R':
        case 'D':
        case 'L':
        case 'P':
        case 'k':
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
        read_config_file(config_file);
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
