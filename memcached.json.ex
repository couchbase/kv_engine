{
    "threads" : 4,
    "interfaces" :
    [
        {
            "maxconn" : 10000,
            "port" : 11210,
            "backlog" : 1024,
            "host" : "*",
            "IPv6" : true,
            "ssl" :
            {
                "key" : "/etc/memcached/pkey",
                "cert" : "/etc/memcached/cert"
            }
        },
        {
            "maxconn" : 1000,
            "port" : 11213,
            "host" : "127.0.0.1",
            "IPv6" : false
        }
    ],
    "extensions" :
    [
        {
            "module" : "stdin_term_handler.so",
            "config" : ""
        },
        {
            "module" : "file_logger.so",
            "config" : "cyclesize=10485760;sleeptime=19;filename=data/n_0/logs/memcached.log"
        }
    ],
    "engine" : {
        "module" : "bucket_engine.so",
        "config" : "admin=_admin;default_bucket_name=default;auto_create=false"
    },
    "require_sasl" : false,
    "prefix_delimiter" : ":",
    "tcp_nodelay" : false,
    "allow_detailed" : true,
    "detail_enabled" : false,
    "reqs_per_event" : 20,
    "verbosity" : 0,
    "lock_memory" : false,
    "large_memory_pages" : false,
    "daemonize" : false,
    "pid_file" : "/var/run/memcached.pid"
}
