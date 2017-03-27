#!/usr/bin/env python

import clitool
import inspect
import mc_bin_client
import memcacheConstants
import sys
import os

def cmd_decorator(f):
    """Decorate a function with code to authenticate based on 1-3
    additional arguments."""

    def g(*args, **kwargs):
        mc = args[0]
        spec = inspect.getargspec(f)
        n = len(spec.args)

        if len(args) > n and spec.varargs is None:
            print >> sys.stderr, ("Error: too many arguments - expected" +
                                  " a maximum of %s but was passed %s: %s" \
                                  % (n - 1, len(args) - 1, list(args[1:])))
            sys.exit(1)

        bucket = kwargs.get('bucketName', None) or 'default'
        username = kwargs.get('username', None) or bucket
        password = kwargs.get('password', None) or ''

        if username:
            try:
                mc.sasl_auth_plain(username, password)
            except mc_bin_client.MemcachedError:
                print ("Authentication error for user:{} bucket:{}"
                       .format(username, bucket))
                sys.exit(1)

        mc.hello("{} {}".format(os.path.split(sys.argv[0])[1],
                                os.getenv("EP_ENGINE_VERSION",
                                          "unknown version")))
        try:
            if kwargs.get('allBuckets', None):
                buckets = mc.list_buckets()
                for bucket in buckets:
                    print '*' * 78
                    print bucket
                    print
                    mc.bucket_select(bucket)
                    f(*args)
            elif bucket is not None:
                mc.bucket_select(bucket)
                f(*args)
            else:
                f(*args)
        except mc_bin_client.ErrorEaccess:
            print ("No access to bucket:{} - permission denied "
                   "or bucket does not exist.".format(bucket))
            sys.exit(1)

    return g



def get_authed_clitool():
    c = clitool.CliTool()

    c.addFlag('-a', 'allBuckets', 'iterate over all buckets')
    c.addOption('-b', 'bucketName', 'the bucket to get stats from (Default: default)')
    c.addOption('-u', 'username', 'the user as which to authenticate (Default: bucketName)')
    c.addOption('-p', 'password', 'the password for the bucket if one exists')

    return c
