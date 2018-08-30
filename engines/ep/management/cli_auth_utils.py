#!/usr/bin/env python

import getpass
import inspect
import os
import sys

import clitool
import mc_bin_client
import memcacheConstants


def cmd_decorator(f):
    """Decorate a function with code to authenticate based on 1-3
    additional arguments."""

    def g(*args, **kwargs):
        mc = args[0]
        spec = inspect.getargspec(f)
        max = len(spec.args)
        defaults = len(spec.defaults) if spec.defaults else 0
        min = max - defaults

        if len(args) < min:
            print >> sys.stderr, ("Error: too few arguments - command "
                                  "expected a minimum of %s but was passed "
                                  "%s: %s"
                                  % (min - 1, len(args) - 1, list(args[1:])))
            sys.exit(2)

        if spec.varargs is None:
            if len(args) > max:
                print >> sys.stderr, ("Error: too many arguments - command "
                                      "expected a maximum of %s but was passed "
                                      "%s: %s"
                                      % (max - 1, len(args) - 1, list(args[1:])))
                sys.exit(2)

        bucket = kwargs.pop('bucketName', None)
        username = kwargs.pop('username', None) or bucket
        cli_password = kwargs.pop('password', None)
        stdin_password = (getpass.getpass()
                          if kwargs.pop('passwordFromStdin', False)
                          else None)
        env_password = os.getenv("CB_PASSWORD", None)

        password = cli_password or stdin_password or env_password


        if username is not None or password is not None:
            bucket = bucket or 'default'
            username = username or bucket
            password = password or ''
            try:
                mc.sasl_auth_plain(username, password)
            except mc_bin_client.MemcachedError:
                print ("Authentication error for user:{0} bucket:{1}"
                       .format(username, bucket))
                sys.exit(1)

        mc.enable_xerror()
        mc.enable_collections()
        mc.hello("{0} {1}".format(os.path.split(sys.argv[0])[1],
                                os.getenv("EP_ENGINE_VERSION",
                                          "unknown version")))
        try:
            if kwargs.pop('allBuckets', None):
                buckets = mc.list_buckets()
                for bucket in buckets:
                    print '*' * 78
                    print bucket
                    print
                    mc.bucket_select(bucket)
                    f(*args, **kwargs)
            elif bucket is not None:
                mc.bucket_select(bucket)
                f(*args, **kwargs)
            else:
                f(*args, **kwargs)
        except mc_bin_client.ErrorEaccess:
            print ("No access to bucket:{0} - permission denied "
                   "or bucket does not exist.".format(bucket))
            sys.exit(1)

    return g



def get_authed_clitool(extraUsage=""):
    c = clitool.CliTool(extraUsage)

    c.addFlag('-a', 'allBuckets', 'iterate over all buckets')
    c.addOption('-b', 'bucketName', 'the bucket to get stats from (Default: default)')
    c.addOption('-u', 'username', 'the user as which to authenticate (Default: bucketName)')
    c.addOption('-p', 'password', 'the password for the bucket if one exists')
    c.addFlag('-S', 'passwordFromStdin', 'read password from stdin')

    return c
