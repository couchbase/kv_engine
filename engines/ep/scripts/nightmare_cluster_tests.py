#!/usr/bin/env python3
#
#     Copyright 2026-Present Couchbase, Inc.
#
#   Use of this software is governed by the Business Source License included
#   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
#   in that file, in accordance with the Business Source License, use of this
#   software will be governed by the Apache License, Version 2.0, included in
#   the file licenses/APL2.txt.
#
"""Run ns_server's cluster_tests under nightmare mode.

Use in place of run.py, from the cluster_tests directory; every run.py
argument works as normal, plus --timeout-scale and --keep-auto-failover
(see below):

    cd <source>/ns_server/cluster_tests
    ep=<source>/kv_engine/engines/ep
    export COUCHBASE_NIGHTMARE_MODE=$ep/nightmare_configuration.json
    python3 $ep/scripts/nightmare_cluster_tests.py \\
        -k --tests KVFileBasedRebalanceTests

With COUCHBASE_NIGHTMARE_MODE set it first:

* scales cluster_tests' timeouts by --timeout-scale N (default 10):
  polled conditions, async tasks, rebalance waits and run.py's per-test
  watchdog. They are sized for a normal build, and nightmare mode is several
  times slower. An explicit --test-timeout still wins.
* disables auto-failover on each cluster it creates, as a node shutdown can
  outlast the 120s timeout. --keep-auto-failover opts out.
* drops EXCLUDED_TESTSETS, from discovery and from any --tests / -t list.

With it unset, run.py runs unchanged, i.e. a baseline run.
"""

import builtins
import functools
import importlib
import inspect
import os
import sys

# Testsets which cannot pass reliably under nightmare mode, however the
# timeouts are scaled, because the outcome depends on machine speed.
EXCLUDED_TESTSETS = {
    # Turns auto-failover on (120s) itself, then stops its nodes. A
    # nightmare-mode shutdown takes minutes, so a node is failed over mid-stop
    # and the remapped cluster built from that config cannot start.
    'NodeRemapTest': 'testsets.node_remap_tests',
}


def scaled(func, scale, *params):
    """Wrap func so the named parameters are multiplied by scale."""
    signature = inspect.signature(func)
    # Check now, at start-up, rather than on the first call mid-test.
    missing = set(params) - signature.parameters.keys()
    if missing:
        raise TypeError(f'{func.__qualname__} has no parameter(s) {missing}: '
                        'cluster_tests has changed, update this wrapper')

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        bound.apply_defaults()
        for param in params:
            if bound.arguments[param] is not None:
                bound.arguments[param] *= scale
        return func(*bound.args, **bound.kwargs)

    return wrapper


def scale_timeouts(scale):
    """Multiply cluster_tests' timeouts by scale."""
    import testlib
    import testlib.cluster
    import testlib.testlib
    from testsets import tasks_test

    # Waiting for a condition to become true. Tests call
    # testlib.poll_for_condition, but testlib's own helpers call the
    # module-level name, so replace both.
    poll = scaled(testlib.testlib.poll_for_condition, scale,
                  'attempts', 'timeout')
    testlib.poll_for_condition = poll
    testlib.testlib.poll_for_condition = poll

    # Waiting for an async ns_server task, e.g. a sample bucket load.
    TasksBase = tasks_test.TasksBase
    TasksBase.wait_for_task = scaled(TasksBase.wait_for_task, scale,
                                     'timeout')

    # Waiting for a rebalance. If this runs out, the rebalance status reads
    # "timeout": the test gave up, the rebalance did not fail.
    Cluster = testlib.cluster.Cluster
    Cluster.wait_for_rebalance = scaled(Cluster.wait_for_rebalance, scale,
                                        'timeout_s', 'balanced_timeout')

    # The time limit for a whole test, which must outlast the scaled waits
    # above. A --test-timeout argument still overrides it, because run.py
    # reads its arguments after this runs.
    testlib.config['test_timeout'] *= scale


def disable_auto_failover():
    """Turn auto-failover off on each cluster the harness creates.

    Hooks ClusterRequirements.create_cluster rather than build_cluster:
    requirements.py does `from testlib.cluster import build_cluster`, so it
    keeps its own reference and would never call a replacement.
    """
    import testlib.requirements

    original = testlib.requirements.ClusterRequirements.create_cluster

    @functools.wraps(original)
    def create_cluster(self, *args, **kwargs):
        cluster = original(self, *args, **kwargs)
        # An unconnected cluster (e.g. NodeRemapTest's remapped one) has no
        # settings to change. Callers pass connect by keyword.
        if kwargs.get('connect', True):
            cluster.disable_auto_failover()
            print(f'nightmare_cluster_tests: auto-failover disabled on '
                  f'cluster #{cluster.index}', file=sys.stderr)
        return cluster

    testlib.requirements.ClusterRequirements.create_cluster = create_cluster


def exclude_testsets(argv):
    """Remove EXCLUDED_TESTSETS from discovery and from argv's --tests / -t.

    run.py discovers BaseTestSet subclasses in the testsets/ modules, which it
    imports as modules, never by class, so deleting the class hides it. A
    --tests list naming an unknown testset is rejected outright, so excluded
    names are stripped from it rather than left to fail.
    """
    for name, module_name in EXCLUDED_TESTSETS.items():
        module = importlib.import_module(module_name)
        if hasattr(module, name):
            delattr(module, name)

    def filtered(value):
        kept = []
        for spec in value.split(','):
            if spec.strip().split('.')[0] in EXCLUDED_TESTSETS:
                print(f'nightmare_cluster_tests: skipping {spec.strip()} '
                      '(excluded under nightmare mode)', file=sys.stderr)
            else:
                kept.append(spec)
        if not kept:
            sys.exit('nightmare_cluster_tests: every requested testset is '
                     'excluded under nightmare mode - nothing to run')
        return ','.join(kept)

    out = []
    args = iter(argv)
    for arg in args:
        if arg in ('--tests', '-t'):
            value = next(args, None)
            out += [arg] if value is None else [arg, filtered(value)]
        elif arg.startswith('--tests='):
            out.append('--tests=' + filtered(arg[len('--tests='):]))
        elif arg.startswith('-t') and not arg.startswith('--'):
            out.append('-t' + filtered(arg[2:]))
        else:
            out.append(arg)
    return out


def pop_own_options(argv):
    """Remove this wrapper's options from argv, as run.py rejects unknown ones.

    Returns (timeout_scale, keep_auto_failover).
    """
    timeout_scale = 10
    keep_auto_failover = False
    remaining = []
    args = iter(argv)
    for arg in args:
        if arg == '--keep-auto-failover':
            keep_auto_failover = True
        elif arg == '--timeout-scale' or arg.startswith('--timeout-scale='):
            value = (arg.partition('=')[2] if '=' in arg
                     else next(args, ''))
            if not value.isdigit() or int(value) < 1:
                sys.exit('nightmare_cluster_tests: --timeout-scale needs a '
                         f'positive integer, got {value!r}')
            timeout_scale = int(value)
        else:
            remaining.append(arg)
    argv[:] = remaining
    return timeout_scale, keep_auto_failover


def main():
    if not os.path.isfile('run.py'):
        sys.exit('nightmare_cluster_tests: run this from the cluster_tests '
                 'directory (no run.py here)')
    # run.py imports testlib and testsets from its own directory, which is
    # not on sys.path when this wrapper is the script being run.
    sys.path.insert(0, os.getcwd())

    scale, keep_auto_failover = pop_own_options(sys.argv)

    if os.environ.get('COUCHBASE_NIGHTMARE_MODE'):
        scale_timeouts(scale)
        if not keep_auto_failover:
            disable_auto_failover()
        sys.argv[1:] = exclude_testsets(sys.argv[1:])
        print(f'nightmare_cluster_tests: timeouts scaled x{scale}',
              file=sys.stderr)
    else:
        print('nightmare_cluster_tests: COUCHBASE_NIGHTMARE_MODE not set, '
              'running run.py unchanged', file=sys.stderr)

    # Run run.py as __main__. __builtins__ must be the builtins module rather
    # than its dict (which runpy would give): run.py stores the original print
    # as an attribute on it.
    sys.argv[0] = 'run.py'
    with open('run.py') as run_py:
        code = compile(run_py.read(), 'run.py', 'exec')
    exec(code, {'__name__': '__main__', '__file__': 'run.py',
                '__builtins__': builtins})


if __name__ == '__main__':
    main()
