"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

import clitool
import cli_auth_utils
import codecs
import datetime
import inspect
import itertools
import json
import math
import mc_bin_client
import re
import sys

from collections import defaultdict
from difflib import SequenceMatcher
from operator import itemgetter

try:
    from natsort import natsorted
except:
    # MB-48540: natsort relies on distutils. Until cbpy is once again packaged with distutils, fall back to
    # normal ("not-natural") sort.
    natsorted = sorted

BIG_VALUE = 2 ** 60
SMALL_VALUE = - (2 ** 60)

output_json = False
force_utf8 = False

def cmd(f):
    f = cli_auth_utils.cmd_decorator(f)
    def g(*args, **kwargs):
        global output_json
        output_json = kwargs.pop('json', None)
        global force_utf8
        force_utf8 = kwargs.pop('force_utf8', None)
        if force_utf8:
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

        f(*args, **kwargs)
    return g

def stats_perform(mc, cmd=''):
    try:
        return mc.stats(cmd)
    except Exception as e:
        print("Stats '%s' are not available from the requested engine. (%s)"
                % (cmd, e))

def stats_formatter(stats, prefix=" "):
    """ Format and output the stats as either json or text
    """
    if stats:
        if output_json:

            # Recreate the dictionary sorted so we can keep the natural ordering
            # that natsort gives us and still print this out as a json
            # dictionary
            dict = {}
            for stat, val in natsorted(stats.items()):
                dict[stat] = val

            for stat, val in dict.items():
                dict[stat] = _maybeInt(val)
                if isinstance(dict[stat], str):
                    dict[stat] = _maybeFloat(val)
            print (json.dumps(dict, indent=4))
        else:
            longest = max((len(x) + 2) for x in stats.keys())
            for stat, val in natsorted(stats.items()):
                s = stat + ":"
                print("%s%s%s" % (prefix, s.ljust(longest), val))


def table_formatter(columns, data, sort_key=None, reverse=False):
    """Formats data in a top-style table.

    Takes a list of Columns (holding a display name and alignment), and a list
    of lists representing each row in the table. The data in the rows should be
    in the same order as the Columns.
    """
    column_widths = [len(c) for c in columns]
    for row in data:
        for index, item in enumerate(row):
            column_widths[index] = max([column_widths[index], len(str(item))])

    template = ""
    for index, column in enumerate(columns[:-1]):
        align = ">" if column.ralign else "<"
        template += "{{{0}:{1}{2}}}  ".format(index, align,
                                              column_widths[index])
    # Last line is not padded unless right aligned
    # so only long lines will wrap, not all of them
    template += ("{{{0}:>{1}}}  ".format(len(columns) - 1, column_widths[-1])
                if columns[-1].ralign else ("{" + str(len(columns) - 1) + "}"))

    print(template.format(*columns), "\n")
    for row in sorted(data, key=sort_key, reverse=reverse):
        print(template.format(*row))

class TaskStat(object):
    """Represents a stat which must be sorted by a different value than is
    displayed, i.e. pretty-printed timestamps
    """
    def __init__(self, display_value, value):
        self.display_value = display_value
        self.value = value

    def __eq__(self, other):
        return self.value == (other.value if hasattr(other, "value") else other)

    def __lt__(self, other):
        return self.value < (other.value if hasattr(other, "value") else other)

    # total_ordering decorator unavailable in Python 2.6, otherwise only
    # __eq__ and one comparision would be necessary

    def __gt__(self, other):
        return self.value > (other.value if hasattr(other, "value") else other)

    def __str__(self):
        return self.display_value

    def __format__(self, format_spec):
        return format(self.display_value, format_spec)

class Column(object):
    def __init__(self, display_name, invert_sort, ralign):
        self.display_name = display_name
        self.invert_sort = invert_sort
        self.ralign = ralign

    def __str__(self):
        return self.display_name

    def __len__(self):
        return len(str(self))

    def __format__(self, format_spec):
        return format(self.display_name, format_spec)

def ps_time_stat(t):
    """convenience method for constructing a stat displaying a ps-style
    timestamp but sorting on the underlying time since epoch.
    """
    t = t / 1000
    return TaskStat(ps_time_label(t), t)

def tasks_stats_formatter(stats, sort_by=None, reverse=False, *args):
    """Formats the data from ep_tasks in a top-like display"""
    if stats:
        if output_json:
            stats_formatter(stats)
        else:
            cur_time = int(stats.pop("ep_tasks:cur_time"))

            total_tasks = {"Reader":0,
                           "Writer":0,
                           "AuxIO":0,
                           "NonIO":0}

            running_tasks = total_tasks.copy()

            states = ["R", "S", "D"]


            tasks = json.loads(stats["ep_tasks:tasks"])

            for task in tasks:
                total_tasks[task["type"]]+=1

                task["waketime_ns"] = (ps_time_stat(
                                        (task["waketime_ns"] - cur_time))
                                    if task["waketime_ns"] < BIG_VALUE
                                    else TaskStat("inf", task["waketime_ns"]))

                task["total_runtime_ns"] = ps_time_stat(
                                                   task["total_runtime_ns"])

                if task["last_starttime_ns"] != 0:
                    # task is running (currently executing).
                    runtime = ps_time_stat(cur_time - task["last_starttime_ns"])

                    #  Mark runtime of running tasks with asterisk
                    runtime.display_value = ("*" + runtime.display_value)
                    task["runtime"] = runtime

                    task["waketime_ns"] = ps_time_stat(0)
                    running_tasks[task["type"]]+=1
                else:
                    task["runtime"] = ps_time_stat(task["previous_runtime_ns"])

                task["state"] = task["state"][0]


            running_tasks["Total"] = sum(running_tasks.values())
            total_tasks["Total"] = len(tasks)

            headers = (
                "Tasks     Writer Reader AuxIO  NonIO  Total      \n"
                "Running   {Writer:<6} {Reader:<6} "
                "{AuxIO:<6} {NonIO:<6} {Total:<6}\n"
                    .format(**running_tasks) +
                "All       {Writer:<6} {Reader:<6} "
                "{AuxIO:<6} {NonIO:<6} {Total:<6}\n"
                    .format(**total_tasks)
            )

            print(headers)

            table_columns = [
                    (key, Column(*options)) for key, options in (
                    # Stat            Display Name, Invert Sort, Right Align
                    ('tid',              ('TID',      False, True )),
                    ('priority',         ('Pri',      False, True )),
                    ('state',            ('St',       False, False)),
                    ('bucket',           ('Bucket',   False, False)),
                    ('waketime_ns',      ('SleepFor', True,  True )),
                    ('runtime',          ('Runtime',  True,  True )),
                    ('total_runtime_ns', ('TotalRun', True,  True )),
                    ('num_runs',         ('#Runs',    True,  True )),
                    ('type',             ('Type',     False, False)),
                    ('name',             ('Name',     False, False)),
                    ('this',             ('Addr',     False, False)),
                    ('description',      ('Descr.',   False, False)),
                )]

            table_column_keys = [x[0] for x in table_columns]
            table_column_values = [x[1] for x in table_columns]

            table_data = []

            for row in tasks:
                table_data.append(tuple(row[key]
                                        for key in table_column_keys))

            sort_key = None
            if sort_by is not None:
                if isinstance(sort_by, int):
                    sort_key = itemgetter(sort_by)

                elif isinstance(sort_by, str):
                    if sort_by.isdigit():
                        sort_key = itemgetter(int(sort_by))
                    else:
                        # Find which column has the lowest string distance
                        # to the requested sort value.
                        sort_by = sort_by.lower()

                        similarity = lambda s: (
                            SequenceMatcher(None,
                                            sort_by,
                                            s.display_name.lower()).ratio())

                        closest = sorted(table_column_values,
                                         key=similarity,
                                         reverse=True)[0]

                        index = table_column_values.index(closest)
                        sort_key = itemgetter(index)
                        reverse ^= closest.invert_sort

            table_formatter(table_column_values, table_data,
                            sort_key, reverse=reverse)


def ps_time_label(microseconds):
    sign = "-" if microseconds < 0 else ""

    centiseconds = int(abs(microseconds)//10000)
    seconds = centiseconds//100

    time_str = str(datetime.timedelta(seconds=seconds))

    #  drop hours and tens of seconds if zero
    if time_str.startswith("0:"):
        time_str = time_str[2:]

    if time_str.startswith("00"):
        time_str = time_str[1:]

    return "{0}{1}.{2:0>2}".format(sign,
                                   time_str,
                                   centiseconds % 100)

def no_label(s):
    return str(s)

def time_label(s):
    # -(2**64) -> '-inf'
    # 2**64 -> 'inf'
    # 0 -> '0us'
    # 4 -> '4us'
    # 9000 -> '9000us'
    # 10000 -> '10ms'
    # 838384 -> '838ms'
    # 8283852 -> '8283ms'
    # 10283852 -> '10s'
    if s > BIG_VALUE:
        return 'inf'
    elif s < SMALL_VALUE:
        return '-inf'
    isNegative = s < 0

    s = abs(s)
    if s <= 1:
        return "%4d%s" % (s, 'us')

    product = 1
    '''
    For unit labeling we define our thresh holds as
    Any value <= 10,000 will represented as a micro second (us)
    Any value <= 10,000,000 will be represented as a milli second (ms)
    Any value <= 600,000,000 will be represented as a second (s)
    Anything > than 600,000,000 will be represented as minutes and seconds
    e.g. 605000000 will be represented as 10m:05s

    We do this unusual thresholding so that we always have than two s.f. to
    for values greater than 10us to distinguish between buckets.
    For instance if we didn't do this bucket start/end values could look like
    this:
     968us - 1ms   : ( 99.82%)    22
    1ms - 1ms     : ( 99.84%)    22
    1ms - 1ms     : ( 99.86%)    23
    ....
    9ms - 15ms    : ( 99.99%)     2
    15ms - 23ms   : ( 99.99%)     1
    23ms - 40ms   : ( 99.99%)     2

    Which isn't very useful. However, with this style of thresholding
    we end up with:
    968us - 1050us : ( 99.8247%)    22
    1050us - 1168us : ( 99.8440%)    22
    1168us - 1323us : ( 99.8641%)    23
    ....
    9278us -   15ms : ( 99.9921%)     2
      15ms -   23ms : ( 99.9930%)     1
      23ms -   40ms : ( 99.9947%)     2

    '''
    sizes = (('us', 1), ('ms', 10000), ('s', 1000), ('m', 60))
    sizeMap = []
    for l, sz in sizes:
        product = sz * product
        sizeMap.insert(0, (l, product))
    lbl, factor = next(itertools.dropwhile(lambda x: x[1] >= s, sizeMap))

    # Give some extra granularity for timings in minutes
    if lbl == 'm':
        # Remove extra factor of ten we used to threshold.
        factor = factor / 10
        mins = s / factor
        secs = (s % factor) / (factor / 60)
        result = '%d%s:%02ds' % (mins, lbl, secs)
    else:
        if lbl == 'us': # case for micro seconds
            result = "%4d%s" % (s, lbl)
        else: # case milli seconds and seconds
            # we need to remove the extra factor of ten we used to threshold,
            # so we only divide by 1000 and not 10,000
            factor /= 10
            result = "%4d%s" % (s / factor, lbl)

    return ("-" if isNegative else "") + result

def sec_label(s):
    return time_label(s * 1000000)

def size_label(s):
    if s == 0:
        return "%4d%s" % (0, 'B ')
    sizes=['B ', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    e = math.floor(math.log(abs(s), 1024))
    suffix = sizes[int(e)]
    div = 1
    if e > 0 :
        div = (e*1024)
    return "%.2f%s" % (s/div, suffix)

def ratio_label(s):
    """A label for a ratio (1.2x, 8.8x) which is encoded from the server as
    an integer 10x the floating point ratio - e.g. '12', '88'
    """
    return str(float(s) / 10.0);

def histograms(mc, raw_stats):
    # Try to figure out the terminal width.  If we can't, 79 is good
    def termWidth():
        try:
            import fcntl, termios, struct
            h, w, hp, wp = struct.unpack('HHHH',
                                         fcntl.ioctl(0, termios.TIOCGWINSZ,
                                                     struct.pack('HHHH', 0, 0, 0, 0)))
            return w
        except:
            return 79

    def useUTF8():
        return force_utf8 or sys.stdout.encoding == 'UTF-8'

    special_labels = {'item_alloc_sizes': size_label,
                      'bg_batch_size' : no_label,
                      'ep_active_or_pending_eviction_values_evicted' : no_label,
                      'ep_replica_eviction_values_evicted' : no_label,
                      'ep_active_or_pending_eviction_values_snapshot' : no_label,
                      'ep_replica_eviction_values_snapshot' : no_label,
                      'paged_out_time': sec_label}

    all_ops_histo_data = defaultdict(dict)
    for k, v in list(raw_stats.items()):
        # Parse out a data point
        ka = k.split('_')
        k = '_'.join(ka[0:-1])

        if str("mean") in ka:  # base case to get hold of mean
            all_ops_histo_data[k]['avg'] = float(v)
            continue

        kstart, kend = [int(x) for x in ka[-1].split(',')]

        # Create a label for the data point
        label_func = time_label
        if k.endswith("Size") or k.endswith("Seek"):
            label_func = size_label
        elif k.endswith("Count"):
            label_func = no_label
        elif k.endswith("Ratio"):
            label_func = ratio_label
        elif k in special_labels:
            label_func = special_labels[k]

        label = "%s - %s" % (label_func(kstart), label_func(kend))

        if not 'data' in all_ops_histo_data[k]:
            all_ops_histo_data[k]['data'] = []
        all_ops_histo_data[k]['data'].append({'start' : int(kstart),
                                     'end'   : int(kend),
                                     'label' : label,
                                     'lb_fun': label_func,
                                     'value' : int(v)})

    for name, current_hist_data in sorted(all_ops_histo_data.items()):
        if 'data' in current_hist_data.keys():
            data_points = current_hist_data['data']
        else:
            continue
        max_label_len = max([len(stat['label']) for stat in data_points])
        widestval = len(str(max([stat['value'] for stat in data_points])))
        total = sum([stat['value'] for stat in data_points])
        if not total:
            # No non-zero datapoints; skip this histogram.
            continue

        print(" %s (%d total)" % (name, total))

        total_seen = 0
        non_zero_dp_seen = False
        for dp in sorted(data_points, key=lambda x: x['start']):
            # Omit any leading zero count data points until we have encountered at
            # least one non-zero datapoint.
            if not dp['value'] and not non_zero_dp_seen:
                continue

            # Omit any trailing zero count data points.
            if total_seen == total:
                continue

            non_zero_dp_seen = True
            total_seen += dp['value']
            pcnt = (total_seen * 100.0) / total
            toprint  = "    %s : (%8.04f%%) %s" % \
                       (dp['label'].ljust(max_label_len), pcnt,
                        str(dp['value']).rjust(widestval))
            print(toprint, end=' ')

            # Render a horizontal bar to represent the size of this value.
            # If available use UTF8 symbols as they give a higher resolution.
            remaining = termWidth() - len(toprint) - 2
            lpcnt = float(dp['value']) / total
            bar_len = lpcnt * remaining;
            whole_blocks = int(bar_len);
            if useUTF8():
                eighths = int((bar_len - whole_blocks) * 8)
                spark_chars = " ▏▎▍▌▋▊▉"
                print("%s%s" % ('█' * whole_blocks,
                                spark_chars[eighths]))
            else:
                print(('#' * whole_blocks))

        if not 'avg' in current_hist_data:
            sum_start_buck_vals = sum([((x['end'] - x['start'])/2) * x['value'] \
                                       for x in data_points])
            current_hist_data['avg'] = sum_start_buck_vals / total

        print("    %s : (%s)" % ("Avg".ljust(max_label_len),
                                 dp['lb_fun'](current_hist_data['avg']).rjust(7)))


def parse_collection_arg(*args):
    if len(args) == 0:
        return False, ""
    elif len(args) == 1:
        # assume single argument is collection path
        return False, args[0]
    elif len(args) == 2 and args[0] == "id":
        cidStr = args[1].lower()
        base = 16 if cidStr.startswith("0x") else 10
        try:
            cid = int(cidStr, base)
        except ValueError:
            raise ValueError(
                'Specified collection ID "%s" is not valid decimal or '
                '"0x" prefixed hex.' % cidStr)
        return True, "{:#x}".format(cid)
    else:
        raise ValueError(
            'Specified collection "%s" is not valid' % " ".join(args))

@cmd
def stats_key(mc, key, vb, *args):
    cmd = ""
    try:
        byId, collection = parse_collection_arg(*args)
        requestStatKey = "key"
        if byId:
            requestStatKey = requestStatKey + "-byid"
        cmd = "%s %s %s %s" % (requestStatKey, key, str(vb), collection)
        print(cmd)
        vbs = mc.stats(cmd)
    except mc_bin_client.MemcachedError as e:
        print(e)
        sys.exit(1)
    except ValueError as e:
        print("Failed to parse collection: %s" % str(e))
        sys.exit(1)
    except Exception as e:
        print("Stats '{}' are not available from the requested engine. Error:{}".format(cmd, str(e)))
        sys.exit(1)

    if vbs:
        print("stats for key", key)
        stats_formatter(vbs)

@cmd
def stats_vkey(mc, key, vb, *args):
    cmd = ""
    vbs = None
    try:
        byId, collection = parse_collection_arg(*args)
        requestStatKey = "vkey"
        if byId:
            requestStatKey = requestStatKey + "-byid"
        cmd = "%s %s %s %s" % (requestStatKey, key, str(vb), collection)
        vbs = mc.stats(cmd)
    except mc_bin_client.MemcachedError as e:
        print(e)
        sys.exit(1)
    except ValueError as e:
        print("Failed to parse collection: %s" % str(e))
        sys.exit(1)
    except Exception as e:
        print("Stats '{}' are not available from the requested engine. Error:{}".format(cmd, str(e)))
        sys.exit(1)

    if vbs is not None:
        print("verification for key", key)
        stats_formatter(vbs)

@cmd
def stats_dcp_takeover(mc, vb, name):
    cmd = "dcp-vbtakeover %s %s" % (str(vb), name)
    stats_formatter(stats_perform(mc, cmd))

@cmd
def stats_all(mc):
    stats_formatter(stats_perform(mc))

@cmd
def stats_timings(mc):
    if output_json:
        print('Json output not supported for timing stats')
        return
    h = stats_perform(mc, 'timings')
    if h:
        histograms(mc, h)

@cmd
def stats_meta_timings(mc):
    if output_json:
        print('Json output not supported for stat timing stats')
        return
    h = stats_perform(mc, 'stat-timings')
    if h:
        histograms(mc, h)

@cmd
def stats_dcp(mc):
    stats_formatter(stats_perform(mc, 'dcp'))

@cmd
def stats_collections(mc, *args):
    cmd = 'collections'
    try:
        if args:
            byId, collection = parse_collection_arg(*args)
            if byId:
                cmd = cmd + "-byid"
            cmd = cmd + " " + collection
        stats_formatter(stats_perform(mc, cmd))
    except ValueError as e:
        print("Failed to parse collection: %s" % str(e))
        sys.exit(1)

@cmd
def stats_collections_details(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'collections-details'
        else:
            cmd = "collections-details %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_scopes(mc, *args):
    if not args:
        cmd = 'scopes'
    else:
        if len(args) == 1:
            # assume single argument is scope name
            cmd = "scopes %s" % args[0]
        elif args[0] == "id":
            sidStr = args[1].lower()
            base = 16 if sidStr.startswith("0x") else 10
            try:
                sid = int(sidStr, base)
            except ValueError:
                print('Specified scope ID "%s" is not valid decimal or '
                      '"0x" prefixed hex.' % sidStr)
                return
            cmd = "scopes-byid %s" % ("0x{:x}".format(sid))
        else:
            print('Specified scope "%s" is not valid' % " ".join(args))
            return

    stats_formatter(stats_perform(mc, cmd))

@cmd
def stats_scopes_details(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'scopes-details'
        else:
            cmd = "scopes-details %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_dcpagg(mc):
    stats_formatter(stats_perform(mc, 'dcpagg :'))

@cmd
def stats_checkpoint(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'checkpoint'
        else:
            cmd = "checkpoint %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_slabs(mc):
    stats_formatter(stats_perform(mc, 'slabs'))

@cmd
def stats_items(mc):
    stats_formatter(stats_perform(mc, 'items'))

@cmd
def stats_uuid(mc):
    stats_formatter(stats_perform(mc, 'uuid'))

@cmd
def stats_vbucket(mc):
    stats_formatter(stats_perform(mc, 'vbucket'))

@cmd
def stats_vbucket_details(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'vbucket-details'
        else:
            cmd = "vbucket-details %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_vbucket_durability_state(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'vbucket-durability-state'
        else:
            cmd = "vbucket-durability-state %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_vbucket_seqno(mc, vb = -1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'vbucket-seqno'
        else:
            cmd = "vbucket-seqno %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))
@cmd
def stats_failovers(mc, vb = -1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'failovers'
        else:
            cmd = "failovers %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_prev_vbucket(mc):
    stats_formatter(stats_perform(mc, 'prev-vbucket'))

@cmd
def stats_memory(mc):
    stats_formatter(stats_perform(mc, 'memory'))

@cmd
def stats_config(mc):
    stats_formatter(stats_perform(mc, 'config'))

@cmd
def stats_warmup(mc):
    stats_formatter(stats_perform(mc, 'warmup'))

@cmd
def stats_info(mc):
    stats_formatter(stats_perform(mc, 'info'))

@cmd
def stats_workload(mc):
    stats_formatter(stats_perform(mc, 'workload'))

@cmd
def stats_raw(mc, arg):
    stats_formatter(stats_perform(mc,arg))

@cmd
def stats_kvstore(mc, args = ""):
    cmd = "kvstore %s" % (args)
    stats_formatter(stats_perform(mc, cmd))

@cmd
def stats_kvtimings(mc):
    if output_json:
        print('Json output not supported for kvtiming stats')
        return
    h = stats_perform(mc, 'kvtimings')
    if h:
        histograms(mc, h)

def avg(s):
    return sum(s) / len(s)

def _maybeInt(x):
    try:
        return int(x)
    except:
        return x

def _maybeFloat(x):
    try:
        return float(x)
    except:
        return x

@cmd
def stats_diskinfo(mc, with_detail=None):
    cmd_str = 'diskinfo'
    with_detail = with_detail == 'detail'
    if with_detail:
        cmd_str = 'diskinfo detail'

    stats_formatter(stats_perform(mc, cmd_str))

@cmd
def stats_diskfailures(mc):
    stats_formatter(stats_perform(mc, "disk-failures"))

@cmd
def stats_eviction(mc):
    if output_json:
        print('Json output not supported for eviction stats')
        return
    h = stats_perform(mc, 'eviction')
    if h:
        histograms(mc, h)

@cmd
def stats_hash(mc, with_detail=None):
    h = stats_perform(mc,'hash')
    if not h:
        return
    with_detail = with_detail == 'detail'

    mins = []
    maxes = []
    counts = []
    for k,v in list(h.items()):
        if 'max_dep' in k:
            maxes.append(int(v))
        if 'min_dep' in k:
            mins.append(int(v))
        if ':counted' in k:
            counts.append(int(v))
        if ':histo' in k:
            vb, kbucket = k.split(':')
            skey = 'summary:' + kbucket
            h[skey] = int(v) + h.get(skey, 0)

    h['avg_min'] = avg(mins)
    h['avg_max'] = avg(maxes)
    h['avg_count'] = avg(counts)
    h['min_count'] = min(counts)
    h['max_count'] = max(counts)
    h['total_counts'] = sum(counts)
    h['largest_min'] = max(mins)
    h['largest_max'] = max(maxes)

    toDisplay = h
    if not with_detail:
        toDisplay = {}
        for k in h:
            if 'vb_' not in k:
                toDisplay[k] = h[k]

    stats_formatter(toDisplay)

@cmd
def stats_scheduler(mc):
    if output_json:
        print('Json output not supported for scheduler stats')
        return
    h = stats_perform(mc, 'scheduler')
    if h:
        histograms(mc, h)

@cmd
def stats_runtimes(mc):
    if output_json:
        print('Json output not supported for runtimes stats')
        return
    h = stats_perform(mc, 'runtimes')
    if h:
        histograms(mc, h)

@cmd
def stats_dispatcher(mc, with_logs='no'):
    if output_json:
        print('Json output not supported for dispatcher stats')
        return
    with_logs = with_logs == 'logs'
    sorig = stats_perform(mc,'dispatcher')
    if not sorig:
        return
    s = {}
    logs = {}
    slowlogs = {}
    for k,v in list(sorig.items()):
        ak = tuple(k.split(':'))
        if ak[-1] == 'runtime':
            v = time_label(int(v))

        dispatcher = ak[0]

        for h in [logs, slowlogs]:
            if dispatcher not in h:
                h[dispatcher] = {}

        if ak[0] not in s:
            s[dispatcher] = {}

        if ak[1] in ['log', 'slow']:
            offset = int(ak[2])
            field = ak[3]
            h = {'log': logs, 'slow': slowlogs}[ak[1]]
            if offset not in h[dispatcher]:
                h[dispatcher][offset] = {}
            h[dispatcher][offset][field] = v
        else:
            field = ak[1]
            s[dispatcher][field] = v

    for dispatcher in sorted(s):
        print(" %s" % dispatcher)
        stats_formatter(s[dispatcher], "     ")
        for l,h in [('Slow jobs', slowlogs), ('Recent jobs', logs)]:
            if with_logs and h[dispatcher]:
                print("     %s:" % l)
                for offset, fields in sorted(h[dispatcher].items()):
                    stats_formatter(fields, "        ")
                    print("        ---------")

@cmd
def stats_durability_monitor(mc, vb):
    try:
        vb = int(vb)
        cmd = "durability-monitor %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def stats_tasks(mc, *args):
    tasks_stats_formatter(stats_perform(mc, 'tasks'), *args)

@cmd
def stats_responses(mc, all=''):
    resps = json.loads(stats_perform(mc, 'responses')['responses'])
    c = mc.get_error_map()['errors']
    d = {}
    for k, v in resps.items():
        try:
            if v > 0 or all:
                d[c[int(k, 16)]['name']] = v
        except KeyError:
            pass # Ignore it, no matching status code

    stats_formatter(d)

@cmd
def stats_range_scans(mc, vb=-1):
    try:
        vb = int(vb)
        if vb == -1:
            cmd = 'range-scans'
        else:
            cmd = "range-scans %s" % (str(vb))
        stats_formatter(stats_perform(mc, cmd))
    except ValueError:
        print('Specified vbucket \"%s\" is not valid' % str(vb))

@cmd
def reset(mc):
    stats_perform(mc, 'reset')

def main():
    c = cli_auth_utils.get_authed_clitool()

    c.addCommand('all', stats_all, 'all')
    c.addCommand('checkpoint', stats_checkpoint, 'checkpoint [vbid]')
    c.addCommand('config', stats_config, 'config')
    c.addCommand('diskinfo', stats_diskinfo, 'diskinfo [detail]')
    c.addCommand('disk-failures', stats_diskfailures, 'disk-failures')
    c.addCommand('durability-monitor', stats_durability_monitor, 'durability-monitor [vbid]')
    c.addCommand('eviction', stats_eviction, 'eviction')
    c.addCommand('scheduler', stats_scheduler, 'scheduler')
    c.addCommand('runtimes', stats_runtimes, 'runtimes')
    c.addCommand('dispatcher', stats_dispatcher, 'dispatcher [logs]')
    c.addCommand('tasks', stats_tasks, 'tasks [sort column]')
    c.addCommand('workload', stats_workload, 'workload')
    c.addCommand('failovers', stats_failovers, 'failovers [vbid]')
    c.addCommand('hash', stats_hash, 'hash [detail]')
    c.addCommand('items', stats_items, 'items (memcached bucket only)')
    c.addCommand('key', stats_key, 'key keyname vbid [scope.collection | id collectionID]')
    c.addCommand('kvstore', stats_kvstore, 'kvstore [args]')
    c.addCommand('kvtimings', stats_kvtimings, 'kvtimings')
    c.addCommand('memory', stats_memory, 'memory')
    c.addCommand('prev-vbucket', stats_prev_vbucket, 'prev-vbucket')
    c.addCommand('raw', stats_raw, 'raw argument')
    c.addCommand('reset', reset, 'reset')
    c.addCommand('responses', stats_responses, 'responses [all]')
    c.addCommand('slabs', stats_slabs, 'slabs (memcached bucket only)')
    c.addCommand('dcp', stats_dcp, 'dcp')
    c.addCommand('dcpagg', stats_dcpagg, 'dcpagg')
    c.addCommand('dcp-vbtakeover', stats_dcp_takeover, 'dcp-vbtakeover vb name')
    c.addCommand('timings', stats_timings, 'timings')
    c.addCommand('stat-timings', stats_meta_timings, 'stat-timings')
    c.addCommand('vbucket', stats_vbucket, 'vbucket')
    c.addCommand('vbucket-details', stats_vbucket_details, 'vbucket-details [vbid]')
    c.addCommand('vbucket-durability-state', stats_vbucket_durability_state, 'vbucket-durability-state [vbid]')
    c.addCommand('vbucket-seqno', stats_vbucket_seqno, 'vbucket-seqno [vbid]')
    c.addCommand('vkey', stats_vkey, 'vkey keyname vbid [scope.collection | id collectionID]')
    c.addCommand('warmup', stats_warmup, 'warmup')
    c.addCommand('uuid', stats_uuid, 'uuid')
    c.addCommand('collections', stats_collections, 'collections [scope.collection | id collectionID]')
    c.addCommand('collections-details', stats_collections_details,
                 'collections-details [vbid]')
    c.addCommand('scopes', stats_scopes, 'scopes [scope | id scopeID]')
    c.addCommand('scopes-details', stats_scopes_details,
                 'scopes-details [vbid]')
    c.addCommand('range-scans', stats_range_scans,
                 'range-scans [vbid]')
    c.addFlag('-j', 'json', 'output the results in json format')
    c.addFlag('-8', 'force_utf8', 'Force use of UTF8 symbols in output')

    c.execute()