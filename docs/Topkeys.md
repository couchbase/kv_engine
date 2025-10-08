# Top keys in Couchbase Server

In the initial versions of Couchbase Server, the "topkeys" feature was
introduced to help administrators identify the most frequently accessed keys
in the database.

There was however a few problems with the initial implementation of topkeys;
it had a peformance impact on the server, and at times it would be a source
of mutex contention causing the system to stop scaling out with more CPU cores.
In a typical deployment people don't really monitor the topkeys all the time,
so we decided to remove the feature rather than trying to make it scale better.

There is however still useful to know which keys are the most frequently
accessed, as there may be bugs in the application layer causing the clients
to hammer a few keys instead of distributing the load evenly across the keyspace.
To address this need, we have introduced a new feature called "key sampling"
which is available in Couchbase Server Totoro.

## How key sampling works

Key sampling works telling the server to start monitor all key usage
and count how many times each of those keys gets accessed. After a short
time the server is instructed to return the list of the keys with the
highest access counts.

To avoid the performance problems of the previous topkeys implementation,
the sampling is by default *off* and should only be enabled when needed
and run for short periods of time. While sampling there is a performance
cost in both CPU and memory usage, and in addition there may be mutex
contention added by the sampling process causing a slowdown of the server.

## When to use key sampling

As mentioned earlier key sampling should typically only be run when one
suspects that there may be a problem with the application layer causing
a few keys to be accessed much more frequently than others. This may
for example be after upgrading an application, or after deploying a new
feature in the application. Or if one sees an unexpected load pattern
on the server that may be caused by a few hot keys.

## How to use key sampling

Trying to identify hot keys using key sampling is a reiterative process
where one starts sampling, waits for a while, and then retrieves the
top keys. It may take a few iterations to find the hot keys, especially
if the sampling period is short. In addition one needs to consider the total
working set of keys in the system; if the working set is large compared to
the sampling set, one may not find any hot keys as the sampling may not cover
them.

One would typically start by grabbing a short sample from the cluster
to get an initial view of the key access patterns:

```shell
$ mctopkeys --host 192.168.86.101 \
            --cluster \
            --user Administrator \
            --password secret \
            --duration 1 \
            --limit 10 \
            --collect-limit 10000 | jq .
```
Which could output something like this:
```json
{
  "localhost": {
    "keys": {
      "travel-sample": {
        "cid:0x0": {
          "airline_10": 4,
          "airline_10123": 4,
          "airline_10226": 4,
          "airline_10642": 4,
          "airline_10748": 4,
          "airline_10765": 4,
          "airline_109": 4,
          "airline_112": 4,
          "airline_1191": 4,
          "airline_1203": 4,
          "airline_19619": 1,
          "airport_1316": 1,
          "airport_8653": 1,
          "hotel_5097": 1,
          "landmark_15898": 1,
          "landmark_25184": 1,
          "landmark_25540": 1,
          "landmark_26004": 1,
          "landmark_26194": 1,
          "landmark_33322": 1,
          "landmark_35852": 1,
          "landmark_39": 1,
          "route_11816": 1,
          "route_13178": 1,
          "route_13181": 1,
          "route_13210": 1,
          "route_15278": 1,
          "route_22985": 1,
          "route_25169": 1,
          "route_25226": 1,
          "route_25334": 1,
          "route_25352": 1,
          "route_26513": 1,
          "route_27260": 1,
          "route_27284": 1,
          "route_27894": 1,
          "route_28960": 1,
          "route_2988": 1,
          "route_34758": 1,
          "route_35542": 1,
          "route_39090": 1,
          "route_46386": 1,
          "route_4787": 1,
          "route_49337": 1,
          "route_51864": 1,
          "route_52432": 1,
          "route_53002": 1,
          "route_5451": 1,
          "route_54704": 1,
          "route_56345": 1,
          "route_57000": 1,
          "route_57108": 1,
          "route_57626": 1,
          "route_57725": 1,
          "route_5895": 1,
          "route_60552": 1,
          "route_61094": 1,
          "route_61763": 1,
          "route_62030": 1,
          "route_64263": 1,
          "route_64481": 1,
          "route_6857": 1,
          "route_6924": 1,
          "route_8453": 1,
          "route_9208": 1,
          "route_9413": 1,
          "route_9540": 1
        },
        "cid:0x14": {
          "airport_476": 1,
          "airport_8366": 1
        },
        "cid:0x16": {
          "landmark_10089": 1,
          "landmark_16307": 1,
          "landmark_21229": 1,
          "landmark_25143": 1,
          "landmark_25758": 1,
          "landmark_25987": 1,
          "landmark_26041": 1,
          "landmark_32052": 1,
          "landmark_37129": 1,
          "landmark_5087": 1
        },
        "cid:0x17": {
          "route_11553": 1,
          "route_11752": 1,
          "route_20189": 1,
          "route_20408": 1,
          "route_21357": 1,
          "route_25504": 1,
          "route_2737": 1,
          "route_27549": 1,
          "route_28203": 1,
          "route_30473": 1,
          "route_3588": 1,
          "route_36724": 1,
          "route_37846": 1,
          "route_4579": 1,
          "route_46968": 1,
          "route_51278": 1,
          "route_53496": 1,
          "route_54118": 1,
          "route_55186": 1,
          "route_56010": 1,
          "route_57549": 1,
          "route_59107": 1,
          "route_59189": 1,
          "route_59648": 1,
          "route_59824": 1,
          "route_59936": 1,
          "route_61692": 1,
          "route_64321": 1,
          "route_64337": 1,
          "route_6728": 1,
          "route_9465": 1
        }
      }
    },
    "num_keys_omitted": 21239,
    "shards": 48,
    "topkey": {
      "bucket": "travel-sample",
      "collection": "cid:0x0",
      "count": 4,
      "key": "airline_109"
    },
    "num_keys_collected": 10000
  }
}
```

As we can see from this output there was an "num_keys_omitted" value of 21239 which
means that the key access working set is larger than the sampling set. We don't
really know how much bigger sampling set we need to do (it could be that the 
21239 omitted keys are all unique keys, or it could be that some of them are
hot keys that just didn't get sampled) but we should try to rerun the sampling
with a larger collect limit until it becomes zero.

Once the "num_keys_omitted" becomes zero, we can be sure that the sampling
covered the entire working set of keys, and we can start analyzing the
output to see if there are any hot keys.

If your cluster have a lot of different buckets and you're only interested in
a few buckets, you can use the `--buckets` option to limit the sampling to
only those buckets (that way you may reduce the sampling size needed to cover
the working set as it only need to cover the working set of the selected
buckets).

The field "num_keys_collected" tells how many unique keys were sampled during
the sampling period.
