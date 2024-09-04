#!/usr/bin/env python3

"""
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# Script to find stale Gerrit changes which need action.

import argparse
import json
import os
import ssl
from typing import Optional
from urllib.request import Request, urlopen
from urllib.parse import quote_plus
from datetime import datetime, timedelta, timezone
from functools import lru_cache

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description=('Finds stale Gerrit changes and optionally sends a '
                 'notification via Webhook'))
parser.add_argument(
    '-t', '--threshold-stale',
    type=int,
    default=2,
    help='Threshold in work days for considering a change stale')
parser.add_argument('--webhook', help='URL to post the output')

# Gerrit instance
base_url = 'https://review.couchbase.com'
# Changes which have multiple members of KV-Engine as reviewers
query_reviewers = ('reviewer:jim@couchbase.com '
                   'reviewer:trond.norbye@couchbase.com')
# Relevant projects
query_projects = ('(project:kv_engine or project:platform or '
                  'project:couchstore or project:tlm or '
                  'project:sigar)')
# Un-reviewed, but passed CV
query_ready_to_review = ('status:open label:Verified+1 '
                         '(label:Code-Review+0 or label:Code-Review+1)')

# Allow insecure TLS
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


@lru_cache
def gerrit_request(*args, **kwargs):
    with urlopen(*args, **kwargs, context=ssl_context) as res:
        res.read(4)
        return json.load(res)


def get_change_url(change_id: str) -> str:
    return 'https://review.couchbase.org/q/' + change_id


def query_changes(*params: list) -> str:
    """Query for changes using a query string"""
    q_value = quote_plus(' '.join(params))
    url = f'{base_url}/changes/?q={q_value}'
    return gerrit_request(url)


def get_account_email(id: str) -> str:
    """Returns the email address associated with account ID"""
    url = f'{base_url}/accounts/{id}'
    return gerrit_request(url)['email']


def get_account_name(id: str) -> str:
    """Returns the name associated with account ID"""
    url = f'{base_url}/accounts/{id}'
    return gerrit_request(url)['name']


def parse_gerrit_datetime(timestamp: str) -> datetime:
    """Parse Gerrit's date string"""
    # The microseconds part has too many zeros and fails parse, so we strip it
    return datetime.strptime(
        timestamp.rsplit('.')[0],
        '%Y-%m-%d %H:%M:%S').replace(
        tzinfo=timezone.utc)


def count_weekend_days(start: datetime, end: datetime) -> int:
    """
    Naive way to count the number of weekend days (Sat/Sun) between two dates
    """
    days = (end - start).days
    weekend_days = 0
    for i in range(1, days):
        day = start + timedelta(days=i)
        if day.weekday() >= 5:
            weekend_days += 1
    return weekend_days


def find_stale_ready_for_review_changes(threshold: timedelta):
    reference_time = datetime.now(timezone.utc)
    changes = query_changes(
        query_reviewers, query_projects, query_ready_to_review)

    for change in changes:
        last_update = parse_gerrit_datetime(change['updated'])
        elapsed_time = reference_time - last_update
        elapsed_time -= timedelta(days=count_weekend_days(last_update,
                                                          reference_time))
        if elapsed_time > threshold:
            yield elapsed_time, change


def main(webhook: Optional[str], threshold_stale: int):
    message_text = ''
    for wait_time, change in find_stale_ready_for_review_changes(
            timedelta(days=threshold_stale)):
        timestr = f'{wait_time.days} days {wait_time.seconds // 3600} hours'
        if wait_time > timedelta(days=5):
            timestr += '❗'
        subject = change['subject']
        url = get_change_url(change['id'])
        author = get_account_name(change['owner']['_account_id'])
        message_text += (f'● {subject}\n'
                         f' ◦ {author} has been waiting for review for '
                         f'{timestr}\n'
                         f' ◦ {url}\n\n')

    message_text = message_text.rstrip()
    if len(message_text) == 0:
        return

    print(message_text)

    if webhook is not None:
        print(f'Sending to {webhook}...')
        urlopen(Request(
            webhook,
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'text': message_text}).encode()
        ))


main(**vars(parser.parse_args()))
