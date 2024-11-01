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
parser.add_argument(
    '-r', '--reviewer',
    action='append',
    type=str,
    required=True,
    help=('The change must have the following reviewer or owner to be '
          'considered')
)
parser.add_argument(
    '-p', '--project',
    action='append',
    type=str,
    required=True,
    help='Search changes in the following projects'
)
parser.add_argument('--webhook', help='URL to post the output')

# Gerrit instance
base_url = 'https://review.couchbase.com'
# Un-reviewed, but passed CV
query_ready_to_review = ('status:open label:Verified+1 '
                         '-(label:Code-Review-2 or label:Code-Review-1 '
                         'or label:Code-Review+2)')

# Allow insecure TLS
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

@lru_cache
def gerrit_request(*args, **kwargs):
    with urlopen(*args, **kwargs, context=ssl_context) as res:
        res.read(4)
        return json.load(res)


def get_change_url(change: dict) -> str:
    """
    Constructs a URL for accessing the change.
    Reference: https://gerrit.googlesource.com/gerrit/+/master/Documentation/rest-api-changes.txt
    """
    return 'https://review.couchbase.org/c/{}/+/{}'.format(
        change['project'],
        change['_number'])


def create_reviewer_query(users: list) -> str:
    """Query changes which have multiple reviewers."""
    return ' '.join(
        f'(reviewer:{user} or owner:{user} or cc:{user})'
        for user in users)


def create_project_query(projects: list) -> str:
    """Query multiple projects."""
    q = ' or '.join(f'project:{project}' for project in projects)
    return f'({q})'


def query_changes(*params: list) -> str:
    """Query for changes using a query string"""
    print('Query:', *params)
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


def find_stale_ready_for_review_changes(
        threshold: timedelta, projects: list, reviewers: list):
    reference_time = datetime.now(timezone.utc)
    changes = query_changes(
        create_project_query(projects),
        create_reviewer_query(reviewers),
        query_ready_to_review)

    for change in changes:
        last_update = parse_gerrit_datetime(change['updated'])
        elapsed_time = reference_time - last_update
        elapsed_time -= timedelta(days=count_weekend_days(last_update,
                                                          reference_time))
        if elapsed_time > threshold:
            yield elapsed_time, change


def main(
        webhook: Optional[str],
        threshold_stale: int, project: list, reviewer: list):
    message_text = ''
    for wait_time, change in find_stale_ready_for_review_changes(
            timedelta(days=threshold_stale),
            projects=project, reviewers=reviewer):
        timestr = f'{wait_time.days} days {wait_time.seconds // 3600} hours'
        if wait_time > timedelta(days=5):
            timestr += '❗'
        subject = change['subject']
        url = get_change_url(change)
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
