"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import json
import tempfile
import requests
import os
from celery import shared_task
from django.conf import settings
from open_humans.models import OpenHumansMember
from datetime import datetime
from ohapi import api

# Set up logging.
logger = logging.getLogger(__name__)

JAWBONE_API_BASE = 'https://jawbone.com'

JAWBONE_ENDPOINTS = {
    'heartrates': '/nudge/api/v.1.1/users/@me/heartrates',
    'moves': '/nudge/api/v.1.1/users/@me/moves',
    'sleeps': '/nudge/api/v.1.1/users/@me/sleeps',
}


@shared_task
def process_jawbone(oh_id):
    """
    Update the Jawbone file for a given OH user
    """
    logger.debug('Starting Jawbone processing for {}'.format(oh_id))
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
    oh_access_token = oh_member.get_access_token(
                            client_id=settings.OPENHUMANS_CLIENT_ID,
                            client_secret=settings.OPENHUMANS_CLIENT_SECRET)
    jawbone_member = oh_member.datasourcemember
    jawbone_access_token = jawbone_member.get_access_token(
                            client_id=settings.JAWBONE_CLIENT_ID,
                            client_secret=settings.JAWBONE_CLIENT_SECRET)
    update_jawbone(oh_member, jawbone_access_token)


def update_jawbone(oh_member, jawbone_access_token):
    for endpoint in JAWBONE_ENDPOINTS.keys():
        print("Trying {}".format(endpoint))
        data = get_aggregate_jawbone_data(
            access_token=jawbone_access_token,
            endpoint=endpoint)
        if data:
            add_jawbone_data(oh_member=oh_member, data=data, endpoint=endpoint)


def add_jawbone_data(oh_member, data, endpoint):
    # delete old file and upload new to open humans
    tmp_directory = tempfile.mkdtemp()
    metadata = {
        'tags': ['Jawbone'],
        'updated_at': str(datetime.utcnow()),
        }
    if endpoint == 'moves':
        metadata['description'] = ('Jawbone "moves" data, including steps, '
                                   'calories, and activity')
        metadata['tags'].append('steps')
    elif endpoint == 'sleeps':
        metadata['description'] = ('Jawbone "sleeps" data, including time, '
                                   'duration, and depth estimates.')
        metadata['tags'].append('sleep')
    elif endpoint == 'heartrates':
        metadata['description'] = ('Jawbone "heartrates" data, including '
                                   'resting heartrates')
        metadata['tags'].append('heartrate')
    out_file = os.path.join(
        tmp_directory,
        'jawbone-{}-data.json'.format(endpoint))
    logger.debug('deleted old file for {}'.format(oh_member.oh_id))
    api.delete_file(oh_member.access_token,
                    oh_member.oh_id,
                    file_basename='jawbone-{}-data.json'.format(endpoint))
    with open(out_file, 'w') as json_file:
        json.dump(data, json_file)
        json_file.flush()
    api.upload_aws(out_file, metadata,
                   oh_member.access_token,
                   project_member_id=oh_member.oh_id)
    logger.debug('added new jawbone {} file for {}'.format(
        endpoint, oh_member.oh_id))


def get_jawbone_data(access_token, url):
    req = requests.get(url, headers={
        'Authorization': 'Bearer {}'.format(access_token)})
    if req.status_code == 200:
        return req.json()
    else:
        return None


def get_aggregate_jawbone_data(access_token, endpoint):
    init_url = JAWBONE_API_BASE + JAWBONE_ENDPOINTS[endpoint]
    apidata = get_jawbone_data(access_token=access_token, url=init_url)
    agg_data = apidata['data']['items']
    while 'links' in apidata['data'] and 'next' in apidata['data']['links']:
        nexturl = JAWBONE_API_BASE + apidata['data']['links']['next']
        apidata = get_jawbone_data(access_token=access_token, url=nexturl)
        if apidata and 'data' in apidata and 'items' in apidata['data']:
            agg_data = agg_data + apidata['data']['items']
        else:
            break

    return agg_data
