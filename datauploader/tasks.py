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
from datetime import datetime, timedelta
from demotemplate.settings import rr
from requests_respectful import RequestsRespectfulRateLimitedError
from ohapi import api
import arrow

# Set up logging.
logger = logging.getLogger(__name__)

JAWBONE_API_BASE = 'https://jawbone.com'
JAWBONE_API_MOVES = JAWBONE_API_BASE + '/nudge/api/v.1.1/users/@me/moves'


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
    jawbone_data = get_existing_jawbone(oh_access_token)
    jawbone_member = oh_member.datasourcemember
    jawbone_access_token = jawbone_member.get_access_token(
                            client_id=settings.JAWBONE_CLIENT_ID,
                            client_secret=settings.JAWBONE_CLIENT_SECRET)
    update_jawbone(oh_member, jawbone_access_token, jawbone_data)


def update_jawbone(oh_member, jawbone_access_token, jawbone_data):
    jawbone_moves_data = get_jawbone_moves(jawbone_access_token)
    add_jawbone_moves(oh_member=oh_member, data=jawbone_moves_data)

    """
    try:
        start_date = get_start_date(moves_data, moves_access_token)
        start_date = datetime.strptime(start_date, "%Y%m%d")
        start_date_iso = start_date.isocalendar()[:2]
        moves_data = remove_partial_data(moves_data, start_date_iso)
        stop_date_iso = (datetime.utcnow()
                         + timedelta(days=7)).isocalendar()[:2]
        while start_date_iso != stop_date_iso:
            print('processing {}-{} for member {}'.format(start_date_iso[0],
                                                          start_date_iso[1],
                                                          oh_member.oh_id))
            query = MOVES_API_STORY + \
                     '/{0}-W{1}?trackPoints=true&access_token={2}'.format(
                        start_date_iso[0],
                        start_date_iso[1],
                        moves_access_token
                     )
            response = rr.get(query, realms=['moves'])
            moves_data += response.json()
            start_date = start_date + timedelta(days=7)
            start_date_iso = start_date.isocalendar()[:2]
        print('successfully finished update for {}'.format(oh_member.oh_id))
        moves_member = oh_member.datasourcemember
        moves_member.last_updated = arrow.now().format()
        moves_member.save()
    except RequestsRespectfulRateLimitedError:
        logger.debug(
            'requeued processing for {} with 60 secs delay'.format(
                oh_member.oh_id)
                )
        process_moves.apply_async(args=[oh_member.oh_id], countdown=61)
    finally:
        replace_moves(oh_member, moves_data)
    """


def add_jawbone_moves(oh_member, data):
    # delete old file and upload new to open humans
    tmp_directory = tempfile.mkdtemp()
    metadata = {
        'description':
        'Jawbone "moves" data, including steps, calories, and activity.',
        'tags': ['Jawbone', 'steps'],
        'updated_at': str(datetime.utcnow()),
        }
    out_file = os.path.join(tmp_directory, 'jawbone-moves-data.json')
    logger.debug('deleted old file for {}'.format(oh_member.oh_id))
    api.delete_file(oh_member.access_token,
                    oh_member.oh_id,
                    file_basename='jawbone-moves-data.json')
    with open(out_file, 'w') as json_file:
        json.dump(data, json_file)
        json_file.flush()
    api.upload_aws(out_file, metadata,
                   oh_member.access_token,
                   project_member_id=oh_member.oh_id)
    logger.debug('added new jawbone moves file for {}'.format(oh_member.oh_id))


"""
def remove_partial_data(moves_data, start_date):
    remove_indexes = []
    for i, element in enumerate(moves_data):
        element_date = datetime.strptime(
                                element['date'], "%Y%m%d").isocalendar()[:2]
        if element_date == start_date:
            remove_indexes.append(i)
    for index in sorted(remove_indexes, reverse=True):
        del moves_data[index]
    return moves_data


def get_start_date(moves_data, moves_access_token):
    if moves_data == []:
        url = MOVES_API_BASE + "/user/profile?access_token={}".format(
                                        moves_access_token
        )
        response = rr.get(url, wait=True, realms=['moves'])
        return response.json()['profile']['firstDate']
    else:
        return moves_data[-1]['date']
"""


def get_existing_jawbone(oh_access_token):
    member = api.exchange_oauth2_member(oh_access_token)
    for dfile in member['data']:
        if 'Jawbone' in dfile['metadata']['tags']:
            # get file here and read the json into memory
            tf_in = tempfile.NamedTemporaryFile(suffix='.json')
            tf_in.write(requests.get(dfile['download_url']).content)
            tf_in.flush()
            jawbone_data = json.load(open(tf_in.name))
            return jawbone_data
    return []


def get_jawbone_data(access_token, url):
    req = requests.get(url, headers={
        'Authorization': 'Bearer {}'.format(access_token)})
    return req.json()


def get_jawbone_moves(access_token):
    apidata = get_jawbone_data(access_token=access_token,
                               url=JAWBONE_API_MOVES)
    agg_data = apidata['data']['items']
    while 'links' in apidata['data'] and 'next' in apidata['data']['links']:
        nexturl = JAWBONE_API_BASE + apidata['data']['links']['next']
        apidata = get_jawbone_data(access_token=access_token, url=nexturl)
        agg_data = agg_data + apidata['data']['items']

    return agg_data
