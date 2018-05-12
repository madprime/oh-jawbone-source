from ohapi import api
from django.conf import settings
import arrow
from datetime import timedelta


def get_jawbone_files(oh_member):
    try:
        files = []
        oh_access_token = oh_member.get_access_token(
                                client_id=settings.OPENHUMANS_CLIENT_ID,
                                client_secret=settings.OPENHUMANS_CLIENT_SECRET)
        user_object = api.exchange_oauth2_member(oh_access_token)
        for dfile in user_object['data']:
            if 'Jawbone' in dfile['metadata']['tags']:
                files.append({'url': dfile['download_url'],
                              'name': dfile['basename']})
        return files

    except:
        return 'error'


def check_update(jawbone_member):
    if jawbone_member.last_submitted < (arrow.now() - timedelta(hours=1)):
        return True
    return False
