from django.core.management.base import BaseCommand
from open_humans.models import OpenHumansMember
from main.models import DataSourceMember
from django.conf import settings
from datauploader.tasks import process_jawbone

import requests
# import vcr


class Command(BaseCommand):
    help = 'Import existing users from legacy project'

    def add_arguments(self, parser):
        parser.add_argument('--infile', type=str,
                            help='CSV with project_member_id & refresh_token')
        parser.add_argument('--delimiter', type=str,
                            help='CSV delimiter')

    # @vcr.use_cassette('import_users.yaml', decode_compressed_response=True)
    #                  record_mode='none')
    def handle(self, *args, **options):
        for line in open(options['infile']):
            line = line.strip().split(options['delimiter'])
            oh_id = line[0]
            oh_access_token = line[1]
            oh_refresh_token = line[2]
            jawbone_access_token = line[3]
            if len(OpenHumansMember.objects.filter(
                     oh_id=oh_id)) == 0:
                print(line)
                jawbone_headers = {'Authorization': 'Bearer {}'.format(
                    jawbone_access_token)}
                req = requests.get(
                    'https://jawbone.com/nudge/api/v.1.1/users/@me',
                    headers=jawbone_headers)
                user_data = req.json()
                req2 = requests.get(
                    'https://jawbone.com/nudge/api/v.1.1/users/@me/refreshToken',
                    headers=jawbone_headers)
                refresh_data = req2.json()
                if req.status_code != 200 or req2.status_code != 200:
                    print("Skipping {}! Token not working.".format(oh_id))
                    continue
                oh_member = OpenHumansMember.create(
                                    oh_id=oh_id,
                                    access_token=oh_access_token,
                                    refresh_token=oh_refresh_token,
                                    expires_in=-3600)
                oh_member.save()
                oh_member._refresh_tokens(client_id=settings.OPENHUMANS_CLIENT_ID,
                                          client_secret=settings.OPENHUMANS_CLIENT_SECRET)
                oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
                jawbone_member = DataSourceMember(
                    jawbone_id=user_data['data']['xid'],
                    access_token=jawbone_access_token,
                    refresh_token=refresh_data['data']['refresh_token'],
                    token_expires=DataSourceMember.get_expiration(
                        -3600)
                )
                jawbone_member.user = oh_member
                jawbone_member.save()
                jawbone_member._refresh_tokens(
                    client_id=settings.JAWBONE_CLIENT_ID,
                    client_secret=settings.JAWBONE_CLIENT_SECRET
                )
                jawbone_member.save()
                process_jawbone.delay(oh_member.oh_id)
                # process_jawbone(oh_member.oh_id)
