from django.core.management.base import BaseCommand
from main.models import DataSourceMember
from datauploader.tasks import process_jawbone
import arrow
from datetime import timedelta


class Command(BaseCommand):
    help = 'Updates data for all members'

    def handle(self, *args, **options):
        users = DataSourceMember.objects.all()
        for jawbone_user in users:
            if jawbone_user.last_submitted < (arrow.now() - timedelta(days=4)):
                oh_id = jawbone_user.user.oh_id
                process_jawbone.delay(oh_id)
            else:
                print("didn't update {}".format(jawbone_user.moves_id))
