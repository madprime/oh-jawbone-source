import logging
import requests
from django.contrib import messages
from django.contrib.auth import login, logout
from django.shortcuts import render, redirect
from django.conf import settings
from open_humans.models import OpenHumansMember
from .models import DataSourceMember
from .helpers import get_jawbone_file, check_update
from datauploader.tasks import process_jawbone
from ohapi import api
import arrow

# Set up logging.
logger = logging.getLogger(__name__)

SCOPES = 'move_read sleep_read heartrate_read'


def index(request):
    """
    Starting page for app.
    """
    if request.user.is_authenticated:
        return redirect('/dashboard')
    else:
        context = {'client_id': settings.OPENHUMANS_CLIENT_ID,
                   'oh_proj_page': settings.OH_ACTIVITY_PAGE}

        return render(request, 'main/index.html', context=context)


def complete(request):
    """
    Receive user from Open Humans. Store data, start upload.
    """
    print("Received user returning from Open Humans.")
    # Exchange code for token.
    # This creates an OpenHumansMember and associated user account.
    code = request.GET.get('code', '')
    oh_member = oh_code_to_member(code=code)

    if oh_member:
        # Log in the user.
        user = oh_member.user
        login(request, user,
              backend='django.contrib.auth.backends.ModelBackend')

        # Initiate a data transfer task, then render `complete.html`.
        # xfer_to_open_humans.delay(oh_id=oh_member.oh_id)
        context = {'oh_id': oh_member.oh_id,
                   'oh_proj_page': settings.OH_ACTIVITY_PAGE}
        if not hasattr(oh_member, 'datasourcemember'):
            jawbone_url = ('https://jawbone.com/auth/oauth2/auth?'
                           'response_type=code&scope={}&'
                           'redirect_uri={}&client_id={}').format(
                            SCOPES,
                            settings.JAWBONE_REDIRECT_URI,
                            settings.JAWBONE_CLIENT_ID)
            logger.debug(jawbone_url)
            context['jawbone_url'] = jawbone_url
            return render(request, 'main/complete.html',
                          context=context)
        return redirect("/dashboard")

    logger.debug('Invalid code exchange. User returned to starting page.')
    return redirect('/')


def dashboard(request):
    if request.user.is_authenticated:
        if hasattr(request.user.oh_member, 'datasourcemember'):
            jawbone_member = request.user.oh_member.datasourcemember
            download_file = get_jawbone_file(request.user.oh_member)
            if download_file == 'error':
                logout(request)
                return redirect("/")
            connect_url = ''
            allow_update = check_update(jawbone_member)
        else:
            allow_update = False
            jawbone_member = ''
            download_file = ''
            connect_url = ('https://jawbone.com/auth/oauth2/auth?'
                           'response_type=code&scope={}&'
                           'redirect_uri={}&client_id={}').format(
                            SCOPES,
                            settings.JAWBONE_REDIRECT_URI,
                            settings.JAWBONE_CLIENT_ID)
        context = {
            'oh_member': request.user.oh_member,
            'jawbone_member': jawbone_member,
            'download_file': download_file,
            'connect_url': connect_url,
            'allow_update': allow_update
        }
        return render(request, 'main/dashboard.html',
                      context=context)
    return redirect("/")


def remove_jawbone(request):
    if request.method == "POST" and request.user.is_authenticated:
        try:
            oh_member = request.user.oh_member
            api.delete_file(oh_member.access_token,
                            oh_member.oh_id,
                            file_basename="moves-storyline-data.json")
            messages.info(request, "Your Jawbone account has been removed")
            jawbone_account = request.user.oh_member.datasourcemember
            jawbone_account.delete()
        except:
            jawbone_account = request.user.oh_member.datasourcemember
            jawbone_account.delete()
            messages.info(request, ("Something went wrong, please"
                          "re-authorize us on Open Humans"))
            logout(request)
            return redirect('/')
    return redirect('/dashboard')


def update_data(request):
    if request.method == "POST" and request.user.is_authenticated:
        oh_member = request.user.oh_member
        # process_jawbone.delay(oh_member.oh_id)
        jawbone_member = oh_member.datasourcemember
        jawbone_member.last_submitted = arrow.now().format()
        jawbone_member.save()
        messages.info(request,
                      ("An update of your Jawbone data has been started! "
                       "It can take some minutes before the first data is "
                       "available. Reload this page in a while to find your "
                       "data"))
        return redirect('/dashboard')


def jawbone_complete(request):
    """
    Receive user from Jawbone. Store data, start processing.
    """
    logger.debug("Received user returning from Jawbone.")
    # Exchange code for token.
    # This creates an OpenHumansMember and associated user account.
    code = request.GET.get('code', '')
    ohmember = request.user.oh_member
    jawbone_member = jawbone_code_to_member(code=code, ohmember=ohmember)

    if jawbone_member:
        messages.info(request, "Your Jawbone account has been connected")
        # process_jawbone.delay(ohmember.oh_id)
        return redirect('/dashboard')

    logger.debug('Invalid code exchange. User returned to starting page.')
    messages.info(request, ("Something went wrong, please try connecting your "
                            "Jawbone account again"))
    return redirect('/dashboard')


def jawbone_code_to_member(code, ohmember):
    """
    Exchange code for token, use this to create and return Jawbone members.
    If a matching jawbone exists, update and return it.
    """
    if settings.JAWBONE_CLIENT_SECRET and \
       settings.JAWBONE_CLIENT_ID and code:
        data = {
            'grant_type': 'authorization_code',
            'redirect_uri': settings.JAWBONE_REDIRECT_URI,
            'code': code,
            'client_id': settings.JAWBONE_CLIENT_ID,
            'client_secret': settings.JAWBONE_CLIENT_SECRET
        }
        req = requests.post('https://jawbone.com/auth/oauth2/token', data=data)
        token_data = req.json()
        if 'access_token' in token_data:
            req = requests.get(
                'https://jawbone.com/nudge/api/v.1.1/users/@me',
                headers={'Authorization': 'Bearer {}'.format(
                    token_data['access_token'])})
            user_data = req.json()
            try:
                jawbone_member = DataSourceMember.objects.get(
                    jawbone_id=user_data['data']['xid'])
                logger.debug('Member {} re-authorized.'.format(
                    jawbone_member.jawbone_id))
                jawbone_member.access_token = token_data['access_token']
                jawbone_member.refresh_token = token_data['refresh_token']
                jawbone_member.token_expires = DataSourceMember.get_expiration(
                    token_data['expires_in'])
                print('got old jawbone member')
            except DataSourceMember.DoesNotExist:
                jawbone_member = DataSourceMember(
                    jawbone_id=user_data['data']['xid'],
                    access_token=token_data['access_token'],
                    refresh_token=token_data['refresh_token'],
                    token_expires=DataSourceMember.get_expiration(
                        token_data['expires_in'])
                        )
                jawbone_member.user = ohmember
                logger.debug('Member {} created.'.format(
                    user_data['data']['xid']))
                print('make new jawbone member')
            jawbone_member.save()

            return jawbone_member

        elif 'error' in req.json():
            logger.debug('Error in token exchange: {}'.format(req.json()))
        else:
            logger.warning('Neither token nor error info in Jawbone response!')
    else:
        logger.error('JAWBONE_CLIENT_SECRET or code are unavailable')
    return None


def oh_code_to_member(code):
    """
    Exchange code for token, use this to create and return OpenHumansMember.
    If a matching OpenHumansMember exists, update and return it.
    """
    if settings.OPENHUMANS_CLIENT_SECRET and \
       settings.OPENHUMANS_CLIENT_ID and code:
        data = {
            'grant_type': 'authorization_code',
            'redirect_uri':
            '{}/complete'.format(settings.OPENHUMANS_APP_BASE_URL),
            'code': code,
        }
        req = requests.post(
            '{}/oauth2/token/'.format(settings.OPENHUMANS_OH_BASE_URL),
            data=data,
            auth=requests.auth.HTTPBasicAuth(
                settings.OPENHUMANS_CLIENT_ID,
                settings.OPENHUMANS_CLIENT_SECRET
            )
        )
        data = req.json()

        if 'access_token' in data:
            oh_id = oh_get_member_data(
                data['access_token'])['project_member_id']
            try:
                oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
                logger.debug('Member {} re-authorized.'.format(oh_id))
                oh_member.access_token = data['access_token']
                oh_member.refresh_token = data['refresh_token']
                oh_member.token_expires = OpenHumansMember.get_expiration(
                    data['expires_in'])
            except OpenHumansMember.DoesNotExist:
                oh_member = OpenHumansMember.create(
                    oh_id=oh_id,
                    access_token=data['access_token'],
                    refresh_token=data['refresh_token'],
                    expires_in=data['expires_in'])
                logger.debug('Member {} created.'.format(oh_id))
            oh_member.save()

            return oh_member

        elif 'error' in req.json():
            logger.debug('Error in token exchange: {}'.format(req.json()))
        else:
            logger.warning('Neither token nor error info in OH response!')
    else:
        logger.error('OH_CLIENT_SECRET or code are unavailable')
    return None


def oh_get_member_data(token):
    """
    Exchange OAuth2 token for member data.
    """
    req = requests.get(
        '{}/api/direct-sharing/project/exchange-member/'
        .format(settings.OPENHUMANS_OH_BASE_URL),
        params={'access_token': token}
        )
    if req.status_code == 200:
        return req.json()
    raise Exception('Status code {}'.format(req.status_code))
    return None
