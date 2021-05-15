import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests_toolbelt import sessions
import base64
import json as jsonlib
from itertools import takewhile
from typing import Dict, Any, Tuple

from pyfuse3 import FUSEError  # pylint: disable=no-name-in-module
import errno

import config

log = logging.getLogger()


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = config.REQUEST_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


# Retry frequently to prevent I/O locking up for long
# replace backoff function in urllib3 Retry
orig_backoff_fun = Retry.get_backoff_time
def custom_backoff_time(self):
    return min(config.REQUEST_BACKOFF_MAX, orig_backoff_fun(self))
Retry.get_backoff_time = custom_backoff_time


retries = Retry(
    total=config.REQUEST_RETRY_COUNT,
    status_forcelist=[429],
    method_whitelist=['GET', 'POST'],
    backoff_factor=config.REQUEST_BACKOFF_FACTOR,
)

http = sessions.BaseUrlSession(base_url=config.METASERVER)
adapter = TimeoutHTTPAdapter(max_retries=retries)
http.mount("http://", adapter)
http.mount("https://", adapter)


def get_requests_session():
    return http


def get_headers():
    return {
        "X-DSN-Username": base64.b64encode(config.USERNAME.encode()),
        "X-DSN-Password": base64.b64encode(config.PASSWORD.encode())
    }


def get(api_method: str, params: Dict[str, Any] = {}) -> Tuple[bool, Any]:
    url = '/client/' + api_method
    log.debug('Making request to url %s with params %s', url, params)
    r = http.get(url, headers=get_headers(), params=params)
    if r.status_code == 200:
        json = r.json()
        if 'error' in json:
            error_code = json['error']
            error_message = json['error_message'] if 'error_message' in json else '?'
            log.debug('API error %s %s (note: in many cases API errors are expected)',
                      error_code, error_message)
            return (False, error_code)
        else:
            return (True, json)
    else:
        log.warn('Status code %s, response is printed below.', r.status_code)
        log.warn(r.text)
        log.warn('Request params below')
        log.warn(params)
        raise FUSEError(errno.EREMOTEIO)


def post(api_method: str, data: Any) -> Tuple[bool, Any]:
    url = '/client/' + api_method
    log.debug('Making request to url %s with params %s', url, data)
    r = http.post(url, headers=get_headers(), json=data)
    if r.status_code == 200:
        try:
            json = r.json()
        except jsonlib.JSONDecodeError as e:
            log.error('Json decode error %s', e)
            log.error('Response as text: %s', r.text)

        if 'error' in json:
            error_code = json['error']
            error_message = json['error_message'] if 'error_message' in json else '?'
            log.debug('API error %s %s (note: in many cases API errors are expected)',
                      error_code, error_message)
            return (False, error_code)
        else:
            return (True, json)
    else:
        log.warn('Status code %s, response is printed below.', r.status_code)
        log.warn(r.text)
        log.warn('Request data below')
        log.warn(data)
        raise FUSEError(errno.EREMOTEIO)
