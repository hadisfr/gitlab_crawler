import json
from sys import stderr
from traceback import format_exc
from datetime import datetime

import requests


class GitlabCtrl(object):
    """GitLab Controller"""
    config_file = 'config.json'

    def __init__(self):
        try:
            with open(self.config_file) as f:
                self.config = json.load(f)['api']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr)
            exit(1)

    def call_api(self, url, query={}, auth=True):
        """Call GitLab API and return raw result."""
        headers = {}
        headers['Content-Type'] = "application/json"
        if auth:
            headers['Private-Token'] = self.config['token']
        while True:
            res = requests.get(url, query, headers=headers)
            if res.status_code is 200:
                return res
            elif res.status_code is 429:
                print("API Rate limit exceeded.", file=stderr)
                while datetime.timestamp(datetime.now()) < res.headers.get(
                        'RateLimit-Reset', datetime.timestamp(datetime.now())):
                    pass
            else:
                print("API returned %d for GET %s\n%s" % (
                    res.status_code, url, json.dumps(json.loads(res.text), indent=4)), file=stderr)

    def single_process(self, url, callback, query={}, auth=True):
        """Call GitLab API and call callback on whole content of every page."""
        query['per_page'] = self.config['per_page']
        query['page'] = 1
        while True:
            res = self.call_api(url, query, auth)
            total_pages = int(res.headers.get('X-Total-Pages', 0))
            print("GET %s  %s" % (url, [
                "", "%d from %d (%.2f%%)" % (query['per_page'], total_pages, query['per_page'] / total_pages)
            ][total_pages != 0]), file=stderr)
            try:
                callback(json.loads(res.text))
            except Exception as ex:
                print("Callback Error: %s\n\033[31m%s\0330m\n" % (ex, format_exc()), file=stderr)
            if not res.headers.get('X-Next-Page', None):
                break
            query['page'] += 1

    def multiple_process(self, url, callback, query={}, auth=True):
        """Call GitLab API and call callback on every part of content of every page."""
        self.single_process(url, lambda x: list(map(callback, x)), query, auth)

    def process_all_projects(self, callback, query={}, auth=False):
        """Call callback on all projects with optional filters in `query`."""
        self.multiple_process(self.config['url']['all_projects'], callback, query, auth)
