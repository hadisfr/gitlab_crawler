import json
import re
from sys import stderr
from traceback import format_exc
from datetime import datetime
from functools import partial

import requests


class GitlabCtrl(object):
    """GitLab Controller"""
    config_file = 'config.json'

    def __init__(self):
        try:
            with open(self.config_file) as f:
                self.config = json.load(f)['api']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr, flush=True)
            exit(1)
        self.project_path_from_dom_regex = re.compile('<a class="project" href="([^"]*)">')

    def call_api(self, url, query={}, auth=True):
        """Call GitLab API and return raw result."""
        headers = {}
        headers['Content-Type'] = "application/json"
        if auth:
            headers['Private-Token'] = self.config['token']
        while True:
            try:
                res = requests.get(url, query, headers=headers, timeout=90)
            except requests.exceptions.Timeout:
                print("API timed out.", file=stderr, flush=True)
                continue
            if res.status_code == 200:
                return res
            elif res.status_code == 429 or not res.headers.get('RateLimit-Remaining', 1):
                print("API Rate limit exceeded. (%s)\n%s" % (res.status_code, res.text), file=stderr, flush=True)
                while datetime.timestamp(datetime.now()) < res.headers.get(
                        'RateLimit-Reset', datetime.timestamp(datetime.now())):
                    pass
            else:
                print("API returned %d for GET %s\n%s" % (
                    res.status_code, url, res.text), file=stderr, flush=True)

    def single_process(self, url, callback, query={}, auth=True, *args, **kwds):
        """Call GitLab API and call callback on whole content of every page."""
        query['per_page'] = self.config['per_page']
        if 'page' not in query:
            query['page'] = 1
        while True:
            res = self.call_api(url, query, auth)
            total_pages = int(res.headers.get('X-Total-Pages', 0))
            print("\033[96mGET %s \033[0m %s" % (url, [
                "", "%d from %d (%.2f%%)" % (query['page'], total_pages, query['page'] / total_pages * 100)
            ][total_pages != 0]), file=stderr, flush=True)
            try:
                callback(json.loads(res.text), *args, **kwds)
            except Exception as ex:
                print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
            if not res.headers.get('X-Next-Page', None):
                break
            query['page'] += 1

    def multiple_process(self, url, callback, query={}, auth=True, *args, **kwds):
        """Call GitLab API and call callback on every part of content of every page."""
        self.single_process(url, lambda x, *args, **kwds: list(map(partial(callback, *args, **kwds), x)),
                            query, auth, *args, **kwds)

    def process_all_projects(self, callback, query={}, auth=False, start_page=1, *args, **kwds):
        """Call callback on all projects with optional filters in `query`."""
        query['statistics'] = True
        query['page'] = start_page
        self.multiple_process(self.config['url']['all_projects'], callback, query, auth, *args, **kwds)

    def process_project_members(self, callback, project_id, query={}, auth=False, *args, **kwds):
        """Call callback on every member of project found by id"""
        self.multiple_process(self.config['url']['project_members'] % project_id, callback, query, auth,
                              *args, **kwds, project=project_id)

    def process_user_owned_projects(self, callback, user_id, query={}, auth=False, *args, **kwds):
        """Call callback on every project owned by user found by id"""
        self.multiple_process(self.config['url']['user_projects'] % user_id, callback, query, auth,
                              *args, **kwds, user=user_id)

    def process_user_contributed_to_projects(self, callback, username, auth=False, *args, **kwds):
        """Call callable on every project user has contributed to"""
        try:
            for project_full_path in self.project_path_from_dom_regex.findall(
                json.loads(self.call_api(self.config['url']['user_contributions'] % username).text)['html']
            ):
                parsed_path = project_full_path.split('/')
                project = {
                    "owner_path": parsed_path[1:-1],
                    "path": parsed_path[-1]
                }
                callback(project, *args, **kwds)
        except Exception as ex:
            print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
