import json
import re
import os.path
from sys import stderr
from traceback import format_exc
from datetime import datetime

import requests


class GitlabCtrl(object):
    """GitLab Controller"""
    config_file = os.path.join(os.path.dirname(__file__), "config.json")

    def __init__(self):
        try:
            with open(self.config_file) as f:
                self.config = json.load(f)['api']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr, flush=True)
            exit(1)
        self._project_path_from_dom_regex = re.compile('<a class="project" href="([^"]*)">')
        self._project_id_from_dom_regex = re.compile('<input type="hidden" name="group_id" id="group_id" value="([^"]*)" .* />')

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
            elif res.status_code == 404:
                print("API returned %d for GET %s\n%s" % (
                    res.status_code, url, res.text), file=stderr, flush=True)
                raise RuntimeError(res.text)
            elif res.status_code == 429 or not res.headers.get('RateLimit-Remaining', 1):
                print("API Rate limit exceeded. (%s)\n%s" % (res.status_code, res.text), file=stderr, flush=True)
                while datetime.timestamp(datetime.now()) < res.headers.get(
                        'RateLimit-Reset', datetime.timestamp(datetime.now())):
                    pass
            else:
                print("API returned %d for GET %s\n%s" % (
                    res.status_code, url, res.text), file=stderr, flush=True)

    def single_process(self, url, callback, query={}, auth=True, percentage=False, *args, **kwds):
        """Call GitLab API and call callback on whole content of every page."""
        if not query or query == {}:
            query = {}

        # Handle 502 response on users/3585/projects API call
        # See https://gitlab.com/hadi_sfr/gitlab_crawler/issues/3
        if url == 'https://gitlab.com/api/v4/users/3585/projects':
            with open('misc/u3585p.json') as f:
                resjs = json.load(f)
            try:
                callback(resjs, *args, **kwds)
            except Exception as ex:
                print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
            return

        query['per_page'] = self.config['per_page']
        if 'page' not in query:
            query['page'] = 1
        while True:
            try:
                res = self.call_api(url, query, auth)
            except RuntimeError as e:
                break
            total_pages = int(res.headers.get('X-Total-Pages', 0))
            if percentage:
                percentage_str = " (%.2f%%)" % (query['page'] / total_pages * 100)
            else:
                percentage_str = ""
            print("\033[96mGET %s \033[0m %s" % (url,
                  ["", "%d from %d%s" % (query['page'], total_pages, percentage_str)][total_pages != 0]),
                  file=stderr, flush=True)
            try:
                callback(json.loads(res.text), *args, **kwds)
            except Exception as ex:
                print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
            if not res.headers.get('X-Next-Page', None):
                break
            query['page'] += 1

    def multiple_process(self, url, callback, query={}, auth=True, *args, **kwds):
        """Call GitLab API and call callback on every part of content of every page."""
        def _callback(l, *args, **kwds):
            for x in l:
                try:
                    callback(x, *args, **kwds)
                except Exception as ex:
                    print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)
        self.single_process(url, _callback, query, auth, *args, **kwds)

    def process_all_projects(self, callback, query={}, auth=False, start_page=1, *args, **kwds):
        """Call callback on all projects with optional filters in `query`."""
        query['statistics'] = True
        query['page'] = start_page
        self.multiple_process(self.config['url']['all_projects'], callback, query, auth, *args, **kwds)

    def _process_project_pure_members(self, callback, project_id, query={}, auth=False, *args, **kwds):
        """Call callback on every member of project found by id."""
        self.multiple_process(self.config['url']['project_members'] % project_id, callback, query, auth,
                              *args, **kwds, project=project_id, from_group=None)

    def _get_group_id_by_path(self, path):
        """Get ID of group or subgroup using its path."""
        try:
            return int(self._project_id_from_dom_regex.findall(self.call_api(self.config['url']['group'] % path).text)[0])
        except IndexError as ex:
            print("Project ID  of %s not found." % path, file=stderr, flush=True)

    def process_group_members(self, callback, group_id, group_path, query={}, auth=False, *args, **kwds):
        """Call callback on every member of group or subgroup found by id."""
        self.multiple_process(self.config['url']['group_members'] % group_id, callback, query, auth,
                              *args, **kwds, from_group=group_path)

    def process_project_members(self, callback, project_id, owner_path, owned_by_user, query={}, auth=False, *args, **kwds):
        """Call callback on every member of project found by id."""
        self._process_project_pure_members(callback, project_id, query, auth, *args, **kwds)
        if not owned_by_user:
            groups = owner_path.split("/")
            for group in ["/".join(groups[:i + 1]) for i in range(len(groups))]:
                group_id = self._get_group_id_by_path(group)
                if not group_id:
                    continue
                self.process_group_members(callback, group_id, group, query, auth, project=project_id, *args, **kwds)

    def process_user_owned_projects(self, callback, user_id, query={}, auth=False, *args, **kwds):
        """Call callback on every project owned by user found by id."""
        self.multiple_process(self.config['url']['user_projects'] % user_id, callback, query, auth,
                              *args, **kwds, user=user_id)

    def process_user_contributed_to_projects(self, callback, username, auth=False, *args, **kwds):
        """Call callable on every project user has contributed to."""
        try:
            url = self.config['url']['user_contributions'] % username
            print("\033[96mGET %s \033[0m %s" % (url), file=stderr, flush=True)
            projects = self._project_path_from_dom_regex.findall(json.loads(self.call_api(url).text)['html'])
            for project_full_path in projects:
                parsed_path = project_full_path.split('/')
                project = {
                    "owner_path": '/'.join(parsed_path[1:-1]),
                    "path": parsed_path[-1]
                }
                callback(project, *args, **kwds)
        except Exception as ex:
            print("Callback Error: %s\n\033[31m%s\033[0m\n" % (ex, format_exc()), file=stderr, flush=True)

    def process_fork(self, callback, source, query={}, auth=False, *args, **kwds):
        self.multiple_process(self.config['url']['project_forks'] % source, callback, query, auth, source=source)
