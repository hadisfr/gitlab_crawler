#!/usr/bin/env python3

import json
import os.path
from sys import stderr
from pathlib import Path

from MySQLdb import OperationalError as MySQLdbOperationalError

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl


class Crawler(object):
    """Main crawler class"""
    status_file = str(Path.home()) + "/.glc"
    config_file = os.path.join(os.path.dirname(__file__), "config.json")

    def __init__(self):
        self._read_configs()
        self.db_ctrl = DBCtrl()
        self.gitlab = GitlabCtrl()
        self.status['stage'] = {key: set(value) for (key, value) in self.status['stage'].items()}

    def _read_configs(self):
        """Read global configurations and status."""
        self.status = {}
        try:
            with open(self.status_file) as f:
                self.status = json.load(f)
        except Exception as ex:
            print("Status file (%s) error: %s\nUse default status.\n" % (self.status_file, ex), file=stderr, flush=True)
        try:
            with open(self.config_file) as f:
                conf = json.load(f)
                self.phases = conf['phases']
                if not self.status or self.status == {}:
                    self.status = conf['default_status']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr, flush=True)
            exit(1)

    def _add_project_to_db(self, project):
        """Add data of a project to database."""
        data = {
            "id": project['id'],
            "path": project['path'],
            "owner_path": '/'.join(project['path_with_namespace'].split("/")[0:-1]),
            "display_name": project['name'],
            "description": project['description'],
            "avatar": project['avatar_url'],
            "stars": project['star_count'],
            "forks": project['forks_count'],
            "owned_by_user": 'owner' in project,
            "created_at": project['created_at'][:-1],
            "last_activity": project['last_activity_at'][:-1]
        }
        if 'statistics' in project:
            data['commit_count'] = project['statistics']['commit_count']
            data['storage_size'] = project['statistics']['storage_size']
            data['repository_size'] = project['statistics']['repository_size']
            data['lfs_objects_size'] = project['statistics']['lfs_objects_size']
        if 'archived' in project:
            data['archived'] = project['archived']
        if 'issues_enabled' in project:
            data['issues_enabled'] = project['issues_enabled']
        if 'merge_requests_enabled' in project:
            data['merge_requests_enabled'] = project['merge_requests_enabled']
        if 'wiki_enabled' in project:
            data['wiki_enabled'] = project['wiki_enabled']
        if 'jobs_enabled' in project:
            data['jobs_enabled'] = project['jobs_enabled']
        if 'snippets_enabled' in project:
            data['snippets_enabled'] = project['snippets_enabled']
        if 'ci_config_path' in project:
            data['ci_config_path'] = project['ci_config_path']
        while True:
            try:
                self.db_ctrl.add_row("projects", data)
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def _add_user_to_db(self, user):
        """Add data of a user to database."""
        while True:
            try:
                self.db_ctrl.add_row("users", {
                    "id": user['id'],
                    "name": user['name'],
                    "username": user['username'],
                    "avatar": user['avatar_url']
                })
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def _add_project_members(self, user, project, from_group=None):
        """Add project members and membership relations to database and stage."""
        user_from_db = self.db_ctrl.get_rows("users", {"id": user['id']})
        if not len(user_from_db):
            self._add_user_to_db(user)
            user_from_db = (user,)
            user_from_db[0]['contributions_processed'] = False
        user_from_db = user_from_db[0]
        if not user_from_db['contributions_processed']:
            self.status['stage']['users'].add(user['id'])
        while True:
            try:
                self.db_ctrl.add_row("membership", {
                    "user": user['id'],
                    "from_group": from_group,
                    "project": project
                })
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def _add_user_owned_project(self, project, user):
        """Add user projects and contribuition relations to database and stage."""
        project_from_db = self.db_ctrl.get_rows("projects", {"id": project['id']})
        if not len(project_from_db):
            self._add_project_to_db(project)
            project_from_db = (project,)
            project_from_db[0]['members_processed'] = False
        project_from_db = project_from_db[0]
        if not project_from_db['members_processed']:
            self.status['stage']['projects'].add(project['id'])
        while True:
            try:
                self.db_ctrl.add_row("contributions", {
                    "user": user,
                    "project": project['id']
                })
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def _add_user_contributed_to_project(self, project, user):
        """Add user projects and contribuition relations to database and stage."""
        project_from_db = self.db_ctrl.get_rows(
            "projects",
            {"owner_path": project['owner_path'], "path": project['path']}
        )
        if not len(project_from_db):
            raise ValueError('Project %s/%s not found in db.' % (project['owner_path'], project['path']))
        project_from_db = project_from_db[0]
        if not project_from_db['members_processed']:
            self.status['stage']['projects'].add(project_from_db['id'])
        while True:
            try:
                self.db_ctrl.add_row("contributions", {
                    "user": user,
                    "project": project_from_db['id']
                })
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def _add_fork_source(self, destination, source):
        while True:
            try:
                self.db_ctrl.add_row("forks", {
                    "source": source,
                    "destination": destination['id']
                })
            except MySQLdbOperationalError as ex:
                if len(ex.args) > 0 and ex[0] == self.db_ctrl.SERVER_HAS_GONE:
                    continue
                else:
                    break
            break

    def run(self):
        """Run crawler."""
        try:
            if self.phases.get('get_all_projects', False):
                self.gitlab.process_all_projects(
                    self._add_project_to_db,
                    {},
                    auth=True,
                    percentage=False,
                    start_page=self.status['get_all_projects_start_page']
                )
            if self.phases.get("contributions", False):
                while True:
                    if self.status['on_projects']:
                        if not self.status['stage']['projects']:
                            break
                        print("\033[95mProjects on Stage\033[0m: %s" % self.status['stage']['projects'],
                              file=stderr, flush=True)
                        for project in self.status['stage']['projects']:
                            project_from_db = self.db_ctrl.get_rows("projects", {"id": project})
                            if not len(project_from_db):
                                print('Project with id %d not found in db.' % project, file=stderr, flush=True)
                            elif project_from_db[0]['members_processed']:
                                continue
                            project_from_db = project_from_db[0]
                            print("\033[95mProject\033[0m: %s" % project, file=stderr, flush=True)
                            self.gitlab.process_project_members(
                                self._add_project_members,
                                project,
                                project_from_db['owner_path'],
                                project_from_db['owned_by_user'],
                                auth=True
                            )
                            self.db_ctrl.update_rows('projects', {"id": project}, {"members_processed": True})
                        self.status['stage']['projects'] = set()
                    else:
                        if not self.status['stage']['users']:
                            break
                        print("\033[95mUsers on Stage\033[0m: %s" % self.status['stage']['users'],
                              file=stderr, flush=True)
                        for user in self.status['stage']['users']:
                            print("\033[95mUser\033[0m: %s" % user, file=stderr, flush=True)
                            user_from_db = self.db_ctrl.get_rows("users", {"id": user})
                            if not len(user_from_db):
                                raise ValueError('User with id %d not found in db.' % user)
                            elif user_from_db[0]['contributions_processed']:
                                continue
                            user_from_db = user_from_db[0]
                            self.gitlab.process_user_owned_projects(self._add_user_owned_project, user, auth=True)
                            self.gitlab.process_user_contributed_to_projects(
                                self._add_user_contributed_to_project,
                                user_from_db['username'],
                                user=user
                            )
                            self.db_ctrl.update_rows('users', {"id": user}, {"contributions_processed": True})
                        self.status['stage']['users'] = set()
                    self.status['on_projects'] = not self.status['on_projects']
            if self.phases.get("get_all_forks", False):
                sources = [source['id'] for source in self.db_ctrl.get_rows_by_query("projects", "forks > %s", [0])]
                print("\033[93mProjects with fork\033[0m: %s" % sources, file=stderr, flush=True)
                total = len(sources)
                current = 0
                for source in sources:
                    current += 1
                    print("\033[93mProject\033[0m: %s (%.2f%%)" % (source, current / total * 100),
                          file=stderr, flush=True)
                    self.gitlab.process_fork(self._add_fork_source, source, {"page": 1}, auth=True)

        except KeyboardInterrupt as ex:
            print("KeyboardInterrupt", file=stderr, flush=True)
        except Exception:
            raise
        finally:
            try:
                # TODO: save current get_all_projects_start_page
                self.status['stage'] = {key: list(value) for (key, value) in self.status['stage'].items()}
                with open(self.status_file, 'w') as f:
                    print(json.dumps(self.status, indent=4), file=f)
            except Exception as ex:
                print("Saving status file (%s) error: %s\n" % (self.status_file, ex), file=stderr, flush=True)


def main():
    Crawler().run()


if __name__ == "__main__":
    main()
