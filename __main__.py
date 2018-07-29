#!/usr/bin/env python3

import json
from sys import stderr
from pathlib import Path

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl


class Crawler(object):
    """Main crawler class"""
    status_file = str(Path.home()) + "/.glc"
    config_file = "config.json"

    def __init__(self):
        self._read_configs()
        self.db_ctrl = DBCtrl()
        self.gitlab = GitlabCtrl()
        self.status['stage'] = {key: set(value) for (key, value) in self.status['stage'].items()}
        # print(self.status)

    def _read_configs(self):
        """Read global configurations and status"""
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
        self.db_ctrl.add_row("projects", {
            "id": project['id'],
            "path": project['path'],
            "owner_path": project['path_with_namespace'].split("/")[0],
            "display_name": project['name'],
            "description": project['description'],
            "avatar": project['avatar_url'],
            "stars": project['star_count'],
            "forks": project['forks_count'],
            "last_activity": project['last_activity_at'][:-1]
        })

    def _add_project_members(self, user, project):
        """Add project members and membership relations to database and stage"""
        project_from_db = self.db_ctrl.get_rows("projects", {"id": project})
        if not len(project_from_db):
            raise ValueError('Project with id %d not found in db.' % project)
        elif project_from_db[0]['members_processed']:
            return
        user_from_db = self.db_ctrl.get_rows("users", {"id": user['id']})
        if not len(user_from_db):
            self.db_ctrl.add_row("users", {
                "id": user['id'],
                "name": user['name'],
                "username": user['username'],
                "avatar": user['avatar_url']
            })
            user_from_db = (user,)
            user_from_db[0]['contributions_processed'] = False
        user_from_db = user_from_db[0]
        if not user_from_db['contributions_processed']:
            self.status['stage']['users'].add(user['id'])
        self.db_ctrl.add_row("membership", {
            "user": user['id'],
            "project": project
        })

    def _add_user_owned_project(self, project, user):
        """Add user projects and contribuition relations to database and stage"""
        user_from_db = self.db_ctrl.get_rows("users", {"id": user})
        if not len(user_from_db):
            raise ValueError('User with id %d not found in db.' % user)
        elif user_from_db[0]['contributions_processed']:
            return
        project_from_db = self.db_ctrl.get_rows("projects", {"id": project['id']})
        if not len(project_from_db):
            self.db_ctrl.add_row("projects", {
                "id": project['id'],
                "path": project['path'],
                "owner_path": project['path_with_namespace'].split("/")[0],
                "display_name": project['name'],
                "description": project['description'],
                "avatar": project['avatar_url'],
                "stars": project['star_count'],
                "forks": project['forks_count'],
                "last_activity": project['last_activity_at'][:-1]
            })
            project_from_db = (project,)
            project_from_db[0]['members_processed'] = False
        project_from_db = project_from_db[0]
        if not project_from_db['members_processed']:
            self.status['stage']['projects'].add(project['id'])
        self.db_ctrl.add_row("contributions", {
            "user": user,
            "project": project['id']
        })

    def _add_user_contributed_to_project(self, project, user):
        """Add user projects and contribuition relations to database and stage"""
        user_from_db = self.db_ctrl.get_rows("users", {"id": user})
        if not len(user_from_db):
            raise ValueError('User with id %d not found in db.' % user)
        elif user_from_db[0]['contributions_processed']:
            return
        project_from_db = self.db_ctrl.get_rows(
            "projects",
            {"owner_path": project['owner_path'], "path": project['path']}
        )
        if not len(project_from_db):
            raise ValueError('Project %s/%s not found in db.' % (project['owner_path'], project['path']))
        project_from_db = project_from_db[0]
        if not project_from_db['members_processed']:
            self.status['stage']['projects'].add(project['id'])
        self.db_ctrl.add_row("contributions", {
            "user": user,
            "project": project['id']
        })

    def run(self):
        """Run crawler."""
        try:
            if self.phases.get('get_all_projects', False):
                self.gitlab.process_all_projects(
                    self._add_project_to_db,
                    {"archived": True},
                    start_page=self.status['get_all_projects_start_page']
                )
            if self.phases.get("contributions", False):
                while True:
                    if self.status['on_projects']:
                        if not self.status['stage']['projects']:
                            break
                        print("\033[95mProjects on Stage\033[0m: %s" % self.status['stage']['projects'])
                        for project in self.status['stage']['projects']:
                            print("\033[95mProject\033[0m: %s" % project)
                            self.gitlab.process_project_members(self._add_project_members, project)
                            self.db_ctrl.update_rows('projects', {"id": project}, {"members_processed": True})
                        self.status['stage']['projects'] = set()
                    else:
                        if not self.status['stage']['users']:
                            break
                        print("\033[95mUsers on Stage\033[0m: %s" % self.status['stage']['users'])
                        for user in self.status['stage']['users']:
                            print("\033[95mUser\033[0m: %s" % user)
                            self.gitlab.process_user_owned_projects(self._add_user_owned_project, user)
                            self.gitlab.process_user_contributed_to_projects(self._add_user_contributed_to_project, user)
                            self.db_ctrl.update_rows('users', {"id": user}, {"contributions_processed": True})
                        self.status['stage']['users'] = set()
                    self.status['on_projects'] = not self.status['on_projects']

        except KeyboardInterrupt as ex:
            print("KeyboardInterrupt")
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
