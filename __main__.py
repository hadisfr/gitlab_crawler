#!/usr/bin/env python3

import json
from sys import stderr
from pathlib import Path

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl


class Crawler(object):
    """Main crawler class"""
    status_file = str(Path.home()) + '/.glc'
    config_file = 'config.json'

    def __init__(self):
        self._read_configs()
        self.db_ctrl = DBCtrl()
        self.gitlab = GitlabCtrl()

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
                if not self.status:
                    self.status = conf['default_status']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (self.config_file, ex), file=stderr, flush=True)
            exit(1)

    def _add_project_to_db(self, project):
        """Add data of a project to database."""
        self.db_ctrl.add_row('projects', {
            "id": project['id'],
            "path": project['path'],
            "owner_path": project['path_with_namespace'].split('/')[0],
            "display_name": project['name'],
            "description": project['description'],
            "avatar": project['avatar_url'],
            "stars": project['star_count'],
            "forks": project['forks_count'],
            "last_activity": project['last_activity_at'][:-1]
        })

    def run(self):
        """Run crawler."""
        try:
            if self.phases.get('get_all_projects', False):
                self.gitlab.process_all_projects(
                    self._add_project_to_db,
                    {'archived': True},
                    start_page=self.status['get_all_projects_start_page']
                )

        except KeyboardInterrupt as ex:
            print("KeyboardInterrupt")
        except ...:
            raise
        finally:
            try:
                with open(self.status_file, 'w') as f:
                    print(json.dumps(self.status, indent=4), file=f)
            except Exception as ex:
                print("Saving status file (%s) error: %s\n" % (self.status_file, ex), file=stderr, flush=True)


def main():
    Crawler().run()


if __name__ == '__main__':
    main()
