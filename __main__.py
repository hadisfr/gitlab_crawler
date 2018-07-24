#!/usr/bin/env python3

import json
from sys import stderr
from traceback import format_exc
from pathlib import Path

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl

config_file = 'config.json'
status_file = str(Path.home()) + '/.glc'


def main():
    status = {}
    try:
        with open(status_file) as f:
            status = json.load(f)
    except Exception as ex:
        print("Status file (%s) error: %s\nUse default status.\n" % (status_file, ex), file=stderr)
    try:
        try:
            with open(config_file) as f:
                conf = json.load(f)
                phases = conf['phases']
                if not status:
                    status = conf['default_status']
        except Exception as ex:
            print("Config file (%s) error: %s\n" % (config_file, ex), file=stderr)
            exit(1)

        db_ctrl = DBCtrl()
        gitlab = GitlabCtrl()

        if phases.get('get_all_projects', False):
            gitlab.process_all_projects(
                lambda project: db_ctrl.add_row('projects', {
                    "id": project['id'],
                    "path": project['path'],
                    "owner_path": project['path_with_namespace'].split('/')[0],
                    "owner_type": project.get('namespace', {}).get('kind', 'user'),
                    "display_name": project['name'],
                    "description": project['description'],
                    "avatar": project['avatar_url'],
                    "stars": project['star_count'],
                    "forks": project['forks_count'],
                    "last_activity": project['last_activity_at'][:-1]
                }), {'archived': True}, start_page=status['get_all_projects_start_page'])
    except KeyboardInterrupt as ex:
        print("KeyboardInterrupt")
    except ...:
        raise
    finally:
        try:
            with open(status_file, 'w') as f:
                print(json.dumps(status, indent=4), file=f)
        except Exception as ex:
            print("Saving status file (%s) error: %s\n" % (status_file, ex), file=stderr)


if __name__ == '__main__':
    main()
