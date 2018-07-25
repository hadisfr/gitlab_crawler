#!/usr/bin/env python3

import json
from sys import stderr
from pathlib import Path

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl


def read_configs(config_file, status_file):
    """Read global configurations and status"""
    status = {}
    try:
        with open(status_file) as f:
            status = json.load(f)
    except Exception as ex:
        print("Status file (%s) error: %s\nUse default status.\n" % (status_file, ex), file=stderr, flush=True)
    try:
        with open(config_file) as f:
            conf = json.load(f)
            phases = conf['phases']
            if not status:
                status = conf['default_status']
    except Exception as ex:
        print("Config file (%s) error: %s\n" % (config_file, ex), file=stderr, flush=True)
        exit(1)
    return (status, phases)


def main():
    status_file = str(Path.home()) + '/.glc'

    try:
        (status, phases) = read_configs('config.json', status_file)
        db_ctrl = DBCtrl()
        gitlab = GitlabCtrl()

        if phases.get('get_all_projects', False):
            gitlab.process_all_projects(
                lambda project: db_ctrl.add_row('projects', {
                    "id": project['id'],
                    "path": project['path'],
                    "owner_path": project['path_with_namespace'].split('/')[0],
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
            print("Saving status file (%s) error: %s\n" % (status_file, ex), file=stderr, flush=True)


if __name__ == '__main__':
    main()
