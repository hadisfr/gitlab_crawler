#!/usr/bin/env python3

import json
from sys import stderr

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl

config_file = 'config.json'


def main():
    try:
        with open(config_file) as f:
            phases = json.load(f)['phases']
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
            }), {'archived': True})


if __name__ == '__main__':
    main()
