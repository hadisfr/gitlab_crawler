#!/usr/bin/env python3

from db_ctrl import DBCtrl
from gitlab_ctrl import GitlabCtrl


def main():
    db_ctrl = DBCtrl()
    gitlab = GitlabCtrl()
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
        }), {'search': "tehran-thesis"})


if __name__ == '__main__':
    main()
