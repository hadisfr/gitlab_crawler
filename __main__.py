#!/usr/bin/env python3

from db_ctrl import DBCtrl


def main():
    db_ctrl = DBCtrl()
    db_ctrl.add_row('projects', {
        "id": 7555106,
        "path": "pretrain_tc",
        "owner_path": "nct_tso_public",
        "owner_type": "user",
        "display_name": "Pretrain TC",
        "description": "PyTorch implementation of temporal coherence-based self-supervised learning for laparoscopic workflow analysis.",
        "avatar": None,
        "stars": 0,
        "forks": 0,
        "last_activity": "2018-07-19T15:51:44.369Z"[:-1]
    })


if __name__ == '__main__':
    main()
